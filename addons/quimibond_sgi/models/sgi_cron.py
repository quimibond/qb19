# -*- coding: utf-8 -*-
import base64
import logging
from collections import defaultdict
from dateutil.relativedelta import relativedelta

from odoo import models, fields, api

_logger = logging.getLogger(__name__)


class SgiCron(models.AbstractModel):
    _name = 'sgi.cron'
    _description = "Tareas programadas SGI"

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @api.model
    def _sgi_activity_exists(self, record, summary, user_id):
        """Evita duplicar actividades (idempotencia)."""
        activity_type = self.env.ref('mail.mail_activity_data_todo')
        return bool(self.env['mail.activity'].search_count([
            ('res_model', '=', record._name),
            ('res_id', '=', record.id),
            ('summary', '=', summary),
            ('user_id', '=', user_id),
            ('activity_type_id', '=', activity_type.id),
        ]))

    @api.model
    def _sgi_schedule(self, record, summary, note, user_id):
        if not user_id:
            return
        if self._sgi_activity_exists(record, summary, user_id):
            return
        record.activity_schedule(
            'mail.mail_activity_data_todo',
            summary=summary, note=note or '', user_id=user_id)

    def _sgi_first_user_id(self, group):
        """Primer usuario ACTIVO del grupo, por id (determinista). all_user_ids
        no garantiza orden: con 2+ usuarios en el grupo, el destinatario de los
        escalamientos cambiaba entre corridas y burlaba la deduplicación de
        actividades por usuario."""
        users = group.all_user_ids.sorted('id') if group else False
        return users[:1].id if users else False

    def _sgi_manager_user_id(self):
        group = self.env.ref('quimibond_sgi.group_sgi_manager', raise_if_not_found=False)
        return self._sgi_first_user_id(group)

    def _sgi_director_user_id(self):
        group = self.env.ref('quimibond_sgi.group_sgi_director', raise_if_not_found=False)
        return self._sgi_first_user_id(group) or self._sgi_manager_user_id()

    def _sgi_sales_admin_user_id(self):
        """Administrador de ventas (P-A28): primer usuario del grupo Admin de ventas
        de Odoo; fallback al Jefe MAST/SGI."""
        group = self.env.ref('sales_team.group_sale_manager', raise_if_not_found=False)
        return self._sgi_first_user_id(group) or self._sgi_manager_user_id()

    # ------------------------------------------------------------------
    # Aislamiento de errores: un registro/paso envenenado no debe tumbar
    # (ni revertir) la corrida completa del cron. Es el patrón que ya
    # protege cron_measure_activities — puesto ahí después de que un solo
    # tropiezo tumbó la medición completa en producción.
    # ------------------------------------------------------------------
    def _sgi_step(self, label, func):
        """Ejecuta un paso independiente del cron en su propio savepoint:
        si truena, se registra y se continúa con el siguiente paso."""
        try:
            with self.env.cr.savepoint():
                func()
        except Exception:
            _logger.exception("SGI cron: falló el paso «%s»; continúo.", label)

    def _sgi_for_each(self, records, func, label):
        """Aplica func(record) con savepoint por registro: el registro que
        truena se revierte y se loggea, sin abortar el resto de la corrida."""
        for record in records:
            try:
                with self.env.cr.savepoint():
                    func(record)
            except Exception:
                _logger.exception(
                    "SGI cron (%s): falló el registro %s (id %s); continúo.",
                    label, record.display_name, record.id)

    # ------------------------------------------------------------------
    # 1. Cron diario — No Conformidades
    # ------------------------------------------------------------------
    @api.model
    def cron_nonconformities(self):
        today = fields.Date.context_today(self)
        Param = self.env['ir.config_parameter'].sudo()
        default_days = int(Param.get_param('quimibond_sgi.nc_escalation_days', 5))
        external_days = int(Param.get_param('quimibond_sgi.nc_escalation_days_external', 3))

        # Marca acciones vencidas (recomputo del store)
        self._sgi_step(
            "recomputar estado de acciones",
            lambda: self.env['sgi.action.line'].search(
                [('date_done', '=', False)])._compute_state())

        open_alerts = self.env['quality.alert'].search([
            ('team_id.sgi_sequence_id', '!=', False),
            ('stage_id.sgi_is_closing_stage', '=', False),
            ('stage_id.sgi_is_cancel_stage', '=', False),
        ])

        def _process(alert):
            days = external_days if alert.sgi_origin_type in ('auditoria_externa', 'reclamacion') else default_days
            deadline = fields.Datetime.to_datetime(alert.create_date).date() + relativedelta(days=days)
            no_action = not alert.sgi_action_line_ids.filtered(lambda l: l.progress != '0')

            # Escalamiento por inacción
            if no_action and today >= deadline:
                user_id = alert.user_id.id or self._sgi_manager_user_id()
                self._sgi_schedule(
                    alert,
                    "NC sin acción: %s" % (alert.sgi_folio or alert.name),
                    "La NC lleva más de %d días abierta sin acción registrada." % days,
                    user_id)

            # Verificación de eficacia pendiente
            all_done = alert.sgi_action_line_ids and all(l.date_done for l in alert.sgi_action_line_ids)
            if all_done and not alert.sgi_effectiveness_date:
                user_id = alert.sgi_effectiveness_by.id or alert.user_id.id or self._sgi_manager_user_id()
                self._sgi_schedule(
                    alert,
                    "Verificar eficacia: %s" % (alert.sgi_folio or alert.name),
                    "Todas las acciones terminaron; falta registrar la verificación de eficacia.",
                    user_id)

        self._sgi_for_each(open_alerts, _process, "seguimiento de NC")
        return True

    # ------------------------------------------------------------------
    # 1b. Cron diario — Escalamiento de acciones vencidas (H12)
    # ------------------------------------------------------------------
    @api.model
    def cron_overdue_actions(self):
        """Escalamiento en 3 niveles de acciones vencidas:
        - nivel 1 (responsable): ya lo recuerda la actividad espejo (Ola 0);
        - > N días: además su jefe directo (employee_id.parent_id.user_id,
          fallback Jefe MAST);
        - > M días: además Dirección (group_sgi_director).
        Idempotente por nivel: el resumen difiere por nivel, así que no duplica
        actividades ya agendadas del mismo nivel."""
        today = fields.Date.context_today(self)
        Param = self.env['ir.config_parameter'].sudo()
        d_mgr = int(Param.get_param('quimibond_sgi.action_escalation_manager_days', 7))
        d_dir = int(Param.get_param('quimibond_sgi.action_escalation_director_days', 15))
        Line = self.env['sgi.action.line']
        overdue = Line.search([('date_done', '=', False), ('date_commit', '<', today)])
        self._sgi_step("recomputar estado de acciones vencidas",
                       lambda: overdue._compute_state())  # asegura el estado 'vencida'
        manager_fallback = self._sgi_manager_user_id()
        director_id = self._sgi_director_user_id()

        def _process(line):
            origin = line._sgi_origin()
            if not origin:
                return
            days = (today - line.date_commit).days
            who = line.responsible_id.display_name or '-'
            if days > d_mgr:
                boss = line.responsible_id.employee_id.parent_id.user_id
                self._sgi_schedule(
                    origin,
                    "Acción vencida (+%dd) escalada al jefe: %s" % (d_mgr, line.name),
                    "La acción de %s lleva %d días vencida (compromiso %s); se "
                    "escala a su jefe directo." % (who, days, line.date_commit),
                    boss.id or manager_fallback)
            if days > d_dir:
                self._sgi_schedule(
                    origin,
                    "Acción vencida (+%dd) escalada a Dirección: %s" % (d_dir, line.name),
                    "La acción de %s lleva %d días vencida (compromiso %s); se "
                    "escala a Dirección." % (who, days, line.date_commit),
                    director_id)

        self._sgi_for_each(overdue, _process, "escalamiento de acciones")
        return True

    # ------------------------------------------------------------------
    # 2. Cron diario — Documentos
    # ------------------------------------------------------------------
    @api.model
    def cron_documents(self):
        today = fields.Date.context_today(self)
        Doc = self.env['documents.document']
        Param = self.env['ir.config_parameter'].sudo()
        notice_days = int(Param.get_param('quimibond_sgi.doc_review_notice_days', 60))
        notice_final = int(Param.get_param('quimibond_sgi.doc_review_notice_days_final', 30))
        pilot_days = int(Param.get_param('quimibond_sgi.doc_pilot_notice_days', 7))
        ack_days = int(Param.get_param('quimibond_sgi.doc_ack_pending_days', 7))

        # Revisión bienal: dos avisos configurables (por defecto 60 y 30 días).
        # Por RANGO (<=), no por igualdad exacta: con igualdad, un día sin
        # corrida del cron (mantenimiento, error previo) perdía el aviso de esa
        # cohorte para siempre. La dedup por resumen evita repetirlo; el
        # resumen lleva el nivel de aviso (60/30), no la fecha.
        for offset in sorted({notice_days, notice_final}, reverse=True):
            target = today + relativedelta(days=offset)
            docs = Doc.search([
                ('sgi_state', '=', 'vigente'),
                ('sgi_next_review_date', '!=', False),
                ('sgi_next_review_date', '<=', target),
            ])

            def _review_notice(doc, offset=offset):
                self._sgi_schedule(
                    doc,
                    "Revisión bienal (aviso %d días): %s" % (offset, doc.sgi_code or doc.name),
                    "El documento requiere revisión antes del %s." % doc.sgi_next_review_date,
                    doc.sgi_owner_id.id or self._sgi_manager_user_id())

            self._sgi_for_each(docs, _review_notice, "aviso de revisión bienal")

        # Pilotos que vencen (aviso configurable, por defecto 7 días). También
        # por rango: un piloto ya vencido sin cerrar sigue mereciendo su aviso.
        pilot_target = today + relativedelta(days=pilot_days)
        pilots = Doc.search([
            ('sgi_state', '=', 'piloto'),
            ('sgi_pilot_end_date', '!=', False),
            ('sgi_pilot_end_date', '<=', pilot_target),
        ])

        def _pilot_notice(doc):
            self._sgi_schedule(
                doc,
                "Piloto por vencer: %s" % (doc.sgi_code or doc.name),
                "La prueba piloto vence el %s." % doc.sgi_pilot_end_date,
                doc.sgi_owner_id.id or self._sgi_manager_user_id())

        self._sgi_for_each(pilots, _pilot_notice, "aviso de piloto")

        # Acuses pendientes (umbral configurable, por defecto 7 días).
        limit_date = fields.Datetime.now() - relativedelta(days=ack_days)
        acks = self.env['sgi.document.ack'].search([
            ('state', '=', 'pendiente'),
            ('create_date', '<=', limit_date),
        ])
        manager_id = self._sgi_manager_user_id()

        def _ack_notice(ack):
            user_id = ack.user_id.id or manager_id
            self._sgi_schedule(
                ack.document_id,
                "Acuse pendiente: %s" % (ack.employee_id.name),
                "El acuse de lectura lleva más de %d días pendiente." % ack_days,
                user_id)

        self._sgi_for_each(acks, _ack_notice, "acuses pendientes")
        return True

    # ------------------------------------------------------------------
    # 3. Cron mensual — NEWS (F-P-G01-16)
    # ------------------------------------------------------------------
    @api.model
    def cron_news(self):
        today = fields.Date.context_today(self)
        first_this_month = today.replace(day=1)
        first_prev_month = first_this_month - relativedelta(months=1)

        requests = self.env['approval.request'].search([
            ('sgi_is_doc_change', '=', True),
            ('request_status', '=', 'approved'),
            ('sgi_applied', '=', True),
            ('write_date', '>=', fields.Datetime.to_datetime(first_prev_month)),
            ('write_date', '<', fields.Datetime.to_datetime(first_this_month)),
        ])
        if not requests:
            return True

        report = self.env.ref('quimibond_sgi.action_report_news', raise_if_not_found=False)
        manager_id = self._sgi_manager_user_id()
        # La actividad se agenda sobre una approval.request (tiene mail.activity.mixin);
        # res.company NO hereda el mixin.
        anchor = requests[:1]
        if report and anchor:
            pdf_content, _ = self.env['ir.actions.report']._render_qweb_pdf(
                report.report_name, requests.ids)
            attachment = self.env['ir.attachment'].create({
                'name': "NEWS_%s.pdf" % first_prev_month.strftime('%Y_%m'),
                'type': 'binary',
                'datas': base64.b64encode(pdf_content),
                'res_model': 'approval.request',
                'res_id': anchor.id,
                'mimetype': 'application/pdf',
            })
            if manager_id:
                anchor.activity_schedule(
                    'mail.mail_activity_data_todo',
                    summary="Revisar y difundir NEWS %s" % first_prev_month.strftime('%m/%Y'),
                    note="Boletín de cambios documentales aprobados del mes (adjunto ID %d)." % attachment.id,
                    user_id=manager_id)
        return True

    # ------------------------------------------------------------------
    # 4. Cron mensual — Indicadores (F-P-A10-03)
    # ------------------------------------------------------------------
    @api.model
    def cron_indicators(self):
        """Cron mensual: mide los indicadores de frecuencia mensual del mes anterior.

        Los tres bloques (mediciones, cierre de mes de presupuestos, refresco de
        la foto) son independientes y van cada uno en su savepoint: antes, una
        excepción en el refresco de UN presupuesto revertía TODAS las mediciones
        del mes y sus NCs — y al ser mensual, el mes quedaba sin medir hasta una
        corrida manual."""
        today = fields.Date.context_today(self)
        first_this = today.replace(day=1)
        first_prev = first_this - relativedelta(months=1)
        last_prev = first_this - relativedelta(days=1)
        deadline = first_this + relativedelta(days=4)
        indicators = self.env['sgi.indicator'].search([('frequency', '=', 'monthly')])
        self._sgi_step(
            "mediciones mensuales",
            lambda: self._sgi_generate_measures(
                indicators, first_prev, first_prev, last_prev, deadline,
                "%s" % first_prev.strftime('%m/%Y')))
        self._sgi_step(
            "cierre de mes de presupuestos",
            lambda: self._sgi_sales_budget_month_close(first_prev, last_prev))
        # Refresca la foto de facturado/pedido de los presupuestos vigentes.
        self._sgi_step(
            "refresco de foto de presupuestos",
            lambda: self.env['sgi.sales.budget'].search(
                [('state', '!=', 'obsoleto')]).action_refresh_actuals())
        return True

    @api.model
    def _sgi_team_net_invoiced(self, team, date_from, date_to):
        """Facturación neta (out_invoice − out_refund, sin impuestos, moneda
        compañía) de un equipo en el rango de fechas de factura."""
        moves = self.env['account.move'].search([
            ('move_type', 'in', ('out_invoice', 'out_refund')),
            ('state', '=', 'posted'),
            ('team_id', '=', team.id),
            ('invoice_date', '>=', date_from), ('invoice_date', '<=', date_to),
        ])
        return sum(moves.mapped('amount_untaxed_signed'))

    @api.model
    def cron_forecast_coverage(self):
        """Cron semanal (lunes): por cada pronóstico vigente/revisado, agrupa las
        líneas descubiertas EN HORIZONTE y los pedidos fuera de pronóstico, y crea
        UNA actividad al coordinador con el resumen. Idempotente (dedup por
        resumen). No aplica a presupuestos (P-A28 4.2.2.7)."""
        def _fmt(value):
            return '{:,.0f}'.format(value or 0)
        Budget = self.env['sgi.sales.budget']
        Line = self.env['sgi.sales.budget.line']

        def _process(budget):
            budget.action_refresh_actuals()  # cobertura con el horizonte de hoy
            uncovered = budget.line_ids.filtered(
                lambda l: l.coverage_state in ('sin_pedido', 'parcial'))
            orphans = budget._sgi_orders_without_forecast()
            if not uncovered and not orphans:
                return
            user_id = budget.team_id.user_id.id or self._sgi_sales_admin_user_id()
            if not user_id:
                return
            parts = []
            if uncovered:
                rows = ["%s · %s: pronosticado %s, comprometido %s, faltante %s %s" % (
                    l.product_id.default_code or l.product_id.name, l.date,
                    _fmt(l.qty_budget), _fmt(l.qty_real),
                    _fmt(l.qty_budget - l.qty_real), l.uom_id.name or '')
                    for l in uncovered.sorted(
                        lambda x: (x.date, x.product_id.default_code or ''))]
                parts.append("Semanas descubiertas:\n- " + "\n- ".join(rows))
            if orphans:
                rows = ["Pedido sin pronóstico: %s · %s (%s) — agrégalo al pronóstico" % (
                    sol.product_id.default_code or sol.product_id.name,
                    Line._sgi_effective_monday(sol.order_id), sol.order_id.name)
                    for sol in orphans.sorted(
                        lambda s: s.product_id.default_code or '')]
                parts.append("Pedidos fuera de pronóstico:\n- " + "\n- ".join(rows))
            self._sgi_schedule(
                budget,
                "Cobertura del pronóstico %s (P-A28 4.2.2.7)" % (
                    budget.partner_id.name or budget.name),
                "\n\n".join(parts), user_id)

        budgets = Budget.search([('kind', '=', 'pronostico'),
                                 ('state', '!=', 'obsoleto')])
        self._sgi_for_each(budgets, _process, "cobertura del pronóstico")
        return True

    @api.model
    def _sgi_sales_budget_month_close(self, first_prev, last_prev):
        """Aviso de cierre de mes: por cada equipo con presupuesto aprobado del
        año, si el acumulado facturado del año va por debajo del umbral del
        presupuesto acumulado (sales_budget_alert_pct), agenda una actividad al
        responsable del equipo. Idempotente (dedup por resumen)."""
        Param = self.env['ir.config_parameter'].sudo()
        pct = float(Param.get_param('quimibond_sgi.sales_budget_alert_pct', 80) or 0)
        # Umbral de justificación (P-A28 4.3.6.1): puede diferir del de aviso.
        min_pct = float(Param.get_param('quimibond_sgi.budget_fulfillment_min', 80) or 0)
        sales_admin_id = self._sgi_sales_admin_user_id()
        year = first_prev.year
        year_start = first_prev.replace(month=1, day=1)
        budgets = self.env['sgi.sales.budget'].search([
            ('kind', '=', 'presupuesto'),
            ('state', '=', 'aprobado'), ('year', '=', year)])

        def _process(budget):
            budgeted = sum(budget.line_ids.filtered(
                lambda l: l.date and l.date <= last_prev).mapped('amount_budget'))
            if not budgeted:
                return
            real = self._sgi_team_net_invoiced(budget.team_id, year_start, last_prev)
            achieved = real / budgeted * 100.0
            # Justificación del incumplimiento (P-A28 4.3.6.1): si va por debajo del
            # mínimo y AÚN no hay justificación capturada, se pide al Admin de ventas.
            # No bloquea nada: es evidencia del análisis.
            if achieved < min_pct and not (budget.nonfulfillment_note or '').strip() \
                    and sales_admin_id:
                self._sgi_schedule(
                    budget,
                    "Justificar incumplimiento del presupuesto %s (%.0f%%) — "
                    "P-A28 4.3.6.1" % (budget.team_id.name, achieved),
                    "El presupuesto va en %.1f%% (mínimo %.0f%%) y no tiene "
                    "justificación. Captura el análisis del incumplimiento en el "
                    "campo «Justificación de incumplimiento»." % (achieved, min_pct),
                    sales_admin_id)
            if achieved >= pct:
                return
            user = budget.team_id.user_id
            if not user:
                return
            top = self._sgi_budget_top_gaps(budget, last_prev)
            note = ("El acumulado facturado del equipo va en %.1f%% del presupuesto "
                    "aprobado del año. Revisa el pipeline y las acciones "
                    "comerciales." % achieved)
            if top:
                note += "\nProductos con mayor brecha (ppto − facturado):\n- " + \
                    "\n- ".join(top)
            self._sgi_schedule(
                budget,
                "Presupuesto %s por debajo del %.0f%% al cierre de %s" % (
                    budget.team_id.name, pct, first_prev.strftime('%m/%Y')),
                note, user.id)

        self._sgi_for_each(budgets, _process, "cierre de mes de presupuesto")
        return True

    @api.model
    def _sgi_budget_top_gaps(self, budget, last_prev, limit=5):
        """Los productos con mayor brecha (amount_budget − amount_real) del
        presupuesto hasta el mes (accionable: dónde se está quedando corto)."""
        gaps = defaultdict(lambda: [0.0, 0.0])  # product -> [ppto, real]
        for line in budget.line_ids.filtered(
                lambda l: l.date and l.date <= last_prev):
            gaps[line.product_id][0] += line.amount_budget
            gaps[line.product_id][1] += line.amount_real
        ranked = sorted(
            ((product, vals[0] - vals[1]) for product, vals in gaps.items()),
            key=lambda kv: kv[1], reverse=True)
        currency = budget.currency_id
        out = []
        for product, gap in ranked[:limit]:
            if gap <= 0:
                break
            out.append("%s: %s %s" % (
                product.default_code or product.name,
                '{:,.0f}'.format(gap), currency.name or ''))
        return out

    # ------------------------------------------------------------------
    # 4b. Cron anual (junio) — Revaluación del S2 (P-A28 Nota 1)
    # ------------------------------------------------------------------
    @api.model
    def cron_budget_revaluation(self):
        """P-A28 Nota 1: en JUNIO, pide al Admin de ventas revaluar las cantidades
        del segundo semestre de cada presupuesto aprobado del año en curso. Corre
        anual pero se protege con la guarda de mes (idempotente el resto del año)."""
        if fields.Date.context_today(self).month != 6:
            return True
        return self._sgi_sales_budget_revaluation(
            fields.Date.context_today(self).year)

    @api.model
    def _sgi_sales_budget_revaluation(self, year):
        """Agenda al Admin de ventas una actividad de revaluación del S2 por cada
        presupuesto aprobado del año (enlaza a la acción «Revisar (nueva Rev.)»).
        Idempotente (dedup por resumen)."""
        sales_admin_id = self._sgi_sales_admin_user_id()
        if not sales_admin_id:
            return True
        budgets = self.env['sgi.sales.budget'].search([
            ('kind', '=', 'presupuesto'),
            ('state', '=', 'aprobado'), ('year', '=', year)])

        def _process(budget):
            self._sgi_schedule(
                budget,
                "Revaluar cantidades del S2 — P-A28 Nota 1",
                "Revaluación de mitad de año (P-A28 Nota 1): revisa y ajusta las "
                "cantidades del segundo semestre del presupuesto %s. Si cambian, "
                "genera la siguiente revisión con «Revisar (nueva Rev.)»." % (
                    budget.folio or budget.name),
                sales_admin_id)

        self._sgi_for_each(budgets, _process, "revaluación del S2")
        return True

    @api.model
    def cron_indicators_weekly(self):
        """Cron semanal: mide los indicadores de frecuencia semanal de la semana previa."""
        today = fields.Date.context_today(self)
        this_monday = today - relativedelta(days=today.weekday())
        prev_monday = this_monday - relativedelta(days=7)
        prev_sunday = this_monday - relativedelta(days=1)
        deadline = this_monday + relativedelta(days=2)
        indicators = self.env['sgi.indicator'].search([('frequency', '=', 'weekly')])
        self._sgi_generate_measures(
            indicators, prev_monday, prev_monday, prev_sunday, deadline,
            "semana del %s" % prev_monday.strftime('%d/%m/%Y'))
        return True

    @api.model
    def _sgi_generate_measures(self, indicators, period_date, date_from, date_to,
                               deadline, period_label):
        """Genera (idempotente) las mediciones del periodo y evalúa la NC
        automática. Cada indicador va en su savepoint: un cálculo que truene
        (fuente de datos rota) no deja sin medir a los demás."""
        Measure = self.env['sgi.indicator.measure']
        manager_id = self._sgi_manager_user_id()

        def _process(indicator):
            measure = Measure.search([
                ('indicator_id', '=', indicator.id),
                ('period_date', '=', period_date),
            ], limit=1)
            if not measure:
                vals = {'indicator_id': indicator.id, 'period_date': period_date}
                value = indicator._sgi_compute_value(date_from, date_to)
                if indicator.calc_mode != 'manual' and value is not None:
                    vals['value'] = value
                    vals['state'] = 'capturado'
                    note = indicator._sgi_compute_note(date_from, date_to)
                    if note:
                        vals['note'] = note
                else:
                    vals['state'] = 'pendiente'
                    note = indicator._sgi_compute_note(date_from, date_to)
                    if note:
                        vals['note'] = note
                measure = Measure.create(vals)
                if measure.state == 'pendiente':
                    user_id = indicator.responsible_id.id or manager_id
                    if user_id:
                        self._sgi_schedule_deadline(
                            indicator, measure,
                            "Capturar indicador %s (%s)" % (indicator.code, period_label),
                            "Registre el valor del indicador del periodo (%s) antes del %s." % (
                                period_label, deadline),
                            user_id, deadline)
            # NC automática (solo mediciones rojas validadas con nc_on_red)
            measure._sgi_maybe_create_nc()

        self._sgi_for_each(indicators, _process, "medición de indicadores")
        return True

    @api.model
    def _sgi_schedule_deadline(self, anchor, measure, summary, note, user_id, deadline):
        """Agenda una actividad con fecha límite, evitando duplicados por medición."""
        if self._sgi_activity_exists(anchor, summary, user_id):
            return
        anchor.activity_schedule(
            'mail.mail_activity_data_todo',
            date_deadline=deadline,
            summary=summary, note=note or '', user_id=user_id)

    # ------------------------------------------------------------------
    # 5. Cron diario — Programa de auditorías
    # ------------------------------------------------------------------
    @api.model
    def cron_audit_program(self):
        today = fields.Date.context_today(self)
        lines = self.env['sgi.audit.program.line'].search([
            ('state', '=', 'pendiente'),
            ('program_id.state', '=', 'aprobado'),
        ])
        manager_id = self._sgi_manager_user_id()

        def _process(line):
            month_start = fields.Date.to_date(
                '%s-%02d-01' % (line.program_id.year, int(line.planned_month)))
            notice_date = month_start - relativedelta(days=15)
            if notice_date <= today < month_start:
                user_id = line.lead_auditor_id.id or manager_id
                self._sgi_schedule(
                    line.program_id,
                    "Preparar auditoría de %s (%s/%s)" % (
                        line.process_id.name or 'proceso',
                        line.planned_month, line.program_id.year),
                    "La auditoría planificada inicia el mes próximo; cree la auditoría.",
                    user_id)

        self._sgi_for_each(lines, _process, "programa de auditorías")
        return True

    # ------------------------------------------------------------------
    # 6. Cron diario — Revisión de riesgos vencidos
    # ------------------------------------------------------------------
    @api.model
    def cron_risk_review(self):
        today = fields.Date.context_today(self)
        risks = self.env['sgi.risk'].search([
            ('next_review_date', '!=', False),
            ('next_review_date', '<=', today),
            ('state', '!=', 'cerrado'),
        ])
        manager_id = self._sgi_manager_user_id()

        def _process(risk):
            owner = risk.process_id.owner_id.user_id
            user_id = owner.id or manager_id
            self._sgi_schedule(
                risk,
                "Revisar riesgo %s" % (risk.folio or risk.name),
                "La revisión del riesgo/oportunidad venció el %s." % risk.next_review_date,
                user_id)

        self._sgi_for_each(risks, _process, "revisión de riesgos")
        return True

    # ------------------------------------------------------------------
    # 7. Cron trimestral — Evaluación de proveedores
    # ------------------------------------------------------------------
    @api.model
    def cron_supplier_eval(self):
        today = fields.Date.context_today(self)
        # Trimestre anterior
        current_q_start_month = ((today.month - 1) // 3) * 3 + 1
        current_q_start = today.replace(month=current_q_start_month, day=1)
        prev_q_end = current_q_start - relativedelta(days=1)
        prev_q_start = prev_q_end.replace(day=1) - relativedelta(months=2)
        dt_from = fields.Datetime.to_datetime(prev_q_start)
        dt_to = fields.Datetime.to_datetime(prev_q_end) + relativedelta(days=1)

        Eval = self.env['sgi.supplier.eval']
        pickings = self.env['stock.picking'].search([
            ('picking_type_id.code', '=', 'incoming'),
            ('state', '=', 'done'),
            ('date_done', '>=', dt_from), ('date_done', '<', dt_to),
            ('partner_id', '!=', False),
        ])
        partners = pickings.mapped('partner_id.commercial_partner_id')
        purchase_user_id = self._sgi_purchase_user_id()

        def _process(partner):
            existing = Eval.search([
                ('partner_id', '=', partner.id),
                ('date_from', '=', prev_q_start),
                ('date_to', '=', prev_q_end),
            ], limit=1)
            if not existing:
                existing = Eval.create({
                    'partner_id': partner.id,
                    'date_from': prev_q_start,
                    'date_to': prev_q_end,
                })
            # Refresca las métricas antes de aplicar: pudieron llegar
            # recepciones o NCs después de creada la evaluación.
            existing.action_recompute()
            existing.action_apply_to_partner()
            if existing.supplier_class in ('condicionado', 'baja') and purchase_user_id:
                self._sgi_schedule(
                    existing.partner_id,
                    "Proveedor %s: %s" % (partner.name, existing.supplier_class),
                    "La evaluación trimestral dejó al proveedor como %s (calif. %s)." % (
                        existing.supplier_class, existing.score),
                    purchase_user_id)

        self._sgi_for_each(partners, _process, "evaluación de proveedores")
        return True

    def _sgi_purchase_user_id(self):
        group = self.env.ref('purchase.group_purchase_manager', raise_if_not_found=False)
        return self._sgi_first_user_id(group) or self._sgi_manager_user_id()

    def _sgi_rh_user_id(self):
        """Coordinador de RH (parametrizable); fallback al Jefe MAST."""
        user_id = int(self.env['ir.config_parameter'].sudo().get_param(
            'quimibond_sgi.rh_user_id', 0))
        if user_id and self.env['res.users'].browse(user_id).exists():
            return user_id
        return self._sgi_manager_user_id()

    # ------------------------------------------------------------------
    # 8. Cron diario — Calibraciones (P-C03)
    # ------------------------------------------------------------------
    @api.model
    def cron_calibrations(self):
        today = fields.Date.context_today(self)
        Equipment = self.env['maintenance.equipment']
        manager_id = self._sgi_manager_user_id()

        # Recomputa el estado de calibración (store) antes de evaluar.
        measuring = Equipment.search([('sgi_is_measuring', '=', True)])
        self._sgi_step("recomputar estado de calibración",
                       lambda: measuring._compute_calibration_state())

        # Por vencer (<= 30 días) y vencidos.
        def _calibration(eq):
            if not eq.sgi_next_calibration_date:
                return
            owner = eq.technician_user_id or eq.owner_user_id
            user_id = owner.id or manager_id
            if eq.sgi_calibration_state == 'por_vencer':
                self._sgi_schedule(
                    eq,
                    "Calibración por vencer: %s" % eq.name,
                    "El equipo vence su calibración el %s. Programe la calibración." % (
                        eq.sgi_next_calibration_date),
                    user_id)
            elif eq.sgi_calibration_state == 'vencido':
                # Vencido: bloquear y avisar al Jefe MAST.
                if not eq.sgi_do_not_use:
                    eq.sgi_do_not_use = True
                self._sgi_schedule(
                    eq,
                    "Calibración VENCIDA: %s" % eq.name,
                    "El equipo tiene la calibración vencida desde el %s y quedó "
                    "bloqueado (No usar)." % eq.sgi_next_calibration_date,
                    manager_id or user_id)

        self._sgi_for_each(measuring, _calibration, "calibraciones")

        # EPP por vencer (P-S03).
        ppe = Equipment.search([
            ('sgi_is_ppe', '=', True),
            ('sgi_ppe_expiry_date', '!=', False),
        ])

        def _ppe(eq):
            if eq.sgi_ppe_expiry_date <= today + relativedelta(days=30):
                owner = eq.technician_user_id or eq.owner_user_id
                user_id = owner.id or manager_id
                self._sgi_schedule(
                    eq,
                    "EPP por vencer/vencido: %s" % eq.name,
                    "El EPP vence (o venció) el %s. Gestione su reposición." % (
                        eq.sgi_ppe_expiry_date),
                    user_id)

        self._sgi_for_each(ppe, _ppe, "EPP")
        return True

    # ------------------------------------------------------------------
    # 9. Cron diario — Competencias, certificaciones y currículos (P-A01)
    # ------------------------------------------------------------------
    @api.model
    def cron_competences(self):
        today = fields.Date.context_today(self)
        soon = today + relativedelta(days=30)
        manager_id = self._sgi_manager_user_id()
        rh_id = self._sgi_rh_user_id()

        # Certificaciones (hr.employee.skill de tipo certificación) con vigencia.
        certs = self.env['hr.employee.skill'].search([
            ('is_certification', '=', True),
            ('valid_to', '!=', False),
            ('valid_to', '<=', soon),
        ])

        def _cert(cert):
            employee = cert.employee_id
            emp_user_id = employee.user_id.id
            label = "%s — %s" % (employee.name, cert.skill_id.name or '')
            if cert.valid_to < today:
                summary = "Certificación VENCIDA: %s" % label
                note = "La certificación venció el %s. Reprograme la recertificación." % cert.valid_to
                for user_id in {emp_user_id, rh_id, manager_id}:
                    self._sgi_schedule(employee, summary, note, user_id)
            else:
                summary = "Certificación por vencer: %s" % label
                note = "La certificación vence el %s (≤30 días)." % cert.valid_to
                for user_id in {emp_user_id, rh_id}:
                    self._sgi_schedule(employee, summary, note, user_id)

        self._sgi_for_each(certs, _cert, "certificaciones")

        # Currículos / cursos con fecha de fin próxima (hr.resume.line).
        resume_lines = self.env['hr.resume.line'].search([
            ('date_end', '!=', False),
            ('date_end', '<=', soon),
            ('date_end', '>=', today),
        ])

        def _resume(line):
            employee = line.employee_id
            self._sgi_schedule(
                employee,
                "Formación por concluir: %s (%s)" % (line.name, employee.name),
                "La formación registrada concluye el %s." % line.date_end,
                employee.user_id.id or rh_id)

        self._sgi_for_each(resume_lines, _resume, "formación por concluir")
        return True

    # ------------------------------------------------------------------
    # Fase 7 — Voz del cliente, DNC y emergencias
    # ------------------------------------------------------------------
    @api.model
    def _sgi_quarter_label(self, day):
        return "T%d %d" % ((day.month - 1) // 3 + 1, day.year)

    @api.model
    def cron_satisfaction_survey(self):
        """Cron trimestral: recuerda al Admin de Ventas distribuir la Encuesta
        de Satisfacción del Cliente (9001 9.1.2). Las respuestas alimentan el
        KPI CA-02 automáticamente. No envía correos a clientes por sí solo:
        el envío es una acción humana desde la app Encuestas."""
        survey = self.env.ref('quimibond_sgi.sgi_survey_satisfaction',
                              raise_if_not_found=False)
        user_id = self._sgi_sales_admin_user_id()
        if not survey or not user_id:
            return True
        label = self._sgi_quarter_label(fields.Date.context_today(self))
        self._sgi_schedule(
            survey,
            "Enviar encuesta de satisfacción del cliente (%s)" % label,
            "Comparta la encuesta con los clientes activos desde la app "
            "Encuestas (botón Compartir). Las respuestas del periodo alimentan "
            "el KPI CA-02 (Satisfacción del cliente) automáticamente.",
            user_id)
        return True

    @api.model
    def cron_dnc(self):
        """Cron trimestral: cierra el ciclo de la DNC (P-A01). Cuenta las
        brechas de competencia abiertas y agenda al coordinador de RH la
        distribución de la encuesta DNC (F-P-A01-17) y el plan de
        capacitación. Idempotente por trimestre."""
        survey = self.env.ref('quimibond_sgi.sgi_survey_dnc',
                              raise_if_not_found=False)
        rh_id = self._sgi_rh_user_id()
        if not survey or not rh_id:
            return True
        gaps = self.env['sgi.competence.gap'].search_count([])
        label = self._sgi_quarter_label(fields.Date.context_today(self))
        self._sgi_schedule(
            survey,
            "Revisar DNC y plan de capacitación (%s)" % label,
            "Hay %d brecha(s) de competencia abiertas (SGI → Medición → "
            "Brechas de competencia). Distribuya la encuesta DNC (F-P-A01-17) "
            "desde la app Encuestas y arme el plan de capacitación del "
            "periodo." % gaps,
            rh_id)
        return True

    @api.model
    def cron_emergency_drills(self):
        """Cron diario: vigila los simulacros de los planes de emergencia
        vigentes (14001/45001 8.2). Idempotente por resumen."""
        today = fields.Date.context_today(self)
        soon = today + relativedelta(days=30)
        manager_id = self._sgi_manager_user_id()
        plans = self.env['sgi.emergency.plan'].search([('state', '=', 'vigente')])

        def _plan(plan):
            user_id = plan.responsible_id.id or manager_id
            if not user_id:
                return
            if not plan.next_drill_date:
                self._sgi_schedule(
                    plan,
                    "Programar el primer simulacro: %s" % (plan.folio or plan.name),
                    "El plan de emergencia está vigente y no tiene ningún "
                    "simulacro realizado. Programe y ejecute el primero.",
                    user_id)
            elif plan.next_drill_date < today:
                self._sgi_schedule(
                    plan,
                    "Simulacro VENCIDO: %s" % (plan.folio or plan.name),
                    "El simulacro venció el %s (frecuencia: cada %d meses)."
                    % (plan.next_drill_date, plan.drill_frequency_months or 12),
                    manager_id or user_id)
            elif plan.next_drill_date <= soon:
                self._sgi_schedule(
                    plan,
                    "Simulacro por vencer: %s" % (plan.folio or plan.name),
                    "El próximo simulacro vence el %s. Prográmelo."
                    % plan.next_drill_date,
                    user_id)

        self._sgi_for_each(plans, _plan, "planes de emergencia")

        overdue_drills = self.env['sgi.emergency.drill'].search([
            ('state', '=', 'programado'),
            ('date_planned', '<', today),
        ])

        def _drill(drill):
            user_id = drill.plan_id.responsible_id.id or manager_id
            if user_id:
                self._sgi_schedule(
                    drill,
                    "Simulacro no realizado: %s" % (drill.folio or ''),
                    "El simulacro estaba programado para el %s y sigue sin "
                    "realizarse." % drill.date_planned,
                    user_id)

        self._sgi_for_each(overdue_drills, _drill, "simulacros vencidos")
        return True

    # ------------------------------------------------------------------
    # Fase 8 — Señales operativas: lo que Odoo ya registra alimenta la
    # mejora continua sin captura adicional.
    # ------------------------------------------------------------------
    @api.model
    def cron_operational_signals(self):
        """Cron diario. (a) Falla repetitiva: ≥3 correctivas del mismo equipo
        en 90 días → actividad al Jefe MAST sugiriendo levantar NC y revisar
        el plan de mantenimiento. (b) Reclamación abierta con SLA vencido →
        actividad al Jefe MAST. Idempotente por resumen."""
        now = fields.Datetime.now()
        manager_id = self._sgi_manager_user_id()
        if not manager_id:
            return True
        # (a) Mantenimiento repetitivo (los datos ya están en la app nativa).
        since = now - relativedelta(days=90)
        requests = self.env['maintenance.request'].search([
            ('maintenance_type', '=', 'corrective'),
            ('create_date', '>=', since),
            ('equipment_id', '!=', False),
        ])
        by_equipment = {}
        for request in requests:
            by_equipment.setdefault(request.equipment_id, 0)
            by_equipment[request.equipment_id] += 1

        def _repetitive(equipment):
            count = by_equipment[equipment]
            if count < 3:
                return
            self._sgi_schedule(
                equipment,
                "Falla repetitiva: %s (%d correctivas en 90 días)"
                % (equipment.name, count),
                "El equipo acumula %d solicitudes correctivas en 90 días. "
                "Evalúe levantar una NC (botón «Levantar NC» en la solicitud) "
                "y revisar su plan de mantenimiento preventivo." % count,
                manager_id)

        self._sgi_for_each(list(by_equipment), _repetitive, "falla repetitiva")
        # (b) Reclamaciones con SLA vencido que siguen abiertas.
        team = self.env.ref('quimibond_sgi.sgi_helpdesk_team_complaints',
                            raise_if_not_found=False)
        Ticket = self.env['helpdesk.ticket']
        if team and 'sla_deadline' in Ticket._fields:
            tickets = Ticket.search([
                ('team_id', '=', team.id),
                ('stage_id.fold', '=', False),
                ('sla_deadline', '!=', False),
                ('sla_deadline', '<', now),
            ])

            def _sla(ticket):
                self._sgi_schedule(
                    ticket,
                    "SLA vencido: reclamación %s" % (ticket.name or ticket.id),
                    "La reclamación superó su SLA de respuesta y sigue "
                    "abierta. Escale la respuesta al cliente.",
                    manager_id)

            self._sgi_for_each(tickets, _sla, "SLA de reclamaciones")
        return True

    # ------------------------------------------------------------------
    # Cumplimiento legal (14001/45001 6.1.3 / 9.1.2)
    # ------------------------------------------------------------------
    @api.model
    def cron_legal_requirements(self):
        """Cron diario: evaluaciones de cumplimiento vencidas y permisos por
        vencer (≤60 días) o vencidos. Idempotente por resumen."""
        today = fields.Date.context_today(self)
        soon = today + relativedelta(days=60)
        manager_id = self._sgi_manager_user_id()
        Requirement = self.env['sgi.legal.requirement']
        overdue = Requirement.search([
            ('next_eval_date', '!=', False),
            ('next_eval_date', '<=', today),
        ])

        def _overdue(req):
            self._sgi_schedule(
                req,
                "Evaluar cumplimiento legal: %s" % req.display_name,
                "La evaluación periódica del cumplimiento (9.1.2) venció el "
                "%s. Evalúe y registre el resultado (Cumple / Parcial / No "
                "cumple)." % req.next_eval_date,
                req.responsible_id.id or manager_id)

        self._sgi_for_each(overdue, _overdue, "evaluaciones legales vencidas")

        expiring = Requirement.search([
            ('expiry_date', '!=', False), ('expiry_date', '<=', soon),
        ])

        def _expiring(req):
            summary = ("Permiso VENCIDO: %s" if req.expiry_date < today
                       else "Permiso por vencer: %s") % req.display_name
            self._sgi_schedule(
                req, summary,
                "El permiso/licencia vence el %s. Gestione la renovación."
                % req.expiry_date,
                req.responsible_id.id or manager_id)

        self._sgi_for_each(expiring, _expiring, "permisos por vencer")
        return True

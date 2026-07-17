# -*- coding: utf-8 -*-
import base64
from dateutil.relativedelta import relativedelta

from odoo import models, fields, api


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

    def _sgi_manager_user_id(self):
        group = self.env.ref('quimibond_sgi.group_sgi_manager', raise_if_not_found=False)
        return group.all_user_ids[:1].id if group and group.all_user_ids else False

    # ------------------------------------------------------------------
    # 1. Cron diario — No Conformidades
    # ------------------------------------------------------------------
    @api.model
    def cron_nonconformities(self):
        today = fields.Date.context_today(self)
        Param = self.env['ir.config_parameter'].sudo()
        default_days = int(Param.get_param('quimibond_sgi.nc_escalation_days', 5))

        # Marca acciones vencidas (recomputo del store)
        self.env['sgi.action.line'].search([('date_done', '=', False)])._compute_state()

        open_alerts = self.env['quality.alert'].search([
            ('team_id.sgi_sequence_id', '!=', False),
            ('stage_id.sgi_is_closing_stage', '=', False),
            ('stage_id.sgi_is_cancel_stage', '=', False),
        ])
        for alert in open_alerts:
            days = 3 if alert.sgi_origin_type in ('auditoria_externa', 'reclamacion') else default_days
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
        return True

    # ------------------------------------------------------------------
    # 2. Cron diario — Documentos
    # ------------------------------------------------------------------
    @api.model
    def cron_documents(self):
        today = fields.Date.context_today(self)
        Doc = self.env['documents.document']

        # Revisión bienal: avisos a 60 y 30 días
        for offset in (60, 30):
            target = today + relativedelta(days=offset)
            docs = Doc.search([
                ('sgi_state', '=', 'vigente'),
                ('sgi_next_review_date', '=', target),
            ])
            for doc in docs:
                self._sgi_schedule(
                    doc,
                    "Revisión bienal en %d días: %s" % (offset, doc.sgi_code or doc.name),
                    "El documento requiere revisión antes de %s." % doc.sgi_next_review_date,
                    doc.sgi_owner_id.id or self._sgi_manager_user_id())

        # Pilotos que vencen en 7 días
        pilot_target = today + relativedelta(days=7)
        pilots = Doc.search([
            ('sgi_state', '=', 'piloto'),
            ('sgi_pilot_end_date', '=', pilot_target),
        ])
        for doc in pilots:
            self._sgi_schedule(
                doc,
                "Piloto por vencer: %s" % (doc.sgi_code or doc.name),
                "La prueba piloto vence el %s." % doc.sgi_pilot_end_date,
                doc.sgi_owner_id.id or self._sgi_manager_user_id())

        # Acuses pendientes > 7 días
        limit_date = fields.Datetime.now() - relativedelta(days=7)
        acks = self.env['sgi.document.ack'].search([
            ('state', '=', 'pendiente'),
            ('create_date', '<=', limit_date),
        ])
        manager_id = self._sgi_manager_user_id()
        for ack in acks:
            user_id = ack.user_id.id or manager_id
            self._sgi_schedule(
                ack.document_id,
                "Acuse pendiente: %s" % (ack.employee_id.name),
                "El acuse de lectura lleva más de 7 días pendiente.",
                user_id)
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
        today = fields.Date.context_today(self)
        first_this = today.replace(day=1)
        first_prev = first_this - relativedelta(months=1)
        last_prev = first_this - relativedelta(days=1)

        Measure = self.env['sgi.indicator.measure']
        manager_id = self._sgi_manager_user_id()
        indicators = self.env['sgi.indicator'].search([])
        for indicator in indicators:
            measure = Measure.search([
                ('indicator_id', '=', indicator.id),
                ('period_date', '=', first_prev),
            ], limit=1)
            if not measure:
                vals = {'indicator_id': indicator.id, 'period_date': first_prev}
                value = indicator._sgi_compute_value(first_prev, last_prev)
                if indicator.calc_mode != 'manual' and value is not None:
                    vals['value'] = value
                    vals['state'] = 'capturado'
                else:
                    vals['state'] = 'pendiente'
                measure = Measure.create(vals)
                if measure.state == 'pendiente':
                    user_id = indicator.responsible_id.id or manager_id
                    deadline = first_this + relativedelta(days=4)
                    if user_id:
                        self._sgi_schedule_deadline(
                            indicator, measure,
                            "Capturar indicador %s (%s)" % (
                                indicator.code, first_prev.strftime('%m/%Y')),
                            "Registre el valor del indicador del mes anterior antes del %s." % deadline,
                            user_id, deadline)
            # NC automática (solo mediciones rojas validadas con nc_on_red)
            measure._sgi_maybe_create_nc()
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
        for line in lines:
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
        for risk in risks:
            owner = risk.process_id.owner_id.user_id
            user_id = owner.id or manager_id
            self._sgi_schedule(
                risk,
                "Revisar riesgo %s" % (risk.folio or risk.name),
                "La revisión del riesgo/oportunidad venció el %s." % risk.next_review_date,
                user_id)
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
        for partner in partners:
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
            existing.action_apply_to_partner()
            if existing.supplier_class in ('condicionado', 'baja') and purchase_user_id:
                self._sgi_schedule(
                    existing.partner_id,
                    "Proveedor %s: %s" % (partner.name, existing.supplier_class),
                    "La evaluación trimestral dejó al proveedor como %s (calif. %s)." % (
                        existing.supplier_class, existing.score),
                    purchase_user_id)
        return True

    def _sgi_purchase_user_id(self):
        group = self.env.ref('purchase.group_purchase_manager', raise_if_not_found=False)
        return group.all_user_ids[:1].id if group and group.all_user_ids else self._sgi_manager_user_id()

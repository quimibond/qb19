# -*- coding: utf-8 -*-
"""Lógica de indicadores (I-1 e I-2, 2026-09-24).

I-1 — cada medición guarda su detalle: numerador, denominador, cuántos
casos la forman y qué registros entraron («Ver registros»). Un modo de
cálculo lo aporta con ``_detail_<modo>(date_from, date_to)`` → dict con
``value``, ``numerator``, ``denominator``, ``model`` e ``ids``. Los modos
que aún no lo tienen conservan su ``_calc_<modo>`` (solo valor) y, cuando
la tabla de evidencia conoce su universo de registros, guardan al menos el
modelo, los ids y el tamaño de muestra. Un periodo agregado (mes desde
semanas, trimestre) se calcula como suma de numeradores entre suma de
denominadores, nunca promediando porcentajes.

I-2 — «sin dato» no es cero: la medición automática sin registros queda
en estado ``sin_dato`` (gris, sin semáforo). Con menos casos que
``quimibond_sgi.indicator_min_sample`` (5) se marca «muestra chica». El
indicador nace en ``prueba`` y solo en ``oficial`` abre NC; ``measure_from``
dice desde cuándo se mide (antes no se crea medición). Ni «prueba», ni
«sin dato», ni «muestra chica» abren no conformidad.
"""
from dateutil.relativedelta import relativedelta

from odoo import api, fields, models
from odoo.exceptions import UserError

from .sgi_calendar import sgi_local_date
from .sgi_health_const import HEALTH_SNAPSHOT_MODES

MIN_SAMPLE_PARAM = 'quimibond_sgi.indicator_min_sample'
DEFAULT_MIN_SAMPLE = 5

# 57.90.0: modos de código que miden el estado de HOY (saldo pendiente,
# existencias, vigencias): un periodo pasado no se puede reconstruir.
# 57.99.0: también los de salud del SGI que miden el estado al calcular.
SNAPSHOT_MODES = ('cartera_vencida', 'cartera_vencida_60',
                  'inventario_diferencia', 'capacitacion') + HEALTH_SNAPSHOT_MODES
SNAPSHOT_NOTE = "Sin dato: indicador de foto, no reconstruible para un periodo pasado."
# 57.102.0 (B4): mediciones anteriores a «Medir desde».
BEFORE_FROM_NOTE = "Antes de «Medir desde» (%s): no cuenta. Valor anterior %s."
BEFORE_FROM_CALC = "Antes de «Medir desde» (%s): no se mide."


def _min_sample(env):
    raw = env['ir.config_parameter'].sudo().get_param(MIN_SAMPLE_PARAM, '')
    return int(raw) if str(raw).strip().isdigit() else DEFAULT_MIN_SAMPLE


class SgiIndicatorDetail(models.Model):
    _inherit = 'sgi.indicator'

    status = fields.Selection([
        ('prueba', "En prueba"),
        ('oficial', "Oficial"),
    ], string="Estado del indicador", default='prueba', required=True, tracking=True,
        help="Nace en prueba. El dueño revisa una vez la lista de registros de "
             "una medición contra la realidad y lo pasa a oficial. Solo los "
             "oficiales pueden abrir una no conformidad.")
    measure_from = fields.Date(
        string="Medir desde", tracking=True,
        help="Fecha desde la que hay dato confiable (el proceso entra a piloto "
             "o existe el campo que lo alimenta). Antes de ella no se crea medición.")

    # 57.90.0: indicador «de foto». Mide cómo están las cosas al calcular
    # (cartera pendiente hoy, existencias de hoy, documentos vigentes hoy), no
    # cómo estaban al cierre del periodo. Solo se mide el último periodo
    # cerrado; pedir uno anterior da «sin dato» en vez del estado de hoy con
    # la etiqueta de otro mes. Los modos de código lo traen marcado; en una
    # fórmula configurable lo marca quien la arma.
    snapshot = fields.Boolean(
        string="Indicador de foto", compute='_compute_snapshot', store=True,
        readonly=False, tracking=True,
        help="Mide el estado al momento de calcular (saldo pendiente, "
             "existencias, vigencias), no el del cierre del periodo. Solo se "
             "mide el último periodo cerrado; un periodo anterior queda «sin "
             "dato: indicador de foto, no reconstruible».")

    @api.depends('calc_mode')
    def _compute_snapshot(self):
        for indicator in self:
            if indicator.calc_mode in SNAPSHOT_MODES:
                indicator.snapshot = True
            elif indicator.calc_mode != 'configurable':
                indicator.snapshot = False
            # configurable: se conserva lo que marcó quien arma la fórmula.

    def _sgi_snapshot_blocked(self, date_from):
        """True si el indicador es de foto y el periodo es anterior al último
        cerrado (no se puede reconstruir)."""
        self.ensure_one()
        return (self.snapshot and self.calc_mode != 'manual'
                and date_from < self._sgi_default_period())

    def _sgi_snapshot_clear_history(self, since):
        """Pasa a «sin dato» las mediciones de periodos anteriores al último
        cerrado de los indicadores de foto que se (re)calcularon desde
        ``since``: traen el estado del día del cálculo con la etiqueta de otro
        mes. Nunca toca una validada. El valor anterior queda en la nota.
        Devuelve las mediciones cambiadas."""
        changed = self.env['sgi.indicator.measure']
        for indicator in self.filtered('snapshot'):
            measures = self.env['sgi.indicator.measure'].search([
                ('indicator_id', '=', indicator.id),
                ('period_date', '<', indicator._sgi_default_period()),
                ('state', 'not in', ('validado', 'sin_dato')),
                ('write_date', '>=', since),
            ])
            for measure in measures:
                measure.with_context(sgi_calc_write=True).write({
                    'state': 'sin_dato', 'value': 0.0, 'numerator': False,
                    'denominator': False, 'sample_size': 0, 'detail_model': False,
                    'detail_ids': False,
                    'note': "%s Valor anterior %s, calculado con el estado del día "
                            "del recálculo." % (SNAPSHOT_NOTE, measure.value),
                })
            changed |= measures
        return changed

    critical = fields.Boolean(
        string="Crítico", default=False, tracking=True,
        help="Un solo periodo en rojo abre la NC (I-5). Sin marcar, hacen falta "
             "dos periodos seguidos en rojo.")

    # 6.1 (56.12.0): el motivo cuando el cálculo automático no da valor, para
    # que un indicador que «nunca mide» se vea y se corrija.
    calc_status = fields.Selection([
        ('ok', "Calcula"),
        ('manual', "Manual (se captura)"),
        ('antes', "Antes de «medir desde»"),
        ('sin_formula', "Sin fórmula o sin fuente"),
        ('sin_datos', "Sin datos en el periodo"),
        ('error', "Error de cálculo"),
    ], string="Último cálculo", readonly=True, copy=False, index=True,
        help="Resultado del último cálculo automático y, si no dio valor, por qué.")
    calc_message = fields.Char(string="Motivo", readonly=True, copy=False)
    calc_checked = fields.Datetime(string="Revisado el", readonly=True, copy=False,
                                   help="Última vez que se revisó el cálculo automático.")

    def _sgi_set_calc(self, status, message=False):
        self.sudo().write({'calc_status': status, 'calc_message': message or False,
                           'calc_checked': fields.Datetime.now()})

    def _sgi_calc_diagnose(self, vals):
        """(estado, motivo) de un resultado de _sgi_measure_vals."""
        self.ensure_one()
        if self.calc_mode == 'manual':
            return 'manual', False
        if vals.get('state') != 'sin_dato':
            return 'ok', False
        if (vals.get('note') or '').startswith("Antes de «Medir desde»"):
            return 'antes', vals['note']
        missing = self.spec_missing or ''
        if any(key in missing for key in ('sin fórmula', 'sin fuente', 'sin actividad', 'sin entregable',
                                          'no dice cuándo')):
            return 'sin_formula', missing
        return 'sin_datos', (vals.get('note') or "Sin registros que contar en el periodo.")

    @api.model
    def _sgi_calc_status_backfill(self):
        """57.1.0: «Último cálculo» vacío en un indicador que ya tiene
        mediciones se llena con el diagnóstico de su última medición, sin
        recalcular nada.

        Antes solo lo escribía el cron al CREAR una medición (y «Recalcular
        ahora»). El campo llegó en 56.12.0 (28-sep-2026), cuando las
        mediciones de agosto ya existían (17-sep), así que los 93
        indicadores de producción se quedaron vacíos hasta el siguiente
        cierre mensual. Lo llama el cron diario de indicadores; solo toca los
        vacíos, así que un indicador ya diagnosticado no cambia."""
        Measure = self.env['sgi.indicator.measure']
        indicators = self.search([('calc_status', '=', False)])
        done = 0
        for indicator in indicators:
            if indicator.calc_mode == 'manual':
                indicator._sgi_set_calc('manual')
                done += 1
                continue
            last = Measure.search([('indicator_id', '=', indicator.id)],
                                  order='period_date desc', limit=1)
            if not last:
                today = fields.Date.context_today(self)
                if indicator.measure_from and indicator.measure_from > today:
                    indicator._sgi_set_calc('antes', "Mide desde el %s." % indicator.measure_from)
                    done += 1
                continue  # sin mediciones: lo diagnostica el cron al medir
            status, reason = indicator._sgi_calc_diagnose(
                {'state': last.state, 'note': last.note})
            prefix = "Según la medición de %s" % last.period_date.strftime('%m/%Y')
            indicator._sgi_set_calc(status, "%s: %s" % (prefix, reason) if reason else prefix + ".")
            done += 1
        return done

    def write(self, vals):
        res = super().write(vals)
        # B4 (57.102.0): cambiar «Medir desde» marca las anteriores. Moverla
        # hacia atrás no revive las ya marcadas: quedan «sin dato» y las
        # re-mide el recálculo (cron con ventana o el botón).
        if 'measure_from' in vals:
            self._sgi_mark_before_measure_from()
        return res

    def _sgi_mark_before_measure_from(self):
        """B4 (57.102.0): las mediciones no validadas cuyo periodo termina antes
        de «Medir desde» pasan a «Sin dato» con el valor anterior en la nota.
        Las validadas no se tocan (evidencia) y se cuentan en el chatter. Nada
        se borra. Idempotente. Devuelve las mediciones cambiadas."""
        changed = self.env['sgi.indicator.measure']
        for indicator in self.filtered('measure_from'):
            measures = indicator.measure_ids.filtered(
                lambda m: indicator._sgi_period_bounds(m.period_date)[1] < indicator.measure_from)
            to_mark = measures.filtered(lambda m: m.state not in ('validado', 'sin_dato'))
            since = indicator.measure_from.strftime('%d/%m/%Y')
            for measure in to_mark:
                prev = "sin dato" if measure.state == 'pendiente' else measure.value
                measure.with_context(sgi_calc_write=True).sudo().write({
                    'state': 'sin_dato', 'value': 0.0, 'numerator': False, 'denominator': False,
                    'sample_size': 0, 'detail_model': False, 'detail_ids': False,
                    'note': "\n".join(n for n in (BEFORE_FROM_NOTE % (since, prev), measure.note) if n),
                })
            if to_mark:
                validated = measures.filtered(lambda m: m.state == 'validado')
                indicator.message_post(body=(
                    "«Medir desde» %s: %d medición(es) anteriores pasan a «Sin dato» (el valor "
                    "anterior queda en su nota)%s." % (
                        since, len(to_mark),
                        "; %d validada(s) no se tocan" % len(validated) if validated else "")))
            changed |= to_mark
        return changed

    def action_set_official(self):
        self.write({'status': 'oficial'})

    def action_set_trial(self):
        self.write({'status': 'prueba'})

    # ------------------------------------------------------------------
    # I-1: valor con detalle
    # ------------------------------------------------------------------
    def _sgi_compute_detail(self, date_from, date_to):
        """Dict {value, numerator, denominator, model, ids} del periodo, o
        None si el modo es manual. ``value`` None = sin dato."""
        self.ensure_one()
        if self.calc_mode == 'manual':
            return None
        detail = getattr(self, '_detail_%s' % self.calc_mode, None)
        if detail:
            out = detail(date_from, date_to)
        else:
            out = {'value': self._sgi_compute_value(date_from, date_to)}
            out.update(self._sgi_evidence_records(date_from, date_to))
        out.setdefault('numerator', None)
        out.setdefault('denominator', None)
        out.setdefault('model', None)
        out.setdefault('ids', [])
        return out

    def _sgi_evidence_records(self, date_from, date_to):
        """Modelo e ids del universo de registros del periodo, para los modos
        que la tabla de evidencia de la medición sabe listar."""
        self.ensure_one()
        table = self.env['sgi.indicator.measure']._EVIDENCE
        if self.calc_mode not in table:
            return {}
        model, domain, date_field, is_dt = table[self.calc_mode]
        if model not in self.env:
            return {}
        domain = list(domain)
        if is_dt:
            dt_from, dt_to = self._sgi_dt_bounds(date_from, date_to)
            domain += [(date_field, '>=', dt_from), (date_field, '<', dt_to)]
        else:
            domain += [(date_field, '>=', date_from), (date_field, '<=', date_to)]
        if model in ('account.move', 'account.move.line', 'sale.order'):
            domain += [('company_id', '=', self._sgi_kpi_company().id)]
        domain += self._sgi_evidence_extra_domain(model)
        return {'model': model, 'ids': self.env[model].sudo().search(domain).ids}

    def _sgi_measure_vals(self, date_from, date_to):
        """Valores de la medición del periodo: valor, detalle, estado y nota.
        Es lo que escriben el cron y «Recalcular»."""
        self.ensure_one()
        if self._sgi_snapshot_blocked(date_from):
            return {'note': SNAPSHOT_NOTE, 'state': 'sin_dato', 'value': 0.0,
                    'numerator': None, 'denominator': None, 'sample_size': 0,
                    'detail_model': False, 'detail_ids': False}
        # B4 (57.102.0): antes de «Medir desde» no se mide (ni se pide la
        # captura de un manual). Así «Recalcular» no vuelve a llenar las
        # marcadas. Los modos con plazo (sgi_indicator_ind2) responden
        # «pendiente» antes de llegar aquí mientras el plazo no vence; el
        # recálculo nunca regresa una medición a pendiente.
        if not self._sgi_measurable_on(date_to):
            return {'note': BEFORE_FROM_CALC % self.measure_from.strftime('%d/%m/%Y'),
                    'state': 'sin_dato', 'value': 0.0, 'numerator': None,
                    'denominator': None, 'sample_size': 0, 'detail_model': False,
                    'detail_ids': False}
        note = self._sgi_compute_note(date_from, date_to)
        vals = {'note': note or False}
        if self.calc_mode == 'manual':
            vals['state'] = 'pendiente'
            return vals
        detail = self._sgi_compute_detail(date_from, date_to) or {}
        # 57.14.0: el modo puede explicar su dato (p. ej. cuántos registros
        # quedaron fuera) con ``note`` en el detalle.
        if detail.get('note'):
            vals['note'] = "\n".join(n for n in (note, detail['note']) if n)
        ids = list(detail.get('ids') or [])
        vals.update({
            'numerator': detail.get('numerator'),
            'denominator': detail.get('denominator'),
            'sample_size': detail.get('sample_size', len(ids)),
            'detail_model': detail.get('model') or False,
            'detail_ids': ",".join(str(i) for i in ids) if ids else False,
        })
        value = detail.get('value')
        if value is None:
            vals.update({'state': 'sin_dato', 'value': 0.0})
        else:
            vals.update({'state': 'capturado', 'value': value})
        return vals

    def _sgi_measurable_on(self, date_to):
        """Falso si el periodo termina antes de «medir desde»."""
        self.ensure_one()
        return not self.measure_from or date_to >= self.measure_from

    # ---- Detalle por modo: primera tanda (a tiempo, completo, OTIF/OTD,
    # entregas, pedidos, NC, calidad, cartera). Los demás llegan por tandas.
    @staticmethod
    def _ratio(num, den, records=None, model=None, pct=True):
        ids = records.ids if records is not None else []
        model = model or (records._name if records is not None else None)
        if not den:
            return {'value': None, 'numerator': num, 'denominator': den,
                    'model': model, 'ids': ids}
        value = round(num / den * (100.0 if pct else 1.0), 2)
        return {'value': value, 'numerator': num, 'denominator': den,
                'model': model, 'ids': ids}

    def _detail_otif_ventas(self, date_from, date_to):
        pickings = self._sgi_outgoing_done(date_from, date_to)
        on_time = sum(1 for p in pickings
                      if (p.date_deadline or p.scheduled_date) and p.date_done
                      and p.date_done <= (p.date_deadline or p.scheduled_date))
        return self._ratio(on_time, len(pickings), pickings)

    def _sgi_otd_receipts(self, date_from, date_to):
        """(a tiempo, medibles, sin promesa) de CO-01. 57.90.0: solo
        recepciones de una orden de compra de la compañía del KPI con fecha
        prometida capturada. Hasta jun-2026 la OC nacía con la fecha
        prometida igual a la del pedido, al segundo (nadie la capturaba), y
        toda recepción salía tarde (2–11 %). Esas quedan fuera y se cuentan
        aparte. A tiempo = recibida a más tardar el día prometido (fecha
        local, no la hora)."""
        dt_from, dt_to = self._sgi_dt_bounds(date_from, date_to)
        pickings = self.env['stock.picking'].search([
            ('picking_type_id.code', '=', 'incoming'), ('state', '=', 'done'),
            ('company_id', '=', self._sgi_kpi_company().id),
            ('purchase_id', '!=', False),
            ('date_done', '>=', dt_from), ('date_done', '<', dt_to)])
        measurable = pickings.filtered(
            lambda p: p.purchase_id.date_planned
            and p.purchase_id.date_planned != p.purchase_id.date_order)
        on_time = measurable.filtered(
            lambda p: sgi_local_date(self.env, p.date_done)
            <= sgi_local_date(self.env, p.purchase_id.date_planned))
        return on_time, measurable, pickings - measurable

    def _detail_otd_compras(self, date_from, date_to):
        if 'purchase_id' not in self.env['stock.picking']._fields:
            return {'value': None}
        on_time, measurable, _without = self._sgi_otd_receipts(date_from, date_to)
        return self._ratio(len(on_time), len(measurable), measurable)

    def _note_otd_compras(self, date_from, date_to):
        if 'purchase_id' not in self.env['stock.picking']._fields:
            return ''
        _on_time, _measurable, without = self._sgi_otd_receipts(date_from, date_to)
        if not without:
            return ''
        return ("%s recepciones sin fecha prometida en la OC (igual a la del "
                "pedido) no cuentan." % len(without))

    def _detail_entregas_completas(self, date_from, date_to):
        pickings = self._sgi_outgoing_done(date_from, date_to)
        complete = len(pickings) - len(pickings.filtered('backorder_ids'))
        return self._ratio(complete, len(pickings), pickings)

    def _detail_embarques_sin_error(self, date_from, date_to):
        pickings = self._sgi_outgoing_done(date_from, date_to)
        ok = len(pickings) - len(self._sgi_shipments_with_return(pickings))
        return self._ratio(ok, len(pickings), pickings)

    def _detail_pedidos_cancelados(self, date_from, date_to):
        dt_from, dt_to = self._sgi_dt_bounds(date_from, date_to)
        orders = self.env['sale.order'].search([
            ('company_id', '=', self._sgi_kpi_company().id),
            ('date_order', '>=', dt_from), ('date_order', '<', dt_to),
            ('state', 'in', ('sale', 'cancel'))])
        cancelled = len(orders.filtered(lambda o: o.state == 'cancel'))
        return self._ratio(cancelled, len(orders), orders)

    def _detail_dso_cartera(self, date_from, date_to):
        receivable = self._sgi_receivable_balance(date_to)
        sales_90 = self._sgi_net_invoiced(date_to - relativedelta(days=89), date_to, taxed=True)
        if sales_90 <= 0:
            return {'value': None, 'numerator': receivable, 'denominator': sales_90}
        return {'value': round(receivable / sales_90 * 90.0, 1),
                'numerator': receivable, 'denominator': sales_90}

    def _sgi_detail_overdue(self, date_to, min_days):
        moves = self._sgi_open_moves(('out_invoice', 'out_refund'), date_to)
        total = sum(moves.mapped('amount_residual_signed'))
        limit = date_to - relativedelta(days=min_days)
        overdue_moves = moves.filtered(lambda m: (m.invoice_date_due or m.invoice_date) < limit)
        overdue = sum(overdue_moves.mapped('amount_residual_signed'))
        if total <= 0:
            return {'value': None, 'numerator': overdue, 'denominator': total,
                    'model': 'account.move', 'ids': overdue_moves.ids}
        return {'value': round(overdue / total * 100.0, 2), 'numerator': overdue,
                'denominator': total, 'model': 'account.move', 'ids': overdue_moves.ids}

    def _sgi_capacitacion_employees(self):
        """RH-02 (57.102.0): empleados activos de la empresa del SGI (D-03).
        Antes eran todos los que veía el usuario del cron."""
        company = self.env['sgi.config']._sgi_company()
        return self.env['hr.employee'].sudo().search([('company_id', '=', company.id)])

    def _detail_capacitacion(self, date_from, date_to):
        """RH-02 (57.102.0): competencias del puesto vigentes ÷ requeridas, solo
        empleados activos de la empresa del SGI. Numerador = requeridas −
        brechas (nunca negativo); registros = las brechas. Foto a hoy: las
        cotas del periodo no aplican."""
        employees = self._sgi_capacitacion_employees()
        JobSkill = self.env['hr.job.skill'].sudo()
        jobs = employees.job_id
        counts = {job.id: n for job, n in JobSkill._read_group(
            [('job_id', 'in', jobs.ids)], ['job_id'], ['__count'])} if jobs else {}
        required = sum(counts.get(e.job_id.id, 0) for e in employees if e.job_id)
        gaps = self.env['sgi.competence.gap'].sudo().search([('employee_id', 'in', employees.ids)])
        covered = max(required - len(gaps), 0)
        return self._ratio(covered, required, gaps)

    def _detail_cartera_vencida(self, date_from, date_to):
        return self._sgi_detail_overdue(date_to, 0)

    def _detail_cartera_vencida_60(self, date_from, date_to):
        return self._sgi_detail_overdue(date_to, 60)


class SgiIndicatorMeasureDetail(models.Model):
    _inherit = 'sgi.indicator.measure'

    state = fields.Selection(selection_add=[('sin_dato', "Sin dato")],
                             ondelete={'sin_dato': 'set default'})
    numerator = fields.Float(string="Numerador", digits=(16, 2), help="Numerador del cálculo.")
    denominator = fields.Float(string="Denominador", digits=(16, 2),
                               help="Denominador del cálculo (la base contra la que se mide).")
    sample_size = fields.Integer(string="Casos", help="Registros que forman la medición.")
    small_sample = fields.Boolean(
        string="Muestra chica", compute='_compute_small_sample', store=True,
        help="Menos casos que el mínimo (quimibond_sgi.indicator_min_sample): "
             "se mide, pero no abre NC.")
    detail_model = fields.Char(string="Modelo de los registros")
    detail_ids = fields.Text(string="Ids de los registros")
    detail_count = fields.Integer(string="Registros", compute='_compute_detail_count')
    value_is_pct = fields.Boolean(compute='_compute_value_is_pct')
    indicator_status = fields.Selection(related='indicator_id.status',
                                        string="Estado del indicador",
                                        help="Si el indicador es oficial o está a prueba.")
    # 57.102.0 (B3): el recálculo diario respeta un valor corregido a mano.
    sgi_value_by_hand = fields.Boolean(
        string="Valor corregido a mano", readonly=True, copy=False,
        help="Alguien escribió a mano el valor de esta medición automática: el "
             "recálculo diario ya no la toca. «Recalcular valor» quita la marca.")

    @api.depends('sample_size', 'state', 'detail_ids')
    def _compute_small_sample(self):
        minimum = _min_sample(self.env)
        for measure in self:
            measure.small_sample = bool(
                measure.state in ('capturado', 'validado')
                and measure.detail_ids and measure.sample_size < minimum)

    @api.depends('detail_ids')
    def _compute_detail_count(self):
        for measure in self:
            measure.detail_count = len([i for i in (measure.detail_ids or '').split(',') if i])

    @api.depends('indicator_id.uom')
    def _compute_value_is_pct(self):
        for measure in self:
            measure.value_is_pct = '%' in (measure.indicator_id.uom or '')

    @api.depends('value', 'state', 'indicator_id.direction',
                 'indicator_id.target_objective', 'indicator_id.target_acceptable')
    def _compute_semaphore(self):
        without = self.filtered(lambda m: m.state == 'sin_dato')
        without.semaphore = False
        super(SgiIndicatorMeasureDetail, self - without)._compute_semaphore()

    def action_view_records(self):
        """Abre los registros guardados en la medición (la lista que explica el
        valor), no una consulta nueva."""
        self.ensure_one()
        if not self.detail_model or not self.detail_count:
            raise UserError("Esta medición no guardó registros. Recalcúlela con el "
                            "modo actual o use «Ver evidencia».")
        if self.detail_model not in self.env:
            raise UserError("El modelo %s ya no existe." % self.detail_model)
        ids = [int(i) for i in self.detail_ids.split(',') if i]
        return {
            'type': 'ir.actions.act_window',
            'name': "%s — registros de %s" % (self.indicator_id.code, self.period_date),
            'res_model': self.detail_model,
            'view_mode': 'list,form',
            'domain': [('id', 'in', ids)],
            'context': {'active_test': False},
        }

    def action_recompute_value(self):
        """Recalcula valor y detalle con el modo actual (sustituye al recálculo
        de solo valor). Solo mediciones no validadas: la validada es evidencia."""
        for measure in self:
            indicator = measure.indicator_id
            if measure.state == 'validado':
                raise UserError(
                    "La medición de %s ya está validada (es evidencia): pida "
                    "al Jefe MAST regresarla a pendiente antes de recalcular."
                    % indicator.code)
            if indicator.calc_mode == 'manual':
                raise UserError(
                    "El indicador %s es de captura manual: no hay nada que "
                    "recalcular." % indicator.code)
            date_from, date_to = indicator._sgi_period_bounds(measure.period_date)
            vals = indicator._sgi_measure_vals(date_from, date_to)
            # 57.102.0 (B3): lo escribe el SGI; deja de estar «corregida a mano».
            measure.with_context(sgi_calc_write=True).write(dict(vals, sgi_value_by_hand=False))
            # 57.1.0: el recálculo desde la medición también deja el motivo.
            indicator._sgi_set_calc(*indicator._sgi_calc_diagnose(vals))
            label = ("sin dato calculable" if vals['state'] == 'sin_dato'
                     else "valor recalculado: %s (%s casos)" % (vals['value'], vals['sample_size']))
            indicator.message_post(
                body="Medición de %s recalculada con el modo «%s» — %s." % (
                    measure.period_date, indicator.calc_mode, label))
        return True

    # ---- B5 (57.102.0): una manual sin valor no se captura ni se valida ----
    def _sgi_without_value(self):
        """Mediciones de indicador manual sin valor capturado: valor 0, sin
        numerador, sin denominador y sin nota. Un ``Float`` no distingue «nadie
        escribió» de «escribieron 0»; para un 0 de verdad, el responsable lo
        explica en la nota."""
        return self.filtered(
            lambda m: m.indicator_id.calc_mode == 'manual' and not m.value
            and not m.numerator and not m.denominator and not (m.note or '').strip())

    def _sgi_check_has_value(self):
        missing = self._sgi_without_value()
        if missing:
            raise UserError(
                "Capture el valor de %s antes de marcarla capturada o validarla. "
                "Si el valor de verdad es 0, escriba en la nota por qué (p. ej. «0: sin "
                "caídas en el mes»)." % ", ".join(missing.mapped('display_name')))

    def write(self, vals):
        # B5: solo cuando una persona (no el sistema) la PASA a capturada o
        # validada. Se revisa después de escribir, con los valores nuevos
        # aplicados; el UserError revierte todo.
        moving = self.browse()
        if vals.get('state') in ('capturado', 'validado') and not self.env.su:
            moving = self.filtered(lambda m: m.state != vals['state'])
        # B3: una persona que cambia el valor de una medición automática la
        # marca «corregida a mano». Las rutas del sistema escriben con
        # ``sgi_calc_write`` (recálculo, «Recalcular valor», «Recalcular
        # ahora», foto, «medir desde»); el sistema (sudo) tampoco marca.
        if ('value' in vals and 'sgi_value_by_hand' not in vals and not self.env.su
                and not self.env.context.get('sgi_calc_write')):
            auto = self.filtered(lambda m: m.indicator_id.calc_mode != 'manual'
                                 and round(m.value or 0.0, 6) != round(vals['value'] or 0.0, 6))
            if auto:
                super(SgiIndicatorMeasureDetail, auto).write({'sgi_value_by_hand': True})
        res = super().write(vals)
        if moving:
            moving._sgi_check_has_value()
        return res

    def action_validate(self):
        """Una medición sin dato no se valida: no hay nada que confirmar y
        validarla la convertiría en un cero rojo."""
        without = self.filtered(lambda m: m.state == 'sin_dato')
        return super(SgiIndicatorMeasureDetail, self - without).action_validate()

    def _sgi_previous_measure(self):
        """La medición del periodo inmediato anterior (semana o mes)."""
        self.ensure_one()
        indicator = self.indicator_id
        step = relativedelta(days=7) if indicator.frequency == 'weekly' \
            else relativedelta(months=1)
        return self.search([
            ('indicator_id', '=', indicator.id),
            ('period_date', '=', self.period_date - step)], limit=1)

    def _sgi_red_with_data(self):
        self.ensure_one()
        return bool(self.semaphore == 'rojo' and self.state in ('capturado', 'validado')
                    and not self.small_sample)

    def _sgi_maybe_create_nc(self):
        """NC por persistencia (I-5): solo un indicador oficial, con dato y con
        muestra suficiente, y solo con dos periodos seguidos en rojo (uno si
        es crítico). Si la NC del periodo anterior sigue abierta, esta
        medición se liga a ella en vez de abrir otra."""
        eligible = self.browse()
        for measure in self:
            indicator = measure.indicator_id
            if (indicator.status != 'oficial' or measure.state == 'sin_dato'
                    or measure.small_sample or measure.alert_id):
                continue
            if measure.semaphore == 'rojo' and not indicator.critical:
                previous = measure._sgi_previous_measure()
                if not previous or not previous._sgi_red_with_data():
                    continue
                alert = previous.alert_id
                if alert and not (alert.stage_id.sgi_is_closing_stage
                                  or alert.stage_id.sgi_is_cancel_stage):
                    if measure.state == 'validado':
                        measure.alert_id = alert
                    continue
            eligible |= measure
        return super(SgiIndicatorMeasureDetail, eligible)._sgi_maybe_create_nc()

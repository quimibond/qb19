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

MIN_SAMPLE_PARAM = 'quimibond_sgi.indicator_min_sample'
DEFAULT_MIN_SAMPLE = 5


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
    ], string="Último cálculo", readonly=True, copy=False, index=True)
    calc_message = fields.Char(string="Motivo", readonly=True, copy=False)
    calc_checked = fields.Datetime(string="Revisado el", readonly=True, copy=False)

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
        return {'model': model, 'ids': self.env[model].sudo().search(domain).ids}

    def _sgi_measure_vals(self, date_from, date_to):
        """Valores de la medición del periodo: valor, detalle, estado y nota.
        Es lo que escriben el cron y «Recalcular»."""
        self.ensure_one()
        note = self._sgi_compute_note(date_from, date_to)
        vals = {'note': note or False}
        if self.calc_mode == 'manual':
            vals['state'] = 'pendiente'
            return vals
        detail = self._sgi_compute_detail(date_from, date_to) or {}
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

    def _detail_otd_compras(self, date_from, date_to):
        dt_from, dt_to = self._sgi_dt_bounds(date_from, date_to)
        pickings = self.env['stock.picking'].search([
            ('picking_type_id.code', '=', 'incoming'), ('state', '=', 'done'),
            ('date_done', '>=', dt_from), ('date_done', '<', dt_to)])
        on_time = 0
        for pick in pickings:
            po = pick.purchase_id if 'purchase_id' in pick._fields else False
            deadline = (po and po.date_planned) or pick.date_deadline or pick.scheduled_date
            if deadline and pick.date_done and pick.date_done <= deadline:
                on_time += 1
        return self._ratio(on_time, len(pickings), pickings)

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

    def _detail_cartera_vencida(self, date_from, date_to):
        return self._sgi_detail_overdue(date_to, 0)

    def _detail_cartera_vencida_60(self, date_from, date_to):
        return self._sgi_detail_overdue(date_to, 60)


class SgiIndicatorMeasureDetail(models.Model):
    _inherit = 'sgi.indicator.measure'

    state = fields.Selection(selection_add=[('sin_dato', "Sin dato")],
                             ondelete={'sin_dato': 'set default'})
    numerator = fields.Float(string="Numerador", digits=(16, 2))
    denominator = fields.Float(string="Denominador", digits=(16, 2))
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
                                        string="Estado del indicador")

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
            raise UserError("Esta medición no guardó registros. Recalcúlala con el "
                            "modo actual o usa «Ver evidencia».")
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
                    "La medición de %s ya está validada (es evidencia): pide "
                    "al Jefe MAST regresarla a pendiente antes de recalcular."
                    % indicator.code)
            if indicator.calc_mode == 'manual':
                raise UserError(
                    "El indicador %s es de captura manual: no hay nada que "
                    "recalcular." % indicator.code)
            date_from, date_to = indicator._sgi_period_bounds(measure.period_date)
            vals = indicator._sgi_measure_vals(date_from, date_to)
            measure.write(vals)
            # 57.1.0: el recálculo desde la medición también deja el motivo.
            indicator._sgi_set_calc(*indicator._sgi_calc_diagnose(vals))
            label = ("sin dato calculable" if vals['state'] == 'sin_dato'
                     else "valor recalculado: %s (%s casos)" % (vals['value'], vals['sample_size']))
            indicator.message_post(
                body="Medición de %s recalculada con el modo «%s» — %s." % (
                    measure.period_date, indicator.calc_mode, label))
        return True

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

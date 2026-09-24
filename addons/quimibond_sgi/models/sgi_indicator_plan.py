# -*- coding: utf-8 -*-
"""I-4, I-6, I-8 y P-40 (2026-09-24).

- I-4 **Plan de acción en la medición roja**: causa, acciones (``sgi.action.line``
  con origen ``measure_id``: responsable y fecha compromiso) y, solo en
  indicadores **oficiales**, una actividad al dueño del indicador que vence el
  día 10 del mes siguiente al periodo (``quimibond_sgi.red_plan_due_day``). Si
  el día pasa sin causa ni acción, el cron diario escala a Dirección. En prueba
  el rojo pide el plan en la ficha, sin actividad ni escalamiento. Nunca con
  «sin dato» ni «muestra chica». Las acciones vencidas ya escalan por
  ``cron_overdue_actions``.
- I-6 **Calendario de cálculo**: los crons de medición corren a diario y solo
  miden el tercer día hábil del mes (``quimibond_sgi.monthly_measure_business_day``)
  y el lunes; si un día se perdió, miden en cuanto vuelven a correr mientras
  el periodo siga sin mediciones.
- I-8 **Ventana visible**: ``window_label`` en el indicador y la medición
  («Mes», «Semana», «3 meses móviles», «12 meses móviles», «90 días al
  cierre», «Al cierre» o la de la fórmula configurable).
- P-40 **Validación masiva en la Revisión por la Dirección**: un botón valida
  las mediciones capturadas del periodo y abre los rojos que aún no tienen
  causa ni acción.
"""
from dateutil.relativedelta import relativedelta
from markupsafe import Markup

from odoo import api, fields, models
from odoo.exceptions import ValidationError

from .sgi_calendar import sgi_nth_business_day

_WINDOW_BY_MODE = {
    'desperdicio_kg': "3 meses móviles",
    'compras_mp_vs_ventas': "3 meses móviles",
    'margen_ebitda': "12 meses móviles",
    'concentracion_top3': "12 meses móviles",
    'retencion_clientes': "12 meses móviles",
    'dso_cartera': "90 días al cierre",
    'dpo_pagos': "90 días al cierre",
    'cartera_vencida': "Al cierre",
    'cartera_vencida_60': "Al cierre",
    'inventario_diferencia': "Mes (existencias al día del cálculo)",
}
_PLAN_SUMMARY = "Causa y acción: %s (%s)"
_ESCALATION_SUMMARY = "Sin causa ni acción (escalado a Dirección): %s (%s)"


class SgiIndicatorWindow(models.Model):
    _inherit = 'sgi.indicator'

    window_label = fields.Char(string="Ventana", compute='_compute_window_label',
                               help="Qué periodo de datos resume cada medición.")

    @api.depends('calc_mode', 'frequency', 'term_ids.window')
    def _compute_window_label(self):
        for indicator in self:
            base = "Semana" if indicator.frequency == 'weekly' else "Mes"
            if indicator.calc_mode == 'configurable':
                num, den = indicator._sgi_terms()
                labels = dict(num._fields['window'].selection) if num else {}
                parts = [labels.get(t.window, t.window) for t in (num, den) if t]
                if parts and parts[0] == 'El periodo':
                    parts[0] = base
                indicator.window_label = " / ".join(dict.fromkeys(parts)) if parts else base
            else:
                indicator.window_label = _WINDOW_BY_MODE.get(indicator.calc_mode, base)


class SgiActionLinePlan(models.Model):
    _inherit = 'sgi.action.line'

    measure_id = fields.Many2one('sgi.indicator.measure', string="Medición roja",
                                 ondelete='cascade', index=True,
                                 help="Plan de acción de una medición en rojo (I-4).")

    @api.depends('measure_id')
    def _compute_origin_display(self):
        with_measure = self.filtered('measure_id')
        for line in with_measure:
            line.origin_display = line.measure_id.display_name
        super(SgiActionLinePlan, self - with_measure)._compute_origin_display()

    def _sgi_origin(self):
        self.ensure_one()
        if self.measure_id:
            return self.measure_id.indicator_id
        return super()._sgi_origin()

    @api.constrains('alert_id', 'risk_id', 'fmea_line_id', 'incident_id',
                    'drill_id', 'objective_id', 'measure_id', 'name')
    def _check_parent_xor(self):
        with_measure = self.filtered('measure_id')
        for line in with_measure:
            others = [line.alert_id, line.risk_id, line.fmea_line_id,
                      line.incident_id, line.drill_id, line.objective_id]
            if any(others):
                raise ValidationError("Una acción de medición roja no puede tener otro origen.")
        return super(SgiActionLinePlan, self - with_measure)._check_parent_xor()

    @api.model_create_multi
    def create(self, vals_list):
        lines = super().create(vals_list)
        lines.measure_id._sgi_plan_captured()
        return lines


class SgiIndicatorMeasurePlan(models.Model):
    _inherit = 'sgi.indicator.measure'

    window_label = fields.Char(related='indicator_id.window_label', string="Ventana")
    cause = fields.Text(string="Causa", help="Por qué salió en rojo (I-4).")
    action_line_ids = fields.One2many('sgi.action.line', 'measure_id', string="Acciones")
    plan_required = fields.Boolean(compute='_compute_plan', string="Requiere plan")
    plan_due = fields.Date(compute='_compute_plan', string="Plan antes del",
                           help="Día 10 del mes siguiente al periodo.")
    plan_done = fields.Boolean(compute='_compute_plan', string="Plan capturado")

    @api.depends('semaphore', 'state', 'small_sample', 'period_date', 'cause',
                 'action_line_ids', 'indicator_id.frequency')
    def _compute_plan(self):
        day = int(self.env['ir.config_parameter'].sudo().get_param(
            'quimibond_sgi.red_plan_due_day', 10) or 10)
        for measure in self:
            measure.plan_required = measure._sgi_red_with_data()
            measure.plan_done = bool(measure.cause and measure.action_line_ids)
            if measure.period_date and measure.indicator_id:
                period_end = measure.indicator_id._sgi_period_bounds(measure.period_date)[1]
                measure.plan_due = (period_end.replace(day=1)
                                    + relativedelta(months=1)).replace(day=min(day, 28))
            else:
                measure.plan_due = False

    # ---- actividad al dueño ---------------------------------------------
    def _sgi_plan_summary(self):
        self.ensure_one()
        return _PLAN_SUMMARY % (self.indicator_id.code, self.display_name.split(' — ')[-1])

    def _sgi_plan_activity(self):
        """Medición roja con dato y sin plan: actividad al dueño con vencimiento
        el día 10 (idempotente)."""
        Cron = self.env['sgi.cron']
        for measure in self:
            indicator = measure.indicator_id
            # Solo indicadores oficiales agendan y escalan; en prueba el rojo
            # pide causa y acción en la ficha, sin actividad.
            if (indicator.status != 'oficial' or not measure.plan_required
                    or measure.plan_done):
                continue
            user_id = indicator.responsible_id.id or Cron._sgi_manager_user_id()
            if not user_id:
                continue
            Cron._sgi_schedule_deadline(
                indicator, measure, measure._sgi_plan_summary(),
                "La medición %s salió en rojo (%s %s). Capture la causa y al menos "
                "una acción con responsable y fecha en la medición antes del %s." % (
                    measure.display_name, measure.value, indicator.uom or '',
                    measure.plan_due),
                user_id, measure.plan_due)

    def _sgi_plan_captured(self):
        """Con causa y acción, la actividad del dueño se da por hecha."""
        for measure in self.filtered('plan_done'):
            activities = self.env['mail.activity'].search([
                ('res_model', '=', 'sgi.indicator'),
                ('res_id', '=', measure.indicator_id.id),
                ('summary', 'in', [measure._sgi_plan_summary(),
                                   _ESCALATION_SUMMARY % (
                                       measure.indicator_id.code,
                                       measure.display_name.split(' — ')[-1])])])
            activities.action_feedback(feedback="Causa y acción capturadas en la medición.")

    @api.model
    def _sgi_escalate_red_plans(self, today):
        """Cron diario: rojos con el plan vencido y sin causa ni acción → Dirección."""
        Cron = self.env['sgi.cron']
        director_id = Cron._sgi_director_user_id()
        if not director_id:
            return
        # Solo periodos recientes: los rojos anteriores a I-4 no inundan a
        # Dirección el primer día.
        candidates = self.search([
            ('semaphore', '=', 'rojo'), ('state', 'in', ('capturado', 'validado')),
            ('small_sample', '=', False), ('indicator_id.status', '=', 'oficial'),
            ('period_date', '>=', today - relativedelta(months=3))])
        for measure in candidates.filtered(
                lambda m: m.plan_required and not m.plan_done and m.plan_due and m.plan_due < today):
            Cron._sgi_schedule(
                measure.indicator_id,
                _ESCALATION_SUMMARY % (measure.indicator_id.code,
                                       measure.display_name.split(' — ')[-1]),
                "La medición %s lleva desde el %s sin causa ni acción; el dueño es %s." % (
                    measure.display_name, measure.plan_due,
                    measure.indicator_id.responsible_id.display_name or 'sin responsable'),
                director_id)

    @api.model_create_multi
    def create(self, vals_list):
        measures = super().create(vals_list)
        measures._sgi_plan_activity()
        return measures

    def write(self, vals):
        res = super().write(vals)
        if {'value', 'state', 'cause', 'action_line_ids'} & set(vals):
            self._sgi_plan_activity()
            self._sgi_plan_captured()
        return res

    def action_open_plan(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window', 'res_model': 'sgi.indicator.measure',
            'res_id': self.id, 'view_mode': 'form', 'target': 'current',
        }


class SgiCronCalendar(models.Model):
    _inherit = 'sgi.cron'

    @api.model
    def _sgi_monthly_run_due(self, today):
        """Tercer día hábil del mes (parámetro), o después si el mes anterior
        sigue sin mediciones (el cron no corrió ese día)."""
        nth = int(self.env['ir.config_parameter'].sudo().get_param(
            'quimibond_sgi.monthly_measure_business_day', 3) or 3)
        run_day = sgi_nth_business_day(self.env, today.year, today.month, nth)
        if today < run_day:
            return False
        if today == run_day:
            return True
        first_prev = today.replace(day=1) - relativedelta(months=1)
        return not self.env['sgi.indicator.measure'].search_count([
            ('period_date', '=', first_prev), ('indicator_id.frequency', '=', 'monthly')])

    @api.model
    def _sgi_weekly_run_due(self, today):
        if today.weekday() == 0:
            return True
        prev_monday = today - relativedelta(days=today.weekday() + 7)
        has_weekly = self.env['sgi.indicator'].search_count([('frequency', '=', 'weekly')])
        return bool(has_weekly) and not self.env['sgi.indicator.measure'].search_count([
            ('period_date', '=', prev_monday), ('indicator_id.frequency', '=', 'weekly')])

    @api.model
    def cron_indicators(self, scheduled=False):
        """Diario desde I-6: escala los planes vencidos todos los días y mide
        solo el tercer día hábil (o cuando el mes anterior siga sin medir).
        Sin ``scheduled`` (a mano) mide siempre, como antes."""
        today = fields.Date.context_today(self)
        self._sgi_step("escalamiento de planes de mediciones rojas",
                       lambda: self.env['sgi.indicator.measure']._sgi_escalate_red_plans(today))
        if scheduled and not self._sgi_monthly_run_due(today):
            return True
        return super().cron_indicators()

    @api.model
    def cron_indicators_weekly(self, scheduled=False):
        today = fields.Date.context_today(self)
        if scheduled and not self._sgi_weekly_run_due(today):
            return True
        return super().cron_indicators_weekly()


class SgiManagementReviewValidate(models.Model):
    _inherit = 'sgi.management.review'

    def action_validate_measures(self):
        """P-40: valida las mediciones capturadas del periodo y abre los rojos
        que aún no tienen causa ni acción."""
        self.ensure_one()
        Measure = self.env['sgi.indicator.measure']
        captured = Measure.search([
            ('state', '=', 'capturado'),
            ('period_date', '>=', self.period_from), ('period_date', '<=', self.period_to)])
        captured.action_validate()
        reds = Measure.search([
            ('semaphore', '=', 'rojo'), ('state', '=', 'validado'),
            ('period_date', '>=', self.period_from), ('period_date', '<=', self.period_to),
        ]).filtered(lambda m: m.plan_required and not m.plan_done)
        self.message_post(body=Markup(
            "Revisión: %d mediciones validadas; %d rojas sin causa ni acción.") % (
            len(captured), len(reds)))
        return {
            'type': 'ir.actions.act_window', 'name': "Rojos sin plan de acción",
            'res_model': 'sgi.indicator.measure', 'view_mode': 'list,form',
            'domain': [('id', 'in', reds.ids)],
        }

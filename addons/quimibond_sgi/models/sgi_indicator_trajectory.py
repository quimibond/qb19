# -*- coding: utf-8 -*-
"""I-7 (2026-09-24): meta con trayectoria y sentido «dentro de un rango».

- **Trayectoria**: arranque (``baseline_value`` desde ``baseline_date``) y meta
  final (``target_objective`` el ``target_date``) se interpolan linealmente en
  **escalones trimestrales** (``sgi.indicator.step``): el objetivo de cada
  trimestre es el valor de la recta al cierre del trimestre. El aceptable de
  cada escalón guarda la misma distancia que el aceptable de la meta final.
  Un trimestre se puede corregir a mano con motivo: queda marcado ``manual``,
  el cambio va al chatter del indicador y «Generar trayectoria» lo respeta.
- **Metas por periodo**: la medición toma objetivo y aceptable del escalón
  cuyo trimestre contiene su periodo; sin escalones, los del indicador; después
  del último escalón, la meta final.
- **Dentro de un rango** (``direction = 'range'``): verde entre ``range_min``
  y ``range_max``, amarillo dentro de ``range_tolerance`` fuera del rango, rojo
  más allá. La trayectoria no aplica a este sentido.
"""
from datetime import date

from dateutil.relativedelta import relativedelta
from markupsafe import Markup

from odoo import api, fields, models
from odoo.exceptions import UserError, ValidationError


def _quarter_start(day):
    return date(day.year, 3 * ((day.month - 1) // 3) + 1, 1)


class SgiIndicatorStep(models.Model):
    _name = 'sgi.indicator.step'
    _description = "Escalón trimestral de la meta de un indicador SGI"
    _order = 'indicator_id, date_from'

    indicator_id = fields.Many2one('sgi.indicator', required=True, ondelete='cascade', index=True)
    date_from = fields.Date(string="Trimestre desde", required=True)
    name = fields.Char(string="Trimestre", compute='_compute_name', store=True)
    objective = fields.Float(string="Objetivo", digits=(16, 2))
    acceptable = fields.Float(string="Aceptable", digits=(16, 2))
    manual = fields.Boolean(string="Corregido a mano", readonly=True)
    reason = fields.Text(string="Motivo", help="Obligatorio al corregir un escalón a mano.")

    _indicator_quarter_uniq = models.Constraint(
        'unique(indicator_id, date_from)', "Ya hay un escalón para ese trimestre.")

    @api.depends('date_from')
    def _compute_name(self):
        for step in self:
            d = step.date_from
            step.name = "T%d %d" % ((d.month - 1) // 3 + 1, d.year) if d else ''

    @api.constrains('date_from')
    def _check_quarter(self):
        for step in self:
            if step.date_from and step.date_from != _quarter_start(step.date_from):
                raise ValidationError("El escalón empieza el primer día de un trimestre.")

    def write(self, vals):
        """Corregir objetivo o aceptable exige motivo; queda en el chatter."""
        if {'objective', 'acceptable'} & set(vals) and not self.env.context.get('sgi_trajectory'):
            for step in self:
                reason = (vals.get('reason') or step.reason or '').strip()
                if not reason:
                    raise UserError("Para corregir el escalón %s hay que escribir el motivo." % step.name)
                before = "objetivo %s / aceptable %s" % (step.objective, step.acceptable)
                after = "objetivo %s / aceptable %s" % (
                    vals.get('objective', step.objective), vals.get('acceptable', step.acceptable))
                step.indicator_id.message_post(body=Markup(
                    "Escalón %s corregido a mano.<br/>Antes: %s<br/>Ahora: %s<br/>Motivo: %s") % (
                    step.name, before, after, reason))
            vals = dict(vals, manual=True)
        return super().write(vals)


class SgiIndicatorTrajectory(models.Model):
    _inherit = 'sgi.indicator'

    direction = fields.Selection(selection_add=[('range', "Dentro de un rango")],
                                 ondelete={'range': 'set default'})
    range_min = fields.Float(string="Mínimo", digits=(16, 2))
    range_max = fields.Float(string="Máximo", digits=(16, 2))
    range_tolerance = fields.Float(
        string="Tolerancia", digits=(16, 2),
        help="Fuera del rango pero dentro de esta distancia el semáforo es amarillo.")
    baseline_date = fields.Date(string="Arranque desde",
                                help="Fecha del valor de arranque; inicio de la trayectoria.")
    step_ids = fields.One2many('sgi.indicator.step', 'indicator_id', string="Escalones")
    has_trajectory = fields.Boolean(compute='_compute_has_trajectory')

    @api.depends('step_ids')
    def _compute_has_trajectory(self):
        for indicator in self:
            indicator.has_trajectory = bool(indicator.step_ids)

    @api.constrains('direction', 'range_min', 'range_max', 'range_tolerance')
    def _check_range(self):
        for indicator in self.filtered(lambda i: i.direction == 'range'):
            if indicator.range_min >= indicator.range_max:
                raise ValidationError("%s: el mínimo del rango debe ser menor que el máximo." % indicator.code)
            if indicator.range_tolerance < 0:
                raise ValidationError("%s: la tolerancia no puede ser negativa." % indicator.code)

    def _sgi_spec_problems(self):
        problems = super()._sgi_spec_problems()
        if self.direction == 'range':
            problems = [p for p in problems if p != "sin meta"]
            if not self.range_max or self.range_min >= self.range_max:
                problems.append("sin rango")
        return problems

    # ---- trayectoria automática (55.0.0) -----------------------------------
    _TRAJECTORY_FIELDS = ('baseline_value', 'baseline_date', 'target_objective', 'target_date',
                          'target_acceptable', 'direction')

    def _sgi_trajectory_ready(self):
        self.ensure_one()
        return bool(self.baseline_date and self.target_date and self.direction != 'range'
                    and self.target_date > self.baseline_date)

    def _sgi_auto_trajectory(self):
        """Genera los escalones en cuanto el indicador tiene arranque, fecha de
        arranque y fecha de meta (antes había que pulsar el botón; EX-01 a
        EX-16 y VE-01 llevaban semanas sin escalones). Silencioso si falta algo."""
        for indicator in self.filtered(lambda i: i._sgi_trajectory_ready()):
            try:
                indicator.action_generate_trajectory()
            except UserError:
                continue
        return True

    @api.model_create_multi
    def create(self, vals_list):
        indicators = super().create(vals_list)
        indicators._sgi_auto_trajectory()
        return indicators

    def write(self, vals):
        res = super().write(vals)
        if set(vals) & set(self._TRAJECTORY_FIELDS):
            self._sgi_auto_trajectory()
        return res

    @api.model
    def cron_missing_trajectories(self):
        """Paso del cron de indicadores: escalones para los que ya tienen fechas."""
        pending = self.search([('baseline_date', '!=', False), ('target_date', '!=', False),
                               ('direction', '!=', 'range'), ('step_ids', '=', False)])
        pending._sgi_auto_trajectory()
        return True

    # ---- trayectoria ------------------------------------------------------
    def _sgi_trajectory_value(self, day):
        """Valor de la recta arranque → meta final en ``day`` (recortado)."""
        self.ensure_one()
        total = (self.target_date - self.baseline_date).days
        if total <= 0:
            return self.target_objective
        t = min(max((day - self.baseline_date).days / total, 0.0), 1.0)
        return round(self.baseline_value + (self.target_objective - self.baseline_value) * t, 2)

    def action_generate_trajectory(self):
        """Escalones trimestrales entre arranque y meta final. Los corregidos a
        mano se conservan; los demás se recalculan."""
        for indicator in self:
            if indicator.direction == 'range':
                raise UserError("La trayectoria no aplica al sentido «dentro de un rango».")
            if not indicator.baseline_date or not indicator.target_date:
                raise UserError("%s: capture «Arranque desde» y «Llegar a la meta el»." % indicator.code)
            if indicator.target_date <= indicator.baseline_date:
                raise UserError("%s: la fecha de la meta debe ser posterior al arranque." % indicator.code)
            gap = indicator.target_objective - indicator.target_acceptable
            Step = self.env['sgi.indicator.step'].with_context(sgi_trajectory=True)
            existing = {s.date_from: s for s in indicator.step_ids}
            wanted = []
            q = _quarter_start(indicator.baseline_date)
            last_q = _quarter_start(indicator.target_date)
            while q <= last_q:
                wanted.append(q)
                q = q + relativedelta(months=3)
            for q in wanted:
                q_end = min(q + relativedelta(months=3, days=-1), indicator.target_date)
                objective = indicator._sgi_trajectory_value(q_end)
                vals = {'objective': objective, 'acceptable': round(objective - gap, 2)}
                step = existing.get(q)
                if step and step.manual:
                    continue
                if step:
                    step.with_context(sgi_trajectory=True).write(vals)
                else:
                    Step.create(dict(vals, indicator_id=indicator.id, date_from=q))
            stale = indicator.step_ids.filtered(lambda s: s.date_from not in wanted and not s.manual)
            stale.unlink()
            indicator.message_post(body="Trayectoria generada: %d escalones trimestrales de %s (%s) a %s (%s); %d corregidos a mano se conservan." % (
                len(wanted), indicator.baseline_value, indicator.baseline_date,
                indicator.target_objective, indicator.target_date,
                len(indicator.step_ids.filtered('manual'))))
        return True

    def _sgi_targets_on(self, period_date):
        """(objetivo, aceptable) vigentes en el periodo: el escalón de su
        trimestre; sin escalones o después del último, los del indicador."""
        self.ensure_one()
        if not self.step_ids or not period_date or self.direction == 'range':
            return self.target_objective, self.target_acceptable
        q = _quarter_start(period_date)
        step = self.step_ids.filtered(lambda s: s.date_from == q)[:1]
        if step:
            return step.objective, step.acceptable
        if q > max(self.step_ids.mapped('date_from')):
            return self.target_objective, self.target_acceptable
        earlier = self.step_ids.filtered(lambda s: s.date_from < q).sorted('date_from')
        if earlier:
            return earlier[-1].objective, earlier[-1].acceptable
        return self.target_objective, self.target_acceptable


class SgiIndicatorMeasureTrajectory(models.Model):
    _inherit = 'sgi.indicator.measure'

    # Odoo hereda los atributos del campo base al redefinirlo: sin related=None
    # el campo seguiría siendo el related al indicador y el compute no correría.
    target_objective = fields.Float(string="Objetivo", compute='_compute_targets', related=None)
    target_acceptable = fields.Float(string="Aceptable", compute='_compute_targets', related=None)
    range_min = fields.Float(related='indicator_id.range_min')
    range_max = fields.Float(related='indicator_id.range_max')

    @api.depends('indicator_id.target_objective', 'indicator_id.target_acceptable',
                 'indicator_id.direction', 'period_date',
                 'indicator_id.step_ids.objective', 'indicator_id.step_ids.acceptable',
                 'indicator_id.step_ids.date_from')
    def _compute_targets(self):
        for measure in self:
            if measure.indicator_id:
                measure.target_objective, measure.target_acceptable = \
                    measure.indicator_id._sgi_targets_on(measure.period_date)
            else:
                measure.target_objective = measure.target_acceptable = 0.0

    @api.depends('value', 'state', 'period_date', 'indicator_id.direction',
                 'indicator_id.target_objective', 'indicator_id.target_acceptable',
                 'indicator_id.range_min', 'indicator_id.range_max',
                 'indicator_id.range_tolerance',
                 'indicator_id.step_ids.objective', 'indicator_id.step_ids.acceptable',
                 'indicator_id.step_ids.date_from')
    def _compute_semaphore(self):
        for measure in self:
            if measure.state in ('pendiente', 'sin_dato'):
                measure.semaphore = False
                continue
            indicator = measure.indicator_id
            val = measure.value
            if indicator.direction == 'range':
                lo, hi, tol = indicator.range_min, indicator.range_max, indicator.range_tolerance
                if lo <= val <= hi:
                    measure.semaphore = 'verde'
                elif lo - tol <= val <= hi + tol:
                    measure.semaphore = 'amarillo'
                else:
                    measure.semaphore = 'rojo'
                continue
            obj, acc = indicator._sgi_targets_on(measure.period_date)
            if indicator.direction == 'lower_better':
                measure.semaphore = 'verde' if val <= obj else 'amarillo' if val <= acc else 'rojo'
            else:
                measure.semaphore = 'verde' if val >= obj else 'amarillo' if val >= acc else 'rojo'

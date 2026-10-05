# -*- coding: utf-8 -*-
"""57.100.0 (K-04, auditoría 2026-10): la medición validada guarda las metas
con las que se juzgó.

Antes, el semáforo guardado de una medición validada se recalculaba al
cambiar la meta del indicador o un escalón de su trayectoria: la evidencia
cambiaba de color sin que nadie la tocara. Ahora, al pasar a «Validado» (por
cualquier vía: botón, lote, ``write``, ``create`` o el sistema), la medición
guarda sentido, objetivo, aceptable y rango del periodo. Su semáforo, el
objetivo y el aceptable que muestra y el semáforo de su desglose salen de esas
metas guardadas. Reabrirla (solo el Jefe MAST, por el candado de la validada)
las suelta; validarla otra vez guarda las de ese momento. Si el Jefe MAST
corrige el valor sin reabrirla, el color se recalcula contra las metas
guardadas (P3).

Las metas guardadas solo las escribe el SGI con sudo (el contexto
``sgi_freeze`` solo marca el camino; por sí solo no basta): por RPC se
rechazan.
"""
from odoo import api, fields, models
from odoo.exceptions import UserError

from .sgi_calendar import sgi_today

FROZEN_FIELDS = ('sgi_targets_frozen', 'sgi_frozen_date', 'sgi_frozen_direction',
                 'sgi_frozen_objective', 'sgi_frozen_acceptable', 'sgi_frozen_range_min',
                 'sgi_frozen_range_max', 'sgi_frozen_range_tolerance')
# Metas del indicador que, al cambiar, ya no alcanzan a las validadas.
TARGET_FIELDS = ('target_objective', 'target_acceptable', 'direction',
                 'range_min', 'range_max', 'range_tolerance')


def sgi_semaphore(value, direction, objective, acceptable, lo=0.0, hi=0.0, tol=0.0):
    """La misma regla que la medición y el desglose (metas o rango)."""
    if direction == 'range':
        if lo <= value <= hi:
            return 'verde'
        return 'amarillo' if lo - tol <= value <= hi + tol else 'rojo'
    if direction == 'lower_better':
        return 'verde' if value <= objective else 'amarillo' if value <= acceptable else 'rojo'
    return 'verde' if value >= objective else 'amarillo' if value >= acceptable else 'rojo'


class SgiIndicatorMeasureIntegrity(models.Model):
    _inherit = 'sgi.indicator.measure'

    sgi_targets_frozen = fields.Boolean(
        string="Metas congeladas", readonly=True, copy=False, index=True,
        help="La medición está validada y conserva las metas con las que se validó: "
             "cambiar la meta del indicador no le cambia el color.")
    sgi_frozen_date = fields.Date(
        string="Metas guardadas el", readonly=True, copy=False,
        help="Día en que la medición guardó sus metas (al validarla).")
    sgi_frozen_direction = fields.Char(
        string="Sentido al validar", readonly=True, copy=False,
        help="Sentido del indicador cuando se validó la medición.")
    # I-1: sin «digits», como sus fuentes (sgi.indicator.target_*): redondear
    # a dos decimales podía cambiar el color en la frontera.
    sgi_frozen_objective = fields.Float(
        string="Objetivo al validar", readonly=True, copy=False,
        help="Objetivo del periodo con el que se validó la medición.")
    sgi_frozen_acceptable = fields.Float(
        string="Aceptable al validar", readonly=True, copy=False,
        help="Aceptable del periodo con el que se validó la medición.")
    sgi_frozen_range_min = fields.Float(
        string="Mínimo al validar", digits=(16, 2), readonly=True, copy=False,
        help="Límite inferior del rango con el que se validó la medición.")
    sgi_frozen_range_max = fields.Float(
        string="Máximo al validar", digits=(16, 2), readonly=True, copy=False,
        help="Límite superior del rango con el que se validó la medición.")
    sgi_frozen_range_tolerance = fields.Float(
        string="Tolerancia al validar", digits=(16, 2), readonly=True, copy=False,
        help="Tolerancia del rango con la que se validó la medición.")

    # ---- metas y semáforo ---------------------------------------------------
    def _sgi_frozen_semaphore(self, value=None):
        self.ensure_one()
        return sgi_semaphore(
            self.value if value is None else value, self.sgi_frozen_direction,
            self.sgi_frozen_objective, self.sgi_frozen_acceptable, self.sgi_frozen_range_min,
            self.sgi_frozen_range_max, self.sgi_frozen_range_tolerance)

    @api.depends('sgi_targets_frozen', 'sgi_frozen_objective', 'sgi_frozen_acceptable')
    def _compute_targets(self):
        frozen = self.filtered('sgi_targets_frozen')
        for measure in frozen:
            measure.target_objective = measure.sgi_frozen_objective
            measure.target_acceptable = measure.sgi_frozen_acceptable
        super(SgiIndicatorMeasureIntegrity, self - frozen)._compute_targets()

    @api.depends('sgi_targets_frozen', 'sgi_frozen_direction', 'sgi_frozen_objective',
                 'sgi_frozen_acceptable', 'sgi_frozen_range_min', 'sgi_frozen_range_max',
                 'sgi_frozen_range_tolerance')
    def _compute_semaphore(self):
        frozen = self.filtered(lambda m: m.sgi_targets_frozen
                               and m.state not in ('pendiente', 'sin_dato'))
        for measure in frozen:
            measure.semaphore = measure._sgi_frozen_semaphore()
        super(SgiIndicatorMeasureIntegrity, self - frozen)._compute_semaphore()

    # ---- guardar y soltar las metas -----------------------------------------
    def _sgi_targets_snapshot(self, when=None):
        self.ensure_one()
        indicator = self.indicator_id
        objective, acceptable = indicator._sgi_targets_on(self.period_date)
        return {
            'sgi_targets_frozen': True,
            'sgi_frozen_date': when or sgi_today(self.env),
            'sgi_frozen_direction': indicator.direction or 'higher_better',
            'sgi_frozen_objective': objective,
            'sgi_frozen_acceptable': acceptable,
            'sgi_frozen_range_min': indicator.range_min,
            'sgi_frozen_range_max': indicator.range_max,
            'sgi_frozen_range_tolerance': indicator.range_tolerance,
        }

    def _sgi_freeze_targets(self, when=None):
        """Guarda las metas del periodo de cada medición (validadas)."""
        for measure in self.filtered('indicator_id'):
            measure.sudo().with_context(sgi_freeze=True).write(
                measure._sgi_targets_snapshot(when))
        return True

    def _sgi_thaw_targets(self):
        """Al reabrir: la medición vuelve a juzgarse con las metas actuales."""
        for measure in self.filtered('sgi_targets_frozen'):
            if measure.sgi_frozen_direction == 'range':
                before = "rango %s a %s, tolerancia %s" % (
                    measure.sgi_frozen_range_min, measure.sgi_frozen_range_max,
                    measure.sgi_frozen_range_tolerance)
            else:
                before = "objetivo %s, aceptable %s" % (
                    measure.sgi_frozen_objective, measure.sgi_frozen_acceptable)
            measure.sudo().with_context(sgi_freeze=True).write(
                {'sgi_targets_frozen': False, 'sgi_frozen_date': False})
            measure.message_post(body=(
                "Metas liberadas al reabrir la medición (%s). Se juzga con las metas "
                "actuales del indicador hasta que se valide de nuevo." % before))
        return True

    def _sgi_check_frozen_vals(self, vals):
        # Solo el sistema (sudo): el contexto lo puede mandar cualquier
        # cliente RPC, así que por sí solo no basta.
        if set(vals) & set(FROZEN_FIELDS) and not self.env.su:
            raise UserError("Las metas guardadas de una medición validada las escribe el SGI "
                            "al validarla; no se editan a mano. Para juzgarla con otra meta, "
                            "el Jefe MAST la reabre y se valida de nuevo.")

    @api.model_create_multi
    def create(self, vals_list):
        for vals in vals_list:
            self._sgi_check_frozen_vals(vals)
        measures = super().create(vals_list)
        measures.filtered(lambda m: m.state == 'validado'
                          and not m.sgi_targets_frozen)._sgi_freeze_targets()
        return measures

    def write(self, vals):
        self._sgi_check_frozen_vals(vals)
        to_freeze = to_thaw = self.browse()
        if 'state' in vals:
            if vals['state'] == 'validado':
                to_freeze = self.filtered(lambda m: m.state != 'validado')
            else:
                to_thaw = self.filtered(lambda m: m.state == 'validado')
        res = super().write(vals)
        if to_freeze:
            to_freeze._sgi_freeze_targets()
        if to_thaw:
            to_thaw._sgi_thaw_targets()
        return res


class SgiIndicatorMeasureSplitIntegrity(models.Model):
    _inherit = 'sgi.indicator.measure.split'

    @api.depends('measure_id.sgi_targets_frozen', 'measure_id.sgi_frozen_objective',
                 'measure_id.sgi_frozen_acceptable', 'measure_id.sgi_frozen_direction',
                 'measure_id.sgi_frozen_range_min', 'measure_id.sgi_frozen_range_max',
                 'measure_id.sgi_frozen_range_tolerance')
    def _compute_semaphore(self):
        frozen = self.filtered(lambda r: r.state != 'sin_dato' and r.measure_id.sgi_targets_frozen)
        for row in frozen:
            row.semaphore = row.measure_id._sgi_frozen_semaphore(row.value)
        super(SgiIndicatorMeasureSplitIntegrity, self - frozen)._compute_semaphore()


class SgiIndicatorIntegrity(models.Model):
    _inherit = 'sgi.indicator'

    def write(self, vals):
        res = super().write(vals)
        if set(vals) & set(TARGET_FIELDS):
            data = self.env['sgi.indicator.measure'].sudo()._read_group(
                [('indicator_id', 'in', self.ids), ('sgi_targets_frozen', '=', True)],
                ['indicator_id'], ['__count'])
            for indicator, count in data:
                self.browse(indicator.id).message_post(body=(
                    "%d medición(es) validada(s) conservan las metas con las que se "
                    "validaron (K-04); la meta nueva aplica a las que no están validadas."
                    % count))
        return res

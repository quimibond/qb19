# -*- coding: utf-8 -*-
from odoo import fields, models


class TintoreriaFormulaTiempo(models.Model):
    """Minutos del proceso de Tintorería por fórmula y HTJ (diseño §3.1).

    Son minutos fijos por ORDEN (por baño): el tiempo no depende de los kg ni del número de rollos.
    """
    _name = 'tintoreria.formula.tiempo'
    _description = 'Tiempo de Tintorería por fórmula y HTJ'
    _order = 'formula_id, workcenter_id'

    formula_id = fields.Many2one('tintoreria.formula', 'Fórmula', required=True, ondelete='cascade', index=True)
    workcenter_id = fields.Many2one(
        'mrp.workcenter', 'HTJ', required=True, ondelete='cascade', index=True,
        domain="[('ft_tipo_id.name', '=', 'Tintorería')]")
    minutos = fields.Float('Minutos por baño', required=True, digits=(12, 2))
    active = fields.Boolean(default=True)

    _unique_formula_htj = models.Constraint(
        'unique(formula_id, workcenter_id)',
        'Ya existe un tiempo para esa fórmula y ese HTJ.')
    _check_minutos = models.Constraint(
        'CHECK(minutos > 0)', 'Los minutos por baño deben ser mayores a cero.')

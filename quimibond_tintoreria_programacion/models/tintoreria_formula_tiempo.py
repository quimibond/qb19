# -*- coding: utf-8 -*-
from odoo import api, fields, models


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
    horas = fields.Float('Horas por baño', compute='_compute_horas', digits=(12, 2))  # 1.4.0: columna de la lista
    active = fields.Boolean(default=True)

    @api.depends('minutos')
    def _compute_horas(self):
        for tiempo in self:
            tiempo.horas = tiempo.minutos / 60.0

    _unique_formula_htj = models.Constraint(
        'unique(formula_id, workcenter_id)',
        'Ya existe un tiempo para esa fórmula y ese HTJ.')
    _check_minutos = models.Constraint(
        'CHECK(minutos > 0)', 'Los minutos por baño deben ser mayores a cero.')


class TintoreriaFormula(models.Model):
    _inherit = 'tintoreria.formula'

    # 1.4.0: los minutos por jet se ven y se capturan en la propia fórmula.
    tiempo_ids = fields.One2many('tintoreria.formula.tiempo', 'formula_id', 'Tiempos por jet')


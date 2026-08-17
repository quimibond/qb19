# -*- coding: utf-8 -*-
from odoo import models, fields, api

# Orden de severidad del semáforo de salud (peor color gana en la agregación).
SGI_HEALTH_ORDER = {'verde': 0, 'amarillo': 1, 'rojo': 2}


def sgi_worst_health(values):
    """Devuelve el color más severo de una lista (verde si está vacía)."""
    worst = 'verde'
    for value in values:
        if value and SGI_HEALTH_ORDER.get(value, 0) > SGI_HEALTH_ORDER[worst]:
            worst = value
    return worst


class SgiObjective(models.Model):
    _name = 'sgi.objective'
    _description = "Objetivo Integral SGI"
    _order = 'target_year, name'

    name = fields.Char(string="Objetivo", required=True, translate=False)
    description = fields.Text(string="Descripción")
    policy_id = fields.Many2one('sgi.policy', string="Política integral",
                                default=lambda self: self.env['sgi.policy'].search(
                                    [('state', '=', 'vigente')], limit=1),
                                help="Política de la que se despliega este objetivo (cascada ISO).")
    target_year = fields.Integer(string="Año meta")
    indicator_ids = fields.One2many('sgi.indicator', 'objective_id', string="Indicadores")
    indicator_count = fields.Integer(string="# Indicadores", compute='_compute_indicator_count')
    health = fields.Selection([
        ('verde', "Verde"),
        ('amarillo', "Amarillo"),
        ('rojo', "Rojo"),
    ], string="Salud agregada", compute='_compute_health',
        help="Peor color entre los procesos de sus indicadores.")
    active = fields.Boolean(default=True)

    @api.depends('indicator_ids', 'indicator_ids.process_id')
    def _compute_health(self):
        for objective in self:
            processes = objective.indicator_ids.mapped('process_id')
            objective.health = sgi_worst_health(processes.mapped('health'))

    def _compute_indicator_count(self):
        data = self.env['sgi.indicator']._read_group(
            [('objective_id', 'in', self.ids)], ['objective_id'], ['__count'])
        mapped = {obj.id: count for obj, count in data}
        for objective in self:
            objective.indicator_count = mapped.get(objective.id, 0)

    @api.depends('name', 'target_year')
    def _compute_display_name(self):
        for objective in self:
            if objective.target_year:
                objective.display_name = "%s (%s)" % (objective.name, objective.target_year)
            else:
                objective.display_name = objective.name

    def action_open_indicators(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Indicadores — %s" % self.name,
            'res_model': 'sgi.indicator',
            'view_mode': 'list,form',
            'domain': [('objective_id', '=', self.id)],
            'context': {'default_objective_id': self.id},
        }

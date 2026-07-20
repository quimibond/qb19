# -*- coding: utf-8 -*-
from odoo import models, fields, api


class SgiObjective(models.Model):
    _name = 'sgi.objective'
    _description = "Objetivo Integral SGI"
    _order = 'target_year, name'

    name = fields.Char(string="Objetivo", required=True, translate=False)
    description = fields.Text(string="Descripción")
    policy_id = fields.Many2one('sgi.policy', string="Política integral",
                                help="Política de la que se despliega este objetivo (cascada ISO).")
    target_year = fields.Integer(string="Año meta")
    indicator_ids = fields.One2many('sgi.indicator', 'objective_id', string="Indicadores")
    indicator_count = fields.Integer(string="# Indicadores", compute='_compute_indicator_count')
    active = fields.Boolean(default=True)

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

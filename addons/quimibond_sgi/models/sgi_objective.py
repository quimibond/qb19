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
    # mail.activity.mixin: las acciones del plan (6.2.2) anclan su actividad
    # espejo SOBRE el objetivo; sin el mixin, agendar tronaría (lección C1).
    _inherit = ['mail.thread', 'mail.activity.mixin']
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
    # Plan de acción del objetivo (ISO 6.2.2: qué se hará, responsable y
    # plazo). Reutiliza el modelo CAPA transversal: sexta fuente del XOR.
    action_line_ids = fields.One2many('sgi.action.line', 'objective_id',
                                      string="Plan de acción (6.2.2)")
    health = fields.Selection([
        ('verde', "Verde"),
        ('amarillo', "Amarillo"),
        ('rojo', "Rojo"),
    ], string="Salud agregada", compute='_compute_health',
        help="Peor color entre los procesos de sus indicadores Y el último "
             "semáforo de cada indicador.")
    active = fields.Boolean(default=True)

    @api.depends('indicator_ids', 'indicator_ids.process_id')
    def _compute_health(self):
        """Agrega la salud del proceso Y el último semáforo del indicador:
        antes solo el proceso, así que un objetivo cuyos KPIs no tuvieran
        proceso salía verde siempre (deuda B.18)."""
        semaphore_to_health = {'rojo': 'rojo', 'amarillo': 'amarillo',
                               'verde': 'verde'}
        for objective in self:
            values = objective.indicator_ids.mapped('process_id').mapped('health')
            values += [semaphore_to_health.get(s) for s in
                       objective.indicator_ids.mapped('last_semaphore') if s]
            objective.health = sgi_worst_health(values)

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

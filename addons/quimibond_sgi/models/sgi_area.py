# -*- coding: utf-8 -*-
from odoo import models, fields, api


class SgiArea(models.Model):
    _name = 'sgi.area'
    _description = "Área documental SGI"
    _order = 'code'

    code = fields.Char(string="Clave", required=True, index=True)
    name = fields.Char(string="Nombre", required=True, translate=False)
    department_id = fields.Many2one('hr.department', string="Departamento")
    active = fields.Boolean(default=True)

    _sql_constraints = [
        ('code_uniq', 'unique(code)', "La clave de área debe ser única."),
    ]

    @api.depends('code', 'name')
    def _compute_display_name(self):
        for area in self:
            area.display_name = "%s - %s" % (area.code, area.name) if area.code else area.name

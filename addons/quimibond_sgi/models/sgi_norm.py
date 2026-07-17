# -*- coding: utf-8 -*-
from odoo import models, fields, api


class SgiNorm(models.Model):
    _name = 'sgi.norm'
    _description = "Norma ISO"
    _order = 'code'

    code = fields.Char(string="Clave", required=True, index=True)
    name = fields.Char(string="Nombre", required=True)
    clause_ids = fields.One2many('sgi.norm.clause', 'norm_id', string="Cláusulas")
    active = fields.Boolean(default=True)

    _code_uniq = models.Constraint(
        'unique(code)',
        "La clave de norma debe ser única.",
    )


class SgiNormClause(models.Model):
    _name = 'sgi.norm.clause'
    _description = "Cláusula de norma ISO"
    _order = 'norm_id, code'

    norm_id = fields.Many2one('sgi.norm', string="Norma", required=True, ondelete='cascade')
    code = fields.Char(string="Numeral", required=True)
    name = fields.Char(string="Requisito", required=True)

    @api.depends('code', 'name')
    def _compute_display_name(self):
        for clause in self:
            clause.display_name = "%s %s" % (clause.code, clause.name) if clause.code else clause.name

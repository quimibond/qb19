# -*- coding: utf-8 -*-
from odoo import fields, models


class HrAttendance(models.Model):
    _inherit = 'hr.attendance'

    in_mode = fields.Selection(selection_add=[('checador', "Checador")], ondelete={'checador': 'set default'})
    out_mode = fields.Selection(selection_add=[('checador', "Checador")], ondelete={'checador': 'set default'})
    qb_checada_in_id = fields.Many2one('qb.checada', string="Checada de entrada", readonly=True, ondelete='set null')
    qb_checada_out_id = fields.Many2one('qb.checada', string="Checada de salida", readonly=True, ondelete='set null')

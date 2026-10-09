# -*- coding: utf-8 -*-
from odoo import fields, models


class HrEmployee(models.Model):
    _inherit = 'hr.employee'

    qb_checador_pin = fields.Char(
        string="Usuario en el checador", groups="hr.group_hr_user",
        help="El número con el que el empleado está dado de alta en los relojes ZKTeco. Si está vacío, se busca "
             "por la Referencia de empleado con el prefijo del equipo (S-123 → usuario 123).")

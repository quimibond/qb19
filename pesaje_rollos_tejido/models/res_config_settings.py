# -*- coding: utf-8 -*-
from odoo import models, fields, api

class ResConfigSettings(models.TransientModel):
    _inherit = 'res.config.settings'

    # Este campo guardará de manera global si la planta está en modo mantenimiento de básculas
    group_mrp_scale_maintenance = fields.Boolean(
        string="Habilitar Pesaje Manual",
        implied_group='pesaje_rollos_tejido.group_mrp_scale_manual_mode',
        help="Al marcar esta casilla, se permitirá a todos los operadores capturar el peso por teclado de forma temporal."
    )

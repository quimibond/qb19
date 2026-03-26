# -*- coding: utf-8 -*-
from odoo import models, fields

class ResConfigSettings(models.TransientModel):
    _inherit = 'res.config.settings'

    qc_sample_percentage = fields.Float(
        string="Muestreo de Calidad (%)",
        config_parameter='pesaje_rollos_tejido.qc_sample_percentage',
        default=20.0
    )
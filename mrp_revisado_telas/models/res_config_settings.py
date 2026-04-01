# -*- coding: utf-8 -*-
from odoo import models, fields

class ResConfigSettings(models.TransientModel):
    _inherit = 'res.config.settings'

    mrp_revision_percentage = fields.Float(
        string="Porcentaje de Revisión Textil",
        config_parameter='mrp_revisado_telas.revision_percentage',
        default=0.1
    )
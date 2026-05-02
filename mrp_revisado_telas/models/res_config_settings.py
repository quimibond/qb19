# -*- coding: utf-8 -*-
from odoo import models

class ResConfigSettings(models.TransientModel):
    _inherit = 'res.config.settings'
    # Se eliminó el campo mrp_revision_percentage
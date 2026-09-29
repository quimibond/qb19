# -*- coding: utf-8 -*-
from odoo import fields, models


class ResConfigSettings(models.TransientModel):
    _inherit = 'res.config.settings'

    # Hasta quimibond_sgi 57.9.0 lo declaraba el núcleo (A-020); la clave del
    # parámetro no cambia.
    sgi_pesaje_tolerance_kg = fields.Float(
        string="Tolerancia de peso de rollo (kg)",
        config_parameter='quimibond_sgi.pesaje_tolerance_kg',
        help="Rollo confirmado fuera de esta tolerancia → alerta de calidad automática.")

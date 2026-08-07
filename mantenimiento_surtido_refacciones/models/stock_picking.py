# -*- coding: utf-8 -*-
from odoo import fields, models


class StockPicking(models.Model):
    """Regla de Negocio 4: enlaza el traslado interno generado con la
    solicitud de mantenimiento que lo originó, para trazabilidad y
    para el botón inteligente en la solicitud.
    """
    _inherit = 'stock.picking'

    maintenance_request_id = fields.Many2one(
        'maintenance.request',
        string='Solicitud de Mantenimiento',
        copy=False,
        index=True,
        help='Solicitud de mantenimiento que originó este traslado de refacciones.',
    )

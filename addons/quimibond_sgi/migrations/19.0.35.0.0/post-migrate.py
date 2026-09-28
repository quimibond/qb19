# -*- coding: utf-8 -*-
"""Liga los traslados internos de 2026 con la entrega de su pedido
(sgi_delivery_picking_id), para que C2.21 y C2.26 tengan historia."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    pickings = env['stock.picking'].search([
        ('picking_type_code', '=', 'internal'), ('sale_id', '!=', False),
        ('create_date', '>=', '2026-01-01')])
    env.add_to_compute(pickings._fields['sgi_delivery_picking_id'], pickings)
    pickings.flush_recordset(['sgi_delivery_picking_id'])
    _logger.info("SGI: %d traslados internos ligados a su entrega (%d con entrega).",
                 len(pickings), len(pickings.filtered('sgi_delivery_picking_id')))

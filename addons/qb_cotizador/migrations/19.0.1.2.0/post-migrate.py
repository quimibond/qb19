# -*- coding: utf-8 -*-
"""19.0.1.2.0 — Jose 2026-10-08, 5.6: las cotizaciones que ya tenían precio en
tarifa reciben `tarifa_fecha` (fecha de alta del renglón de la tarifa) y
`tarifa_user_id` (quien lo dio de alta) para que C1.17 las cuente. No crea ni
cambia precios. Idempotente. Prefijo en el log: «qb_cotizador 1.2.0»."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    cots = env['qb.cotizador.cotizacion'].sudo().with_context(active_test=False).search(
        [('pricelist_item_id', '!=', False), ('tarifa_fecha', '=', False)])
    for cot in cots:
        item = cot.pricelist_item_id
        cot.write({'tarifa_fecha': item.create_date or cot.ganada_date or cot.write_date,
                   'tarifa_user_id': item.create_uid.id or False})
    _logger.info("qb_cotizador 1.2.0: tarifa_fecha rellenada en %d cotización(es).", len(cots))

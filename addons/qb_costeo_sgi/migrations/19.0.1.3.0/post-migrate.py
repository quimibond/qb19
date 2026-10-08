# -*- coding: utf-8 -*-
"""19.0.1.3.0 — Jose 2026-10-08, 5.6: C1.17 se mide con el precio de la cotización
ganada en la tarifa del cliente (`_qb_sgi_apuntar_entregables` re-apunta el
entregable C1-ARTICULO a `qb.cotizador.cotizacion`). No toca cotizaciones ni
tarifas. Idempotente. Prefijo en el log: «qb_costeo_sgi 1.3.0»."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    env['qb.cotizador.cotizacion']._qb_sgi_apuntar_entregables()
    acts = env['sgi.process.activity'].sudo().search([('output_deliverable_ids.code', '=', 'C1-ARTICULO')])
    _logger.info("qb_costeo_sgi 1.3.0: %s.", "; ".join(
        "%s → %s / %s" % (a.number, a.measure_method, a.measure_model_id.model) for a in acts) or 'sin fichas')

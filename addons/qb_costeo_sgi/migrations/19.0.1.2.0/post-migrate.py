# -*- coding: utf-8 -*-
"""19.0.1.2.0 — Jose 2026-10-08, punto 3: C1.06 se mide con la cotización nueva
presentada (`_qb_sgi_apuntar_entregables` pasa al entregable C1-COTIZACION la
actividad que aún se medía con `qb.cotizacion`) y C1-COSTO / C1-COTIZACION
dicen cuándo están completos. No toca datos de origen de las cotizaciones.
Idempotente. Prefijo en el log: «qb_costeo_sgi 1.2.0»."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    env['qb.cotizador.cotizacion']._qb_sgi_apuntar_entregables()
    acts = env['sgi.process.activity'].sudo().search(
        [('output_deliverable_ids.code', 'in', ('C1-COSTO', 'C1-COTIZACION'))])
    _logger.info("qb_costeo_sgi 1.2.0: %s.", "; ".join(
        "%s → %s / %s" % (a.number, a.measure_method, a.measure_model_id.model) for a in acts) or 'sin fichas')

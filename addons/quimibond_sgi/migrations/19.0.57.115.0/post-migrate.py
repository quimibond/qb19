# -*- coding: utf-8 -*-
"""57.115.0: «Pantalla que no va con su medición» reconoce la pantalla que
produce la evidencia (la NC y sus acciones, la revisión y sus acuerdos, el
pedido y sus facturas, la conciliación y las líneas del banco, el inventario
físico y sus movimientos…). Recalcula los faltantes de las actividades que
tenían el aviso.

Esperado en producción (MCP, 2026-10-06): 25 avisos antes; quedan 4 que
esperan decisión (C1.06, C1.11, C4.13, S4.25).
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    Gap = env['sgi.activity.spec.gap']
    before = Gap.search([('code', '=', 'menu_model_mismatch')])
    before.activity_id._sgi_refresh_spec_gaps()
    left = Gap.search([('code', '=', 'menu_model_mismatch')])
    _logger.info("SGI 57.115.0: «Pantalla que no va con su medición» de %d a %d (%s).",
                 len(before), len(left),
                 ", ".join(sorted(left.activity_id.mapped('display_name'))) or "ninguna")

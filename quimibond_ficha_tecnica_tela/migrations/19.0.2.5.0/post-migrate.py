# -*- coding: utf-8 -*-
"""19.0.2.5.0 — Avisos de máquina en producción.

Dirección General 2026-10-13: las órdenes de tintorería muestran las cargas
por jet. Este paso marca «Calcular cargas por jet» en los tipos de operación
de fabricación cuyo nombre dice «Tintorería» (en producción, «Toluca:
Tintorería», id 88). Solo prende la marca nueva; no toca órdenes ni otros
datos. Se puede cambiar a mano en el tipo de operación. Idempotente."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    types = env['stock.picking.type'].with_context(active_test=False).search([
        ('code', '=', 'mrp_operation'), ('name', 'ilike', 'tintorer'), ('ft_cargas_jet', '=', False)])
    if types:
        types.write({'ft_cargas_jet': True})
    _logger.info("ficha_tecnica_tela 2.5.0: «Calcular cargas por jet» en %s.", types.mapped('display_name'))

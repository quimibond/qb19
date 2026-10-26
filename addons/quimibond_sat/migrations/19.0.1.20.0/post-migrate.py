# -*- coding: utf-8 -*-
"""Lista 69-B (1.20.0): el estado de cada proveedor sale de su CFDI recibido
más reciente. Al migrar se llena con lo que ya está en sat.cfdi (2026-10-26:
EORZA y PLEURE, definitivos, con 29 CFDI vigentes de 2025 publicados en Odoo)."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    cr.execute("""
        SELECT DISTINCT ON (partner_id) id
          FROM sat_cfdi
         WHERE direction = 'received' AND partner_id IS NOT NULL AND fecha_emision IS NOT NULL
         ORDER BY partner_id, fecha_emision DESC, id DESC
    """)
    cfdis = env['sat.cfdi'].browse([row[0] for row in cr.fetchall()])
    cfdis._update_partner_69b()
    listed = env['res.partner'].search_count([('sat_69b_status', 'in', ('presumed', 'definitive'))])
    _logger.info('quimibond_sat: estado 69-B revisado en %s proveedores, %s en la lista', len(cfdis), listed)

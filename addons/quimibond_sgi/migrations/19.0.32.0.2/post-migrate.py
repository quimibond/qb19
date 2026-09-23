# -*- coding: utf-8 -*-
"""Recalcula el estado del COA de las salidas abiertas que ya tienen
«Requiere COA»: en 19.0.32.0.1 se quedaron en «no aplica» porque solo se
recalculó el requisito y no el estado que depende de él."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    pickings = env['stock.picking'].search([
        ('picking_type_code', '=', 'outgoing'),
        ('state', 'not in', ('done', 'cancel')),
    ])
    for name in ('sgi_requires_coa', 'sgi_coa_status'):
        env.add_to_compute(pickings._fields[name], pickings)
    pickings.flush_recordset(['sgi_requires_coa', 'sgi_coa_status'])
    _logger.info("SGI COA: estado recalculado en %d salidas abiertas; %d pendientes.",
                 len(pickings), len(pickings.filtered(lambda p: p.sgi_coa_status == 'pendiente')))

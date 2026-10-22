# -*- coding: utf-8 -*-
"""19.0.57.188.0 — Revisión de medición trimestral para las actividades ya
confirmadas dos veces seguidas. Las revisiones pendientes que con la regla
nueva no tocaban quedan «No tocaba (trimestral)» y salen de Mis pendientes en
la siguiente corrida."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    Review = env['sgi.measure.review']
    pending = Review.search([('state', '=', 'pendiente')])
    closed = Review.browse()
    for month in set(pending.mapped('month')):
        of_month = pending.filtered(lambda r: r.month == month)
        skip = Review._sgi_quarterly_skip(of_month.activity_id, month)
        closed |= of_month.filtered(lambda r: r.activity_id.id in skip)
    if closed:
        closed.write({'state': 'no_toca'})
        env['sgi.my.pending']._sgi_refresh_for('sgi.measure.review', closed.ids)
    _logger.info("SGI 57.188.0: %d revisión(es) de medición pendiente(s) pasan a «No tocaba (trimestral)».",
                 len(closed))

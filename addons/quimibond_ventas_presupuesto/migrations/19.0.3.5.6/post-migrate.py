# -*- coding: utf-8 -*-
"""3.5.6: la desviación de precio se mide contra el precio presupuestal del renglón; se
recalcula el real y la desviación de todos los renglones de presupuesto."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    lines = env['sgi.sales.budget.line'].search([('kind', '=', 'presupuesto')])
    lines._compute_real()
    _logger.info("3.5.6: desviación contra el precio presupuestal en %d renglón(es).", len(lines))

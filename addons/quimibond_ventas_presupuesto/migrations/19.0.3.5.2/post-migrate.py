# -*- coding: utf-8 -*-
"""3.5.2: el real de los renglones contaba también las dos líneas de costo (cogs) de cada
factura: la cantidad facturada salía al triple y el precio real a un tercio (desviación de
precio «grave» de −70 %). El importe no cambiaba (las líneas de costo se cancelan). Se
recalcula el real, el pedido y la desviación de todos los renglones de presupuesto."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    lines = env['sgi.sales.budget.line'].search([('kind', '=', 'presupuesto')])
    lines._compute_real()
    _logger.info("3.5.2: real recalculado en %d renglón(es) de presupuesto.", len(lines))

# -*- coding: utf-8 -*-
"""1.1.0: horas por producto y centro. Habilita el modelo en MCP y corre el
primer recálculo para que las pantallas no nazcan vacías."""
import logging

from odoo import api, SUPERUSER_ID

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    from odoo.addons.qb_costeo import _habilitar_mcp
    _habilitar_mcp(env)
    for company in env['res.company'].search([]):
        try:
            filas = env['qb.producto.horas'].with_company(company).recalcular()
            _logger.info('qb_costeo 1.1.0: %d filas de horas en %s',
                         len(filas), company.name)
        except Exception:  # noqa: BLE001 - la migración no debe tumbar el build
            _logger.exception('qb_costeo 1.1.0: falló el recálculo de horas en %s',
                              company.name)

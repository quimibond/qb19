# -*- coding: utf-8 -*-
"""1.2.0: materia prima y rendimiento por producto. Habilita los modelos
en MCP, importa los rendimientos capturados del módulo anterior y corre
el primer recálculo."""
import logging

from odoo import api, SUPERUSER_ID

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    from odoo.addons.qb_costeo import _habilitar_mcp
    _habilitar_mcp(env)
    for company in env['res.company'].search([]):
        try:
            n = env['qb.producto.rendimiento'].with_company(
                company).importar_manuales_legados()
            env['qb.producto.rendimiento'].with_company(company).recalcular()
            filas = env['qb.producto.mp'].with_company(company).recalcular()
            _logger.info('qb_costeo 1.2.0: %d rendimientos manuales importados, '
                         '%d filas de MP en %s', n, len(filas), company.name)
        except Exception:  # noqa: BLE001 - la migración no debe tumbar el build
            _logger.exception('qb_costeo 1.2.0: falló el recálculo en %s',
                              company.name)

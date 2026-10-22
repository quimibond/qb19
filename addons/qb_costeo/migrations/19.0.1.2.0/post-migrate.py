# -*- coding: utf-8 -*-
"""1.2.0: materia prima y rendimiento por producto. Habilita los modelos
en MCP y corre el primer recálculo. (La importación de rendimientos de
`qb_capacidad_costeo` se retiró con ese módulo el 2026-10-22.)"""
import logging

from odoo import api, SUPERUSER_ID

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    from odoo.addons.qb_costeo import _habilitar_mcp
    _habilitar_mcp(env)
    for company in env['res.company'].search([]):
        try:
            env['qb.producto.rendimiento'].with_company(company).recalcular()
            filas = env['qb.producto.mp'].with_company(company).recalcular()
            _logger.info('qb_costeo 1.2.0: %d filas de MP en %s', len(filas), company.name)
        except Exception:  # noqa: BLE001 - la migración no debe tumbar el build
            _logger.exception('qb_costeo 1.2.0: falló el recálculo en %s',
                              company.name)

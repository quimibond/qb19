# -*- coding: utf-8 -*-
"""1.3.0: costo por producto y peso. Habilita los modelos en MCP y calcula
los costos de los períodos en borrador que ya tienen tarifas. (La
importación de pesos de `qb_capacidad_costeo` se retiró el 2026-10-22.)"""
import logging

from odoo import api, SUPERUSER_ID

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    from odoo.addons.qb_costeo import _habilitar_mcp
    _habilitar_mcp(env)
    for company in env['res.company'].search([]):
        try:
            env['qb.producto.kg'].with_company(company).recalcular()
            for periodo in env['qb.periodo'].search(
                    [('company_id', '=', company.id), ('state', '=', 'borrador'),
                     ('calculado_el', '!=', False)]):
                periodo.with_company(company)._calcular_costos(refrescar=False)
        except Exception:  # noqa: BLE001 - la migración no debe tumbar el build
            _logger.exception('qb_costeo 1.3.0: falló en %s', company.name)

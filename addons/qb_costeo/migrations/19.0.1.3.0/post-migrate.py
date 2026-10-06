# -*- coding: utf-8 -*-
"""1.3.0: costo por producto y peso. Habilita los modelos en MCP, importa
los pesos medidos del módulo anterior y calcula los costos de los períodos
en borrador que ya tienen tarifas."""
import logging

from odoo import api, SUPERUSER_ID

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    from odoo.addons.qb_costeo import _habilitar_mcp
    _habilitar_mcp(env)
    for company in env['res.company'].search([]):
        try:
            n = env['qb.producto.kg'].with_company(company).importar_legados()
            env['qb.producto.kg'].with_company(company).recalcular()
            _logger.info('qb_costeo 1.3.0: %d pesos importados en %s', n, company.name)
            for periodo in env['qb.periodo'].search(
                    [('company_id', '=', company.id), ('state', '=', 'borrador'),
                     ('calculado_el', '!=', False)]):
                periodo.with_company(company)._calcular_costos(refrescar=False)
        except Exception:  # noqa: BLE001 - la migración no debe tumbar el build
            _logger.exception('qb_costeo 1.3.0: falló en %s', company.name)

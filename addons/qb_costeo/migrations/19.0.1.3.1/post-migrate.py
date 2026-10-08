# -*- coding: utf-8 -*-
"""1.3.1: horas por driver del centro y calidad baja cuando falta un centro
de la ruta. Recalcula horas y los costos de los períodos en borrador."""
import logging

from odoo import api, SUPERUSER_ID

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    for company in env['res.company'].search([]):
        try:
            env['qb.producto.horas'].with_company(company).recalcular()
            for periodo in env['qb.periodo'].search(
                    [('company_id', '=', company.id), ('state', '=', 'borrador'),
                     ('calculado_el', '!=', False)]):
                periodo.with_company(company)._calcular_costos(refrescar=False)
        except Exception:  # noqa: BLE001 - la migración no debe tumbar el build
            _logger.exception('qb_costeo 1.3.1: falló en %s', company.name)

# -*- coding: utf-8 -*-
"""Crudo nuevo reconocido por su código (1.69.4).

Un crudo que nunca se ha tejido (WJ080Q21HNT165, alta del 13-sep-2026) no
tenía órdenes, ruta ni familia que lo marcaran como crudo, y la tela que lo
consume salía con conversión $0. Ahora la etapa H de la nomenclatura lo
reconoce si tiene receta activa, y toma el promedio del centro (estimado).
Se recalculan los períodos abiertos desde el corte; los cerrados no se tocan.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    cortes = env['qb.costeo.centro'].search([
        ('modo_costeo', '=', 'absorcion_odoo'),
        ('fecha_absorcion', '!=', False),
    ]).mapped('fecha_absorcion')
    if not cortes:
        _logger.info('qb_capacidad_costeo 1.69.4: sin centros absorbidos, '
                     'nada que recalcular.')
        return
    corte = min(cortes).replace(day=1)
    periodos = env['qb.costo.factores'].search([
        ('period', '>=', corte), ('state', '!=', 'cerrado'),
    ]).mapped('period')
    for period in sorted(set(periodos)):
        env['qb.costo.producto'].action_recompute_period(period)
    _logger.info('qb_capacidad_costeo 1.69.4: %s períodos abiertos desde %s '
                 'recalculados con la conversión absorbida.',
                 len(set(periodos)), corte)

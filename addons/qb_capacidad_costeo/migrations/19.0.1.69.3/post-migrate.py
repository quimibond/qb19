# -*- coding: utf-8 -*-
"""Receta vigente = la de más cantidad en 90 días (1.69.3).

Para un producto con varias recetas activas, el costo (MP y conversión)
seguía la receta de su ÚLTIMA OP. WJ060Q21JNT165 quedó con la receta vieja
en su última OP de sep-2026 aunque en 90 días la nueva hizo casi el doble.
Ahora manda la receta con más cantidad producida en 90 días; sin órdenes en
la ventana, la de la última OP. Medido sobre las 95 plantillas con varias
recetas: solo cambia WJ060Q21JNT165. Se recalculan los períodos abiertos
desde el corte; los cerrados no se tocan.
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
        _logger.info('qb_capacidad_costeo 1.69.3: sin centros absorbidos, '
                     'nada que recalcular.')
        return
    corte = min(cortes).replace(day=1)
    periodos = env['qb.costo.factores'].search([
        ('period', '>=', corte), ('state', '!=', 'cerrado'),
    ]).mapped('period')
    for period in sorted(set(periodos)):
        env['qb.costo.producto'].action_recompute_period(period)
    _logger.info('qb_capacidad_costeo 1.69.3: %s períodos abiertos desde %s '
                 'recalculados con la conversión absorbida.',
                 len(set(periodos)), corte)

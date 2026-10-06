# -*- coding: utf-8 -*-
"""Tarifa de conversión con doce meses de historia y crudo hermano (1.69.2).

La tarifa de cada crudo sale ahora de las horas REALES de sus órdenes de
doce meses a la tarifa $/h de hoy (sin cronómetros fuera de banda), no solo
de las del mes. Sin historia, la de su crudo hermano (mismo código salvo
color o ancho); sin hermano, el promedio del centro. Las familias de
máquinas ya no entran al costo de un producto existente. Se recalculan los
períodos abiertos desde el corte; los cerrados no se tocan.
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
        _logger.info('qb_capacidad_costeo 1.69.2: sin centros absorbidos, '
                     'nada que recalcular.')
        return
    corte = min(cortes).replace(day=1)
    periodos = env['qb.costo.factores'].search([
        ('period', '>=', corte), ('state', '!=', 'cerrado'),
    ]).mapped('period')
    for period in sorted(set(periodos)):
        env['qb.costo.producto'].action_recompute_period(period)
    _logger.info('qb_capacidad_costeo 1.69.2: %s períodos abiertos desde %s '
                 'recalculados con la conversión absorbida.',
                 len(set(periodos)), corte)

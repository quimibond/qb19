# -*- coding: utf-8 -*-
"""Conversión absorbida en el costo unitario (1.69).

Desde el corte de TEJIDO (1-sep-2026) su mano de obra se capitaliza por
workcenter contra 504.01.0099 y sale del pool fabril, pero el costo unitario
explotaba la receta hasta el hilo y la perdía: una tela de 40 g cargaba la
misma fabricación que una de 135 g, y el resultado del modelo de septiembre
($3.07M) no cargaba los $585,531 de tejido que llegaron a ventas.

La 1.69 agrega la capa `conv_unit` (tarifa del crudo × el crudo que consume
la receta) y carga a los totales del período la conversión que la traza por
lotes encontró en las entregas. Hay que recalcular los períodos ABIERTOS
desde el corte para que la capa aparezca. Los cerrados no se tocan: el motor
se niega a recalcularlos y, como en ellos no había centro absorbido, su
conversión es cero por construcción.
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
        _logger.info('qb_capacidad_costeo 1.69: sin centros absorbidos, '
                     'nada que recalcular.')
        return
    corte = min(cortes).replace(day=1)
    periodos = env['qb.costo.factores'].search([
        ('period', '>=', corte), ('state', '!=', 'cerrado'),
    ]).mapped('period')
    for period in sorted(set(periodos)):
        env['qb.costo.producto'].action_recompute_period(period)
    _logger.info('qb_capacidad_costeo 1.69: %s períodos abiertos desde %s '
                 'recalculados con la conversión absorbida.',
                 len(set(periodos)), corte)

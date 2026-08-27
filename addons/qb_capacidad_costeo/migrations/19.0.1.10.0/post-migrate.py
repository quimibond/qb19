# -*- coding: utf-8 -*-
"""Marcar las cuentas de renta de inmueble y recalcular los factores.

19.0.1.10.0 arregla un bug con dinero real: la renta contractual de los
centros solo llegaba al costo del producto en ENTRETELAS. Tejido,
tintorería y acabado tenían su renta capturada en la configuración y solo
alimentaba la vista de ociosidad — al costo del producto no llegaba nunca,
aunque la cuenta del GL se hubiera excluido (`no_costeo`) justamente para
dejarle el lugar.

Ahora la renta contractual de TODOS los centros fabriles entra al pool. Para
que eso no cuente la renta dos veces, las clasificaciones cuyas cuentas son
renta de inmueble se marcan `es_renta` y el motor las saca del pool. Esta
migración hace ese marcado sobre lo ya clasificado; de ahí en adelante se
mantiene desde Configuración → Clasificación de cuentas.

Los factores ya calculados quedan obsoletos (su pool fabril cambia), así que
se recalculan los períodos que existan.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    marcadas = env['qb.costeo.cuenta.class'].marcar_cuentas_de_renta()
    _logger.info('qb_capacidad_costeo: %s clasificaciones marcadas como renta '
                 'de inmueble', len(marcadas))

    contractual = sum(env['qb.costeo.centro'].search([
        ('nature', 'in', ('fabril_directo', 'fabril_indirecto')),
    ]).mapped('renta_contractual_mxn'))
    if contractual and not marcadas:
        # No es un error: puede que ninguna cuenta de renta estuviera en un
        # bucket fabril. Pero si SÍ lo estaba y no se reconoció por nombre,
        # la renta se contaría doble — que se vea en el log.
        _logger.warning(
            'qb_capacidad_costeo: hay renta contractual capturada (%.2f/mes) '
            'y ninguna cuenta marcada como renta de inmueble. Si alguna '
            'cuenta de renta está clasificada en un bucket fabril, márcala a '
            'mano en Configuración → Clasificación de cuentas para no contar '
            'la renta dos veces.', contractual)

    periodos = env['qb.costo.factores'].search([]).mapped('period')
    for period in sorted(set(periodos)):
        env['qb.costo.producto'].action_recompute_period(period)
    _logger.info('qb_capacidad_costeo: %s períodos recalculados con la renta '
                 'contractual en el pool', len(set(periodos)))

# -*- coding: utf-8 -*-
"""Recalcular con capacidad normal en el denominador (costeo normal, IAS 2).

19.0.1.13.0 deja de dividir el pool fijo entre la producción real del mes y
lo divide entre la capacidad NORMAL del centro. Dividir entre producción le
carga la ociosidad al producto: un mes flojo lo encarece, y el modelo
entonces recomienda subir el precio justo cuando lo que hace falta es vender
más. El README del módulo ya prometía IAS 2 y la vista `qb.ociosidad` ya lo
hacía así — era el motor el que no estaba de acuerdo con las otras dos
mitades.

La capacidad normal se lee de `qb.ociosidad`, que la deriva igual que el
campo promete: `capacidad_normal` capturada, o calendario real × throughput
nominal. Un centro sin capacidad derivable cae a su producción real, así que
el cambio degrada con gracia.

De paso corrige el denominador de la ENERGÍA, que es variable y por lo tanto
va sobre los kilos realmente producidos: con capacidad normal, un mes al 60%
de utilización habría dado una energía por kilo 40% baja.

Se recalculan los períodos existentes.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    periodos = env['qb.costo.factores'].search([]).mapped('period')
    for period in sorted(set(periodos)):
        env['qb.costo.producto'].action_recompute_period(period)

    ultimo = env['qb.costo.factores'].search([], order='period DESC', limit=1)
    if ultimo:
        _logger.info(
            'qb_capacidad_costeo: %s períodos recalculados con capacidad '
            'normal. Utilización kg %.1f%%, m %.1f%%; fabricación no '
            'absorbida %.2f/mes (esa ociosidad va al resultado del período, '
            'no al costo del producto).', len(set(periodos)),
            ultimo.utilizacion_kg_pct, ultimo.utilizacion_m_pct,
            ultimo.fab_ocioso_month)
        if ultimo.utilizacion_kg_pct and ultimo.utilizacion_kg_pct < 25.0:
            _logger.warning(
                'qb_capacidad_costeo: utilización de kg en %.1f%%. Si no es '
                'real, el throughput nominal o los calendarios están '
                'inflando la capacidad normal y el costo unitario saldrá '
                'bajo — revísalos en Centros de costo.',
                ultimo.utilizacion_kg_pct)

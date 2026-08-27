# -*- coding: utf-8 -*-
"""Simetría de la renta de entretelas + medir el camino al costeo por ruta.

19.0.1.16.0 corrige una asimetría que dejó el arreglo de la renta
(19.0.1.10.0): la renta contractual se sumaba al pool solo para los centros
NO-entretela, pero el pool de entretelas —que sí incluye su renta— se restaba
completo del pool de tela. Tela terminaba pagando una renta que nunca se le
sumó, y su factor $/kg salía bajo.

Ahora la renta contractual entra al total de TODOS los centros fabriles y lo
que entretelas se lleva sale después con su pool propio. El overhead extra
capturado a mano para entretelas dejó de restarse de tela: nunca estuvo en la
bolsa común.

Agrega además `fab_pool_con_centro_pct`, que no cambia ningún número: mide qué
parte del gasto fabril tiene centro de costo asignado. Es el prerrequisito
para costear por ruta real, y hasta que suba, la fabricación solo se puede
repartir a nivel planta.

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
            'qb_capacidad_costeo: %s períodos recalculados. Pool fabril con '
            'centro asignado: %.1f%% — mientras siga bajo, la fabricación se '
            'reparte a nivel planta y no por la ruta real del producto.',
            len(set(periodos)), ultimo.fab_pool_con_centro_pct)

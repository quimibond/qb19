# -*- coding: utf-8 -*-
"""Marcar los períodos cuyo costo unitario no compara con uno normal.

La fabricación se divide entre capacidad normal, así que no se mueve con la
producción. La ENERGÍA sí: es variable y se divide entre los kilos REALES,
que es lo correcto físicamente. Pero cuando los kilos de la ventana quedan
muy por debajo de la capacidad, su $/kg se infla en esa misma proporción y el
producto sale caro por una razón que no es su costo.

Se vio crudo al costear 2024: enero dio energía a $34.22/kg y diciembre a
$11.09/kg —3.1×— porque la ventana de los primeros meses cae en 2023, cuando
la producción todavía no se registraba en Odoo: 372 órdenes en todo 2023
contra 4,715 en 2024. El margen de esos meses salía negativo por eso, y las
filas se veían tan autoritativas como las de 2025.

Da igual si es subregistro o paro real: en los dos casos el unitario no
compara contra un mes normal, y quien lea el reporte tiene que saberlo sin ir
a investigar. Ahora el período lo dice, con su porcentaje y su factor de
inflación.

Se recalculan los períodos abiertos para poblar el marcado. Los cerrados se
respetan — un período cerrado conserva lo que se congeló, incluido el hecho
de que no traiga esta marca.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    periodos = env['qb.costo.factores'].search([]).mapped('period')
    for period in sorted(set(periodos)):
        env['qb.costo.producto'].action_recompute_period(period)

    marcados = env['qb.costo.factores'].search(
        [('confiabilidad', '!=', 'ok')], order='period')
    _logger.info(
        'qb_capacidad_costeo: %s períodos recalculados; %s marcados como no '
        'comparables: %s', len(set(periodos)), len(marcados),
        ', '.join('%s (%s, %.1f%%)' % (m.period, m.confiabilidad,
                                       m.utilizacion_pond_pct)
                  for m in marcados) or 'ninguno')

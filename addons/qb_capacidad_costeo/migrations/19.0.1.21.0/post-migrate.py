# -*- coding: utf-8 -*-
"""Los totales de margen se derivan del ingreso, no de `precio × qty`.

Una fila cuya cantidad NETA del período es ≤ 0 —las devoluciones superaron a
las ventas— se trata como «sin ventas»: sin eso el precio promedio saldría
negativo y envenenaría los márgenes unitarios y las alertas. Pero
`ventas_total` sí conserva el ingreso negativo, porque es el hecho contable.

El resultado era una fila con ventas de −$242,363 y margen de $0, y con ella
la identidad `ventas − costo = margen` dejaba de cumplirse. La conciliación
contra el mayor lo veía como gasto sin explicar: 11 filas metieron $561,866
de residuo entre enero y julio de 2026 que no correspondía a ninguna causa
real.

Derivar el total del ingreso (`revenue − costo × qty_efectiva`) es
algebraicamente idéntico en toda fila con precio válido —revenue es
precio × qty— y arregla justo las filas de devolución neta. El guard del
precio se conserva: esas filas siguen sin precio ni margen unitario.

Se recalculan los períodos abiertos. Los cerrados se respetan.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    periodos = env['qb.costo.factores'].search([]).mapped('period')
    for period in sorted(set(periodos)):
        env['qb.costo.producto'].action_recompute_period(period)
    _logger.info(
        'qb_capacidad_costeo: %s períodos recalculados; los totales de '
        'margen ahora se derivan del ingreso.', len(set(periodos)))

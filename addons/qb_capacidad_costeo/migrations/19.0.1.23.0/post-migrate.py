# -*- coding: utf-8 -*-
"""Ver años anteriores: asistente de rango y póliza de cierre fuera de los pools.

**1. Asistente de rango.** El motor siempre supo costear cualquier período
—`action_recompute_period` recibe una fecha—, pero desde la UI solo se podía
pedir el mes anterior o el año EN CURSO: el menú llamaba
`action_recompute_year()` sin argumento. Para ver 2025 había que entrar al
shell, así que en la práctica no se veía. El asistente nuevo expone el rango,
respeta los períodos cerrados y dice cuántos saltó.

**2. La póliza de CIERRE ANUAL sale de los pools y de la conciliación.**
Reversa las cuentas de resultados del año ENTERO contra un solo asiento de
diciembre —en producción `Dr/2025/12/32`, «POLIZA DE CIERRE ANUAL»,
$190,684,760—. Dejarla dentro hacía dos daños:

  · la conciliación de diciembre salía sin sentido: −$163M de «ventas» y
    −$147M de «gasto», con una diferencia de ventas de $174,966,216 que es,
    exactamente, el total facturado del año;
  · y el promedio de cada pool perdía diciembre ENTERO, porque `_smooth`
    descarta los meses negativos. O sea: cada año que se quisiera ver perdía
    un mes real de los doce.

Se filtra por la referencia del asiento, sin `%` en la expresión SQL (las
vistas pasan por formateo estilo printf).

Se recalculan los períodos abiertos, que ahora incluyen diciembre de verdad.
Los cerrados se respetan.
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
        'qb_capacidad_costeo: %s períodos recalculados sin la póliza de '
        'cierre anual dentro de los pools.', len(set(periodos)))

# -*- coding: utf-8 -*-
"""La conciliación mira todas las cuentas de resultados, no solo unos tipos.

Filtraba por TIPO de cuenta —`income`, `expense`, `expense_direct_cost`,
`expense_depreciation`— mientras el motor filtra por BUCKET. Esa asimetría
dejaba fuera del mayor gasto que el modelo SÍ le cobraba al producto, así que
la brecha —el único número que dice si el costeo sirve como piso de precio—
se leía mal.

Lo que faltaba, todo en cuentas `income_other`, medido 2025-2026:

  701.11.0001 ARRENDAMIENTO FINANCIERO     +13,907,465  <- el modelo SÍ lo cobra
  701.01.0001 PERDIDA CAMBIARIA             +6,929,162
  701.04 / 701.10 intereses y comisiones    +1,309,819
  704.23.0003 UTILIDAD EN VENTA ACTIVO      -6,545,551
  702.01.0001 UTILIDAD CAMBIARIA            -4,527,270
  704.23.0001 OTROS INGRESOS                -2,928,358
                                            -----------
                                     neto   +8,065,226 de gasto invisible

El arrendamiento es el caso que importa: son las máquinas con las que se
produce —una venta con arrendamiento en reversa—, tiene bucket fabril y el
modelo lo cobra bien. Ahora el mayor también lo cuenta, en `gl_otros_costeo`,
y entra a `gl_gasto_total`.

El resto es resultado integral de financiamiento. No es costo de producto ni
debe serlo, pero sí es resultado de la empresa: va en `gl_resultado_integral`
y entra a `resultado_gl`, que ahora sí es el resultado de la empresa.

La vista es SQL en vivo (`_auto = False`): no hay nada que recalcular, se lee
distinto en cuanto el módulo se actualiza. Esta migración solo deja el
tamaño del cambio en el log.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    filas = env['qb.costo.conciliacion'].search([])
    _logger.info(
        'qb_capacidad_costeo: conciliación sobre %s períodos. Costeo en otras '
        'cuentas %.2f, resultado integral %.2f.', len(filas),
        sum(filas.mapped('gl_otros_costeo')),
        sum(filas.mapped('gl_resultado_integral')))

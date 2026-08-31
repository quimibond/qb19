# -*- coding: utf-8 -*-
"""Recálculo de 2026 con la capacidad que de verdad quedó capturada (1.52).

La 1.51 escribió la capacidad de Acabado (915,733 → 1,175,313 m/mes) y
recalculó los ocho períodos de 2026 en la misma transacción. Los ocho
salieron con el denominador VIEJO.

`qb.ociosidad` es un `_table_query`: lee `qb_costeo_centro` con SQL
crudo, y el ORM no tiene cómo saber que ese SELECT depende de un write
pendiente sobre los centros. El write seguía en el buffer, la vista leyó
el renglón anterior y el motor costeó con 915,733 mientras la tabla ya
decía otra cosa. Nada falló: los factores se veían normales
(`m_denom_month` 915,733, utilización 98.8%) y solo comparándolos contra
el centro se notaba que el número no correspondía. Es el modo de falla
más caro que tiene el módulo — el que no avisa.

El arreglo de fondo va en `_capacidad_normal_map`, que ahora hace flush
antes de leer la vista, así que cualquier flujo que escriba un centro y
recalcule en la misma transacción queda cubierto. Esta migración repara
lo que la 1.51 ya dejó guardado en las bases donde corrió.

Solo 2026, como la 1.51: 2024 y 2025 se congelan con el visto bueno de
dirección, no con una migración.
"""
import logging
from datetime import date

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    corte = date(2026, 1, 1)
    periodos = sorted(p for p in set(
        env['qb.costo.factores'].search([]).mapped('period')) if p >= corte)
    for period in periodos:
        env['qb.costo.producto'].action_recompute_period(period)

    _logger.info('qb_capacidad_costeo 1.52: %s períodos de 2026 recalculados '
                 'con la capacidad capturada (2024-2025 intactos).',
                 len(periodos))

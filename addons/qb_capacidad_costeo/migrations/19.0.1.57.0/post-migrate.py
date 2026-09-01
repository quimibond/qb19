# -*- coding: utf-8 -*-
"""Refresca el peso copiado en las fichas técnicas (1.57).

`qb.producto.ficha.peso_kg_unidad` es una copia del maestro de pesos que
se llenaba al generar la ficha y ahí se quedaba. La carga de pesos medidos
de báscula del 31-ago no la movió, así que las hojas técnicas quedaron con
la adivinanza del código: el WJ032Q22JNT160 con 0.0512 kg/m (32 g/m² ×
1.60 m) contra 0.059114 medidos — 13% abajo, y esa hoja es la que se le
manda al cliente.

De aquí en adelante lo mantiene al día el propio maestro, que refresca las
fichas al escribirse. Esta migración pone al corriente lo que ya estaba
guardado.

Toca SOLO los dos campos de peso y respeta las fichas `manual`, igual que
el generador: correr `action_generar_fichas` completo reescribiría además
gramaje, ancho, estado y color desde el parser, que es un martillo más
grande que el clavo — y pisaría correcciones hechas a mano en fichas que
no están marcadas como manuales.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    tocadas = env['qb.producto.ficha'].sync_pesos()
    _logger.info('qb_capacidad_costeo 1.57: %s fichas con el peso '
                 'refrescado desde el maestro.', tocadas)

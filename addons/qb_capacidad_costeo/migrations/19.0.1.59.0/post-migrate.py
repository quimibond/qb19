# -*- coding: utf-8 -*-
"""Repone los decimales que la ficha venía redondeando (1.59).

`qb.producto.ficha.peso_kg_unidad` guardaba cuatro decimales mientras el
maestro (`qb.producto.peso.kg_per_unit`) guarda seis, así que la copia
nunca podía ser exacta: un peso de báscula de 0.059114 kg/m se archivaba
como 0.0591. Ampliar la columna no devuelve los dígitos ya perdidos —los
valores guardados siguen redondeados—, así que hay que volver a copiarlos
del maestro.

Es la ÚNICA migración de esta versión que toca datos, y no recalcula
períodos: `sync_pesos` mueve los dos campos de peso de las fichas
derivadas y respeta las `manual`, igual que en la 1.57.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    tocadas = env['qb.producto.ficha'].sync_pesos()
    _logger.info('qb_capacidad_costeo 1.59: %s fichas recopiadas del '
                 'maestro con los seis decimales.', tocadas)

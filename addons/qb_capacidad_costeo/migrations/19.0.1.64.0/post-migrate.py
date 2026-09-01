# -*- coding: utf-8 -*-
"""Capacidad INSTALADA: la velocidad de la HTJ-5 y el conteo de máquinas (1.64).

Dos cosas que el seed no puede hacer solo porque las familias ya existen
en producción y su bloque va con `noupdate="1"`:

1. La HTJ-5 se dio de alta con velocidad cero. Estando parada eso daba
   igual —capacidad normal cero de todos modos—, pero ahora el panel lee
   capacidad INSTALADA y sin velocidad la tina se ve como si no existiera,
   que es justo lo contrario de lo que hay que ver. El 233.47 kg/h no es
   una estimación aparte: los cuatro jets en operación rinden 0.19456 kg/h
   por kg de carga, así que los 1,200 kg de la HTJ-5 salen de la misma
   regla del formato de planta. La ICOMATEX se queda en cero a propósito:
   de ella no hay velocidad de ninguna fuente.

2. `machines_installed` es un campo calculado almacenado nuevo. Odoo lo
   llena al crear la columna, pero el orden en que corre eso respecto de
   los datos no está garantizado, y una familia con instaladas en cero
   dejaría su capacidad instalada en cero sin que nada avise. Se fuerza el
   recálculo, que es barato (quince filas) y determinista.

No recalcula períodos: no toca costos ni factores, solo capacidad.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

# kg/h por kg de carga, medido sobre los cuatro jets en operación.
RENDIMIENTO_POR_KG_DE_CARGA = 0.19456
CARGA_HTJ5_KG = 1200.0


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    Familia = env['qb.costeo.familia']

    htj5 = Familia.with_context(active_test=False).search(
        [('code', '=', 'TIN_HTJ5')])
    for fam in htj5:
        if fam.std_output_per_hour:
            continue
        fam.std_output_per_hour = round(
            RENDIMIENTO_POR_KG_DE_CARGA * CARGA_HTJ5_KG, 2)
        _logger.info('qb_capacidad_costeo 1.64: HTJ-5 a %s kg/h — su '
                     'capacidad instalada ya se puede leer.',
                     fam.std_output_per_hour)

    todas = Familia.with_context(active_test=False).search([])
    todas._compute_machines_installed()
    env.flush_all()
    _logger.info('qb_capacidad_costeo 1.64: %s familias con máquinas '
                 'instaladas contadas (%s en total).',
                 len(todas), sum(todas.mapped('machines_installed')))

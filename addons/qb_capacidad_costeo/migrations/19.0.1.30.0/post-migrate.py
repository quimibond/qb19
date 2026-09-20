# -*- coding: utf-8 -*-
"""Encadena retroactivamente las cotizaciones existentes como revisiones.

Antes de la 1.30.0 cada recotización del mismo producto al mismo cliente
quedaba como registro suelto: nadie sabía cuál era la vigente ni qué precio
se ofreció antes. Desde ahora `create()` encadena solo; esta migración pone
al día lo ya guardado: agrupa por (cliente, producto), numera por fecha de
creación y liga cada una con su anterior. Solo los BORRADORES viejos pasan a
«Reemplazada» — una presentada/ganada/perdida es historia del trato y su
estado lo decide ventas, no una migración.

No hay recálculo de períodos: este cambio es de reporte y trazabilidad, no
toca ningún número de costeo.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    cotis = env['qb.cotizacion'].search(
        [('product_id', '!=', False), ('partner_id', '!=', False)],
        order='create_date, id')
    cadenas = {}
    for c in cotis:
        cadenas.setdefault((c.partner_id.id, c.product_id.id), []).append(c)

    encadenadas = reemplazadas = 0
    for grupo in cadenas.values():
        anterior = None
        for i, c in enumerate(grupo, start=1):
            vals = {}
            if c.revision != i:
                vals['revision'] = i
            if anterior is not None and c.revision_anterior_id != anterior:
                vals['revision_anterior_id'] = anterior.id
            if vals:
                c.write(vals)
                encadenadas += 1
            # Solo el último de la cadena queda vivo; de los anteriores,
            # únicamente los borradores se marcan reemplazados.
            if c is not grupo[-1] and c.state == 'draft':
                c.state = 'superseded'
                reemplazadas += 1
            anterior = c

    _logger.info(
        'qb_capacidad_costeo: historial de revisiones — %s cadenas '
        'cliente+producto, %s cotizaciones encadenadas, %s borradores '
        'viejos marcados como reemplazados.',
        len(cadenas), encadenadas, reemplazadas)

# -*- coding: utf-8 -*-
"""Rellena `capacity_status` en las cotizaciones y tramos ya guardados.

El campo nace ahora, así que sin este paso todo lo anterior quedaría vacío y
el PDF las mostraría como «verificada solo en parte» — incluidas las que sí
se validaron completas. El estado se reconstruye del `capacity_detail`, que
es el registro textual de lo que pasó: una línea por centro de la ruta, y la
frase «no se puede validar» en las que no tenían throughput ni turnos.

Los tramos no guardan detalle propio, pero comparten producto y ruta con su
cotización: los centros sin datos son los mismos, lo único que cambia entre
tramos son las horas requeridas. Por eso heredan el estado del padre salvo
cuando ese tramo concreto ya no cabía.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def _status_de(detail, capacity_ok):
    detail = (detail or '').strip()
    if not detail:
        return False
    if detail.startswith('Sin ruta de fabricación'):
        return 'sin_ruta'
    if detail.startswith('Captura el volumen'):
        return 'sin_volumen'
    if not capacity_ok:
        return 'no_cabe'
    lineas = [ln for ln in detail.splitlines() if ln.strip()]
    sin_datos = [ln for ln in lineas if 'no se puede validar' in ln]
    if not sin_datos:
        return 'ok'
    return 'sin_datos' if len(sin_datos) >= len(lineas) else 'parcial'


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    cotis = env['qb.cotizacion'].search([('capacity_status', '=', False)])
    conteo = {}
    for c in cotis:
        status = _status_de(c.capacity_detail, c.capacity_ok)
        if not status:
            continue
        c.capacity_status = status
        conteo[status] = conteo.get(status, 0) + 1
        for t in c.tramo_ids:
            t.capacity_status = (
                status if t.capacity_ok and status != 'no_cabe' else 'no_cabe')

    _logger.info(
        'qb_capacidad_costeo: capacity_status reconstruido en %s cotizaciones '
        '— %s. Las que afirmaban «cabe» con centros sin medir quedan ahora '
        'como «parcial»/«sin datos», que es lo que de verdad se sabía.',
        sum(conteo.values()),
        ', '.join('%s=%s' % kv for kv in sorted(conteo.items())) or 'ninguna')

# -*- coding: utf-8 -*-
"""Los márgenes de las cotizaciones siguen al precio; la capacidad sin datos
no reprueba.

**Márgenes vivos.** `margen_contribucion`, `margen_bruto_pct`,
`margen_neto_pct` y `semaforo` eran floats sueltos que el cotizador escribía
una vez. Al editar después el precio objetivo sobre la cotización, el margen
se quedaba con el del precio anterior. Pasó tres veces en producción; la
peor, una cotización a $16.00 presumiendo 5.0% de margen cuando a ese precio
el real era 1.5% — el 5.0% correspondía a $16.72, el precio de antes de la
rebaja. Ahora son computados almacenados sobre el precio vigente y el
snapshot de costos. Los costos siguen siendo snapshot a propósito: son la
foto de los factores del día en que se cotizó.

Esta migración fuerza el recálculo sobre las cotizaciones existentes, con lo
que los tres márgenes viejos se corrigen solos.

**Capacidad honesta.** Un centro sin workcenters NI turnos configurados caía
a «0 horas libres» y cualquier volumen reprobaba por él: las 15 cotizaciones
de agosto salieron «no cabe» porque a ACABADO le faltaba 1 hora contra un
cero inventado. Ahora ese caso dice «no se puede validar» y no reprueba. El
`capacity_ok` guardado en cotizaciones viejas es un snapshot: para
refrescarlo en una cotización abierta, botón «Recotizar».
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    cots = env['qb.cotizacion'].search([])
    antes = {c.id: (c.margen_neto_pct, c.semaforo) for c in cots}
    env.add_to_compute(cots._fields['margen_contribucion'], cots)
    env.add_to_compute(cots._fields['margen_contribucion_pct'], cots)
    env.add_to_compute(cots._fields['margen_bruto_pct'], cots)
    env.add_to_compute(cots._fields['margen_neto_pct'], cots)
    env.add_to_compute(cots._fields['semaforo'], cots)
    cots.invalidate_recordset(
        ['margen_contribucion', 'margen_contribucion_pct',
         'margen_bruto_pct', 'margen_neto_pct', 'semaforo'])
    # Leer los campos dispara el cómputo pendiente; flush lo persiste.
    cambiadas = [
        c for c in cots
        if abs((antes[c.id][0] or 0.0) - (c.margen_neto_pct or 0.0)) > 0.05
        or antes[c.id][1] != c.semaforo]
    _logger.info(
        'qb_capacidad_costeo: márgenes recalculados en %s cotizaciones; %s '
        'traían un margen o semáforo viejo: %s', len(cots), len(cambiadas),
        ', '.join('%s (%.1f%% -> %.1f%%)' % (
            c.name, antes[c.id][0] or 0.0, c.margen_neto_pct or 0.0)
            for c in cambiadas) or 'ninguna')
    env['qb.cotizacion'].flush_model()

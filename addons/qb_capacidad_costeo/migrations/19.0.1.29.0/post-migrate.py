# -*- coding: utf-8 -*-
"""El recálculo de la cadena se parte: año corriente síncrono, historia a cron.

La migración más nueva recalcula todos los períodos guardados — regla de la
cadena desde la 19.0.1.20.0. Con 8 períodos eran ~80 segundos de build. Pero
al cargar 2024 y 2025 los períodos son 32, y ese único recálculo pasó a 5-6
minutos que TODO build de migración paga, más los ~24 tests que invocan el
motor completo sobre una copia de producción. Por eso «antes era mucho más
rápido»: antes había una cuarta parte de la historia y dos tercios de los
tests.

El reparto ahora:

  · SÍNCRONO en la migración: los períodos del año corriente — los que se
    usan para cotizar y decidir. Con 8 períodos, ~80 s, como antes.
  · DIFERIDO: los años anteriores quedan en el parámetro
    `recalculo_pendiente` y el cron «Recálculo diferido de históricos» los
    vacía por lotes de 6 cada 10 minutos, y se apaga solo al terminar. La
    historia converge en menos de una hora sin bloquear el despliegue.

El orden dentro del diferido va del más reciente al más viejo, para que lo
que más probablemente se consulte converja primero.
"""
import logging
from datetime import date, datetime, timedelta

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    periodos = sorted(set(
        env['qb.costo.factores'].search([]).mapped('period')))
    corte = date(date.today().year, 1, 1)
    ahora = [p for p in periodos if p >= corte]
    viejos = sorted((p for p in periodos if p < corte), reverse=True)

    for period in ahora:
        env['qb.costo.producto'].action_recompute_period(period)

    Config = env['qb.costeo.factor.config']
    rec = Config.search([('key', '=', 'recalculo_pendiente')], limit=1)
    valor = ','.join(p.isoformat() for p in viejos)
    if rec:
        rec.value_text = valor
    elif viejos:
        Config.create({
            'key': 'recalculo_pendiente', 'value': 0, 'value_text': valor,
            'descripcion': 'Períodos históricos que una migración dejó '
                           'diferidos, en orden de recálculo. Lo vacía el '
                           'cron «Recálculo diferido de históricos» por '
                           'lotes; vacío = nada pendiente. Manejado por el '
                           'sistema.'})
    if viejos:
        cron = env.ref('qb_capacidad_costeo.cron_recalculo_pendientes',
                       raise_if_not_found=False)
        if cron:
            cron.write({'active': True,
                        'nextcall': datetime.now() + timedelta(minutes=5)})

    ultimo = env['qb.costo.factores'].search([], order='period DESC', limit=1)
    _logger.info(
        'qb_capacidad_costeo: %s períodos del año corriente recalculados en '
        'el build; %s históricos diferidos al cron. Pool fabril %.2f/mes, '
        'ajuste de MP %.4f.', len(ahora), len(viejos),
        ultimo.fab_pool_month if ultimo else 0.0,
        ultimo.mp_ajuste if ultimo else 0.0)

    marcados = env['qb.costo.factores'].search(
        [('confiabilidad', '!=', 'ok')], order='period')
    _logger.info(
        'qb_capacidad_costeo: %s períodos marcados como no comparables: %s',
        len(marcados),
        ', '.join('%s (%s, %.1f%%)' % (m.period, m.confiabilidad,
                                       m.utilizacion_pond_pct)
                  for m in marcados) or 'ninguno')

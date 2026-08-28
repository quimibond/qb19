# -*- coding: utf-8 -*-
"""Recalcula los períodos tras quitar el AVCO como MP de fabricados ambiguos.

El motor usaba el AVCO de Odoo como costo MP de los semiterminados con
receta ambigua (>1 BOM). El AVCO de un fabricado trae las capas de
conversión de las órdenes de producción (horas × tarifa de workcenter),
no solo materiales, y el modelo ya cobra la conversión vía fab_unit: se
cobraba dos veces. El caso medido: la cruda de WC090 con AVCO $107/kg
cuando el hilo cuesta ~$40/kg — CONTITECH cargaba ~$3M/año de costo
fantasma y el segmento industrial completo salía con margen neto rojo.
Ahora la receta ambigua explota TODAS las BOMs y toma la más cara.

Todos los mp_unit/costo_variable guardados con el criterio viejo están
inflados en los productos afectados, así que se recalcula la historia —
con el reparto de la 1.29.0 (regla de la cadena: SOLO la migración más
nueva recalcula): el año corriente síncrono en el build y los años
anteriores diferidos al cron «Recálculo diferido de históricos».
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
        'qb_capacidad_costeo: fin del AVCO como MP de fabricados ambiguos — '
        '%s períodos del año corriente recalculados en el build, %s '
        'históricos diferidos al cron. Pool fabril %.2f/mes, ajuste de MP '
        '%.4f.', len(ahora), len(viejos),
        ultimo.fab_pool_month if ultimo else 0.0,
        ultimo.mp_ajuste if ultimo else 0.0)

# -*- coding: utf-8 -*-
"""Recalcula tras el barrido de limpieza del motor (1.37-1.41).

Tres cambios del motor tocan costos guardados:

· Reventa (bucket servicio, 1.37): fibras e hilos que se compran y se
  revenden (PES1.4NG1.5) cargaban energía y ajuste de merma de una
  fabricación que no existe.
· Receta ambigua con historial (1.38): el costo sigue a la BOM de la
  última OP terminada. Los genéricos de prueba («MUESTRA PILOTO», con 29
  BOMs de relleno) cuelgan de BOMs activas de productos reales y su
  explosión «más cara» inflaba la MP — TJ085Q22JNT157 salía a 11.30/m
  cuando su receta real (53 de sus 55 OPs) da ~6.2/m.
· AVCO negativo acotado a 0 (1.38): una herida de valuación
  (PESFCHMO1.5X2.0 en -0.30/kg) producía MP negativa en los velos P17/P18.
· MP al precio de la ÉPOCA (1.39): cada período usa la última compra
  conocida a su corte, no la de hoy — recalcular marzo con el hilo de
  agosto pintaba márgenes que nunca existieron. El cotizador sigue a
  reposición.
· Inspección de importados (1.41): todo lo importado pasa por una OP
  TL/CONV y la gente que la trabaja cobraba por el pool fabril que solo
  absorben los fabricados — las telas pagaban la inspección de la
  reventa. Ahora los ' I' cargan inspección por metro y esa parte se
  resta del pool.
· (config en producción, sin código): clasificación luz/servicios
  administrativos intercambiada y prorrateo de aduana encendido
  (importacion_driver=compras, factor ~16% sobre valor importado).

Reparto de la 1.29.0 (regla de la cadena: SOLO la migración más nueva
recalcula): el año corriente síncrono en el build y los años anteriores
diferidos al cron «Recálculo diferido de históricos».
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

    _logger.info(
        'qb_capacidad_costeo 1.41: limpieza del motor — %s períodos '
        'del año corriente recalculados en el build, %s históricos '
        'diferidos al cron.', len(ahora), len(viejos))

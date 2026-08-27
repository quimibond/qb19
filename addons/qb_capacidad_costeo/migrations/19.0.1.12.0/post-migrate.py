# -*- coding: utf-8 -*-
"""Conciliar la MP de receta contra la materia prima realmente consumida.

19.0.1.12.0 pone a trabajar el bucket `mp`, que estaba declarado y ningún
cálculo consultaba. La MP del motor es la receta explotada al último precio
de compra: un costo de reposición teórico, sin merma, sin rendimiento real y
sin la variación entre ese último precio y lo que de verdad se pagó. La
contabilidad sí sabe cuánta materia prima se consumió — es el costo primo más
los ajustes de inventario. El cociente entre las dos es el factor de ajuste.

Esta migración mueve al bucket `mp` las cuentas de costo primo que hoy están
en `no_costeo`. Es seguro por construcción y por partida doble: esas cuentas
aportan cero a cualquier pool hoy, y el bucket `mp` tampoco se suma a ningún
pool — es únicamente el número contra el que se concilia. Lo único que hace
es destapar el ajuste.

Los períodos existentes se recalculan para que el ajuste quede aplicado.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    movidas = env['qb.costeo.cuenta.class'].reclasificar_cuentas_de_materia_prima()
    if not movidas:
        _logger.warning(
            'qb_capacidad_costeo: ninguna cuenta de costo primo reconocida. '
            'La MP de receta seguirá sin conciliarse contra el mayor — '
            'clasifica a mano las cuentas de consumo de materia prima en el '
            'bucket «Materia prima».')

    # El recálculo de los períodos NO va aquí, a propósito. Las migraciones
    # corren en orden de versión y cada una pisa el resultado de la anterior,
    # así que recalcular en las trece dejaba ~130,600 recálculos de producto
    # (13 migraciones x 8 períodos x ~1,256 productos) para un build donde
    # bastan los ~10,000 de la última pasada. Las doce primeras se tiraban a
    # la basura y el build de Odoo.sh las pagaba enteras.
    #
    # Lo hace la migración MÁS NUEVA, una sola vez, con todos los cambios de
    # datos de esta cadena ya aplicados. Si alguna vez se despliega una
    # versión intermedia suelta —hoy no pasa: el módulo se instala en la
    # versión del manifest— queda el botón «Recalcular» del Panel.

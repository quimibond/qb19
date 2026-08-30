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
def migrate(cr, version):
    # Regla de la cadena: SOLO la migración más nueva recalcula. El
    # recálculo (año corriente síncrono + históricos al cron) vive hoy en
    # la 19.0.1.39.0; recalcular también aquí pagaría el mismo build dos
    # veces para tirar la primera.
    pass

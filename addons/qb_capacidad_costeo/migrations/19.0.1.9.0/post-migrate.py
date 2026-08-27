# -*- coding: utf-8 -*-
"""Rellenar los totales del período en qb_costo_producto.

19.0.1.9.0 agrega el dinero REAL del mes al reporte de costo por producto:
lo vendido en pesos (``ventas_total``), el costo de lo vendido por capa
(``mp_total`` … ``costo_absorbido_total``) y el precio en la divisa original
(``divisa_id``, ``precio_prom_divisa``, ``ventas_total_divisa``, ``tc_prom``).

Los totales son aritmética exacta sobre columnas que ya existen
(unitario × qty vendida, con la misma compuerta de precio válido que usa el
motor), así que se rellenan aquí y el histórico queda utilizable de
inmediato — sin esperar un recálculo de meses cerrados.

Los campos de divisa NO se pueden derivar de lo ya guardado: salen de
``amount_currency`` de las facturas. Se pueblan solos en el siguiente
recálculo (el cron del mes en curso, o «Recalcular costeo (año en curso)»
para el histórico). Mientras tanto quedan vacíos, no equivocados.
"""
import logging

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    cr.execute("""
        UPDATE qb_costo_producto SET
            ventas_total            = precio_prom * qty_vendida,
            mp_total                = mp_unit * qty_vendida,
            energia_total           = energia_unit * qty_vendida,
            fab_total               = fab_unit * qty_vendida,
            op_total                = op_unit * qty_vendida,
            costo_variable_total    = costo_variable * qty_vendida,
            costo_produccion_total  = costo_produccion * qty_vendida,
            costo_absorbido_total   = costo_absorbido * qty_vendida
        WHERE COALESCE(precio_prom, 0) > 0
    """)
    _logger.info('qb_capacidad_costeo: totales del período rellenados en %s '
                 'filas de costo por producto', cr.rowcount)
    # Filas sin precio válido (sin venta, o devoluciones > ventas): los
    # totales quedan en 0, igual que los deja el motor.
    cr.execute("""
        UPDATE qb_costo_producto SET
            ventas_total = 0, mp_total = 0, energia_total = 0,
            fab_total = 0, op_total = 0, costo_variable_total = 0,
            costo_produccion_total = 0, costo_absorbido_total = 0
        WHERE COALESCE(precio_prom, 0) <= 0
    """)

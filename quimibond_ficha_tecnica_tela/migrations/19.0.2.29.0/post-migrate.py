# -*- coding: utf-8 -*-
"""2.29.0: tolerancia hacia arriba y hacia abajo en todas las medidas de tela de las fichas.

Antes solo el peso tenía «+ / −» y un «−» en 0 significaba «igual que el +». Ahora cada lado vale lo que dice (cabe
+0 / −5 y +5 / −0), así que lo que ya estaba capturado se pasa tal cual: el «−» toma el valor del «+» donde estaba en
0 (peso) o donde la columna es nueva (ancho, espesor, encogimientos y elongaciones). Ninguna ficha cambia de rango."""
import logging

_logger = logging.getLogger(__name__)

PARES = {
    'ficha_tecnica_acabado': ('peso_acabado_tol', 'ancho_acabado_tol', 'encogimiento_largo_tol',
                              'encogimiento_ancho_tol', 'espesor_acabado_tol', 'elongacion_largo_acabado_tol',
                              'elongacion_ancho_acabado_tol'),
    'ficha_tecnica_tejido': ('peso_acondicionado_tol', 'ancho_acondicionado_tol', 'espesor_acondicionado_tol',
                             'elongacion_carga_largo_tol', 'elongacion_carga_ancho_tol'),
}


def migrate(cr, version):
    if not version:
        return
    for table, columnas in PARES.items():
        for col in columnas:
            cr.execute(f'UPDATE {table} SET {col}_minus = {col} '
                       f'WHERE COALESCE({col}_minus, 0) = 0 AND COALESCE({col}, 0) <> 0')
            _logger.info('fichas 2.29.0: %s.%s_minus = %s en %s fichas', table, col, col, cr.rowcount)

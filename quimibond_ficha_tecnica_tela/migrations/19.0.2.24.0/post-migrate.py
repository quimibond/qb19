# -*- coding: utf-8 -*-
"""19.0.2.24.0 — La lista en metros ya no lleva operación.

Dirección General 2026-11-03: Inspección (en la orden de acabado en kg) corta, pesa, clasifica y etiqueta cada rollo,
y la orden en metros se registra sola al cerrar el baño. Se quitan de las rutas por familia los pasos «Clasificación
final» (Empaque); al volver a generar las listas, la lista en metros queda sin la operación de Empaque. No toca
listas activas ni órdenes. Idempotente."""
import logging

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    cr.execute("DELETE FROM ficha_tecnica_ruta_paso WHERE paso = 'clasificacion'")
    _logger.info("ficha_tecnica_tela 2.24.0: %s pasos «Clasificación final» quitados de las rutas.", cr.rowcount)

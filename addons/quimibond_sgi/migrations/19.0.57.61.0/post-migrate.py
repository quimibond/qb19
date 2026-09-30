# -*- coding: utf-8 -*-
"""57.61.0 (bloque 2 de formularios 2/6, decisión 2026-09-30 «Inventario de
formularios»): cada orden de producción, vale y transferencia de C4 y C6
imprime su propia clave (``sgi.format.map._sgi_seed_operation_maps``, tabla
``SGI_OPERATION_FORMAT_MAPS`` en ``models/sgi_format_map_seed.py``).

Formato por clave vigente, tipos de operación por nombre exacto en la empresa
del SGI. Nunca pisa un mapeo que ya exista con ese documento o esa clave.
Idempotente. Nada se borra.

Esperado en producción (MCP, solo lectura, 2026-09-30), 7 mapeos nuevos:
F-P-P02-01 (doc 3999; Carda 80, V10- 89, V18 90), F-IT-P-P01-02-02 (3993;
Cocina Entretelas 82), F-IT-P-P01-12-01 (5057; Tintorería 88) en
``mrp.production``; F-IT-P-A07-01-02 (3701; Reetiquetado 209),
F-IT-P-A07-01-01 (3700; Salida de consumibles 242, Salida Refacciones a
Gasto 264), F-IT-P-A05-01-06 (3699; destino proveedor) y F-P-A07-04 (3708;
origen cliente) en ``stock.picking``. Los generales de producción
(F-IT-P-P01-08-01, mapeo 4) y de salidas (F-P-A16-01, mapeo 3) no cambian.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    created = env['sgi.format.map']._sgi_seed_operation_maps()
    _logger.info("SGI 57.61.0: mapeos de formato por operación creados: %s (esperado: 7).",
                 ", ".join(created) or "ninguno nuevo")

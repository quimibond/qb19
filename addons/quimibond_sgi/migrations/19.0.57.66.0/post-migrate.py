# -*- coding: utf-8 -*-
"""57.66.0: vuelve a sembrar los mapeos de formato por tipo de operación de
C4 y C6 (``sgi.format.map._sgi_seed_operation_maps``, 57.61.0).

En el build de staging (copia de producción) la 57.61.0 corrió sin idioma y
buscó los tipos de operación por su nombre en inglés, contando también los
archivados: saltó «Carda» y «V10-» (0), «Cocina Entretelas» (2),
«Tintorería» (3) y «Salida de consumibles» (0). Desde 57.66.0 el nombre se
busca en es_MX primero y solo entre los activos. Las bases que ya pasaron por
57.61.0 reciben aquí los mapeos que faltaron; los que existen se respetan
(mismo documento o misma clave, activos o archivados). Idempotente. Nada se
borra.

En producción (es_MX, compañía 1, MCP solo lectura, 2026-09-30) cada nombre
existe una sola vez y activo: Carda 80, V10- 89, Cocina Entretelas 82,
Tintorería 88, Salida de consumibles 242.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    created = env['sgi.format.map']._sgi_seed_operation_maps()
    _logger.info("SGI 57.66.0: mapeos de formato por operación que faltaban: %s.",
                 ", ".join(created) or "ninguno")

# -*- coding: utf-8 -*-
"""19.0.57.142.0 — Dígitos de galga dentro del rango y nombre del proyecto con el código final.

Vuelve a armar el nombre de los desarrollos cuyo nombre arma Odoo (folio o artículo): el 499 decía
«Análisis WJ053Q21JCO160» y su artículo acabado ya es WJ053Q22JCO160 (corregido a mano). Los
dígitos de galga (`sgi_dev_code_galga_digits`) los calcula Odoo al crear la columna. No toca
artículos ni listas. Idempotente. Prefijo en el log: «SGI 57.142.0»."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    projects = env['project.project'].with_context(active_test=False).search([('sgi_is_ft', '=', True)])
    before = {p.id: p.name for p in projects}
    projects._sgi_dev_sync_name()
    changed = [(pid, before[pid], p.name) for pid, p in ((p.id, p) for p in projects) if p.name != before[pid]]
    _logger.info("SGI 57.142.0: %d nombre(s) actualizados: %s.", len(changed),
                 "; ".join("%d: %s → %s" % c for c in changed) or "ninguno")

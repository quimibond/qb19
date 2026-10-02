# -*- coding: utf-8 -*-
"""19.0.57.99.0 — Salud del SGI: liga los diez indicadores nuevos al proceso
E2 de la empresa del SGI y les pone responsable (usuario del dueño de E2 o el
Jefe MAST) si no lo tienen. Las <function> de un XML noupdate solo corren al
instalar; en una base existente lo hace este script. Idempotente; no toca
ningún otro indicador ni cambia datos de negocio."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    changed = env['sgi.indicator']._sgi_health_link_process()
    _logger.info("SGI 57.99.0: %d indicador(es) de salud ligados al proceso E2 (ids %s).",
                 len(changed), changed)

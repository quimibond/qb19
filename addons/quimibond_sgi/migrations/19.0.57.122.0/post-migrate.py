# -*- coding: utf-8 -*-
"""19.0.57.122.0 — Uso diario del proyecto de desarrollo. Solo datos: expone al
MCP el asistente de parecidos (sgi.dev.similar y sus renglones, lectura).
Idempotente; nada más cambia en la base."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    models = env['project.project']._sgi_dev_enable_mcp_models()
    _logger.info("SGI 57.122.0: %d modelo(s) expuestos al MCP: %s.", len(models), ", ".join(models.mapped('model')) or '—')

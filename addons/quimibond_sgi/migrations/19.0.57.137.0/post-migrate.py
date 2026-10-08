# -*- coding: utf-8 -*-
"""19.0.57.137.0 — Aprobación para iniciar, solicitud de modificación, ruta a la vista y expediente
8.3 / APQP (Jessica 2026-10-08).

Solo expone el modelo nuevo (`sgi.dev.change.request`) al MCP. No crea documentos, no toca
proyectos ni revisiones. Idempotente. Prefijo en el log: «SGI 57.137.0»."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    mcp = env['project.project']._sgi_dev_enable_mcp_models()
    _logger.info("SGI 57.137.0: MCP %s.", mcp.mapped('model') or '—')

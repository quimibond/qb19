# -*- coding: utf-8 -*-
"""19.0.57.130.0 — Reporte de conformidad desde la tabla (Jose 2026-10-08, 3.4).

Solo expone al MCP los modelos nuevos (`sgi.dev.coa`, `sgi.dev.coa.line`).
Sin datos que mover. Idempotente. Prefijo en el log: «SGI 57.130.0»."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    mcp = env['project.project']._sgi_dev_enable_mcp_models()
    _logger.info("SGI 57.130.0: MCP %s.", mcp.mapped('model') or '—')

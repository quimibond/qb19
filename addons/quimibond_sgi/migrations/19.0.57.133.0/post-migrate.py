# -*- coding: utf-8 -*-
"""19.0.57.133.0 — Pilotaje y estudio de habilidad (Jose 2026-10-08, 5.4).

Solo expone los modelos nuevos al MCP. Los parámetros nuevos (lecturas por
lote, Cpk mínimo) se quedan vacíos; «Lotes del pilotaje» vacío significa tres.
La marca «Crítica» de las características nace apagada. Idempotente. Prefijo
en el log: «SGI 57.133.0»."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    mcp = env['project.project']._sgi_dev_enable_mcp_models()
    _logger.info("SGI 57.133.0: MCP %s.", mcp.mapped('model') or '—')

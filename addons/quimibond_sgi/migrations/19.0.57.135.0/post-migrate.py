# -*- coding: utf-8 -*-
"""19.0.57.135.0 — Escalamiento por tiempo de los pasos del desarrollo (Jose 2026-10-08, 5.5).

Solo expone el modelo nuevo al MCP. Los tres parámetros (horas al responsable, horas al dueño del
proceso, días hábiles a Dirección de Operaciones) se quedan vacíos: sin ellos el cron no avisa.
Idempotente. Prefijo en el log: «SGI 57.135.0»."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    mcp = env['project.project']._sgi_dev_enable_mcp_models()
    _logger.info("SGI 57.135.0: MCP %s.", mcp.mapped('model') or '—')

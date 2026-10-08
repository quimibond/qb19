# -*- coding: utf-8 -*-
"""19.0.57.137.0 — Aprobación para iniciar, solicitud de modificación, ruta a la vista y expediente
8.3 / APQP (Jessica 2026-10-08).

- El rol «Aprueba» de C1.13 sin botón (Dirección de Operaciones) apunta a «Firmar» de la solicitud
  de modificación (`_sgi_dev_link_c1_approvals`, solo los roles de tipo botón sin método).
- Expone `sgi.dev.change.request` al MCP.
No crea documentos ni toca proyectos, revisiones ni datos de origen. Idempotente. Prefijo en el
log: «SGI 57.137.0»."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    roles = env['sgi.process.activity']._sgi_dev_link_c1_approvals()
    mcp = env['project.project']._sgi_dev_enable_mcp_models()
    _logger.info("SGI 57.137.0: roles de C1 con botón %s; MCP %s.",
                 ["%s → %s" % (r.display_name, r.approval_state) for r in roles] or 'ninguno',
                 mcp.mapped('model') or '—')

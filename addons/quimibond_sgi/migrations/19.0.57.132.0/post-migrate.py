# -*- coding: utf-8 -*-
"""19.0.57.132.0 — Ficha técnica interna y especificaciones del producto (Jose 2026-10-08, 5.3).

- Puestos que firman la ficha interna: los seis del brief por nombre, solo si
  el parámetro está vacío; los que no existan en RH se reportan.
- El rol «Aprueba» de C1.15 sin botón (1928, Administrador de Ventas) apunta
  a «Aprobar ficha» de la ficha técnica interna. La regla nativa la crea el
  satélite quimibond_sgi_studio (1.0.5), que carga después de esta migración.
- Modelos nuevos al MCP.
No toca datos de origen. Idempotente. Prefijo en el log: «SGI 57.132.0»."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    lang = env.company.partner_id.lang or 'en_US'
    env = api.Environment(cr, SUPERUSER_ID, {'lang': lang})
    found, missing = env['sgi.dev.tech.sheet']._sgi_dev_set_sign_jobs_default()
    roles = env['sgi.process.activity']._sgi_dev_link_c1_approvals()
    mcp = env['project.project']._sgi_dev_enable_mcp_models()
    _logger.info("SGI 57.132.0: firman la ficha interna %s; sin puesto en RH %s; roles de C1 con botón %s; MCP %s.",
                 found.mapped('name') or 'ya estaban', missing or 'ninguno',
                 ["%s → %s" % (r.display_name, r.approval_state) for r in roles] or 'ninguno',
                 mcp.mapped('model') or '—')

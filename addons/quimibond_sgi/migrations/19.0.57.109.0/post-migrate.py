# -*- coding: utf-8 -*-
"""57.109.0: asistentes en lenguaje normal.

1. Las actividades con rol «Aprueba» recalculan sus faltantes (nuevo
   ``approval_missing``, «Aprobación sin activar»).
2. El Jefe MAST recibe en Mis pendientes un aviso por proceso con
   aprobaciones sin activar (se cierra solo al activarlas).

Esperado en producción (MCP, solo lectura, 2026-10-05): 82 roles «Aprueba»
en actividades vigentes; 16 activos, 1 activado el 2026-10-05 por MCP
(S6.03, solicitud en Aprobaciones) y unos 64 sin activar (61 sin configurar,
2 por sincronizar, 1 sin personas, 1 con otra regla en el botón).
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    Role = env['sgi.activity.role']
    roles = Role.search([('role', '=', 'aprueba'), ('activity_id.active', '=', True),
                         ('activity_id.process_id.active', '=', True)])
    roles.activity_id._sgi_refresh_spec_gaps()
    pending = Role._sgi_notice_not_active(roles)
    missing = env['sgi.activity.spec.gap'].search_count([('code', '=', 'approval_missing')])
    _logger.info("SGI 57.109.0: %d roles «Aprueba» revisados; %d faltantes «Aprobación sin activar»; "
                 "avisos al Jefe MAST en %d procesos.", len(roles), missing, len(pending))

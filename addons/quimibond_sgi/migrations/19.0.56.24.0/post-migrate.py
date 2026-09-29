# -*- coding: utf-8 -*-
"""56.24.0 (2026-09-29): borra, con respaldo, los roles de actividades
archivadas (223 en producción, todos «ejecuta», de los 15 procesos viejos
P-xxx). Ya no se veían en ningún lado (activity_active) y su liga al puesto
(ondelete restrict) impedía borrar o fusionar los puestos viejos.

Respaldo: un JSON por proceso, adjunto al proceso archivado
(«roles_borrados_56.24.0_<clave>.json»), con actividad, rol, puesto o
familia, condición y días. La actividad archivada conserva su numeral y su
texto. Decisión del CEO, 2026-09-29.
"""
import base64
import json
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    Role = env['sgi.activity.role'].with_context(active_test=False)
    roles = Role.search([('activity_active', '=', False)])
    if not roles:
        _logger.info("SGI 56.24.0: sin roles de actividades archivadas.")
        return
    by_process = {}
    for role in roles:
        by_process.setdefault(role.activity_id.process_id, env['sgi.activity.role'])
        by_process[role.activity_id.process_id] |= role
    Attachment = env['ir.attachment']
    for process, proc_roles in by_process.items():
        rows = [{
            'role_id': r.id,
            'activity_id': r.activity_id.id,
            'activity_number': r.activity_id.number or r.activity_id.legacy_number or '',
            'activity_name': r.activity_id.name or '',
            'role': r.role,
            'target_type': r.target_type or '',
            'job_id': r.job_id.id or None,
            'job': r.job_id.name or '',
            'family_id': r.family_id.id or None,
            'family': r.family_id.display_name or '',
            'relative_role': r.relative_role or '',
            'condition': r.condition or '',
            'after_days': r.after_days or 0,
        } for r in proc_roles]
        code = process.code or str(process.id)
        Attachment.create({
            'name': "roles_borrados_56.24.0_%s.json" % code,
            'res_model': 'sgi.process', 'res_id': process.id,
            'mimetype': 'application/json',
            'datas': base64.b64encode(json.dumps(rows, ensure_ascii=False, indent=1).encode('utf-8')),
        })
    total = len(roles)
    # sgi_roles_via_activity: la actividad está archivada; no se exige que
    # conserve quién la ejecuta.
    roles.with_context(sgi_roles_via_activity=True).unlink()
    _logger.info("SGI 56.24.0: %d rol(es) de actividades archivadas borrados, respaldo en %d proceso(s): %s",
                 total, len(by_process), ", ".join(p.code or str(p.id) for p in by_process))

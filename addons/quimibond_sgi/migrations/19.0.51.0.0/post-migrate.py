# -*- coding: utf-8 -*-
"""51.0.0 (PR 4: DOC-1, DOC-3, DIR-1). DIR-1 hace obligatorio el responsable
de cada requisito legal: los que estaban sin responsable reciben al Jefe MAST
(primer usuario del grupo) y quedan en el log para reasignarlos. Idempotente."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    manager_id = env['sgi.cron']._sgi_manager_user_id() or SUPERUSER_ID
    Requirement = env['sgi.legal.requirement'].sudo().with_context(active_test=False)
    orphans = Requirement.search([('responsible_id', '=', False)])
    for req in orphans:
        _logger.info("SGI 51: requisito legal sin responsable «%s»; se asigna al Jefe MAST (uid %s).",
                     req.display_name, manager_id)
    if orphans:
        orphans.write({'responsible_id': manager_id})
    _logger.info("SGI 51: %d requisito(s) legal(es) con responsable asignado.", len(orphans))

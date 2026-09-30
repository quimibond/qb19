# -*- coding: utf-8 -*-
"""57.19.0 (entrega 5, reclamaciones: D-006, D-010; decisión 9 de la tanda 2).

Marca ``helpdesk.team.sgi_is_complaint`` en el equipo del SGI
(``sgi_helpdesk_team_complaints``, sus datos son ``noupdate``) y en los
equipos de la decisión 9 de la empresa del SGI, por nombre exacto:
«Reclamaciones entretelas» y «ATENCION A CLIENTES»
(``helpdesk.team._sgi_mark_complaint_teams``). Idempotente; nada se borra ni
se mueve de equipo.

Esperado en producción (MCP, solo lectura, 2026-09-30): equipos 30
(«Reclamaciones de clientes», 0 tickets), 2 («Reclamaciones entretelas», 2
tickets) y 14 («ATENCION A CLIENTES», 13 tickets), todos de la empresa 1.
«Reclamación Industrial» (11, 0 tickets) no está en la decisión y no se marca.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    marked = env['helpdesk.team']._sgi_mark_complaint_teams()
    _logger.info("SGI 57.19.0: equipos marcados como reclamación: %s (esperado: 30, 2 y 14).",
                 ", ".join("%s «%s»" % (t.id, t.name) for t in marked) or "ninguno nuevo")

# -*- coding: utf-8 -*-
"""1.0.7 (Dirección General 2026-10-12): las reglas que Sistemas archivó a mano
se quedan archivadas, y las aprobaciones que el núcleo dejó en pausa
(57.144.0: Contabilidad e Inventario) pierden su regla activa.

1. Regla archivada por una persona (no por OdooBot ni por el sistema) con rol
   del SGI y rol sin pausa → el rol queda en pausa con la nota de quién la
   archivó. Es lo que Sistemas ya decidió; aquí solo se respeta.
2. Rol en pausa con regla activa → la regla se archiva (contexto
   ``sgi_approval_sync``: no es una decisión a mano).

Idempotente. No se vuelve a sincronizar nada «por sincronizar» (a diferencia
de 1.0.4 y 1.0.5): eso lo hace el cron con los roles que no están en pausa.
Prefijo en el log: «SGI Studio 1.0.7».
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

SYSTEM_UIDS = (1, 2)   # OdooBot y el administrador base


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    Rule = env['studio.approval.rule'].sudo().with_context(active_test=False)
    paused = []
    for rule in Rule.search([('sgi_role_id', '!=', False), ('active', '=', False)]):
        role = rule.sgi_role_id
        if role.approval_paused or not role.activity_id.active or rule.write_uid.id in SYSTEM_UIDS:
            continue
        who = rule.write_uid.name or 'alguien'
        when = rule.write_date.date().isoformat() if rule.write_date else ''
        role.write({'approval_paused': True,
                    'approval_paused_note': "regla archivada a mano en Studio por %s el %s" % (who, when)})
        paused.append("%s %s" % (role.activity_id.number or '', role.display_name))
    released = []
    for rule in Rule.search([('sgi_role_id', '!=', False), ('active', '=', True)]):
        if rule.sgi_role_id.approval_paused:
            rule.with_context(sgi_approval_sync=True).write({'active': False})
            released.append(rule.name)
    roles = env['sgi.activity.role'].sudo().search([('role', '=', 'aprueba'), ('approval_paused', '=', True)])
    roles.activity_id._sgi_refresh_spec_gaps()
    env.registry.clear_cache()
    _logger.info("SGI Studio 1.0.7: en pausa por archivado a mano: %s; reglas archivadas por pausa: %s",
                 paused or 'ninguna', released or 'ninguna')

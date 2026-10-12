# -*- coding: utf-8 -*-
"""57.144.0 (Dirección General 2026-10-12): las aprobaciones nativas de
**Contabilidad e Inventario** quedan en pausa.

Sistemas las desactivaba a mano en Studio y cada actualización del módulo (y
el cron nocturno) las volvía a activar porque leían «por sincronizar». Ahora
el rol «Aprueba» tiene ``approval_paused``: el SGI archiva la regla o la
categoría y no la vuelve a tocar hasta «Reanudar».

Aquí se marcan en pausa los roles «Aprueba» cuyo documento es de
Contabilidad (``account.*``) o de Inventario (``stock.*``) y se sincronizan:
la categoría propia de Aprobaciones se archiva aquí; la regla de Studio la
archiva ``quimibond_sgi_studio`` 1.0.7 al cargar (este script corre antes de
que el satélite entre al registro). No se cambia ningún rol del
procedimiento: solo la aprobación nativa. Idempotente; prefijo en el log
«SGI 57.144.0».
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

PAUSE_NOTE = ("Dirección General 2026-10-12: aprobaciones de Contabilidad e Inventario en pausa "
              "(Sistemas las desactiva a mano; el SGI ya no las reactiva)")


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    Role = env['sgi.activity.role'].sudo()
    roles = Role.search(['&', ('role', '=', 'aprueba'), ('approval_paused', '=', False),
                         '|', ('approval_model_id.model', '=like', 'account.%'),
                         ('approval_model_id.model', '=like', 'stock.%')])
    for role in roles:
        role.write({'approval_paused': True, 'approval_paused_note': PAUSE_NOTE})
    done, failed = [], []
    for role in roles:
        try:
            with cr.savepoint():
                role._sgi_sync_approval_rule()
            done.append("%s %s (%s)" % (role.activity_id.number or '', role.display_name,
                                        role.approval_model_id.model))
        except Exception as exc:  # noqa: BLE001 — un rol no detiene a los demás
            failed.append("%s: %s" % (role.display_name, exc))
    roles.activity_id._sgi_refresh_spec_gaps()
    _logger.info("SGI 57.144.0: %d aprobaciones de Contabilidad e Inventario en pausa: %s; con error: %s",
                 len(roles), done or 'ninguna', failed or 'ninguna')

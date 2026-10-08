# -*- coding: utf-8 -*-
"""1.0.4 (Jose 2026-10-08, punto 4): las aprobaciones por botón se sincronizan
desde el satélite, que es el único que entra al registro con la regla nativa.

Las migraciones de quimibond_sgi corren antes de cargar este módulo: ahí todo
botón se lee «por sincronizar» (57.127.0 no pudo ligar C1.10 y los faltantes
de C1.02 y C1.11 quedaron grabados así). Aquí, ya cargado, se sincronizan los
roles «Aprueba» por botón que no están activos (con savepoint, uno a uno) y
se refrescan los faltantes de sus actividades. Idempotente. Prefijo en el
log: «SGI Studio 1.0.4»."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    Role = env['sgi.activity.role'].sudo()
    roles = Role.search([('role', '=', 'aprueba'), ('approval_kind', '=', 'boton'),
                         ('activity_id.active', '=', True)])
    synced, failed = [], []
    for role in roles.filtered(lambda r: r.approval_state in ('por_sincronizar', 'conflicto')):
        try:
            with cr.savepoint():
                role._sgi_sync_approval_rule()
            synced.append('%s: %s' % (role.display_name, role.approval_state))
        except Exception as exc:  # noqa: BLE001 — un rol mal configurado no detiene a los demás
            failed.append('%s: %s' % (role.display_name, exc))
    roles.activity_id._sgi_refresh_spec_gaps()
    env.registry.clear_cache()
    _logger.info("SGI Studio 1.0.4: sincronizadas %s; sin sincronizar %s; faltantes refrescados en %d actividades.",
                 synced or 'ninguna', failed or 'ninguna', len(roles.activity_id))

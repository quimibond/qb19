# -*- coding: utf-8 -*-
"""19.0.57.127.0 — Correcciones de Jose a C1 (2026-10-08, punto 1).

a) C1.04b: canal Odoo; la ruta se atribuye a quien la asigna (campos nuevos
   en la lista de materiales); el plazo se queda vacío a propósito.
b) C1.13 y C1.14 se miden con el usuario que movió el proyecto a la etapa
   (sgi.dev.stage.log.user_id); se apaga «quién lo pasó a su estado».
c) C1.09 y C1.10 se miden por proyecto (orden de muestra emitida y corrida
   validada), no por el tipo 86 con registros de julio.
d) Las aprobaciones de C1.10 (roles 1202 y 1203) apuntan al botón «Validar
   corrida» y se intenta sincronizarlas.
No toca nombres, pasos, roles ni menús que Jose cambió a mano.

Idempotente. Prefijo en el log: «SGI 57.127.0»."""
import logging

from odoo import SUPERUSER_ID, api
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    lang = env.company.partner_id.lang or 'en_US'
    env = api.Environment(cr, SUPERUSER_ID, {'lang': lang})
    Project = env['project.project']
    Activity = env['sgi.process.activity'].sudo().with_context(active_test=False)
    # a) canal de C1.04b
    c104b = Activity.search([('process_id.code', '=', 'C1'), ('number_label', '=', 'C1.04b')], limit=1)
    if c104b and not c104b.exec_channel:
        c104b.write({'exec_channel': 'odoo'})
    # a/b/c) mediciones
    report = Project._sgi_dev_apply_c1_measures()
    # b) sin historial en las actividades que miden con el reloj por etapa
    for number in ('C1.13', 'C1.14'):
        act = Activity.search([('process_id.code', '=', 'C1'), ('number', '=', number)], limit=1)
        if act and 'measure_user_history' in act._fields and act.measure_user_history:
            act.write({'measure_user_history': False})
    # d) aprobaciones de C1.10 al botón de validación de la corrida
    c110 = Activity.search([('process_id.code', '=', 'C1'), ('number', '=', 'C1.10')], limit=1)
    synced = []
    if c110:
        roles = c110.role_ids.filtered(lambda r: r.role == 'aprueba' and r.approval_kind == 'boton')
        mo_model = env['ir.model'].sudo()._get('mrp.production')
        roles.sudo().write({'approval_model_id': mo_model.id, 'approval_method': 'action_sgi_dev_validate_run'})
        for role in roles:
            try:
                with cr.savepoint():
                    role.sudo().action_sgi_sync_approval()
                synced.append('%s: %s' % (role.display_name, role.approval_state))
            except UserError as exc:
                synced.append('%s: por sincronizar (%s)' % (role.display_name, exc))
    _logger.info("SGI 57.127.0: C1.04b canal %s; mediciones %s; aprobaciones C1.10: %s.",
                 c104b.exec_channel if c104b else '—', report, "; ".join(synced) or '—')

# -*- coding: utf-8 -*-
"""56.17.0 (2026-09-28): cambio documental firmado en Sign y categorías de
Aprobaciones que sí se pueden aprobar.

- «Modificación de documento SGI»: se aprueba firmando en Sign (elaboró →
  revisó → aprobó). Firman quien pide, el dueño del proceso y el Jefe MAST,
  así que ya no entra el jefe directo (manager_approval), el mínimo es 1 y
  cada firmante es aprobador requerido (lo arma la solicitud al enviarse).
  Con mínimo 2 y un solo aprobador, quien no tuviera jefe con usuario se
  quedaba atorado.
- «Cambio de proceso / infraestructura (MOC SGI)»: pedía 2 aprobaciones sin
  ningún aprobador, nunca se podía aprobar. Queda el Jefe MAST como
  aprobador requerido y mínimo 1.

No hay solicitudes de cambio documental ni MOC en producción (0 al
2026-09-28). No borra nada; idempotente."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {'active_test': False})
    Category = env['approval.category']
    doc_change = env.ref('quimibond_sgi.sgi_approval_category_doc_change', raise_if_not_found=False) \
        or Category.search([('sgi_is_doc_change', '=', True)], limit=1)
    if doc_change:
        doc_change.write({'sgi_sign_required': True, 'manager_approval': False,
                          'approval_minimum': 1, 'approver_sequence': True})
        doc_change.approver_ids.write({'required': True})
    moc = env.ref('quimibond_sgi.sgi_approval_category_moc', raise_if_not_found=False) \
        or Category.search([('sgi_is_moc', '=', True)], limit=1)
    mast_id = env['sgi.cron']._sgi_manager_user_id()
    if moc:
        if not moc.approver_ids and mast_id:
            moc.write({'approver_ids': [(0, 0, {'user_id': mast_id, 'required': True})]})
        if moc.approval_minimum > max(len(moc.approver_ids), 1):
            moc.approval_minimum = max(len(moc.approver_ids), 1)
    _logger.info("SGI 56.17.0: cambio documental por Sign; categorías de Aprobaciones corregidas.")

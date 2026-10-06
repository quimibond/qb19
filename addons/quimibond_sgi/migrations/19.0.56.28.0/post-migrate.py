# -*- coding: utf-8 -*-
"""56.28.0 (entrega 1c de la auditoría, N-001): Areli (mas@quimibond.com, Jefe
MAST y SGI) queda como responsable SGI de los documentos controlados que no
tienen (489 en producción el 2026-09-29: 487 vigentes y 2 obsoletos), antes
de que la regla «controlado ⇒ con responsable» empiece a exigirlo. Ella los
reasigna con el tiempo (decisión de Jose, 2026-09-29). Idempotente: solo toca
los que siguen sin responsable.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

MAST_LOGIN = 'mas@quimibond.com'


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {'tracking_disable': True, 'active_test': False})
    areli = env['res.users'].search([('login', '=', MAST_LOGIN)], limit=1)
    if not areli:
        _logger.warning("SGI 56.28.0: no existe el usuario %s; los documentos controlados sin "
                        "responsable se quedan así y la regla se aplicará al editarlos.", MAST_LOGIN)
        return
    docs = env['documents.document'].search([
        ('sgi_is_controlled', '=', True), ('sgi_owner_id', '=', False)])
    if docs:
        docs.write({'sgi_owner_id': areli.id})
    _logger.info("SGI 56.28.0: %d documento(s) controlado(s) quedan con responsable %s.",
                 len(docs), areli.name)

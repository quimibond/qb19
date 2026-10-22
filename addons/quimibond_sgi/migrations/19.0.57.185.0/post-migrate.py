# -*- coding: utf-8 -*-
"""19.0.57.185.0 — Los acuses pendientes de documentos obsoletos se archivan
(ya no se piden; quedan como evidencia). En producción eran 7 (2026-10-22),
de revisiones de «Mi procedimiento» ya reemplazadas."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    Ack = env['sgi.document.ack'].with_context(active_test=False)
    pending = Ack.search([('state', '=', 'pendiente'), ('active', '=', True),
                          ('document_id.sgi_state', '=', 'obsoleto')])
    pending.write({'active': False})
    _logger.info("SGI 57.185.0: %d acuse(s) pendiente(s) de documentos obsoletos archivados.", len(pending))

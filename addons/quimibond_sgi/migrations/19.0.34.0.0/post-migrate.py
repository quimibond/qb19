# -*- coding: utf-8 -*-
"""P-3: llena las ligas nuevas con lo que ya existe.

- ``documents.document.sgi_doc_change_id``: la última solicitud de cambio
  documental aprobada que apunta al documento.
- ``sgi.management.review.audit_ids``: las auditorías del periodo de cada
  revisión que no tenga ninguna.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    requests = env['approval.request'].search([
        ('sgi_is_doc_change', '=', True), ('request_status', '=', 'approved'),
        ('sgi_document_id', '!=', False)], order='write_date asc, id asc')
    linked = 0
    for request in requests:
        doc = request.sgi_document_id
        if doc.sgi_doc_change_id != request:
            doc.write({'sgi_doc_change_id': request.id})
            linked += 1
    reviews = env['sgi.management.review'].search([('audit_ids', '=', False)])
    filled = 0
    for review in reviews:
        audits = review._sgi_period_audits()
        if audits:
            review.write({'audit_ids': [(6, 0, audits.ids)]})
            filled += 1
    _logger.info("SGI P-3: %d documentos ligados a su cambio documental; "
                 "%d revisiones por la dirección con sus auditorías.", linked, filled)

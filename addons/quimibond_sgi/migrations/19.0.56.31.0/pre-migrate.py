# -*- coding: utf-8 -*-
"""56.31.0 (auditoría 2026-09, C-001 corregido por L-003; entrega 3
«documentos», bloque 2): ``sgi.process.replaced_document_ids`` deja de ser un
Many2many (tabla ``sgi_process_replaced_doc_rel``) y pasa a ser el One2many
inverso de ``documents.document.sgi_replaced_by_process_id``. Decisión 3 de
Jose: el DOCUMENTO es la única fuente de verdad y **no se rellena nada desde el
lado del proceso**.

Este script corre ANTES de que el ORM cambie el tipo del campo y solo:

1. Respalda la tabla vieja: copia ``sgi_process_replaced_doc_rel_bak_563100``
   y un adjunto JSON por proceso («procedimientos_sustituidos_56.31.0_<clave>
   .json», patrón de 56.24.0) con documento, clave, nombre, estado y lo que
   decía el documento en ese momento.
2. Registra en el log los conflictos: documento en 2+ procesos, documento cuyo
   proceso difiere del del documento, y los que solo estaban del lado del
   proceso (que dejan de verse ahí, a propósito).

NO copia nada a ``sgi_replaced_by_process_id`` (L-003). Después Odoo recalcula
el One2many desde el documento.

Verificado en producción por MCP (solo lectura, 2026-09-29):
- ``search_records sgi.process [replaced_document_ids != False]``: 26
  documentos en 12 procesos (C1 1, C2 3, C3 1, C4 3, C5 8, C6 1, E1 2, E2 2,
  S1 1, S3 1, S4 2, S5 1).
- ``search_records documents.document [sgi_doc_type = procedimiento]``: 23
  con ``sgi_replaced_by_process_id`` en 11 procesos (S1, C3, C6, E1, C2, S3,
  E2, C5, C1, S5, C4).
- Solo del lado del proceso (se quedan vacíos a propósito): 3538, 3573, 3560,
  3558, 3557, 3555, 3548, 3546, 3545, 3532, 3505, 3536, 3499 (13, P-I01 = 3560
  entre ellos: queda fuera de C4). Sin conflictos de proceso distinto.
Idempotente: la copia es IF NOT EXISTS y el adjunto no se duplica.
"""
import base64
import json
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

TABLE = 'sgi_process_replaced_doc_rel'
BACKUP = 'sgi_process_replaced_doc_rel_bak_563100'


def _table_exists(cr, name):
    cr.execute("SELECT 1 FROM information_schema.tables WHERE table_name = %s", (name,))
    return bool(cr.fetchone())


def migrate(cr, version):
    if not _table_exists(cr, TABLE):
        _logger.info("SGI 56.31.0: no existe %s; nada que respaldar.", TABLE)
        return
    cr.execute("CREATE TABLE IF NOT EXISTS %s AS SELECT * FROM %s" % (BACKUP, TABLE))
    cr.execute("""
        SELECT r.process_id, p.code, r.document_id, d.sgi_code, d.name, d.sgi_state,
               d.active, d.sgi_replaced_by_process_id
          FROM %s r
          JOIN sgi_process p ON p.id = r.process_id
          JOIN documents_document d ON d.id = r.document_id
         ORDER BY p.code, d.sgi_code, d.id
    """ % TABLE)
    rows = cr.fetchall()
    if not rows:
        _logger.info("SGI 56.31.0: %s vacía; respaldo en %s.", TABLE, BACKUP)
        return

    by_process, by_doc = {}, {}
    for process_id, code, doc_id, sgi_code, name, state, active, m2o in rows:
        by_process.setdefault((process_id, code), []).append({
            'document_id': doc_id, 'sgi_code': sgi_code or '', 'name': name or '',
            'sgi_state': state or '', 'active': bool(active),
            'sgi_replaced_by_process_id': m2o,
        })
        by_doc.setdefault(doc_id, set()).add(process_id)

    env = api.Environment(cr, SUPERUSER_ID, {})
    Attachment = env['ir.attachment'].sudo()
    for (process_id, code), docs in by_process.items():
        filename = "procedimientos_sustituidos_56.31.0_%s.json" % (code or process_id)
        if Attachment.search_count([('res_model', '=', 'sgi.process'),
                                    ('res_id', '=', process_id), ('name', '=', filename)]):
            continue
        Attachment.create({
            'name': filename, 'res_model': 'sgi.process', 'res_id': process_id,
            'mimetype': 'application/json',
            'datas': base64.b64encode(json.dumps(docs, ensure_ascii=False, indent=1).encode('utf-8')),
        })

    multi = {doc: procs for doc, procs in by_doc.items() if len(procs) > 1}
    differs, only_process = [], []
    for (process_id, code), docs in by_process.items():
        for doc in docs:
            m2o = doc['sgi_replaced_by_process_id']
            if m2o is None:
                only_process.append("%s (%s) en %s" % (doc['sgi_code'], doc['document_id'], code))
            elif m2o != process_id:
                differs.append("%s (%s): proceso %s, documento %s" % (
                    doc['sgi_code'], doc['document_id'], code, m2o))
    _logger.info("SGI 56.31.0: respaldo de %d fila(s) de %s en %d proceso(s) (tabla %s y un "
                 "adjunto JSON por proceso).", len(rows), TABLE, len(by_process), BACKUP)
    if multi or differs:
        _logger.warning("SGI 56.31.0: conflictos en la sustitución (manda el documento; no se "
                        "copia nada): en varios procesos %s; proceso distinto del documento: %s",
                        multi, "; ".join(differs))
    if only_process:
        _logger.warning("SGI 56.31.0: %d documento(s) solo estaban del lado del proceso y "
                        "dejan de verse ahí (decisión 3: se quedan vacíos): %s",
                        len(only_process), "; ".join(only_process))

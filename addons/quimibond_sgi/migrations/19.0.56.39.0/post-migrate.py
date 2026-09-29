# -*- coding: utf-8 -*-
"""56.39.0 (auditoría 2026-09, entrega 6 «Del Dropbox a Odoo», bloque 1;
L-012 con la respuesta 3 de Jose a L): los instructivos, DAT, anexos,
protocolos y reglamentos controlados y activos que NO tienen clase de
migración pasan a clase D («Sigue como documento») y «No aplica (se queda)».
Los que ya pasaron a actividades de Odoo los marca MAST uno por uno después.

P-I01 (documento 3560) y su familia quedan fuera de cualquier escritura (L-001,
credenciales): por la clave (o clave anterior) y por el procedimiento padre.

Verificado en producción por MCP (solo lectura, 2026-09-29; todos con
``company_id`` vacío, compartidos):

- ``aggregate_records documents.document [sgi_is_controlled = True,
  sgi_doc_type in (instructivo, dat, anexo, protocolo, reglamento)]
  groupby [sgi_doc_type, sgi_migration_class, sgi_migration_state]`` →
  115 sin clase, todos vigentes y activos (0 archivados):
  instructivo 48 «pendiente»; DAT 41 «pendiente» + 1 «migrado»; anexo 15
  «migrado»; protocolo 5 «migrado»; reglamento 4 «migrado» + 1 «pendiente».
  Ninguno tiene destino (menú, worksheet ni texto): la misma consulta con
  ``'|', '|', sgi_migration_target != False, sgi_odoo_menu_id != False,
  sgi_migration_point_id != False`` → 0.
- ``search_records documents.document [sgi_is_controlled = True, '|', '|',
  sgi_code ilike P-I01, sgi_previous_code ilike P-I01,
  sgi_parent_document_id = 3560]`` (solo id, clave, tipo y estados; el
  contenido no se abrió) → 17: P-I01 (3560), 3 formatos (clase B, fuera del
  alcance), 7 DAT (4700–4706) y 6 IT (3614–3619). Los 13 DAT e IT se excluyen.

Esperado en producción: **102** filas la primera vez (instructivo 42, DAT 35,
anexo 15, protocolo 5, reglamento 5; 77 venían de «pendiente» y 25 de
«migrado») y **0** la segunda.

Respaldo previo: tabla ``documents_document_bak_563900`` (id, clave, clave
anterior, tipo, clase y estado anteriores) con exactamente las filas que se
van a cambiar; ``CREATE TABLE IF NOT EXISTS``, así que una segunda corrida no
la pisa. Por SQL, sin chatter; nada se borra.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

BACKUP = 'documents_document_bak_563900'
TYPES = ('instructivo', 'dat', 'anexo', 'protocolo', 'reglamento')


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    Doc = env['documents.document']
    codes = Doc._sgi_dropbox_excluded_codes()
    patterns = ['(^|[^A-Z])%s([^0-9]|$)' % c for c in codes]
    cr.execute("""
        CREATE TABLE IF NOT EXISTS %s AS
        SELECT d.id, d.sgi_code, d.sgi_previous_code, t.code AS doc_type,
               d.sgi_migration_class, d.sgi_migration_state, now() AS respaldado
          FROM documents_document d
          JOIN sgi_document_type t ON t.id = d.sgi_doc_type_id
         WHERE t.code IN %%(types)s
           AND d.sgi_is_controlled IS TRUE AND d.active IS TRUE
           AND d.sgi_migration_class IS NULL
           AND NOT (upper(coalesce(d.sgi_previous_code, '')) ~ ANY(%%(patterns)s))
           AND NOT (upper(coalesce(d.sgi_code, '')) ~ ANY(%%(patterns)s))
           AND NOT EXISTS (
               SELECT 1 FROM documents_document p
                WHERE p.id = d.sgi_parent_document_id
                  AND (upper(coalesce(p.sgi_previous_code, '')) ~ ANY(%%(patterns)s)
                       OR upper(coalesce(p.sgi_code, '')) ~ ANY(%%(patterns)s)))
    """ % BACKUP, {'types': TYPES, 'patterns': patterns})
    result = Doc._sgi_classify_legacy_documents(exclude_codes=codes)
    _logger.info(
        "SGI 56.39.0 (L-012): %d documento(s) del Dropbox sin clase → clase D y «No aplica» "
        "(%s; esperado en producción: 102 = IT 42, DAT 35, anexo 15, protocolo 5, "
        "reglamento 5). Excluidos: %s y su familia. Respaldo en %s.",
        sum(result.values()), result, ", ".join(codes), BACKUP)

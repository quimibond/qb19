# -*- coding: utf-8 -*-
"""Aísla las pruebas de los documentos controlados reales.

Las pruebas crean documentos con claves reales (P-A28, F-P-A28-12, MIID…)
porque la nomenclatura y los formatos del procedimiento dependen de ellas. En
una copia de producción esas claves ya existen como vigentes y chocan con los
candados (un vigente por clave, familia, revisión creciente). Dentro de la
transacción de la prueba (que se deshace al final) se les cambia la clave a
``REAL~<id>``, que ninguna nomenclatura reconoce.

Desde 56.30.0 (C-006) los formatos del mapeo apuntan al documento real; se
desligan también, para que cada prueba ligue (o no) el documento que crea.
"""


def sgi_hide_real_documents(env):
    env.flush_all()
    env.cr.execute("""
        UPDATE documents_document
           SET sgi_code = 'REAL~' || id
         WHERE sgi_code IS NOT NULL AND sgi_code NOT LIKE 'REAL~%%'
    """)
    # Desde 56.32.0 la clave del Dropbox vive también en «Clave anterior» y
    # se busca siempre: se oculta igual.
    env.cr.execute("""
        UPDATE documents_document
           SET sgi_previous_code = 'REAL~' || id || '~ANT'
         WHERE sgi_previous_code IS NOT NULL AND sgi_previous_code NOT LIKE 'REAL~%%'
    """)
    env.cr.execute("""
        UPDATE sgi_format_map
           SET document_id = NULL, document_alt_id = NULL
         WHERE document_id IS NOT NULL OR document_alt_id IS NOT NULL
    """)
    env.invalidate_all()


def sgi_neutralize_dropbox_contradictions(env):
    """Las 13 contradicciones de clase y estado de producción (L-013) salen en
    cada modo de prueba como error; en la copia de la prueba se apagan para
    medir solo lo que la prueba siembra (se deshace al final)."""
    env.flush_all()
    env.cr.execute("""
        UPDATE documents_document SET sgi_migration_class = NULL
         WHERE (sgi_migration_class = 'd' AND sgi_migration_state IS DISTINCT FROM 'na')
            OR (sgi_migration_class IN ('a', 'b', 'c') AND sgi_migration_state = 'na')
    """)
    env.invalidate_all()


def sgi_new_company(env, name, **vals):
    """57.164.2: una empresa de prueba que no hereda carpetas de Documentos
    de otra empresa. En producción la carpeta «Aprobaciones» de
    documents_approvals es de la empresa 1 (Odoo la trae compartida) y el
    valor por omisión de ``approvals_folder_id`` la ponía en la empresa nueva:
    ``_check_company`` reventaba la creación («company inconsistencies»)."""
    Company = env['res.company']
    for fname, field in Company._fields.items():
        if (field.type == 'many2one' and field.comodel_name == 'documents.document'
                and field.store and fname not in vals):
            vals[fname] = False
    return Company.create(dict(vals, name=name))

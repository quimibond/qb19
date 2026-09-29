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
    env.cr.execute("""
        UPDATE sgi_format_map
           SET document_id = NULL, document_alt_id = NULL
         WHERE document_id IS NOT NULL OR document_alt_id IS NOT NULL
    """)
    env.invalidate_all()

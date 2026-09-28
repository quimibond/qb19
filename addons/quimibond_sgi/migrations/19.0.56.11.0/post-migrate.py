# -*- coding: utf-8 -*-
"""19.0.56.11.0 (Bloque 5 · 5.2 DOC-2): campos nuevos «Obsoleto desde»,
«Motivo de obsolescencia» y «Lo sustituye el proceso». Los documentos que ya
estaban obsoletos toman como fecha su última modificación y como motivo
«Obsoleto antes de 56.11.0» (no se sabe el motivo real). Solo llena vacíos."""


def migrate(cr, version):
    cr.execute("""
        UPDATE documents_document
           SET sgi_obsolete_date = COALESCE(write_date, create_date)::date,
               sgi_obsolete_reason = 'Obsoleto antes de 56.11.0 (sin motivo registrado)'
         WHERE sgi_state = 'obsoleto' AND sgi_obsolete_date IS NULL
    """)

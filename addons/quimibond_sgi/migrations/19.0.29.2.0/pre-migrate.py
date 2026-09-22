# -*- coding: utf-8 -*-
"""Numeral y sección dejan de ser texto: se guardan antes del update.

El numeral pasa a calcularse (clave del proceso + paso). El texto que tenía
cada actividad («4.2.3.1», «P-C16», «3.4-3.6») se copia a legacy_number para
que se siga encontrando.
"""


def migrate(cr, version):
    if not version:
        return
    cr.execute("ALTER TABLE sgi_process_activity ADD COLUMN IF NOT EXISTS legacy_number varchar")
    cr.execute("""
        UPDATE sgi_process_activity SET legacy_number = number
        WHERE legacy_number IS NULL AND number IS NOT NULL
    """)

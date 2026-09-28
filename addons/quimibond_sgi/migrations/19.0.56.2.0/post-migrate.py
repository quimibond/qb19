# -*- coding: utf-8 -*-
"""56.2.0: la categoría de Aprobaciones «Proponer cambio a mi procedimiento
(SGI)» queda marcada para el botón «Proponer cambio». Idempotente."""


def migrate(cr, version):
    cr.execute("""
        UPDATE approval_category
           SET sgi_is_mp_change = TRUE
         WHERE name::text ILIKE '%%Proponer cambio a mi procedimiento%%'
           AND sgi_is_mp_change IS NOT TRUE
    """)

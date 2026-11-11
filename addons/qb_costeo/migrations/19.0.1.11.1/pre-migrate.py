# -*- coding: utf-8 -*-
"""1.11.1: la capa de absorción cambia de estados («capitaliza» ya no
existe) y se mide en neto por categoría. Las filas de 1.11.0 se recalculan
con el botón del período; se borran para no dejar estados que la versión
nueva no conoce, y se limpian los totales del período."""


def migrate(cr, version):
    cr.execute("SELECT to_regclass('qb_capa_absorcion')")
    if cr.fetchone()[0]:
        cr.execute("DELETE FROM qb_capa_absorcion")
    cr.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'qb_periodo' AND column_name = 'capa_calculada_el'
    """)
    if cr.fetchone():
        cr.execute("""
            UPDATE qb_periodo
               SET capa_calculada_el = NULL, capa_valor_odoo = 0,
                   capa_valor_completo = 0, capa_total = 0,
                   capa_sin_costo_valor = 0
        """)

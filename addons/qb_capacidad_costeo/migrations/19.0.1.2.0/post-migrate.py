# -*- coding: utf-8 -*-
"""Tejido: medir la producción a nivel ORDEN (TL/OP-TE), no por workorder.

La cantidad producida por workorder está mal registrada (p.ej. abril 2026
colapsa a ~11.6 t cuando lo real ronda 57 t; enero sobre-cuenta). La orden de
manufactura (mrp.production.qty_produced) es la fuente confiable, y el promedio
2026 pasa de ~84 t/mes (workorder) a ~96 t/mes (orden), como corrigió el CEO.

El seed es noupdate, así que el centro TEJIDO ya instalado no toma el patrón
nuevo solo: se lo ponemos aquí si no lo tiene. Idempotente."""


def migrate(cr, version):
    cr.execute("""
        UPDATE qb_costeo_centro
           SET mo_name_pattern = 'TL/OP-TE%'
         WHERE code = 'TEJIDO'
           AND (mo_name_pattern IS NULL OR mo_name_pattern = '')
    """)

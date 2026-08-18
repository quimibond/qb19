# -*- coding: utf-8 -*-
"""Medir la producción de los procesos por ORDEN (mrp.production), no por
workorder ni dejándola en cero.

- TEJIDO: la cantidad por workorder está mal registrada (abril 2026 colapsa a
  ~1/5). Patrón de orden TL/OP-TE → promedio 2026 ~84 t (workorder) → ~96 t.
- TINTORERÍA: no tenía patrón ni workcenters → salía en CERO (100% ociosa,
  falso). Patrón TL/OP-TIN (~90 t/mes reales).
- ACABADO: su patrón TL/OP-ACA no incluía la segunda línea TL/OP-V10 (~9% del
  acabado). Se agrega, separado por coma (mo_name_pattern admite varios).

El seed es noupdate, así que los centros ya instalados no toman los patrones
nuevos solos: se los ponemos aquí. Idempotente."""


def migrate(cr, version):
    # TEJIDO — patrón si no lo tiene.
    cr.execute("""
        UPDATE qb_costeo_centro
           SET mo_name_pattern = 'TL/OP-TE%'
         WHERE code = 'TEJIDO'
           AND (mo_name_pattern IS NULL OR mo_name_pattern = '')
    """)
    # TINTORERÍA — patrón si no lo tiene (antes salía en cero).
    cr.execute("""
        UPDATE qb_costeo_centro
           SET mo_name_pattern = 'TL/OP-TIN%'
         WHERE code = 'TINTORERIA'
           AND (mo_name_pattern IS NULL OR mo_name_pattern = '')
    """)
    # ACABADO — sumar la segunda línea V10 si aún no está.
    cr.execute("""
        UPDATE qb_costeo_centro
           SET mo_name_pattern = 'TL/OP-ACA%,TL/OP-V10%'
         WHERE code = 'ACABADO'
           AND mo_name_pattern IS NOT NULL
           AND mo_name_pattern NOT LIKE '%V10%'
    """)

# -*- coding: utf-8 -*-
"""Medir la producción de los procesos por ORDEN (mrp.production), no por
workorder ni dejándola en cero.

- TEJIDO: la cantidad por workorder está mal registrada (abril 2026 colapsa a
  ~1/5). Patrón de orden TL/OP-TE → promedio 2026 ~84 t (workorder) → ~96 t.
- TINTORERÍA: no tenía patrón ni workcenters → salía en CERO (100% ociosa,
  falso). Patrón TL/OP-TIN (~90 t/mes reales).
- ENTRETELAS: TL/OP-V10 (aplicación de resina / fusionable) es entretelas —
  antes NO se contaba en ningún centro. Se agrega a su patrón, separado por
  coma (mo_name_pattern admite varios). (V10 NO es acabado.)

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
    # ENTRETELAS — sumar la resina V10 si aún no está.
    cr.execute("""
        UPDATE qb_costeo_centro
           SET mo_name_pattern = 'TL/OP-CAR%,TL/OP-V10%'
         WHERE code = 'ENTRETELAS'
           AND mo_name_pattern IS NOT NULL
           AND mo_name_pattern NOT LIKE '%V10%'
    """)
    # ACABADO — asegurar que NO cargue V10 (es entretelas, no acabado).
    cr.execute("""
        UPDATE qb_costeo_centro
           SET mo_name_pattern = 'TL/OP-ACA%'
         WHERE code = 'ACABADO'
           AND mo_name_pattern LIKE '%V10%'
    """)

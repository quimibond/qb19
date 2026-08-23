# -*- coding: utf-8 -*-
"""Backfill de las columnas nuevas de márgenes en qb_costo_producto.

Los períodos ya calculados quedarían con margen bruto/neto en cero hasta
el siguiente 'Recalcular', y el reporte histórico mentiría. Las columnas
nuevas son identidades exactas de las existentes, así que se rellenan en
SQL con las mismas fórmulas del motor (precio 0 → márgenes 0, como en
_compute_product_vals). Idempotente."""


def migrate(cr, version):
    cr.execute("""
        UPDATE qb_costo_producto
           SET costo_produccion = costo_variable + fab_unit,
               margen_bruto = CASE WHEN precio_prom > 0
                    THEN precio_prom - costo_variable - fab_unit
                    ELSE 0 END,
               margen_bruto_pct = CASE WHEN precio_prom > 0
                    THEN 100.0 * (precio_prom - costo_variable - fab_unit)
                         / precio_prom
                    ELSE 0 END,
               margen_bruto_total = CASE WHEN precio_prom > 0
                    THEN (precio_prom - costo_variable - fab_unit) * qty_vendida
                    ELSE 0 END,
               margen_neto_total = CASE WHEN precio_prom > 0
                    THEN margen_absorbido * qty_vendida
                    ELSE 0 END
    """)

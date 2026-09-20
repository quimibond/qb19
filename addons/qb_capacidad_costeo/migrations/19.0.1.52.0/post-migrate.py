# -*- coding: utf-8 -*-
"""No-op: el recálculo de esta versión ya lo hace la más nueva (1.53).

Recalculaba 2026 para reparar lo que la 1.51 guardó con la capacidad
vieja (`qb.ociosidad` es un `_table_query` y no veía el write pendiente
sobre los centros; el arreglo de fondo vive en `_capacidad_normal_map`).

Se deja como stub porque un update que salta varias versiones corre todas
las migraciones intermedias, y recalcular el año una vez por versión
convierte el build en minutos de CPU para llegar al mismo resultado. La
regla del módulo: solo la migración más nueva recalcula.
"""


def migrate(cr, version):
    pass

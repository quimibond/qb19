# -*- coding: utf-8 -*-
"""No-op: el recálculo de esta versión ya lo hace la más nueva (1.51).

Recalculaba tras el barrido de limpieza del motor (1.37-1.42): reventa
sin energía, receta ambigua resuelta por la última OP, AVCO negativo
acotado, MP al precio de la época, inspección de importados, aduana por
compra y nómina de Diseño a operación.

Se deja como stub porque un update que salta varias versiones corre
TODAS las migraciones intermedias, y recalcular treinta y dos períodos
una vez por versión convertía el build en media hora de CPU para llegar
al mismo resultado. La regla del módulo: solo la migración más nueva
recalcula.
"""


def migrate(cr, version):
    pass

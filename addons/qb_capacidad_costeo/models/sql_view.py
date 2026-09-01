# -*- coding: utf-8 -*-
"""Base de las vistas SQL del módulo: flush antes de leer.

Las once vistas `_table_query` leen tablas que el ORM administra —
`qb_costeo_centro`, `qb_turno_config`, `qb_costeo_cuenta_map`,
`qb_costeo_familia`— con SQL crudo. El ORM no tiene cómo saber que ese
SELECT depende de un write pendiente, así que no hace flush y la vista
devuelve el renglón anterior.

No es teórico y no es barato. La migración 1.51 escribió la capacidad de
Acabado (915,733 → 1,175,313 m/mes) y recalculó 2026 en la misma
transacción: los ocho períodos salieron con el denominador VIEJO y nada
avisó. Los factores se veían normales; solo comparándolos contra el centro
se notaba que el número ya no correspondía a lo que decía la tabla. Es el
modo de falla más caro que tiene el módulo — el que no falla, el que
contesta mal.

El arreglo puntual vivía en `_capacidad_normal_map` y en `qb.familia.carga`.
Este mixin lo hace estructural: se engancha a `_where_calc`, que es el
embudo por el que pasan `search`, `search_count` y las agrupaciones, así
que cubre las tres con una sola pieza y sin depender de la firma de cada
una. Toda vista SQL nueva hereda de aquí y nace cubierta.
"""
from odoo import api, models


class QbSqlView(models.AbstractModel):
    _name = 'qb.sql.view'
    _description = 'Vista SQL read-only: flush antes de consultar'

    @api.model
    def _where_calc(self, *args, **kwargs):
        # `*args` a propósito: la firma de _where_calc ha cambiado entre
        # versiones de Odoo y este mixin no debe romperse con la próxima.
        self.env.flush_all()
        return super()._where_calc(*args, **kwargs)

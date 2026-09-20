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

El enganche va en `_search`, que en Odoo 19 es el embudo real: `search`,
`search_fetch`, `search_count` y `_read_group` pasan todos por ahí
(`odoo/orm/models.py`). La v1.58 lo enganchó en `_where_calc`, que era el
embudo hasta Odoo 18 y en 19 YA NO EXISTE: el override no lo llamaba
nadie y el mixin era decorativo. Lo destapó el primer corrido de los
tests (`test_vista_sql_ve_el_write_pendiente`) — nueve versiones sin
correrlos y el arreglo estructural llevaba una en producción sin hacer
nada. Por eso el test no es un adorno: comprueba el COMPORTAMIENTO
(escribir y leer en la misma transacción), no que el mixin esté puesto.
"""
from odoo import api, models


class QbSqlView(models.AbstractModel):
    _name = 'qb.sql.view'
    _description = 'Vista SQL read-only: flush antes de consultar'

    @api.model
    def _search(self, *args, **kwargs):
        # `*args` a propósito: la firma de _search cambia entre versiones de
        # Odoo y este mixin no debe romperse con la próxima. Lo que NO se
        # puede tolerar es que el método deje de existir —como le pasó a
        # `_where_calc` en 19— y el override quede huérfano sin avisar; de
        # eso se encarga el test de comportamiento, no la firma.
        self.env.flush_all()
        # Y la otra mitad: la vista devuelve el MISMO id entre consultas, así
        # que sus campos salen de la caché del ORM sin volver a la base. Con
        # solo el flush, escribir la capacidad del centro y releer la fila de
        # ociosidad seguía dando el número viejo — que es exactamente el
        # accidente de la 1.51. La vista es read-only: tirar su caché no
        # pierde nada, y es lo único que la vuelve a leer de verdad.
        self.invalidate_model()
        return super()._search(*args, **kwargs)

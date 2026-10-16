# -*- coding: utf-8 -*-
"""Base de las vistas SQL del costeo (1.6.0): flush antes de leer.

Las vistas `_table_query` (rentabilidad, órdenes fuera de rango) leen con SQL
crudo tablas que el ORM administra (facturas, costos, órdenes de trabajo).
El ORM no sabe que ese SELECT depende de un write pendiente: sin flush, la
vista contesta con el dato anterior. Es la misma lección del módulo anterior
(`qb.sql.view`, migración 1.51 de `qb_capacidad_costeo`): el enganche va en
`_search`, el embudo de `search`, `search_count` y `_read_group` en Odoo 19,
y además se tira la caché del modelo, porque la vista devuelve el mismo id
entre consultas y sus campos saldrían de la caché sin volver a la base.
"""
from odoo import api, models


class QbCosteoVistaSql(models.AbstractModel):
    _name = 'qb.costeo.vista.sql'
    _description = 'Vista SQL del costeo: flush antes de consultar'

    @api.model
    def _search(self, *args, **kwargs):
        self.env.flush_all()
        self.invalidate_model()
        return super()._search(*args, **kwargs)

# -*- coding: utf-8 -*-
"""Órdenes de trabajo con tiempo fuera de rango (1.6.0).

Sustituye a `qb.workorder.excepcion` del módulo anterior. El costeo ya
descarta estas órdenes al medir horas (`qb.producto.horas._medido`, banda
`velocidad_min`/`velocidad_max` del centro); aquí se ven para corregirlas:
una orden con el cronómetro abierto le carga al producto horas que no
trabajó, y una con el cronómetro corto se las quita.

Una fila por orden de trabajo terminada en los últimos 90 días en un centro
directo con banda: unidades terminadas de la orden de fabricación ÷ horas
registradas en la orden de trabajo (la duración real, nunca la esperada).
Vista SQL de solo lectura, sin caracteres de porcentaje.
"""
from odoo import fields, models

TIPOS = [('timer_largo', 'Horas de más (cronómetro abierto)'),
         ('timer_corto', 'Horas de menos (cronómetro corto)')]


class QbOtExcepcion(models.Model):
    _name = 'qb.ot.excepcion'
    _inherit = ['qb.costeo.vista.sql']
    _description = 'Orden de trabajo con tiempo fuera de rango'
    _auto = False
    _order = 'semana desc, horas_desviadas desc'
    _rec_name = 'workorder_id'

    workorder_id = fields.Many2one('mrp.workorder', string='Orden de trabajo', readonly=True)
    production_id = fields.Many2one('mrp.production', string='Orden de fabricación', readonly=True)
    workcenter_id = fields.Many2one('mrp.workcenter', string='Máquina', readonly=True)
    centro_id = fields.Many2one('qb.centro', string='Centro de costo', readonly=True)
    product_id = fields.Many2one('product.product', string='Producto', readonly=True)
    company_id = fields.Many2one('res.company', readonly=True)
    semana = fields.Date(readonly=True, help='Lunes de la semana en que terminó.')
    date_finished = fields.Datetime(string='Terminada', readonly=True)
    qty = fields.Float(string='Unidades terminadas', readonly=True, digits=(16, 2))
    horas = fields.Float(string='Horas registradas', readonly=True, digits=(16, 2))
    rendimiento = fields.Float(string='Unidades / hora', readonly=True, digits=(16, 2))
    velocidad_min = fields.Float(string='Banda mín. u/h', readonly=True, digits=(16, 2))
    velocidad_max = fields.Float(string='Banda máx. u/h', readonly=True, digits=(16, 2))
    horas_banda = fields.Float(
        string='Horas a la orilla de la banda', readonly=True, digits=(16, 2),
        help='Las horas que habría tomado a la velocidad de la orilla de la banda que rebasó.')
    horas_desviadas = fields.Float(
        string='Horas de más (o de menos)', readonly=True, digits=(16, 2),
        help='Registradas − horas a la orilla de la banda. Positivo: el producto carga horas que '
             'probablemente no trabajó.')
    tipo = fields.Selection(TIPOS, readonly=True)

    @property
    def _table_query(self):
        return """
            WITH producido AS (
                SELECT sm.production_id, SUM(sm.quantity) AS qty
                FROM stock_move sm
                JOIN mrp_production mo ON mo.id = sm.production_id
                WHERE sm.state = 'done' AND sm.product_id = mo.product_id
                GROUP BY sm.production_id
            ),
            base AS (
                -- Un centro de trabajo de Odoo en dos centros de costo no duplica la orden.
                SELECT DISTINCT ON (wo.id) wo.id, wo.production_id, wo.workcenter_id, c.id AS centro_id,
                       mo.product_id, c.company_id, wo.date_finished,
                       date_trunc('week', wo.date_finished)::date AS semana,
                       p.qty, wo.duration / 60.0 AS horas,
                       p.qty / (wo.duration / 60.0) AS rendimiento,
                       c.velocidad_min, c.velocidad_max
                FROM mrp_workorder wo
                JOIN mrp_production mo ON mo.id = wo.production_id
                JOIN producido p ON p.production_id = mo.id
                JOIN qb_costeo_centro_workcenter_rel rel ON rel.workcenter_id = wo.workcenter_id
                JOIN qb_centro c ON c.id = rel.centro_id
                WHERE wo.state = 'done' AND mo.state = 'done'
                  AND c.nature = 'directo' AND c.active
                  AND c.company_id = {cid}
                  AND (c.velocidad_min > 0 OR c.velocidad_max > 0)
                  AND wo.duration > 0 AND p.qty > 0
                  AND wo.date_finished >= (CURRENT_DATE - INTERVAL '90 days')
                ORDER BY wo.id, c.id
            )
            SELECT b.id, b.id AS workorder_id, b.production_id, b.workcenter_id,
                   b.centro_id, b.product_id, b.company_id, b.date_finished,
                   b.semana, b.qty, b.horas, b.rendimiento,
                   b.velocidad_min, b.velocidad_max,
                   CASE WHEN b.velocidad_min > 0 AND b.rendimiento < b.velocidad_min
                        THEN b.qty / b.velocidad_min
                        ELSE b.qty / b.velocidad_max END AS horas_banda,
                   b.horas - CASE WHEN b.velocidad_min > 0 AND b.rendimiento < b.velocidad_min
                                  THEN b.qty / b.velocidad_min
                                  ELSE b.qty / b.velocidad_max END AS horas_desviadas,
                   CASE WHEN b.velocidad_min > 0 AND b.rendimiento < b.velocidad_min
                        THEN 'timer_largo' ELSE 'timer_corto' END AS tipo
            FROM base b
            WHERE (b.velocidad_min > 0 AND b.rendimiento < b.velocidad_min)
               OR (b.velocidad_max > 0 AND b.rendimiento > b.velocidad_max)
        """.format(cid=int(self.env.company.id))

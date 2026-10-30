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
    velocidad_min = fields.Float(string='Banda mín.', readonly=True, digits=(16, 2))
    velocidad_max = fields.Float(string='Banda máx.', readonly=True, digits=(16, 2))
    por = fields.Selection([('velocidad', 'Unidades por hora'), ('orden', 'Horas por orden (baño)')],
                           string='Banda en', readonly=True)
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
        # 1.7.0: la banda es la de la unidad del producto de la orden
        # (`qb.centro.banda`; sin filas, la velocidad mínima y máxima del
        # centro). `por = 'orden'` (tintorería, un baño por orden): se
        # comparan las horas de la orden de trabajo, no las unidades por hora.
        return """
            WITH producido AS (
                SELECT sm.production_id, SUM(sm.quantity) AS qty
                FROM stock_move sm
                JOIN mrp_production mo ON mo.id = sm.production_id
                WHERE sm.state = 'done' AND sm.product_id = mo.product_id
                GROUP BY sm.production_id
            ),
            ordenes AS (
                -- Un centro de trabajo de Odoo en dos centros de costo no duplica la orden.
                SELECT DISTINCT ON (wo.id) wo.id, wo.production_id, wo.workcenter_id, c.id AS centro_id,
                       mo.product_id, c.company_id, wo.date_finished,
                       date_trunc('week', wo.date_finished)::date AS semana,
                       p.qty, wo.duration / 60.0 AS horas,
                       p.qty / (wo.duration / 60.0) AS rendimiento,
                       CASE WHEN bh.n > 0 THEN bd.minimo ELSE c.velocidad_min END AS velocidad_min,
                       CASE WHEN bh.n > 0 THEN bd.maximo ELSE c.velocidad_max END AS velocidad_max,
                       CASE WHEN bh.n > 0 THEN COALESCE(bd.por, 'velocidad') ELSE 'velocidad' END AS por
                FROM mrp_workorder wo
                JOIN mrp_production mo ON mo.id = wo.production_id
                JOIN product_product pp ON pp.id = mo.product_id
                JOIN product_template pt ON pt.id = pp.product_tmpl_id
                JOIN producido p ON p.production_id = mo.id
                JOIN qb_costeo_centro_workcenter_rel rel ON rel.workcenter_id = wo.workcenter_id
                JOIN qb_centro c ON c.id = rel.centro_id
                LEFT JOIN LATERAL (
                    SELECT COUNT(*) AS n FROM qb_centro_banda x WHERE x.centro_id = c.id
                ) bh ON true
                LEFT JOIN LATERAL (
                    SELECT x.minimo, x.maximo, x.por FROM qb_centro_banda x
                    WHERE x.centro_id = c.id AND (x.uom_id = pt.uom_id OR x.uom_id IS NULL)
                    ORDER BY x.uom_id IS NULL LIMIT 1
                ) bd ON true
                WHERE wo.state = 'done' AND mo.state = 'done'
                  AND c.nature = 'directo' AND c.active
                  AND c.company_id = {cid}
                  AND wo.duration > 0 AND p.qty > 0
                  AND wo.date_finished >= (CURRENT_DATE - INTERVAL '90 days')
                ORDER BY wo.id, c.id
            ),
            base AS (
                SELECT o.*,
                       CASE WHEN o.por = 'orden' THEN o.horas ELSE o.rendimiento END AS v
                FROM ordenes o
                WHERE COALESCE(o.velocidad_min, 0) > 0 OR COALESCE(o.velocidad_max, 0) > 0
            ),
            marcadas AS (
                SELECT b.*,
                       (b.velocidad_min > 0 AND b.v < b.velocidad_min) AS bajo,
                       (b.velocidad_max > 0 AND b.v > b.velocidad_max) AS alto
                FROM base b
            )
            SELECT m.id, m.id AS workorder_id, m.production_id, m.workcenter_id,
                   m.centro_id, m.product_id, m.company_id, m.date_finished,
                   m.semana, m.qty, m.horas, m.rendimiento,
                   m.velocidad_min, m.velocidad_max, m.por,
                   CASE WHEN m.por = 'orden' THEN
                            CASE WHEN m.bajo THEN m.velocidad_min ELSE m.velocidad_max END
                        WHEN m.bajo THEN m.qty / m.velocidad_min
                        ELSE m.qty / m.velocidad_max END AS horas_banda,
                   m.horas - CASE WHEN m.por = 'orden' THEN
                                      CASE WHEN m.bajo THEN m.velocidad_min ELSE m.velocidad_max END
                                  WHEN m.bajo THEN m.qty / m.velocidad_min
                                  ELSE m.qty / m.velocidad_max END AS horas_desviadas,
                   CASE WHEN (m.por = 'orden') = m.alto THEN 'timer_largo'
                        ELSE 'timer_corto' END AS tipo
            FROM marcadas m
            WHERE m.bajo OR m.alto
        """.format(cid=int(self.env.company.id))

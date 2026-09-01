# -*- coding: utf-8 -*-
"""Excepciones de tiempo en workorders (vista SQL read-only).

Desde que los workcenters capitalizan horas × tarifa, **el tiempo registrado
es dinero**: una orden con el timer desbocado le mete costo a un producto que
no lo consumió, y una con el timer corto se lo quita. Antes un timer malo solo
ensuciaba un reporte de utilización; ahora mueve el AVCO.

La medida es el rendimiento implícito: cantidad producida ÷ horas
registradas. Para tejido circular lo sano ronda 5–14 kg/h (nominal 11), así
que se marca fuera de [2, 25] — una banda ancha a propósito, para señalar
timers rotos y no variación normal de artículo.

Casos vistos en producción (jul–ago 2026): tres órdenes con 1,018 horas
juntas, y una ruta con `duration_expected` de 17,514 horas (minutos capturados
donde van horas). Por eso este reporte mira SIEMPRE la duración real
registrada y nunca la esperada: la esperada está sucia y no se puede usar sin
sanear.
"""
from odoo import fields, models

from .cuenta_map import wo_qty_sql


class QbWorkorderExcepcion(models.Model):
    _name = 'qb.workorder.excepcion'
    _inherit = 'qb.sql.view'
    _description = 'Workorders con rendimiento fuera de rango'
    _auto = False
    _order = 'semana DESC, horas DESC'
    _rec_name = 'workorder_id'

    workorder_id = fields.Many2one('mrp.workorder', readonly=True,
                                   string='Operación')
    production_id = fields.Many2one('mrp.production', readonly=True,
                                    string='Orden')
    workcenter_id = fields.Many2one('mrp.workcenter', readonly=True,
                                    string='Centro de trabajo')
    centro_id = fields.Many2one('qb.costeo.centro', readonly=True,
                                string='Centro de costo')
    product_id = fields.Many2one('product.product', readonly=True)
    semana = fields.Date(string='Semana', readonly=True)
    date_finished = fields.Datetime(string='Terminada', readonly=True)
    qty = fields.Float(string='Cantidad producida', readonly=True)
    horas = fields.Float(string='Horas registradas', readonly=True)
    rendimiento = fields.Float(
        string='Rendimiento (u/h)', readonly=True,
        help='Cantidad ÷ horas registradas. Con la tarifa por hora activa, '
             'este número es el que decide cuánto costo se le carga a la '
             'orden.')
    horas_a_nominal = fields.Float(
        string='Horas al throughput nominal', readonly=True,
        help='Las horas que la orden habría tomado al throughput nominal del '
             'centro. La diferencia contra las registradas es el costo que '
             'sobra o falta en el producto.')
    horas_desviadas = fields.Float(
        string='Horas de más (o de menos)', readonly=True,
        help='Registradas − nominales. Positivo = el producto está cargando '
             'horas que probablemente no trabajó.')
    tipo = fields.Selection([
        ('lento', 'Rendimiento muy bajo — timer desbocado'),
        ('rapido', 'Rendimiento muy alto — horas sin registrar'),
        ('sin_horas', 'Producción sin horas registradas'),
    ], string='Tipo', readonly=True)
    company_id = fields.Many2one('res.company', readonly=True)

    @property
    def _table_query(self):
        company_id = int(self.env.company.id)
        # Sin ningún carácter de porcentaje en el SQL, como el resto de las
        # vistas del módulo (pasa por formateo estilo printf).
        qty_sql = wo_qty_sql(self.env)
        return f"""
            WITH cfg AS (
                SELECT
                    COALESCE(NULLIF((SELECT value FROM qb_costeo_factor_config
                        WHERE key = 'rendimiento_min' AND active LIMIT 1), 0),
                        2.0) AS rmin,
                    COALESCE(NULLIF((SELECT value FROM qb_costeo_factor_config
                        WHERE key = 'rendimiento_max' AND active LIMIT 1), 0),
                        25.0) AS rmax
            ),
            wc_centro AS (
                SELECT rel.workcenter_id, MIN(rel.centro_id) AS centro_id
                FROM qb_centro_workcenter_rel rel
                GROUP BY rel.workcenter_id
            ),
            base AS (
                SELECT wo.id AS workorder_id,
                       wo.production_id,
                       wo.workcenter_id,
                       wc_centro.centro_id,
                       mp.product_id,
                       mp.company_id,
                       date_trunc('week', wo.date_finished)::date AS semana,
                       wo.date_finished,
                       ({qty_sql}) AS qty,
                       COALESCE(wo.duration, 0) / 60.0 AS horas,
                       ctr.std_output_per_hour
                FROM mrp_workorder wo
                JOIN mrp_production mp ON mp.id = wo.production_id
                LEFT JOIN wc_centro ON wc_centro.workcenter_id = wo.workcenter_id
                LEFT JOIN qb_costeo_centro ctr ON ctr.id = wc_centro.centro_id
                WHERE wo.state = 'done'
                  AND wo.date_finished IS NOT NULL
                  AND mp.company_id = {company_id}
            ),
            calc AS (
                SELECT b.*,
                       CASE WHEN b.horas > 0 THEN b.qty / b.horas ELSE NULL END
                           AS rendimiento,
                       CASE WHEN COALESCE(b.std_output_per_hour, 0) > 0
                            THEN b.qty / b.std_output_per_hour
                            ELSE NULL END AS horas_a_nominal
                FROM base b
            )
            SELECT c.workorder_id AS id,
                   c.workorder_id,
                   c.production_id,
                   c.workcenter_id,
                   c.centro_id,
                   c.product_id,
                   c.company_id,
                   c.semana,
                   c.date_finished,
                   c.qty,
                   c.horas,
                   c.rendimiento,
                   c.horas_a_nominal,
                   c.horas - COALESCE(c.horas_a_nominal, c.horas)
                       AS horas_desviadas,
                   CASE WHEN c.horas <= 0 THEN 'sin_horas'
                        WHEN c.rendimiento < cfg.rmin THEN 'lento'
                        ELSE 'rapido' END AS tipo
            FROM calc c
            CROSS JOIN cfg
            WHERE c.qty > 0
              AND (c.horas <= 0
                   OR c.rendimiento < cfg.rmin
                   OR c.rendimiento > cfg.rmax)
        """

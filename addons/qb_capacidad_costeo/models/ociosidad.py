# -*- coding: utf-8 -*-
"""Costo de capacidad ociosa por centro (vista SQL read-only, IAS 2).

Costeo normal: el costo fijo del centro se divide entre la capacidad
NORMAL; lo no utilizado (ociosidad) es costo del período — va al P&L,
no al producto. La vista muestra ambos costos unitarios (a capacidad
normal y a producción real) y el costo hundido del mes.
"""
from odoo import fields, models

from .cuenta_map import CUENTA_MAP_SQL, mo_qty_sql, wo_qty_sql


class QbOciosidad(models.Model):
    _name = 'qb.ociosidad'
    _description = 'Capacidad ociosa y costo hundido por centro'
    _auto = False
    _order = 'idle_cost_month DESC'

    centro_id = fields.Many2one('qb.costeo.centro', readonly=True)
    name = fields.Char(related='centro_id.name', readonly=True)
    fixed_pool_month = fields.Float(
        string='Costo fijo/mes (GL prom. + renta contractual)', readonly=True,
        help='Cuentas fijas asignadas al centro (bucket mod/overhead/'
             'depreciación/arrendamiento, no variables), promedio de la '
             'ventana de suavizado, + renta contractual del centro.')
    capacity_month_units = fields.Float(string='Capacidad normal/mes', readonly=True)
    prod_month_units = fields.Float(string='Producción/mes (prom.)', readonly=True)
    utilization_pct = fields.Float(string='Utilización %', readonly=True)
    idle_pct = fields.Float(string='Ociosidad %', readonly=True)
    idle_cost_month = fields.Float(
        string='Costo ocioso/mes', readonly=True,
        help='Costo fijo × (1 − utilización): capacidad hundida del período.')
    fixed_unit_normal = fields.Float(
        string='Fijo/unidad a capacidad normal', readonly=True,
        help='El costo unitario correcto para costear producto (IAS 2).')
    fixed_unit_real = fields.Float(
        string='Fijo/unidad a producción real', readonly=True,
        help='Lo que absorbería el producto si se le cargara toda la '
             'ociosidad — solo para comparar, NO para costear.')
    company_id = fields.Many2one('res.company', readonly=True)

    @property
    def _table_query(self):
        return """
            WITH cfg AS (
                SELECT
                    COALESCE((SELECT value FROM qb_costeo_factor_config
                              WHERE key = 'weeks_per_month' AND active LIMIT 1), 4.33) AS weeks_per_month,
                    COALESCE(NULLIF((SELECT value FROM qb_costeo_factor_config
                              WHERE key = 'production_window_months' AND active LIMIT 1), 0), 3) AS window_months,
                    COALESCE(NULLIF((SELECT value FROM qb_costeo_factor_config
                              WHERE key = 'smoothing_months' AND active LIMIT 1), 0), 12) AS smoothing_months
            ),
            cuenta_map AS (%(cuenta_map)s),
            gl_fixed AS (
                -- Gasto fijo asignado directamente al centro, suavizado
                SELECT m.centro_id,
                       SUM(aml.balance * m.allocation_pct / 100.0)
                           / (SELECT smoothing_months FROM cfg) AS fixed_month
                FROM account_move_line aml
                JOIN cuenta_map m ON m.account_id = aml.account_id
                JOIN cfg ON TRUE
                WHERE m.bucket IN ('mod', 'overhead_fab', 'depreciacion', 'arrend_maquinaria')
                  AND NOT COALESCE(m.es_variable, FALSE)
                  AND m.centro_id IS NOT NULL
                  AND aml.parent_state = 'posted'
                  AND aml.date >= (date_trunc('month', CURRENT_DATE)
                                   - make_interval(months => cfg.smoothing_months::int))
                  AND aml.date < date_trunc('month', CURRENT_DATE)
                GROUP BY m.centro_id
            ),
            cal AS (
                SELECT rc.id AS calendar_id,
                       SUM(att.hour_to - att.hour_from)
                           / CASE WHEN rc.two_weeks_calendar THEN 2.0 ELSE 1.0 END AS hours_week
                FROM resource_calendar rc
                JOIN resource_calendar_attendance att ON att.calendar_id = rc.id
                WHERE COALESCE(att.day_period, '') != 'lunch'
                GROUP BY rc.id, rc.two_weeks_calendar
            ),
            wc_cap AS (
                SELECT rel.centro_id,
                       SUM(COALESCE(cal.hours_week, 0) * cfg.weeks_per_month
                           * COALESCE(NULLIF(wc.time_efficiency, 0), 100) / 100.0) AS hours_month
                FROM qb_centro_workcenter_rel rel
                JOIN mrp_workcenter wc ON wc.id = rel.workcenter_id AND wc.active
                CROSS JOIN cfg
                LEFT JOIN resource_resource rr ON rr.id = wc.resource_id
                LEFT JOIN cal ON cal.calendar_id = rr.calendar_id
                GROUP BY rel.centro_id
            ),
            turno_cap AS (
                SELECT t.centro_id,
                       SUM(t.hours_per_week * cfg.weeks_per_month
                           * GREATEST(t.machine_count, 1)) AS hours_month
                FROM qb_turno_config t
                CROSS JOIN cfg
                WHERE t.active
                GROUP BY t.centro_id
            ),
            wo_prod AS (
                SELECT rel.centro_id,
                       SUM(%(wo_qty)s) / (SELECT window_months FROM cfg) AS qty_month
                FROM qb_centro_workcenter_rel rel
                JOIN mrp_workorder wo ON wo.workcenter_id = rel.workcenter_id
                JOIN cfg ON TRUE
                WHERE wo.state = 'done'
                  AND wo.date_finished >= (date_trunc('month', CURRENT_DATE)
                                           - make_interval(months => cfg.window_months::int))
                  AND wo.date_finished < date_trunc('month', CURRENT_DATE)
                GROUP BY rel.centro_id
            ),
            mo_prod AS (
                SELECT ctr.id AS centro_id,
                       SUM(%(mo_qty)s) / (SELECT window_months FROM cfg) AS qty_month
                FROM qb_costeo_centro ctr
                JOIN mrp_production mp
                     ON mp.name LIKE ANY(string_to_array(ctr.mo_name_pattern, ','))
                JOIN cfg ON TRUE
                WHERE ctr.mo_name_pattern IS NOT NULL
                  AND mp.state = 'done'
                  AND mp.date_finished >= (date_trunc('month', CURRENT_DATE)
                                           - make_interval(months => cfg.window_months::int))
                  AND mp.date_finished < date_trunc('month', CURRENT_DATE)
                GROUP BY ctr.id
            ),
            base AS (
                SELECT
                    ctr.id AS centro_id,
                    ctr.company_id,
                    COALESCE(gl_fixed.fixed_month, 0)
                        + COALESCE(ctr.renta_contractual_mxn, 0) AS fixed_pool_month,
                    CASE WHEN COALESCE(ctr.capacidad_normal, 0) > 0
                         THEN ctr.capacidad_normal
                         ELSE COALESCE(NULLIF(wc_cap.hours_month, 0), turno_cap.hours_month, 0)
                              * COALESCE(ctr.std_output_per_hour, 0)
                    END AS capacity_month_units,
                    -- Producción a nivel ORDEN manda (workorder está mal
                    -- registrado); workorder sólo como fallback.
                    COALESCE(NULLIF(mo_prod.qty_month, 0), wo_prod.qty_month, 0) AS prod_month_units
                FROM qb_costeo_centro ctr
                LEFT JOIN gl_fixed ON gl_fixed.centro_id = ctr.id
                LEFT JOIN wc_cap ON wc_cap.centro_id = ctr.id
                LEFT JOIN turno_cap ON turno_cap.centro_id = ctr.id
                LEFT JOIN wo_prod ON wo_prod.centro_id = ctr.id
                LEFT JOIN mo_prod ON mo_prod.centro_id = ctr.id
                WHERE ctr.active AND ctr.nature != 'admin'
            )
            SELECT
                b.centro_id AS id,
                b.centro_id,
                b.fixed_pool_month,
                b.capacity_month_units,
                b.prod_month_units,
                CASE WHEN b.capacity_month_units > 0
                     THEN LEAST(100.0 * b.prod_month_units / b.capacity_month_units, 100.0)
                     ELSE 0 END AS utilization_pct,
                CASE WHEN b.capacity_month_units > 0
                     THEN GREATEST(100.0 - 100.0 * b.prod_month_units / b.capacity_month_units, 0)
                     ELSE 0 END AS idle_pct,
                CASE WHEN b.capacity_month_units > 0
                     THEN b.fixed_pool_month
                          * GREATEST(1.0 - b.prod_month_units / b.capacity_month_units, 0)
                     ELSE 0 END AS idle_cost_month,
                CASE WHEN b.capacity_month_units > 0
                     THEN b.fixed_pool_month / b.capacity_month_units
                     ELSE 0 END AS fixed_unit_normal,
                CASE WHEN b.prod_month_units > 0
                     THEN b.fixed_pool_month / b.prod_month_units
                     ELSE 0 END AS fixed_unit_real,
                b.company_id
            FROM base b
        """ % {'cuenta_map': CUENTA_MAP_SQL,
               'wo_qty': wo_qty_sql(self.env),
               'mo_qty': mo_qty_sql(self.env)}

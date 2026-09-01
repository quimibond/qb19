# -*- coding: utf-8 -*-
"""Balance de línea / cuello de botella (vista SQL read-only).

Convierte capacidad y producción de cada centro a una unidad común
(metros-equivalentes, con el factor kg↔m de configuración) y marca el
proceso más angosto = techo de la planta.

Centros SIN workcenters todavía (ej. tintorería/acabado hoy): su capacidad
sale de qb.turno.config × throughput nominal, y su producción del patrón de
órdenes (mo_name_pattern, ej. 'TL/OP-ACA%'). Al darlos de alta como
workcenters reales, entran solos por la vía nativa.
"""
from odoo import fields, models

from .cuenta_map import mo_qty_sql, wo_qty_sql


class QbBalance(models.Model):
    _name = 'qb.balance'
    _inherit = 'qb.sql.view'
    _description = 'Balance de línea por proceso (unidad común)'
    _auto = False
    _order = 'capacity_equiv_m'

    centro_id = fields.Many2one('qb.costeo.centro', readonly=True)
    name = fields.Char(related='centro_id.name', readonly=True)
    nature = fields.Char(readonly=True)
    driver_principal = fields.Char(readonly=True)
    capacity_month_units = fields.Float(string='Capacidad/mes (unidad propia)', readonly=True)
    prod_month_units = fields.Float(string='Producción/mes (unidad propia)', readonly=True)
    capacity_equiv_m = fields.Float(
        string='Capacidad/mes (m-equiv)', readonly=True,
        help='Capacidad en metros-equivalentes (kg × m_per_kg_default para '
             'centros con driver peso).')
    prod_equiv_m = fields.Float(string='Producción/mes (m-equiv)', readonly=True)
    utilization_pct = fields.Float(string='Utilización %', readonly=True)
    free_equiv_m = fields.Float(string='Disponible (m-equiv)', readonly=True)
    is_bottleneck = fields.Boolean(
        string='Cuello de botella', readonly=True,
        help='El centro fabril directo con menor capacidad equivalente — '
             'techo de la planta.')
    capacity_source = fields.Char(
        string='Fuente de capacidad', readonly=True,
        help='workcenters = calendario real de mrp.workcenter; '
             'turnos = qb.turno.config (el centro aún no tiene workcenters).')
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
                              WHERE key = 'm_per_kg_default' AND active LIMIT 1), 0), 8.0) AS m_per_kg
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
                -- Capacidad por centro desde sus workcenters (calendario real)
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
                -- Producción real por centro vía sus workcenters
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
                -- Fallback: producción por patrón de órdenes (centros sin WC)
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
                    ctr.nature,
                    ctr.driver_principal,
                    ctr.company_id,
                    CASE WHEN COALESCE(ctr.capacidad_normal, 0) > 0
                         THEN ctr.capacidad_normal
                         ELSE COALESCE(NULLIF(wc_cap.hours_month, 0), turno_cap.hours_month, 0)
                              * COALESCE(ctr.std_output_per_hour, 0)
                    END AS capacity_month_units,
                    -- Producción a nivel ORDEN manda (workorder está mal
                    -- registrado); workorder sólo como fallback.
                    COALESCE(NULLIF(mo_prod.qty_month, 0), wo_prod.qty_month, 0) AS prod_month_units,
                    CASE WHEN COALESCE(wc_cap.hours_month, 0) > 0 THEN 'workcenters'
                         WHEN COALESCE(turno_cap.hours_month, 0) > 0 THEN 'turnos'
                         ELSE 'sin datos' END AS capacity_source,
                    cfg.m_per_kg
                FROM qb_costeo_centro ctr
                CROSS JOIN cfg
                LEFT JOIN wc_cap ON wc_cap.centro_id = ctr.id
                LEFT JOIN turno_cap ON turno_cap.centro_id = ctr.id
                LEFT JOIN wo_prod ON wo_prod.centro_id = ctr.id
                LEFT JOIN mo_prod ON mo_prod.centro_id = ctr.id
                WHERE ctr.active AND ctr.nature != 'admin'
            ),
            equiv AS (
                SELECT b.*,
                    b.capacity_month_units
                        * CASE WHEN b.driver_principal = 'peso' THEN b.m_per_kg ELSE 1 END AS capacity_equiv_m,
                    b.prod_month_units
                        * CASE WHEN b.driver_principal = 'peso' THEN b.m_per_kg ELSE 1 END AS prod_equiv_m
                FROM base b
            )
            SELECT
                e.centro_id AS id,
                e.centro_id,
                e.nature,
                e.driver_principal,
                e.capacity_month_units,
                e.prod_month_units,
                e.capacity_equiv_m,
                e.prod_equiv_m,
                CASE WHEN e.capacity_equiv_m > 0
                     THEN 100.0 * e.prod_equiv_m / e.capacity_equiv_m
                     ELSE 0 END AS utilization_pct,
                GREATEST(e.capacity_equiv_m - e.prod_equiv_m, 0) AS free_equiv_m,
                (e.nature = 'fabril_directo' AND e.capacity_equiv_m > 0
                 AND e.capacity_equiv_m = MIN(CASE WHEN e.nature = 'fabril_directo'
                                                    AND e.capacity_equiv_m > 0
                                                   THEN e.capacity_equiv_m END) OVER ()
                ) AS is_bottleneck,
                e.capacity_source,
                e.company_id
            FROM equiv e
        """ % {'wo_qty': wo_qty_sql(self.env), 'mo_qty': mo_qty_sql(self.env)}

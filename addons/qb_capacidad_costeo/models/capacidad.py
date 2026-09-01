# -*- coding: utf-8 -*-
"""Capacidad por máquina (vista SQL read-only, siempre en vivo).

Horas disponibles = resource.calendar real del workcenter × time_efficiency.
Producción real = mrp.workorder terminadas, promedio de la ventana (3 meses
completos por default, configurable con production_window_months).
Un workcenter nuevo ligado a un centro entra solo; si aún no tiene
workorders aparece con producción 0 (100% disponible) — no rompe nada.
"""
from odoo import fields, models

from .cuenta_map import wo_qty_sql


class QbCapacidad(models.Model):
    _name = 'qb.capacidad'
    _inherit = 'qb.sql.view'
    _description = 'Capacidad por máquina / centro de trabajo'
    _auto = False
    _order = 'centro_id, workcenter_id'

    workcenter_id = fields.Many2one('mrp.workcenter', readonly=True)
    name = fields.Char(related='workcenter_id.name', readonly=True)
    centro_id = fields.Many2one('qb.costeo.centro', readonly=True)
    calendar_id = fields.Many2one('resource.calendar', readonly=True)
    hours_week = fields.Float(string='Horas/semana (calendario)', readonly=True)
    time_efficiency = fields.Float(string='Eficiencia %', readonly=True)
    hours_month_available = fields.Float(string='Horas disponibles/mes', readonly=True)
    std_output_per_hour = fields.Float(string='Throughput nominal (u/h)', readonly=True)
    capacity_month_qty = fields.Float(string='Capacidad/mes (unidades)', readonly=True)
    prod_qty_month_avg = fields.Float(string='Producción/mes (prom.)', readonly=True)
    prod_hours_month_avg = fields.Float(string='Horas trabajadas/mes (prom.)', readonly=True)
    utilization_hours_pct = fields.Float(string='Utilización horas %', readonly=True)
    utilization_qty_pct = fields.Float(string='Utilización unidades %', readonly=True)
    free_hours_month = fields.Float(string='Horas-máquina libres/mes', readonly=True)
    throughput_real_per_hour = fields.Float(
        string='Throughput real (u/h)', readonly=True,
        help='qty producida ÷ horas trabajadas. Comparar vs nominal para '
             'detectar cuellos reales.')
    company_id = fields.Many2one('res.company', readonly=True)

    @property
    def _table_query(self):
        return """
            WITH cfg AS (
                SELECT
                    COALESCE((SELECT value FROM qb_costeo_factor_config
                              WHERE key = 'weeks_per_month' AND active LIMIT 1), 4.33) AS weeks_per_month,
                    COALESCE(NULLIF((SELECT value FROM qb_costeo_factor_config
                              WHERE key = 'production_window_months' AND active LIMIT 1), 0), 3) AS window_months
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
            wc_centro AS (
                SELECT DISTINCT ON (rel.workcenter_id)
                       rel.workcenter_id, ctr.id AS centro_id,
                       ctr.std_output_per_hour
                FROM qb_centro_workcenter_rel rel
                JOIN qb_costeo_centro ctr ON ctr.id = rel.centro_id AND ctr.active
                ORDER BY rel.workcenter_id, ctr.sequence, ctr.id
            ),
            prod AS (
                SELECT wo.workcenter_id,
                       SUM(%(wo_qty)s) / (SELECT window_months FROM cfg) AS qty_month,
                       SUM(COALESCE(wo.duration, 0)) / 60.0
                           / (SELECT window_months FROM cfg) AS hours_month
                FROM mrp_workorder wo
                JOIN cfg ON TRUE
                WHERE wo.state = 'done'
                  AND wo.date_finished >= (date_trunc('month', CURRENT_DATE)
                                           - make_interval(months => cfg.window_months::int))
                  AND wo.date_finished < date_trunc('month', CURRENT_DATE)
                GROUP BY wo.workcenter_id
            )
            SELECT
                wc.id AS id,
                wc.id AS workcenter_id,
                wcc.centro_id,
                rr.calendar_id,
                COALESCE(cal.hours_week, 0) AS hours_week,
                COALESCE(wc.time_efficiency, 100) AS time_efficiency,
                COALESCE(cal.hours_week, 0) * cfg.weeks_per_month
                    * COALESCE(NULLIF(wc.time_efficiency, 0), 100) / 100.0 AS hours_month_available,
                COALESCE(wcc.std_output_per_hour, 0) AS std_output_per_hour,
                COALESCE(cal.hours_week, 0) * cfg.weeks_per_month
                    * COALESCE(NULLIF(wc.time_efficiency, 0), 100) / 100.0
                    * COALESCE(wcc.std_output_per_hour, 0) AS capacity_month_qty,
                COALESCE(p.qty_month, 0) AS prod_qty_month_avg,
                COALESCE(p.hours_month, 0) AS prod_hours_month_avg,
                CASE WHEN COALESCE(cal.hours_week, 0) > 0
                     THEN 100.0 * COALESCE(p.hours_month, 0)
                          / (cal.hours_week * cfg.weeks_per_month
                             * COALESCE(NULLIF(wc.time_efficiency, 0), 100) / 100.0)
                     ELSE 0 END AS utilization_hours_pct,
                CASE WHEN COALESCE(cal.hours_week, 0) * COALESCE(wcc.std_output_per_hour, 0) > 0
                     THEN 100.0 * COALESCE(p.qty_month, 0)
                          / (cal.hours_week * cfg.weeks_per_month
                             * COALESCE(NULLIF(wc.time_efficiency, 0), 100) / 100.0
                             * wcc.std_output_per_hour)
                     ELSE 0 END AS utilization_qty_pct,
                GREATEST(
                    COALESCE(cal.hours_week, 0) * cfg.weeks_per_month
                        * COALESCE(NULLIF(wc.time_efficiency, 0), 100) / 100.0
                    - COALESCE(p.hours_month, 0), 0) AS free_hours_month,
                CASE WHEN COALESCE(p.hours_month, 0) > 0
                     THEN COALESCE(p.qty_month, 0) / p.hours_month
                     ELSE 0 END AS throughput_real_per_hour,
                wc.company_id
            FROM mrp_workcenter wc
            CROSS JOIN cfg
            LEFT JOIN resource_resource rr ON rr.id = wc.resource_id
            LEFT JOIN cal ON cal.calendar_id = rr.calendar_id
            LEFT JOIN wc_centro wcc ON wcc.workcenter_id = wc.id
            LEFT JOIN prod p ON p.workcenter_id = wc.id
            WHERE wc.active
        """ % {'wo_qty': wo_qty_sql(self.env)}

# -*- coding: utf-8 -*-
"""Personal, turnos y costo MOD por hora por centro (vista SQL read-only).

En Odoo 19 el departamento y el salario del empleado viven en hr.version
(delegación vía current_version_id), no en columnas de hr_employee. El campo
wage existe si la instancia trae la parte contractual; si no, el costo MOD
cae al GL (cuentas bucket=mod del centro) — por eso la vista expone ambos:
mod_hour_wages (desde sueldos) y mod_hour_gl (desde contabilidad).
"""
from odoo import fields, models

from .cuenta_map import CUENTA_MAP_SQL


class QbRhCentro(models.Model):
    _name = 'qb.rh.centro'
    _description = 'RH por centro: dotación, horas y costo MOD/hora'
    _auto = False
    _order = 'centro_id'

    centro_id = fields.Many2one('qb.costeo.centro', readonly=True)
    name = fields.Char(related='centro_id.name', readonly=True)
    employee_count = fields.Integer(string='Empleados', readonly=True)
    dotacion_ajuste = fields.Float(string='Ajuste de dotación', readonly=True)
    hours_month = fields.Float(
        string='Horas/mes del centro', readonly=True,
        help='Horas de calendario de sus workcenters; si no tiene, '
             'horas de qb.turno.config.')
    wage_month_total = fields.Float(
        string='Nómina mensual (sueldos)', readonly=True,
        help='Σ wage de hr.version vigente de los empleados del centro. '
             '0 si la instancia no captura sueldos — usar entonces el GL.')
    gl_mod_month = fields.Float(
        string='MOD según GL (prom. 3m)', readonly=True,
        help='Cuentas bucket=mod asignadas al centro, promedio de los '
             'últimos 3 meses completos.')
    mod_hour_wages = fields.Float(string='MOD $/hora (sueldos)', readonly=True)
    mod_hour_gl = fields.Float(string='MOD $/hora (GL)', readonly=True)
    company_id = fields.Many2one('res.company', readonly=True)

    @property
    def _table_query(self):
        # wage vive en hr.version solo si la instancia trae la parte
        # contractual — degradar con gracia si no existe modelo o columna.
        has_version = 'hr.version' in self.env
        has_wage = has_version and 'wage' in self.env['hr.version']._fields
        wage_expr = 'COALESCE(ver.wage, 0)' if has_wage else '0'
        if has_version:
            emp_join = ('JOIN hr_version ver ON ver.id = e.current_version_id '
                        'AND ver.department_id = rel.department_id')
        else:
            # Pre-Odoo-19: el departamento vive directo en hr_employee.
            emp_join = ('JOIN hr_employee ver ON ver.id = e.id '
                        'AND ver.department_id = rel.department_id')
        return """
            WITH cfg AS (
                SELECT COALESCE((SELECT value FROM qb_costeo_factor_config
                                 WHERE key = 'weeks_per_month' AND active LIMIT 1), 4.33) AS weeks_per_month
            ),
            cuenta_map AS (%(cuenta_map)s),
            emp AS (
                SELECT rel.centro_id,
                       COUNT(e.id) AS employee_count,
                       SUM(%(wage_expr)s) AS wage_month_total
                FROM qb_centro_department_rel rel
                JOIN hr_employee e ON e.active
                %(emp_join)s
                GROUP BY rel.centro_id
            ),
            wc_hours AS (
                SELECT rel.centro_id,
                       SUM(
                           COALESCE(cal.hours_week, 0) * cfg.weeks_per_month
                           * COALESCE(NULLIF(wc.time_efficiency, 0), 100) / 100.0
                       ) AS hours_month
                FROM qb_centro_workcenter_rel rel
                JOIN mrp_workcenter wc ON wc.id = rel.workcenter_id AND wc.active
                CROSS JOIN cfg
                LEFT JOIN resource_resource rr ON rr.id = wc.resource_id
                LEFT JOIN (
                    SELECT rc.id AS calendar_id,
                           SUM(att.hour_to - att.hour_from)
                               / CASE WHEN rc.two_weeks_calendar THEN 2.0 ELSE 1.0 END AS hours_week
                    FROM resource_calendar rc
                    JOIN resource_calendar_attendance att ON att.calendar_id = rc.id
                    WHERE COALESCE(att.day_period, '') != 'lunch'
                    GROUP BY rc.id, rc.two_weeks_calendar
                ) cal ON cal.calendar_id = rr.calendar_id
                GROUP BY rel.centro_id
            ),
            turno AS (
                SELECT t.centro_id,
                       SUM(t.hours_per_week * cfg.weeks_per_month
                           * GREATEST(t.machine_count, 1)) AS hours_month,
                       SUM(t.dotacion_ajuste) AS dotacion_ajuste
                FROM qb_turno_config t
                CROSS JOIN cfg
                WHERE t.active
                GROUP BY t.centro_id
            ),
            gl_mod AS (
                SELECT m.centro_id,
                       SUM(aml.balance * m.allocation_pct / 100.0) / 3.0 AS mod_month
                FROM account_move_line aml
                JOIN cuenta_map m ON m.account_id = aml.account_id
                WHERE m.bucket = 'mod'
                  AND m.centro_id IS NOT NULL
                  AND aml.parent_state = 'posted'
                  AND aml.date >= (date_trunc('month', CURRENT_DATE) - INTERVAL '3 months')
                  AND aml.date < date_trunc('month', CURRENT_DATE)
                GROUP BY m.centro_id
            )
            SELECT
                ctr.id AS id,
                ctr.id AS centro_id,
                COALESCE(emp.employee_count, 0) AS employee_count,
                COALESCE(turno.dotacion_ajuste, 0) AS dotacion_ajuste,
                COALESCE(NULLIF(wc_hours.hours_month, 0), turno.hours_month, 0) AS hours_month,
                COALESCE(emp.wage_month_total, 0) AS wage_month_total,
                COALESCE(gl_mod.mod_month, 0) AS gl_mod_month,
                CASE WHEN COALESCE(NULLIF(wc_hours.hours_month, 0), turno.hours_month, 0) > 0
                     THEN COALESCE(emp.wage_month_total, 0)
                          / COALESCE(NULLIF(wc_hours.hours_month, 0), turno.hours_month)
                     ELSE 0 END AS mod_hour_wages,
                CASE WHEN COALESCE(NULLIF(wc_hours.hours_month, 0), turno.hours_month, 0) > 0
                     THEN COALESCE(gl_mod.mod_month, 0)
                          / COALESCE(NULLIF(wc_hours.hours_month, 0), turno.hours_month)
                     ELSE 0 END AS mod_hour_gl,
                ctr.company_id
            FROM qb_costeo_centro ctr
            LEFT JOIN emp ON emp.centro_id = ctr.id
            LEFT JOIN wc_hours ON wc_hours.centro_id = ctr.id
            LEFT JOIN turno ON turno.centro_id = ctr.id
            LEFT JOIN gl_mod ON gl_mod.centro_id = ctr.id
            WHERE ctr.active
        """ % {'cuenta_map': CUENTA_MAP_SQL, 'wage_expr': wage_expr,
               'emp_join': emp_join}

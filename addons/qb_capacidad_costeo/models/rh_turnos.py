# -*- coding: utf-8 -*-
"""Personal, turnos y costo MOD por hora por centro (vista SQL read-only).

En Odoo 19 el departamento y el salario del empleado viven en hr.version
(delegación vía current_version_id), no en columnas de hr_employee.

El MOD REAL por centro se arma en dos capas:

1. Directo: cuentas bucket=mod con centro asignado en Clasificación de
   cuentas (cuando se conoce el reparto exacto, eso manda).
2. Prorrateado: el resto del pool de nómina del GL (las cuentas mod SIN
   centro — en la práctica toda la nómina 501.06.*, ~$3.1M/mes) se
   reparte entre centros proporcional a su MASA SALARIAL de RH,
   normalizada a mensual por la periodicidad de pago de cada empleado
   (hay sueldos capturados por semana y por mes revueltos; crudos sumaban
   $536k/mes contra $3.1M reales). Un empleado sin sueldo capturado pesa
   como el promedio de los que sí lo tienen.

La columna de sueldos de RH queda como referencia; el número de costeo es
el del GL, que es la nómina que de verdad se pagó (con carga social).
"""
from odoo import fields, models

from .cuenta_map import CUENTA_MAP_SQL, cfg_sql


class QbRhCentro(models.Model):
    _name = 'qb.rh.centro'
    _inherit = 'qb.sql.view'
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
        string='Sueldos RH (normalizado/mes)', readonly=True,
        help='Σ wage de hr.version vigente de los empleados del centro, '
             'normalizado a mensual por la periodicidad de pago '
             '(semanal ×4.33, quincenal ×2.165, etc.). Solo sueldo base y '
             'solo lo capturado: es la REFERENCIA para el reparto, no el '
             'costo — el costo real es el del GL.')
    gl_mod_directo = fields.Float(
        string='MOD directo (GL)', readonly=True,
        help='Cuentas bucket=mod ASIGNADAS a este centro en Clasificación '
             'de cuentas, promedio 3 meses. Cuando se conoce el reparto '
             'exacto, esto manda sobre el prorrateo.')
    gl_mod_prorrateado = fields.Float(
        string='MOD prorrateado (GL)', readonly=True,
        help='Parte del pool de nómina del GL sin centro asignado que le '
             'toca a este centro, proporcional a su masa salarial '
             'normalizada (empleado sin sueldo capturado pesa como el '
             'promedio).')
    nomina_share_pct = fields.Float(
        string='Participación nómina (pct)', readonly=True,
        help='Peso de este centro en el reparto del pool de nómina, en '
             'puntos porcentuales. La suma de todos los centros es 100.')
    gl_mod_month = fields.Float(
        string='MOD del centro (GL, prom. 3m)', readonly=True,
        help='Directo + prorrateado: la nómina REAL del GL (con carga '
             'social) que le toca al centro, promedio de los últimos 3 '
             'meses completos.')
    mod_hour_wages = fields.Float(string='MOD $/hora (sueldos)', readonly=True)
    mod_hour_gl = fields.Float(string='MOD $/hora (GL)', readonly=True)
    company_id = fields.Many2one('res.company', readonly=True)

    @property
    def _table_query(self):
        # wage vive en hr.version solo si la instancia trae la parte
        # contractual — degradar con gracia si no existe modelo o columna.
        has_version = 'hr.version' in self.env
        has_wage = has_version and 'wage' in self.env['hr.version']._fields
        has_sched = has_wage and \
            'schedule_pay' in self.env['hr.version']._fields
        if has_sched:
            # Normalizar a MENSUAL por periodicidad de pago: en la práctica
            # hay sueldos capturados por semana y por mes revueltos, y
            # sumarlos crudos daba una "nómina" imposible.
            wage_expr = """CASE ver.schedule_pay
                    WHEN 'annually' THEN COALESCE(ver.wage, 0) / 12.0
                    WHEN 'semi-annually' THEN COALESCE(ver.wage, 0) / 6.0
                    WHEN 'quarterly' THEN COALESCE(ver.wage, 0) / 3.0
                    WHEN 'bi-monthly' THEN COALESCE(ver.wage, 0) / 2.0
                    WHEN 'semi-monthly' THEN COALESCE(ver.wage, 0) * 2.0
                    WHEN 'bi-weekly' THEN COALESCE(ver.wage, 0) * 2.165
                    WHEN 'weekly' THEN COALESCE(ver.wage, 0) * 4.33
                    WHEN 'daily' THEN COALESCE(ver.wage, 0) * 30.4
                    ELSE COALESCE(ver.wage, 0)
                END"""
        elif has_wage:
            wage_expr = 'COALESCE(ver.wage, 0)'
        else:
            wage_expr = '0'
        if has_version:
            emp_join = ('JOIN hr_version ver ON ver.id = e.current_version_id '
                        'AND ver.department_id = rel.department_id')
        else:
            # Pre-Odoo-19: el departamento vive directo en hr_employee.
            emp_join = ('JOIN hr_employee ver ON ver.id = e.id '
                        'AND ver.department_id = rel.department_id')
        return """
            {cfg},
            cuenta_map AS (%(cuenta_map)s),
            emp_detail AS (
                SELECT rel.centro_id, e.id AS emp_id,
                       %(wage_expr)s AS wage_month
                FROM qb_centro_department_rel rel
                JOIN hr_employee e ON e.active
                                  AND e.company_id = %(company_id)s
                %(emp_join)s
            ),
            avg_wage AS (
                -- promedio de los sueldos capturados: el peso de reparto de
                -- un empleado SIN sueldo (mejor que ignorarlo o contarlo 0)
                SELECT COALESCE(AVG(NULLIF(wage_month, 0)), 0) AS avg_month
                FROM emp_detail
            ),
            emp AS (
                SELECT d.centro_id,
                       COUNT(*) AS employee_count,
                       SUM(d.wage_month) AS wage_month_total,
                       SUM(COALESCE(NULLIF(d.wage_month, 0),
                                    a.avg_month, 0)) AS peso
                FROM emp_detail d
                CROSS JOIN avg_wage a
                GROUP BY d.centro_id
            ),
            peso_total AS (
                SELECT COALESCE(SUM(peso), 0) AS total,
                       COALESCE(SUM(employee_count), 0) AS emps
                FROM emp
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
                -- MOD directo: cuentas mod ASIGNADAS a un centro
                SELECT m.centro_id,
                       SUM(aml.balance * m.allocation_pct / 100.0) / 3.0 AS mod_month
                FROM account_move_line aml
                JOIN cuenta_map m ON m.account_id = aml.account_id
                WHERE m.bucket = 'mod'
                  AND m.centro_id IS NOT NULL
                  AND aml.parent_state = 'posted'
                  AND aml.company_id = %(company_id)s
                  AND aml.date >= (date_trunc('month', CURRENT_DATE) - INTERVAL '3 months')
                  AND aml.date < date_trunc('month', CURRENT_DATE)
                GROUP BY m.centro_id
            ),
            gl_mod_pool AS (
                -- Pool de nómina SIN centro asignado (en la práctica todas
                -- las 501.06.*): se prorratea por masa salarial
                SELECT COALESCE(SUM(aml.balance * m.allocation_pct / 100.0)
                                / 3.0, 0) AS pool
                FROM account_move_line aml
                JOIN cuenta_map m ON m.account_id = aml.account_id
                WHERE m.bucket = 'mod'
                  AND m.centro_id IS NULL
                  AND aml.parent_state = 'posted'
                  AND aml.company_id = %(company_id)s
                  AND aml.date >= (date_trunc('month', CURRENT_DATE) - INTERVAL '3 months')
                  AND aml.date < date_trunc('month', CURRENT_DATE)
            ),
            base AS (
                SELECT
                    ctr.id,
                    ctr.company_id,
                    COALESCE(emp.employee_count, 0) AS employee_count,
                    COALESCE(turno.dotacion_ajuste, 0) AS dotacion_ajuste,
                    COALESCE(NULLIF(wc_hours.hours_month, 0), turno.hours_month, 0) AS hours_month,
                    COALESCE(emp.wage_month_total, 0) AS wage_month_total,
                    COALESCE(gl_mod.mod_month, 0) AS gl_mod_directo,
                    CASE WHEN pt.total > 0
                         THEN pool.pool * COALESCE(emp.peso, 0) / pt.total
                         ELSE 0 END AS gl_mod_prorrateado,
                    CASE WHEN pt.total > 0
                         THEN 100.0 * COALESCE(emp.peso, 0) / pt.total
                         ELSE 0 END AS nomina_share_pct
                FROM qb_costeo_centro ctr
                CROSS JOIN peso_total pt
                CROSS JOIN gl_mod_pool pool
                LEFT JOIN emp ON emp.centro_id = ctr.id
                LEFT JOIN wc_hours ON wc_hours.centro_id = ctr.id
                LEFT JOIN turno ON turno.centro_id = ctr.id
                LEFT JOIN gl_mod ON gl_mod.centro_id = ctr.id
                WHERE ctr.active
                  AND ctr.company_id = %(company_id)s
            )
            SELECT
                b.id AS id,
                b.id AS centro_id,
                b.employee_count,
                b.dotacion_ajuste,
                b.hours_month,
                b.wage_month_total,
                b.gl_mod_directo,
                b.gl_mod_prorrateado,
                b.nomina_share_pct,
                b.gl_mod_directo + b.gl_mod_prorrateado AS gl_mod_month,
                CASE WHEN b.hours_month > 0
                     THEN b.wage_month_total / b.hours_month
                     ELSE 0 END AS mod_hour_wages,
                CASE WHEN b.hours_month > 0
                     THEN (b.gl_mod_directo + b.gl_mod_prorrateado)
                          / b.hours_month
                     ELSE 0 END AS mod_hour_gl,
                b.company_id
            FROM base b
        """.replace(
            '{cfg}', cfg_sql('weeks_per_month')
        ) % {'cuenta_map': CUENTA_MAP_SQL, 'wage_expr': wage_expr,
               'emp_join': emp_join,
               'company_id': int(self.env.company.id)}

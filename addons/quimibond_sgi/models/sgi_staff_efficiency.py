# -*- coding: utf-8 -*-
"""Eficiencias de personal (56.0.0).

Sustituye F-P-A01-32 «Eficiencias de personal» y F-P-A01-34 «Concentrado de
eficiencias»: la calificación mensual por empleado con la que RH paga el
incentivo (asistencia 5.5 %, orden y limpieza 2 %, eficiencia 2 %, calidad
2 % sobre el salario mensual). El porcentaje de eficiencia se propone desde
las órdenes de trabajo del mes (tiempo esperado / tiempo real registrado por
el empleado en mrp.workcenter.productivity); el jefe de área lo ajusta.
Cada fila queda ligada al empleado; nada se calcula en hojas sueltas.
"""

from dateutil.relativedelta import relativedelta

from odoo import api, fields, models
from odoo.exceptions import ValidationError

MAX_ATTENDANCE, MAX_HOUSEKEEPING, MAX_EFFICIENCY, MAX_QUALITY = 5.5, 2.0, 2.0, 2.0


class SgiStaffEfficiency(models.Model):
    _name = 'sgi.staff.efficiency'
    _description = "Eficiencias de personal (F-P-A01-32/34)"
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _order = 'period_date desc, id desc'

    name = fields.Char(compute='_compute_name', store=True)
    period_date = fields.Date(string="Mes", required=True,
                              default=lambda self: fields.Date.context_today(self).replace(day=1))
    department_id = fields.Many2one('hr.department', string="Área")
    prepared_by_id = fields.Many2one('res.users', string="Elaboró (jefe de área)", default=lambda self: self.env.user)
    received_by_id = fields.Many2one('res.users', string="Recibió (coordinador de RH)")
    state = fields.Selection([('borrador', "Borrador"), ('cerrado', "Cerrado")], default='borrador', required=True, tracking=True)
    line_ids = fields.One2many('sgi.staff.efficiency.line', 'sheet_id', string="Empleados")
    employee_count = fields.Integer(compute='_compute_totals')
    amount_total = fields.Monetary(compute='_compute_totals', currency_field='currency_id')
    currency_id = fields.Many2one('res.currency', default=lambda self: self.env.company.currency_id)
    company_id = fields.Many2one('res.company', default=lambda self: self.env.company)
    note = fields.Text(string="Observaciones")

    @api.depends('period_date', 'department_id')
    def _compute_name(self):
        for sheet in self:
            month = sheet.period_date.strftime('%m/%Y') if sheet.period_date else '—'
            sheet.name = "Eficiencias %s%s" % (month, " · %s" % sheet.department_id.name if sheet.department_id else "")

    @api.depends('line_ids.amount')
    def _compute_totals(self):
        for sheet in self:
            sheet.employee_count = len(sheet.line_ids)
            sheet.amount_total = sum(sheet.line_ids.mapped('amount'))

    def _period_bounds(self):
        self.ensure_one()
        start = self.period_date.replace(day=1)
        return start, start + relativedelta(months=1)

    def action_load_employees(self):
        """Agrega los empleados activos del área que aún no están en la hoja."""
        Employee = self.env['hr.employee']
        for sheet in self:
            domain = [('company_id', '=', sheet.company_id.id)]
            if sheet.department_id:
                domain.append(('department_id', 'child_of', sheet.department_id.id))
            present = set(sheet.line_ids.mapped('employee_id').ids)
            vals = [(0, 0, {'employee_id': emp.id}) for emp in Employee.search(domain) if emp.id not in present]
            if vals:
                sheet.write({'line_ids': vals})
        return True

    def action_compute_efficiency(self):
        """Eficiencia = tiempo esperado de las órdenes de trabajo / tiempo real
        registrado por el empleado en el mes. Propone el % (tope 2 %)."""
        Productivity = self.env['mrp.workcenter.productivity'].sudo()
        for sheet in self:
            start, end = sheet._period_bounds()
            start_dt = fields.Datetime.to_datetime(start)
            end_dt = fields.Datetime.to_datetime(end)
            employees = sheet.line_ids.mapped('employee_id')
            if not employees:
                continue
            logs = Productivity.search([
                ('employee_id', 'in', employees.ids), ('date_start', '>=', start_dt),
                ('date_start', '<', end_dt), ('workorder_id', '!=', False)])
            real = {}
            expected = {}
            for log in logs:
                emp = log.employee_id.id
                real[emp] = real.get(emp, 0.0) + (log.duration or 0.0)
                wo = log.workorder_id
                # El tiempo esperado se reparte proporcional al tiempo real que
                # el empleado puso en esa orden (varios operadores por orden).
                if wo.duration and wo.duration_expected:
                    expected[emp] = expected.get(emp, 0.0) + wo.duration_expected * (log.duration or 0.0) / wo.duration
            for line in sheet.line_ids:
                r = real.get(line.employee_id.id, 0.0)
                e = expected.get(line.employee_id.id, 0.0)
                ratio = (e / r) if r else 0.0
                line.write({
                    'real_minutes': r, 'expected_minutes': e, 'efficiency_ratio': ratio,
                    'efficiency_pct': round(min(1.0, ratio) * MAX_EFFICIENCY, 2) if r else line.efficiency_pct,
                })
        return True

    def action_close(self):
        self.write({'state': 'cerrado'})
        return True

    def action_reopen(self):
        self.write({'state': 'borrador'})
        return True

    def sgi_format_info(self):
        self.ensure_one()
        code = 'F-P-A01-34' if self.department_id else 'F-P-A01-32'
        revision = self.env['sgi.format.map'].sudo()._revision_of(code)
        return "%s · Rev. %s" % (code, revision) if revision else code


class SgiStaffEfficiencyLine(models.Model):
    _name = 'sgi.staff.efficiency.line'
    _description = "Calificación mensual de un empleado"
    _order = 'employee_id'

    sheet_id = fields.Many2one('sgi.staff.efficiency', required=True, ondelete='cascade')
    employee_id = fields.Many2one('hr.employee', string="Empleado", required=True, ondelete='restrict')
    job_id = fields.Many2one(related='employee_id.job_id', string="Puesto", store=True)
    department_id = fields.Many2one(related='employee_id.department_id', string="Área", store=True)
    wage_daily = fields.Monetary(string="Salario diario", currency_field='currency_id',
                                 compute='_compute_wage_daily', store=True, readonly=False)
    wage_monthly = fields.Monetary(string="Salario mensual (×30)", compute='_compute_wage_monthly', currency_field='currency_id')
    attendance_pct = fields.Float(string="Asistencia (máx. 5.5 %)", digits=(5, 2))
    housekeeping_pct = fields.Float(string="Orden y limpieza (máx. 2 %)", digits=(5, 2))
    efficiency_pct = fields.Float(string="Eficiencia (máx. 2 %)", digits=(5, 2))
    quality_pct = fields.Float(string="Calidad (máx. 2 %)", digits=(5, 2))
    total_pct = fields.Float(string="Total (%)", compute='_compute_amounts', digits=(5, 2), store=True)
    amount = fields.Monetary(string="A pagar", compute='_compute_amounts', currency_field='currency_id', store=True)
    real_minutes = fields.Float(string="Tiempo real (min)", readonly=True)
    expected_minutes = fields.Float(string="Tiempo esperado (min)", readonly=True)
    efficiency_ratio = fields.Float(string="Esperado / real", readonly=True, digits=(6, 2))
    currency_id = fields.Many2one(related='sheet_id.currency_id')
    note = fields.Char(string="Observaciones")

    _employee_uniq = models.Constraint(
        'unique(sheet_id, employee_id)',
        "Cada empleado va una sola vez por hoja de eficiencias.",
    )

    @api.depends('employee_id')
    def _compute_wage_daily(self):
        for line in self:
            if line.employee_id and not line.wage_daily:
                wage = line.employee_id.sudo().wage if 'wage' in line.employee_id._fields else 0.0
                line.wage_daily = round((wage or 0.0) / 30.0, 2)

    @api.depends('wage_daily')
    def _compute_wage_monthly(self):
        # Método aparte del de total_pct/amount: esos se guardan y este no, y Odoo
        # avisa en el build cuando un mismo compute mezcla campos store y no store.
        for line in self:
            line.wage_monthly = (line.wage_daily or 0.0) * 30.0

    @api.depends('wage_daily', 'attendance_pct', 'housekeeping_pct', 'efficiency_pct', 'quality_pct')
    def _compute_amounts(self):
        for line in self:
            line.total_pct = (line.attendance_pct or 0.0) + (line.housekeeping_pct or 0.0) + \
                (line.efficiency_pct or 0.0) + (line.quality_pct or 0.0)
            line.amount = (line.wage_daily or 0.0) * 30.0 * line.total_pct / 100.0

    @api.constrains('attendance_pct', 'housekeeping_pct', 'efficiency_pct', 'quality_pct')
    def _check_maxima(self):
        for line in self:
            for value, top, label in ((line.attendance_pct, MAX_ATTENDANCE, "Asistencia"),
                                      (line.housekeeping_pct, MAX_HOUSEKEEPING, "Orden y limpieza"),
                                      (line.efficiency_pct, MAX_EFFICIENCY, "Eficiencia"),
                                      (line.quality_pct, MAX_QUALITY, "Calidad")):
                if value < 0 or value > top + 1e-9:
                    raise ValidationError("%s de %s: el porcentaje va de 0 a %s." % (label, line.employee_id.name, top))

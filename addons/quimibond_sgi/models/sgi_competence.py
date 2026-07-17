# -*- coding: utf-8 -*-
from odoo import models, fields, api, tools


class HrEmployeeCompetence(models.Model):
    _inherit = 'hr.employee'

    sgi_skill_gap_count = fields.Integer(string="Brechas de competencia",
                                         compute='_compute_sgi_skill_gap_count')

    def _compute_sgi_skill_gap_count(self):
        data = self.env['sgi.competence.gap']._read_group(
            [('employee_id', 'in', self.ids)], ['employee_id'], ['__count'])
        mapped = {employee.id: count for employee, count in data}
        for employee in self:
            employee.sgi_skill_gap_count = mapped.get(employee.id, 0)

    def action_view_skill_gaps(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Brechas de competencia",
            'res_model': 'sgi.competence.gap',
            'view_mode': 'list,pivot',
            'domain': [('employee_id', '=', self.id)],
            'context': {'search_default_group_skill_type': 1},
        }


class SgiCompetenceGap(models.Model):
    """Vista SQL: brechas entre las competencias esperadas del puesto
    (hr.job.skill) y las que tiene el empleado (hr.employee.skill)."""
    _name = 'sgi.competence.gap'
    _description = "Brecha de competencia (DNC)"
    _auto = False
    _order = 'department_id, employee_id'

    employee_id = fields.Many2one('hr.employee', string="Empleado", readonly=True)
    department_id = fields.Many2one('hr.department', string="Departamento", readonly=True)
    job_id = fields.Many2one('hr.job', string="Puesto", readonly=True)
    skill_id = fields.Many2one('hr.skill', string="Competencia", readonly=True)
    skill_type_id = fields.Many2one('hr.skill.type', string="Tipo", readonly=True)
    required_level_id = fields.Many2one('hr.skill.level', string="Nivel requerido", readonly=True)
    current_level_id = fields.Many2one('hr.skill.level', string="Nivel actual", readonly=True)
    required_progress = fields.Integer(string="% requerido", readonly=True)
    current_progress = fields.Integer(string="% actual", readonly=True)
    gap = fields.Integer(string="Brecha (%)", readonly=True)

    @property
    def _table_query(self):
        # En Odoo 19 el puesto y el departamento del empleado viven en hr.version
        # (delegación vía current_version_id), no en columnas de hr_employee.
        return """
            SELECT
                row_number() OVER () AS id,
                emp.id AS employee_id,
                ver.department_id AS department_id,
                ver.job_id AS job_id,
                js.skill_id AS skill_id,
                js.skill_type_id AS skill_type_id,
                js.skill_level_id AS required_level_id,
                es.skill_level_id AS current_level_id,
                COALESCE(rl.level_progress, 0) AS required_progress,
                COALESCE(cl.level_progress, 0) AS current_progress,
                COALESCE(rl.level_progress, 0) - COALESCE(cl.level_progress, 0) AS gap
            FROM hr_employee emp
            JOIN hr_version ver ON ver.id = emp.current_version_id
            JOIN hr_job_skill js ON js.job_id = ver.job_id
            LEFT JOIN hr_employee_skill es
                ON es.employee_id = emp.id
               AND es.skill_id = js.skill_id
               AND (es.valid_to IS NULL OR es.valid_to >= CURRENT_DATE)
            LEFT JOIN hr_skill_level rl ON rl.id = js.skill_level_id
            LEFT JOIN hr_skill_level cl ON cl.id = es.skill_level_id
            WHERE emp.active = TRUE
              AND COALESCE(cl.level_progress, 0) < COALESCE(rl.level_progress, 0)
        """

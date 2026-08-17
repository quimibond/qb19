# -*- coding: utf-8 -*-
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestCompetence(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.skill_type = cls.env['hr.skill.type'].create({'name': 'Técnica SGI'})
        cls.level_basic = cls.env['hr.skill.level'].create({
            'skill_type_id': cls.skill_type.id, 'name': 'Básico', 'level_progress': 20})
        cls.level_expert = cls.env['hr.skill.level'].create({
            'skill_type_id': cls.skill_type.id, 'name': 'Experto', 'level_progress': 90})
        cls.skill = cls.env['hr.skill'].create({
            'name': 'Operación de extrusora', 'skill_type_id': cls.skill_type.id})
        cls.job = cls.env['hr.job'].create({'name': 'Operador de extrusión'})
        cls.env['hr.job.skill'].create({
            'job_id': cls.job.id,
            'skill_id': cls.skill.id,
            'skill_type_id': cls.skill_type.id,
            'skill_level_id': cls.level_expert.id,
        })
        cls.employee = cls.env['hr.employee'].create({
            'name': 'Operador Sin Skill', 'job_id': cls.job.id})

    def test_01_gap_detected(self):
        # El empleado no tiene la competencia -> brecha respecto al nivel experto.
        gaps = self.env['sgi.competence.gap'].search([
            ('employee_id', '=', self.employee.id)])
        self.assertTrue(gaps, "Debe detectarse una brecha de competencia.")
        self.assertEqual(gaps[0].skill_id, self.skill)
        self.assertEqual(self.employee.sgi_skill_gap_count, len(gaps))

    def test_02_no_gap_when_meets_level(self):
        # Al asignar la competencia al nivel experto, la brecha desaparece.
        self.env['hr.employee.skill'].create({
            'employee_id': self.employee.id,
            'skill_id': self.skill.id,
            'skill_type_id': self.skill_type.id,
            'skill_level_id': self.level_expert.id,
        })
        self.employee.invalidate_recordset(['sgi_skill_gap_count'])
        gaps = self.env['sgi.competence.gap'].search([
            ('employee_id', '=', self.employee.id)])
        self.assertFalse(gaps, "Sin brecha cuando el empleado cumple el nivel.")

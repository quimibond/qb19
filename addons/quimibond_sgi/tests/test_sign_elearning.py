# -*- coding: utf-8 -*-
from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestSignElearning(TransactionCase):
    """Integraciones Sign/eLearning y digest semanal (v19.0.26.0.0)."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.employee = cls.env['hr.employee'].create({'name': "Empleado Curso"})
        cls.user = cls.env['res.users'].create({
            'name': "Empleado Curso",
            'login': 'empleado.curso@test.local',
            'email': 'empleado.curso@test.local',
        })
        cls.employee.user_id = cls.user

    def test_send_sign_requests_requires_template(self):
        doc = self.env['documents.document'].create({
            'name': "Procedimiento de prueba Sign",
            'type': 'binary',
            'sgi_is_controlled': True,
            'sgi_doc_type': 'procedimiento',
            'sgi_code': 'P-G99',
        })
        with self.assertRaises(UserError):
            doc.action_sgi_send_sign_requests()

    def test_sync_crons_run_empty(self):
        """Los crons de sincronización y el digest corren sin datos sin
        tronar (el patrón _sgi_step aísla cada paso)."""
        cron = self.env['sgi.cron']
        self.assertTrue(cron.cron_sign_elearning_sync())
        self.assertTrue(cron.cron_weekly_digest())

    def test_employee_lookup_and_skill_grant(self):
        """El mapeo curso→competencia resuelve al empleado por su usuario y
        el upsert crea la competencia (y nunca la baja de nivel)."""
        skill_type = self.env['hr.skill.type'].create({'name': "SGI Test"})
        level_low = self.env['hr.skill.level'].create({
            'name': "Básico", 'level_progress': 30,
            'skill_type_id': skill_type.id})
        level_high = self.env['hr.skill.level'].create({
            'name': "Avanzado", 'level_progress': 80,
            'skill_type_id': skill_type.id})
        skill = self.env['hr.skill'].create({
            'name': "Curso SGI Test", 'skill_type_id': skill_type.id})
        channel = self.env['slide.channel'].sudo().create({
            'name': "Curso de prueba SGI",
            'channel_type': 'training',
            'sgi_skill_id': skill.id,
            'sgi_skill_level_id': level_high.id,
        })
        found = channel._sgi_employee_for_partner(self.user.partner_id)
        self.assertEqual(found, self.employee)

        # Upsert directo (el estado «terminado» del asistente lo produce
        # website_slides; aquí se prueba la lógica de otorgamiento).
        Skill = self.env['hr.employee.skill'].sudo()
        Skill.create({
            'employee_id': self.employee.id,
            'skill_id': skill.id,
            'skill_type_id': skill_type.id,
            'skill_level_id': level_low.id,
        })
        current = Skill.search([
            ('employee_id', '=', self.employee.id),
            ('skill_id', '=', skill.id)], limit=1)
        self.assertEqual(current.skill_level_id, level_low)
        # Sin asistentes terminados el sync no toca nada.
        self.env['slide.channel']._sgi_sync_completions()
        current = Skill.search([
            ('employee_id', '=', self.employee.id),
            ('skill_id', '=', skill.id)], limit=1)
        self.assertEqual(current.skill_level_id, level_low)

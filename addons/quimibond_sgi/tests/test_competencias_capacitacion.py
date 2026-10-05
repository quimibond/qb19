# -*- coding: utf-8 -*-
"""57.100.0 (N-13, ISO 9001 7.2): certificación aprobada o curso terminado →
competencia del empleado con vigencia; eficacia de la capacitación a los 90
días con el jefe inmediato (7.2 c).

Las pruebas crean su tipo de competencia, sus empleados, su examen y su curso
(nombres Z13): la base del build es copia de producción. Fechas en 2046 donde
importa."""
from datetime import date, timedelta

from odoo import fields
from odoo.exceptions import AccessError, UserError
from odoo.tests import TransactionCase, new_test_user, tagged

from .common_users import sgi_set_mast


@tagged('post_install', '-at_install')
class TestCompetenciasCapacitacion(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.mast = sgi_set_mast(cls.env, login='sgi_mast_n13')
        cls.boss_user = new_test_user(cls.env, login='z13_jefe', groups='base.group_user')
        cls.company = cls.env['sgi.config']._sgi_company()
        cls.boss = cls.env['hr.employee'].create({
            'name': 'Jefe Z13', 'user_id': cls.boss_user.id, 'company_id': cls.company.id})
        cls.emp = cls.env['hr.employee'].create({
            'name': 'Empleado Z13', 'parent_id': cls.boss.id, 'company_id': cls.company.id})
        # Tipo de certificación: Odoo guarda su «válida hasta» tal cual y
        # permite un renglón por periodo (las aserciones de vigencia no
        # dependen de cómo trate Odoo los tipos normales). El tipo normal se
        # prueba aparte (_regular_type).
        cls.stype = cls.env['hr.skill.type'].create({
            'name': 'Z13 Capacitación', 'is_certification': True,
            'skill_ids': [(0, 0, {'name': 'Z13 Extintores'})],
            'skill_level_ids': [
                (0, 0, {'name': 'Z13 Básico', 'level_progress': 50}),
                (0, 0, {'name': 'Z13 Capacitado', 'level_progress': 100, 'default_level': True}),
            ]})
        cls.skill = cls.stype.skill_ids
        cls.level = cls.stype.skill_level_ids.filtered(lambda lv: lv.level_progress == 100)
        cls.level_low = cls.stype.skill_level_ids.filtered(lambda lv: lv.level_progress == 50)
        cls.survey = cls.env['survey.survey'].create({
            'title': 'Z13 Examen', 'certification': True,
            'scoring_type': 'scoring_with_answers', 'certification_validity_months': 12,
            'sgi_skill_id': cls.skill.id, 'sgi_skill_level_id': cls.level.id})
        cls.channel = cls.env['slide.channel'].sudo().create({
            'name': 'Z13 Curso', 'channel_type': 'training', 'sgi_skill_id': cls.skill.id,
            'sgi_skill_level_id': cls.level.id, 'sgi_skill_validity_months': 24})

    def _resume(self, employee=None, **kw):
        vals = {'employee_id': (employee or self.emp).id, 'name': 'Z13',
                'date_start': date(2046, 1, 10)}
        vals.update(kw)
        return self.env['hr.resume.line'].create(vals)

    def _skill(self, employee=None):
        return self.env['hr.employee.skill'].search(
            [('employee_id', '=', (employee or self.emp).id), ('skill_id', '=', self.skill.id)])

    def _regular_type(self, name):
        """Tipo de competencia normal (no certificación) con una competencia."""
        stype = self.env['hr.skill.type'].create({
            'name': name, 'skill_ids': [(0, 0, {'name': name + ' habilidad'})],
            'skill_level_ids': [(0, 0, {'name': name + ' nivel', 'level_progress': 100,
                                        'default_level': True})]})
        return stype, stype.skill_ids, stype.skill_level_ids

    def _evaluation(self):
        return self.env['sgi.training.effectiveness'].search([('employee_id', '=', self.emp.id)])

    def test_01_certificacion_da_competencia_con_vigencia(self):
        self._resume(survey_id=self.survey.id, date_end=date(2047, 1, 10))
        sk = self._skill()
        self.assertEqual(len(sk), 1)
        self.assertEqual(sk.valid_from, date(2046, 1, 10))
        self.assertEqual(sk.valid_to, date(2047, 1, 10))
        self.assertEqual(sk.skill_level_id, self.level)

    def test_02_curso_da_competencia_con_vigencia_del_curso(self):
        self._resume(channel_id=self.channel.id, course_type='elearning')
        self.assertEqual(self._skill().valid_to, date(2048, 1, 10))

    def test_03_sin_competencia_ligada_no_hace_nada(self):
        other = self.env['survey.survey'].create({
            'title': 'Z13 libre', 'certification': True, 'scoring_type': 'scoring_with_answers'})
        self._resume(survey_id=other.id)
        self.assertFalse(self._skill())
        self.assertFalse(self._evaluation())

    def test_04_idempotente_y_renueva(self):
        self._resume(survey_id=self.survey.id, date_end=date(2047, 1, 10))
        self._resume(survey_id=self.survey.id, date_end=date(2047, 1, 10),
                     date_start=date(2046, 1, 11))
        self.assertEqual(len(self._skill()), 1)
        self._resume(survey_id=self.survey.id, date_start=date(2047, 1, 5),
                     date_end=date(2048, 1, 5))
        self.assertEqual(max(self._skill().mapped('valid_to')), date(2048, 1, 5))
        self.assertEqual(len(self._evaluation()), 1, "Renovar no abre otra evaluación (P6).")

    def test_04b_subir_de_nivel_cierra_el_renglon_viejo(self):
        """I-4: subir de nivel deja el renglón viejo cerrado el día anterior."""
        low = self.env['survey.survey'].create({
            'title': 'Z13 Examen básico', 'certification': True,
            'scoring_type': 'scoring_with_answers',
            'sgi_skill_id': self.skill.id, 'sgi_skill_level_id': self.level_low.id})
        today = fields.Date.context_today(self.env.user)
        self._resume(survey_id=low.id, date_start=today - timedelta(days=30))
        self._resume(survey_id=self.survey.id, date_start=today,
                     date_end=today + timedelta(days=365))
        rows = self._skill().sorted('valid_from')
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0].skill_level_id, self.level_low)
        self.assertEqual(rows[0].valid_to, today - timedelta(days=1), "Cerrado ayer.")
        self.assertEqual(rows[1].skill_level_id, self.level)
        self.assertEqual(len(self._evaluation()), 2, "Nueva y subida abren evaluación.")

    def test_05_eficacia_a_90_dias_con_el_jefe(self):
        self._resume(survey_id=self.survey.id, date_end=date(2047, 1, 10))
        ev = self._evaluation()
        self.assertEqual(len(ev), 1)
        self.assertEqual(ev.origin, 'examen')
        self.assertEqual(ev.evaluator_id, self.boss_user)
        self.assertEqual(ev.granted_date, date(2046, 1, 10))
        self.assertEqual(ev.due_date, ev.granted_date + timedelta(days=90))
        act = ev.activity_ids
        self.assertEqual(len(act), 1)
        self.assertEqual(act.user_id, self.boss_user)
        self.assertEqual(act.date_deadline, ev.due_date)
        self.assertTrue(act.sgi_cron_key.startswith('eficacia_capacitacion:'))

    def test_06_jefe_sin_grupo_sgi_responde_solo_la_suya(self):
        self._resume(survey_id=self.survey.id, date_end=date(2047, 1, 10))
        ev = self._evaluation()
        ev.with_user(self.boss_user).action_mark_effective()
        self.assertEqual(ev.state, 'eficaz')
        self.assertTrue(ev.evaluated_date)
        self.assertFalse(ev.activity_ids)
        stranger = new_test_user(self.env, login='z13_otro', groups='base.group_user')
        with self.assertRaises(AccessError):
            ev.with_user(stranger).read(['state'])

    def test_07_no_eficaz_pide_nota_y_avisa_a_rh(self):
        self._resume(survey_id=self.survey.id, date_end=date(2047, 1, 10))
        ev = self._evaluation()
        with self.assertRaises(UserError):
            ev.with_user(self.boss_user).action_mark_ineffective()
        ev.with_user(self.boss_user).write({'result_note': 'No aplica lo visto'})
        ev.with_user(self.boss_user).action_mark_ineffective()
        self.assertEqual(ev.state, 'no_eficaz')
        rh = self.env['mail.activity'].sudo().search(
            [('sgi_cron_key', '=', 'reprogramar_capacitacion:%d' % ev.id)])
        self.assertTrue(rh)
        self.assertTrue(self._skill(), "La competencia no se quita (P7).")

    def test_08_evaluada_solo_mast_la_cambia(self):
        self._resume(survey_id=self.survey.id, date_end=date(2047, 1, 10))
        ev = self._evaluation()
        ev.with_user(self.boss_user).action_mark_effective()
        with self.assertRaises(UserError):
            ev.with_user(self.boss_user).write({'state': 'no_eficaz'})
        # I-11: quien no es RH ni Jefe MAST no cambia los datos de la evaluación.
        with self.assertRaises(UserError):
            ev.with_user(self.boss_user).write({'due_date': date(2046, 12, 1)})
        # El contexto no abre el candado: quién evaluó y cuándo solo los
        # escriben los botones.
        with self.assertRaises(UserError):
            ev.with_user(self.boss_user).with_context(sgi_effectiveness_result=True).write(
                {'evaluated_by': self.boss_user.id, 'evaluated_date': date(2046, 1, 1)})
        ev.with_user(self.mast).write({'state': 'no_eficaz', 'result_note': 'Corrección MAST'})
        self.assertEqual(ev.state, 'no_eficaz')
        with self.assertRaises(UserError):
            ev.with_user(self.mast).unlink()

    def test_09_cron_de_cursos_usa_la_misma_regla(self):
        partner = self.env['res.partner'].create({'name': 'Z13 contacto'})
        self.emp.work_contact_id = partner
        self.env['slide.channel.partner'].sudo().create({
            'channel_id': self.channel.id, 'partner_id': partner.id,
            'member_status': 'completed'})
        self.env['slide.channel']._sgi_sync_completions()
        self.env['slide.channel']._sgi_sync_completions()
        sk = self._skill()
        self.assertEqual(len(sk), 1)
        self.assertTrue(sk.valid_to)
        self.assertEqual(len(self._evaluation()), 1)
        self.assertEqual(self._evaluation().origin, 'curso')

    def test_10_aviso_de_vencimiento_tambien_sin_certificacion(self):
        today = fields.Date.context_today(self.env.user)
        stype, skill, level = self._regular_type('Z13 Normal')
        survey = self.env['survey.survey'].create({
            'title': 'Z13 Examen normal', 'certification': True,
            'scoring_type': 'scoring_with_answers',
            'sgi_skill_id': skill.id, 'sgi_skill_level_id': level.id})
        self._resume(survey_id=survey.id, date_start=today - timedelta(days=1),
                     date_end=today + timedelta(days=10))
        self.assertFalse(stype.is_certification)
        self.env['sgi.cron'].sudo().cron_competences()
        acts = self.env['mail.activity'].sudo().search(
            [('res_model', '=', 'hr.employee'), ('res_id', '=', self.emp.id),
             ('sgi_cron_key', '=like', 'certificacion_por_vencer:%')])
        self.assertTrue(acts)
        self.assertTrue(all(a.summary.startswith('Competencia por vencer') for a in acts))
        # I-5: la línea del examen no pide además «Formación por concluir».
        self.assertFalse(self.env['mail.activity'].sudo().search(
            [('res_model', '=', 'hr.employee'), ('res_id', '=', self.emp.id),
             ('sgi_cron_key', '=like', 'formacion_por_concluir:%')]))

    def test_10b_vencimiento_solo_de_lo_que_otorga_el_sgi(self):
        """I-3: una competencia normal con «válida hasta» que el SGI no otorga
        (sin examen ni curso con vigencia) no avisa; un renglón cerrado al
        subir de nivel tampoco."""
        today = fields.Date.context_today(self.env.user)
        _stype, skill, level = self._regular_type('Z13 Suelta')
        loose = self.env['hr.employee.skill'].create({
            'employee_id': self.emp.id, 'skill_id': skill.id,
            'skill_type_id': skill.skill_type_id.id, 'skill_level_id': level.id,
            'valid_from': today - timedelta(days=10), 'valid_to': today + timedelta(days=5)})
        low = self.env['survey.survey'].create({
            'title': 'Z13 Examen básico 10b', 'certification': True,
            'scoring_type': 'scoring_with_answers',
            'sgi_skill_id': self.skill.id, 'sgi_skill_level_id': self.level_low.id})
        self._resume(survey_id=low.id, date_start=today - timedelta(days=30))
        self._resume(survey_id=self.survey.id, date_start=today,
                     date_end=today + timedelta(days=365))
        closed = self._skill().filtered(lambda r: r.skill_level_id == self.level_low)
        self.assertEqual(closed.valid_to, today - timedelta(days=1))
        self.env['sgi.cron'].sudo().cron_competences()
        Activity = self.env['mail.activity'].sudo()
        for row in loose | closed:
            self.assertFalse(Activity.search(
                [('res_model', '=', 'hr.employee'), ('res_id', '=', self.emp.id),
                 ('sgi_cron_key', '=like', 'certificacion_%%:%d:%%' % row.id)]), row.id)

    def test_11_gancho_no_tumba_la_linea_si_falla(self):
        broken = self.env['survey.survey'].create({
            'title': 'Z13 roto', 'certification': True, 'scoring_type': 'scoring_with_answers',
            'sgi_skill_id': self.skill.id})
        line = self._resume(survey_id=broken.id)   # sin nivel: no otorga, no truena
        self.assertTrue(line.exists())
        self.assertFalse(self._skill())

    def test_12_empleado_de_otra_compania_no_recibe(self):
        """I-6: solo empleados de la empresa del SGI."""
        other = self.env['res.company'].create({'name': 'Z13 otra'})
        emp_b = self.env['hr.employee'].create({'name': 'Empleado Z13 B', 'company_id': other.id})
        self._resume(employee=emp_b, survey_id=self.survey.id, date_end=date(2047, 1, 10))
        self.assertFalse(self._skill(emp_b))

    def test_13_examen_aprobado_sin_usuario(self):
        """I-2: quien aprueba sin usuario se encuentra por su contacto de
        trabajo (lo nativo solo busca por usuario)."""
        survey = self.env['survey.survey'].create({
            'title': 'Z13 Examen sin usuario', 'certification': True,
            'scoring_type': 'scoring_with_answers', 'scoring_success_min': 0.0,
            'certification_validity_months': 12, 'certification_mail_template_id': False,
            'sgi_skill_id': self.skill.id, 'sgi_skill_level_id': self.level.id})
        partner = self.emp.work_contact_id or self.env['res.partner'].create({'name': 'Z13 c'})
        self.emp.work_contact_id = partner
        answer = self.env['survey.user_input'].create({
            'survey_id': survey.id, 'partner_id': partner.id})
        answer._mark_done()
        line = self.env['hr.resume.line'].search(
            [('employee_id', '=', self.emp.id), ('survey_id', '=', survey.id)])
        self.assertEqual(len(line), 1)
        self.assertEqual(len(self._skill()), 1)

    def test_14_lista_de_examenes_solo_liga(self):
        """I-4/I-7: el Jefe MAST sin la app Encuestas liga la competencia,
        pero no cambia nada más del examen; nadie más la liga."""
        self.survey.write({'sgi_skill_id': False, 'sgi_skill_level_id': False})
        as_mast = self.survey.with_user(self.mast)
        if not self.mast.has_group('survey.group_survey_user'):
            with self.assertRaises(UserError):
                as_mast.write({'title': 'Z13 cambiado'})
        as_mast.write({'sgi_skill_id': self.skill.id, 'sgi_skill_level_id': self.level.id})
        self.assertEqual(self.survey.sgi_skill_id, self.skill)
        with self.assertRaises(UserError):
            self.survey.with_user(self.boss_user).write({'sgi_skill_id': False})

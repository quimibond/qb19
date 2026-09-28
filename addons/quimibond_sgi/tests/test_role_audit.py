# -*- coding: utf-8 -*-
"""56.7.0: auditoría funcional por rol (28-sep-2026). Cada prueba entra como
un usuario real del rol (with_user) y revisa lo que ese rol debe poder y no
debe poder hacer."""
from datetime import date

from odoo.exceptions import AccessError
from odoo.tests import TransactionCase, tagged, new_test_user

from .common_documents import sgi_hide_real_documents


@tagged('post_install', '-at_install')
class TestRoleAudit(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        cls.user = new_test_user(cls.env, login='rol_usuario_sgi',
                                 groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.other = new_test_user(cls.env, login='rol_otro_sgi',
                                  groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.mast = new_test_user(cls.env, login='rol_mast_sgi',
                                 groups='base.group_user,quimibond_sgi.group_sgi_manager')
        cls.job = cls.env['hr.job'].create({'name': 'PUESTO AUDITORIA ROL'})
        cls.emp = cls.env['hr.employee'].create({
            'name': 'Empleado Auditoría Rol', 'job_id': cls.job.id, 'user_id': cls.user.id})
        Indicator = cls.env['sgi.indicator']
        cls.mine = Indicator.create({
            'code': 'ZROL-01', 'name': 'Mío en prueba', 'calc_mode': 'manual',
            'responsible_id': cls.user.id, 'target_objective': 1.0, 'target_acceptable': 0.5})
        cls.theirs = Indicator.create({
            'code': 'ZROL-02', 'name': 'De otro', 'calc_mode': 'manual',
            'responsible_id': cls.other.id, 'target_objective': 1.0, 'target_acceptable': 0.5})

    def test_01_mis_indicadores_aunque_esten_en_prueba(self):
        self.assertNotEqual(self.mine.status, 'oficial')
        screen = self.env['sgi.my.procedure'].with_user(self.user).create({'employee_id': self.emp.id})
        self.assertIn(self.mine, screen.indicator_ids, "Antes solo contaban los oficiales: nadie veía los suyos.")
        self.assertNotIn(self.theirs, screen.indicator_ids)
        self.assertEqual(screen.indicator_count, len(screen.indicator_ids))
        self.assertEqual(screen.action_show_indicators()['res_model'], 'sgi.indicator')
        arch = self.env['sgi.my.procedure'].get_views([(False, 'form')])['views']['form']['arch']
        self.assertIn('name="indicadores"', arch)
        action = self.env.ref('quimibond_sgi.sgi_indicator_action_mine')
        self.assertEqual(self.env.ref('quimibond_sgi.menu_sgi_my_indicators').action, action)
        self.assertEqual(self.mine.action_sgi_measures()['domain'], [('indicator_id', '=', self.mine.id)])

    def test_02_usuario_captura_solo_lo_suyo(self):
        Measure = self.env['sgi.indicator.measure']
        own = Measure.with_user(self.user).create({'indicator_id': self.mine.id, 'period_date': date(2046, 1, 1)})
        own.write({'value': 1.0})
        foreign = Measure.create({'indicator_id': self.theirs.id, 'period_date': date(2046, 1, 1)})
        with self.assertRaises(AccessError):
            foreign.with_user(self.user).write({'value': 5.0})
        with self.assertRaises(AccessError):
            Measure.with_user(self.user).create({'indicator_id': self.theirs.id, 'period_date': date(2046, 2, 1)})
        foreign.with_user(self.mast).write({'value': 2.0})
        self.assertEqual(foreign.value, 2.0, "MAST captura cualquiera.")
        # Lee todo el SGI: la medición ajena sí la ve.
        self.assertEqual(foreign.with_user(self.user).indicator_id, self.theirs)

    def test_03_usuario_no_edita_catalogo_y_no_aprueba_todo(self):
        process = self.env['sgi.process'].create({'code': 'ZROL', 'name': 'Proceso rol'})
        with self.assertRaises(AccessError):
            process.with_user(self.user).write({'name': 'Cambiado'})
        self.assertFalse(self.user.has_group('approvals.group_approval_user'),
                         "Usuario SGI ya no implica «aprobar todas las solicitudes».")

    def test_04_escalaciones_van_al_jefe_mast_directo(self):
        Cron = self.env['sgi.cron']
        group = self.env.ref('quimibond_sgi.group_sgi_manager')
        self.assertIn(Cron._sgi_manager_user_id(), group.user_ids.ids,
                      "Primero un miembro directo, no el primero heredado (el CEO).")
        self.env['ir.config_parameter'].sudo().set_param('quimibond_sgi.mast_user_id', str(self.mast.id))
        self.assertEqual(Cron._sgi_manager_user_id(), self.mast.id)

    def test_05_actividad_de_captura_se_cierra_sola(self):
        Cron = self.env['sgi.cron']
        measure = self.env['sgi.indicator.measure'].create({
            'indicator_id': self.mine.id, 'period_date': date(2046, 3, 1)})
        summary = "Capturar indicador ZROL-01 (03/2046)"
        Cron._sgi_schedule(self.mine, summary, '', self.user.id)
        opened = self.env['mail.activity'].search([
            ('res_model', '=', 'sgi.indicator'), ('res_id', '=', self.mine.id), ('summary', '=', summary)])
        self.assertTrue(opened)
        Cron._sgi_close_resolved_activities()
        self.assertEqual(self.env['mail.activity'].search([
            ('res_model', '=', 'sgi.indicator'), ('res_id', '=', self.mine.id), ('summary', '=', summary)]),
            opened, "Con la medición pendiente sigue abierta.")
        measure.write({'value': 1.0, 'state': 'capturado'})
        Cron._sgi_close_resolved_activities()
        still = self.env['mail.activity'].search([
            ('res_model', '=', 'sgi.indicator'), ('res_id', '=', self.mine.id), ('summary', '=', summary)])
        self.assertFalse(still, "Capturada la medición, la actividad se marca hecha.")

    def test_06_mis_pendientes_en_el_menu(self):
        menu = self.env.ref('quimibond_sgi.menu_sgi_my_pending')
        self.assertEqual(menu.parent_id, self.env.ref('quimibond_sgi.menu_sgi_panel'))
        self.assertEqual(menu.action, self.env.ref('quimibond_sgi.sgi_my_pending_action_mine'))
        action = self.env['sgi.my.pending'].with_user(self.user).action_open_mine()
        self.assertEqual(action['res_model'], 'sgi.my.pending')
        self.assertEqual(action['context'].get('search_default_group_state'), 1)
        # Sin empleado ligado también abre (vacía o con lo del usuario).
        action = self.env['sgi.my.pending'].with_user(self.other).action_open_mine()
        self.assertEqual(action['res_model'], 'sgi.my.pending')

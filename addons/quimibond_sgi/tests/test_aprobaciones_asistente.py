# -*- coding: utf-8 -*-
"""57.109.0: configurar una aprobación del procedimiento con tres preguntas,
sugerencia por rol, activación en lote, faltante y aviso al Jefe MAST."""
from odoo.exceptions import AccessError, UserError
from odoo.tests import TransactionCase, new_test_user, tagged


@tagged('post_install', '-at_install')
class TestAprobacionesAsistente(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.manager = new_test_user(cls.env, login='apa_mast',
                                    groups='base.group_user,quimibond_sgi.group_sgi_manager')
        cls.plain = new_test_user(cls.env, login='apa_plain',
                                  groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.approver_user = new_test_user(cls.env, login='apa_dir', groups='base.group_user')
        cls.job_exec = cls.env['hr.job'].create({'name': 'SISTEMAS APROBACION ASISTENTE'})
        cls.job_appr = cls.env['hr.job'].create({'name': 'DIRECTOR APROBACION ASISTENTE'})
        cls.env['hr.employee'].create({'name': 'Director asistente', 'job_id': cls.job_appr.id,
                                       'user_id': cls.approver_user.id})
        cls.process = cls.env['sgi.process'].create({'code': 'ZAP', 'name': 'Proceso aprobaciones'})
        cls.activity = cls.env['sgi.process.activity'].create({
            'process_id': cls.process.id, 'name': 'Dar de alta al usuario nuevo',
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': cls.job_exec.id}),
                         (0, 0, {'role': 'aprueba', 'job_id': cls.job_appr.id})]})
        cls.role = cls.activity.role_ids.filtered(lambda r: r.role == 'aprueba')

    def _codes(self):
        self.activity._sgi_refresh_spec_gaps()
        self.activity.invalidate_recordset(['spec_gap_ids'])
        return set(self.activity.spec_gap_ids.mapped('code'))

    def test_01_sin_configurar_es_faltante_y_se_activa_como_solicitud(self):
        self.role.approval_model_id = False
        self.assertEqual(self.role.approval_state, 'sin_configurar')
        self.assertIn('approval_missing', self._codes())
        self.assertEqual(self.role.approval_suggestion, 'Solicitud en Aprobaciones')
        with self.assertRaises(AccessError):
            self.role.with_user(self.plain).action_sgi_approval_wizard()
        action = self.role.with_user(self.manager).action_sgi_approval_wizard()
        wizard = self.env['sgi.approval.wizard'].browse(action['res_id']).with_user(self.manager)
        self.assertEqual(wizard.q_what, 'decision')
        wizard.category_name = 'Alta de usuario'
        self.assertIn('Aprobaciones', wizard.preview_html)
        self.assertIn(self.approver_user.name, wizard.preview_html)
        wizard.action_activate()
        self.assertEqual(self.role.approval_kind, 'solicitud')
        self.assertEqual(self.role.approval_state, 'activa')
        self.assertEqual(self.role.approval_category_id.name, 'Alta de usuario')
        self.assertIn(self.approver_user, self.role.approval_category_id.approver_ids.user_id)
        self.assertNotIn('approval_missing', self._codes())

    def test_02_documento_con_boton(self):
        self.role.approval_model_id = self.env['ir.model']._get('account.move')
        self.assertTrue(self.role.approval_suggestion.startswith('Botón «action_post»'))
        wizard = self.env['sgi.approval.wizard'].with_user(self.manager).create({'role_id': self.role.id})
        self.assertEqual(wizard.q_what, 'documento')
        wizard.action_next()
        self.assertEqual(wizard.step, 'how')
        self.assertEqual(wizard.button_line_id.method, 'action_post')
        self.assertIn('Odoo le pedirá la aprobación', wizard.preview_html)
        if wizard.button_supported:
            wizard.action_activate()
            self.assertEqual((self.role.approval_kind, self.role.approval_method), ('boton', 'action_post'))
        else:
            with self.assertRaises(UserError):
                wizard.action_activate()

    def test_03_lote_y_aviso_al_jefe_mast(self):
        self.role.approval_model_id = False
        self.env['ir.config_parameter'].sudo().set_param('quimibond_sgi.mast_user_id', str(self.manager.id))
        Role = self.env['sgi.activity.role']
        Role._sgi_notice_not_active(self.role)
        notice = self.env['mail.activity'].search([
            ('res_model', '=', 'sgi.process'), ('res_id', '=', self.process.id),
            ('sgi_cron_kind', '=', 'aprobaciones_sin_activar')])
        self.assertEqual(len(notice), 1)
        self.assertEqual(notice.user_id, self.manager)
        self.role.with_user(self.manager).action_sgi_activate_suggested()
        self.assertEqual(self.role.approval_state, 'activa')
        Role._sgi_notice_not_active(self.role)
        self.assertFalse(notice.exists() and notice.active, "Se cierra solo al activarse.")

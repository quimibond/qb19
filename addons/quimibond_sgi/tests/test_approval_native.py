# -*- coding: utf-8 -*-
"""56.5.0: el rol «Aprueba» ligado a la regla de aprobación nativa de Odoo."""
from odoo.exceptions import UserError, ValidationError
from odoo.tests import TransactionCase, tagged, new_test_user


@tagged('post_install', '-at_install')
class TestApprovalNative(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.job_buyer = cls.env['hr.job'].create({'name': 'COMPRADOR APROB MP'})
        cls.job_boss = cls.env['hr.job'].create({'name': 'DIRECTOR APROB MP'})
        cls.boss_user = new_test_user(cls.env, login='apr_boss', groups='base.group_user,purchase.group_purchase_user')
        cls.env['hr.employee'].create({'name': 'Director Aprob', 'job_id': cls.job_boss.id,
                                       'user_id': cls.boss_user.id})
        process = cls.env['sgi.process'].create({'code': 'ZAPR', 'name': 'Proceso Aprobaciones MP'})
        cls.activity = cls.env['sgi.process.activity'].create({
            'process_id': process.id, 'name': 'Crear la orden de compra', 'number': '9.1',
            'measure_model_id': cls.env['ir.model']._get('purchase.order').id,
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': cls.job_buyer.id}),
                         (0, 0, {'role': 'aprueba', 'job_id': cls.job_boss.id,
                                 'condition': 'si la orden rebasa el monto fijado'})]})
        cls.role = cls.activity.role_ids.filtered(lambda r: r.role == 'aprueba')

    def test_01_sugerencia_y_condicion(self):
        self.assertEqual(self.role.approval_model_id.model, 'purchase.order',
                         "Se sugiere el documento que materializa la actividad.")
        self.assertEqual(self.role.approval_method, 'button_confirm')
        self.assertEqual(self.role.approval_state, 'por_sincronizar')
        self.role.write({
            'condition_field_id': self.env['ir.model.fields']._get('purchase.order', 'amount_total').id,
            'condition_operator': '>', 'condition_value': '50,000'})
        self.assertEqual(self.role.approval_domain, "[('amount_total', '>', 50000.0)]")
        with self.assertRaises(ValidationError):
            self.role.condition_value = 'mucho'

    def test_02_sincroniza_regla_nativa(self):
        self.role.write({
            'condition_field_id': self.env['ir.model.fields']._get('purchase.order', 'amount_total').id,
            'condition_operator': '>', 'condition_value': '50000'})
        self.role.action_sgi_sync_approval()
        rule = self.role.approval_rule_id
        self.assertTrue(rule.active)
        self.assertEqual((rule.model_id.model, rule.method), ('purchase.order', 'button_confirm'))
        self.assertEqual(rule.domain, "[('amount_total', '>', 50000.0)]")
        self.assertEqual(rule.approver_ids, self.boss_user)
        self.assertEqual(rule.sgi_role_id, self.role)
        self.assertEqual(self.role.approval_state, 'activa')
        # Otra persona entra al puesto: la regla queda por sincronizar y el cron la pone al día.
        other = new_test_user(self.env, login='apr_boss2', groups='base.group_user')
        self.env['hr.employee'].create({'name': 'Director Aprob 2', 'job_id': self.job_boss.id,
                                        'user_id': other.id})
        self.role.invalidate_recordset(['approval_state', 'approval_user_ids'])
        self.assertEqual(self.role.approval_state, 'por_sincronizar')
        self.env['sgi.activity.role'].cron_sgi_sync_approvals()
        self.assertEqual(rule.approver_ids, self.boss_user | other)
        # Si la actividad se archiva, la regla también.
        self.activity.active = False
        self.assertFalse(rule.active)

    def test_03_boton_inexistente_y_sin_personas(self):
        self.role.approval_method = 'boton_que_no_existe'
        with self.assertRaises(UserError):
            self.role.action_sgi_sync_approval()
        self.role.write({'approval_method': 'button_confirm', 'job_id': self.job_buyer.id})
        self.assertEqual(self.role.approval_state, 'sin_aprobadores', "El puesto no tiene personas con usuario.")
        self.role.action_sgi_sync_approval()
        self.assertFalse(self.role.approval_rule_id, "Sin aprobadores no se crea una regla que nadie puede aprobar.")

# -*- coding: utf-8 -*-
"""56.5.0: el rol «Aprueba» ligado a la aprobación nativa de Odoo.

57.9.0 (A-010): lo del botón con la regla de Studio se prueba en
quimibond_sgi_studio."""
from odoo.exceptions import ValidationError
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

    # test_02 a test_04 (regla de Studio del botón) se mudaron a
    # quimibond_sgi_studio/tests/test_approval_studio.py en 57.9.0 (A-010, J-018).

    def test_03_sin_personas_en_el_puesto(self):
        self.role.job_id = self.job_buyer
        self.assertEqual(self.role.approval_state, 'sin_aprobadores', "El puesto no tiene personas con usuario.")

    def test_05_solicitud_en_aprobaciones_y_mis_pendientes(self):
        """56.6.0: una decisión sin documento se aprueba con una categoría de
        Aprobaciones cuyos aprobadores siguen a las personas del puesto."""
        self.role.approval_kind = 'solicitud'
        self.assertEqual(self.role.approval_state, 'por_sincronizar')
        self.role.action_sgi_sync_approval()
        category = self.role.approval_category_id
        self.assertEqual(category.sgi_role_id, self.role)
        self.assertEqual(category.approver_ids.user_id, self.boss_user)
        self.assertEqual(self.role.approval_state, 'activa')
        request = self.env['approval.request'].create({
            'name': 'Decisión MP', 'category_id': category.id, 'request_owner_id': self.env.user.id})
        request.action_confirm()
        rows = self.env['sgi.my.pending']._sgi_pending_values(self.boss_user)[self.boss_user.id]
        self.assertTrue([r for r in rows if r['kind'] == 'solicitud' and r['res_id'] == request.id],
                        "La solicitud por aprobar entra a Mis pendientes del aprobador.")
        # Quien entra al puesto se vuelve aprobador con el cron.
        other = new_test_user(self.env, login='apr_boss3', groups='base.group_user')
        self.env['hr.employee'].create({'name': 'Director Aprob 3', 'job_id': self.job_boss.id,
                                        'user_id': other.id})
        self.env['sgi.activity.role'].cron_sgi_sync_approvals()
        self.assertEqual(category.approver_ids.user_id, self.boss_user | other)
        self.activity.active = False
        self.assertFalse(category.active)

    def test_06_firma_sin_plantilla(self):
        self.role.approval_kind = 'firma'
        self.assertEqual(self.role.approval_state, 'sin_configurar')

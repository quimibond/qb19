# -*- coding: utf-8 -*-
"""57.116.0: categorías compartidas con asunto."""
from odoo.exceptions import UserError
from odoo.tests import TransactionCase, new_test_user, tagged


@tagged('post_install', '-at_install')
class TestAprobacionesAsunto(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.approver = new_test_user(cls.env, login='as_aprueba', groups='base.group_user')
        cls.category = cls.env['approval.category'].create({
            'name': 'Autorizaciones de prueba', 'approval_minimum': 1})
        cls.subject = cls.env['sgi.approval.subject'].create({
            'name': 'Propuesta de pago', 'category_id': cls.category.id})

    def test_01_sin_asunto_no_se_envia(self):
        request = self.env['approval.request'].create({
            'name': 'Pago del jueves', 'category_id': self.category.id})
        self.assertTrue(request.sgi_has_subjects)
        with self.assertRaises(UserError):
            request.action_confirm()

    def test_02_el_asunto_pone_los_aprobadores(self):
        job = self.env['hr.job'].create({'name': 'CONTADOR DE PRUEBA ASUNTO'})
        self.env['hr.employee'].create({'name': 'Aprobador asunto', 'user_id': self.approver.id,
                                        'job_id': job.id})
        process = self.env['sgi.process'].create({'code': 'ZAS', 'name': 'Proceso asunto'})
        activity = self.env['sgi.process.activity'].create({
            'process_id': process.id, 'number': 'ZAS.01',
            'name': 'Preparar la propuesta de pago y pedir su autorización'})
        role = self.env['sgi.activity.role'].create({
            'activity_id': activity.id, 'role': 'aprueba', 'job_id': job.id,
            'approval_kind': 'solicitud', 'approval_category_id': self.category.id})
        self.subject.role_id = role
        request = self.env['approval.request'].create({
            'name': 'Pago del jueves', 'category_id': self.category.id,
            'sgi_subject_id': self.subject.id})
        self.assertEqual(request.approver_ids.user_id, self.approver,
                         "Aprueban las personas del puesto del asunto.")
        self.assertEqual(role.approval_state, 'activa',
                         "Un rol en una categoría compartida queda activo.")

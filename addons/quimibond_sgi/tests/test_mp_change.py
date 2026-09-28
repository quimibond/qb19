# -*- coding: utf-8 -*-
"""56.2.0: documentos del puesto desde sus actividades, «Proponer cambio» /
«Proponer nueva actividad» hacia Aprobaciones con aviso a MAST al aprobarse,
y aviso cuando el usuario no está ligado a un empleado."""
from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged, new_test_user

from .common_documents import sgi_hide_real_documents


@tagged('post_install', '-at_install')
class TestMpChange(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        cls.job = cls.env['hr.job'].create({'name': 'PUESTO PROPUESTA MP'})
        cls.user_emp = new_test_user(cls.env, login='mpc_emp',
                                     groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.manager = new_test_user(cls.env, login='mpc_mast',
                                    groups='base.group_user,quimibond_sgi.group_sgi_manager')
        cls.emp = cls.env['hr.employee'].create({
            'name': 'Emp Propuesta MP', 'job_id': cls.job.id, 'user_id': cls.user_emp.id})
        Doc = cls.env['documents.document']

        def doc(code, doc_type, state='vigente'):
            return Doc.create({
                'name': code, 'type': 'binary', 'sgi_is_controlled': True,
                'sgi_doc_type': doc_type, 'sgi_code': code, 'sgi_state': state})

        cls.it = doc('IT-P-C11-66', 'instructivo')
        cls.fmt = doc('F-P-C95-66', 'formato')
        cls.proc_doc = doc('P-A66', 'procedimiento')
        cls.draft_fmt = doc('F-P-C95-67', 'formato', state='borrador')
        cls.process = cls.env['sgi.process'].create({'code': 'ZMPC', 'name': 'Proceso Propuesta MP'})
        cls.activity = cls.env['sgi.process.activity'].create({
            'process_id': cls.process.id, 'name': 'Revisar la muestra', 'number': '4.1',
            'instruction_id': cls.it.id,
            'format_document_ids': [(6, 0, (cls.fmt | cls.draft_fmt).ids)],
            'related_procedure_id': cls.proc_doc.id,
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': cls.job.id})],
            'done_criteria': 'La muestra tiene etiqueta'})
        # En la copia de producción la categoría real ya está marcada: se
        # desmarca dentro de la transacción para que la prueba use la suya.
        cls.env['approval.category'].search([('sgi_is_mp_change', '=', True)]).write(
            {'sgi_is_mp_change': False})
        cls.category = cls.env['approval.category'].create({
            'name': 'Proponer cambio MP (prueba)', 'sgi_is_mp_change': True,
            'has_reference': 'required', 'manager_approval': False, 'approval_minimum': 1,
            'approver_ids': [(0, 0, {'user_id': cls.manager.id, 'required': True})]})

    def test_01_documentos_desde_las_actividades(self):
        expected = self.it | self.fmt | self.proc_doc
        self.assertEqual(self.job._sgi_mp_document_records() & (expected | self.draft_fmt), expected,
                         "Instructivo, formatos y procedimiento vigentes; el borrador no.")
        self.assertEqual(self.emp.sgi_mp_document_ids & expected, expected)
        wiz = self.env['sgi.my.procedure'].create({'employee_id': self.emp.id})
        self.assertEqual(wiz.document_ids & expected, expected)
        self.assertNotIn(self.draft_fmt, wiz.document_ids)

    def test_02_proponer_cambio_y_aprobar(self):
        role = self.activity.role_ids
        action = role.with_user(self.user_emp).action_mp_propose_change()
        wizard = self.env['sgi.mp.change.wizard'].browse(action['res_id']).with_user(self.user_emp)
        self.assertEqual(wizard.change_type, 'cambiar')
        self.assertIn('La muestra tiene etiqueta', wizard.current_text)
        wizard.write({'proposal': 'Tomar foto de la etiqueta', 'reason': 'Evidencia para auditoría'})
        opened = wizard.action_submit()
        request = self.env['approval.request'].browse(opened['res_id'])
        self.assertEqual(request.category_id, self.category)
        self.assertEqual(request.request_owner_id, self.user_emp)
        self.assertEqual(request.sgi_activity_id, self.activity)
        self.assertEqual(request.reference, 'ZMPC / 4.1 Revisar la muestra')
        self.assertEqual(request.request_status, 'pending')
        self.assertIn('Tomar foto de la etiqueta', request.reason)
        request.with_user(self.manager).action_approve()
        self.assertEqual(request.request_status, 'approved')
        self.assertTrue(request.sgi_mp_apply_scheduled)
        todo = self.env['mail.activity'].search([
            ('res_model', '=', 'sgi.process.activity'), ('res_id', '=', self.activity.id),
            ('user_id', '=', self.manager.id)])
        self.assertEqual(todo.summary, 'Aplicar cambio aprobado y republicar')

    def test_03_proponer_nueva_actividad(self):
        screen = self.env['sgi.my.procedure'].with_user(self.user_emp).create({'employee_id': self.emp.id})
        action = screen.action_propose_new_activity()
        wizard = self.env['sgi.mp.change.wizard'].browse(action['res_id']).with_user(self.user_emp)
        self.assertEqual(wizard.change_type, 'agregar')
        self.assertEqual(wizard.process_id, self.process, "Un solo proceso: queda elegido.")
        # 56.3.2: abre vacío y no deja enviar sin propuesta ni motivo.
        with self.assertRaises(UserError):
            wizard.action_submit()
        wizard.write({'proposal': 'Registrar la merma', 'reason': 'No se mide hoy'})
        request = self.env['approval.request'].browse(wizard.action_submit()['res_id'])
        self.assertEqual(request.reference, 'ZMPC / Nueva actividad')
        self.assertEqual(request.sgi_mp_change_type, 'agregar')
        self.assertEqual(request.sgi_affected_process_ids, self.process)
        request.with_user(self.manager).action_approve()
        self.assertTrue(self.process.activity_ids.filtered(
            lambda a: a.user_id == self.manager and a.summary == 'Aplicar cambio aprobado y republicar'))

    def test_04_usuario_sin_empleado(self):
        lonely = new_test_user(self.env, login='mpc_lonely',
                               groups='base.group_user,quimibond_sgi.group_sgi_user')
        Wiz = self.env['sgi.my.procedure'].with_user(lonely)
        wiz = Wiz.browse(Wiz.action_open_mine()['res_id'])
        self.assertTrue(wiz.no_employee)
        mine = self.env['sgi.my.procedure'].with_user(self.user_emp)
        self.assertFalse(mine.browse(mine.action_open_mine()['res_id']).no_employee)

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

    def _approve(self, proposal):
        request = proposal.request_id
        self.assertEqual(request.request_status, 'pending')
        request.with_user(self.manager).action_approve()
        self.assertEqual(request.request_status, 'approved')
        return request

    def test_02_proponer_cambio_edita_la_actividad(self):
        """56.4.0: la propuesta edita los campos reales de la actividad y, al
        aprobarse, se aplica sola."""
        role = self.activity.role_ids
        action = role.with_user(self.user_emp).action_mp_propose_change()
        proposal = self.env['sgi.activity.change'].browse(action['res_id']).with_user(self.user_emp)
        self.assertEqual(proposal.change_type, 'cambiar')
        self.assertEqual(proposal.done_criteria, 'La muestra tiene etiqueta', "Nace con los valores de hoy.")
        self.assertEqual(len(proposal.role_line_ids), 1)
        with self.assertRaises(UserError):
            proposal.write({'reason': 'Sin cambios'})
            proposal.action_submit()
        other_job = self.env['hr.job'].create({'name': 'SUPERVISOR PROPUESTA MP'})
        proposal.write({
            'done_criteria': 'La muestra tiene etiqueta y foto',
            'measure_cadence': 'semanal', 'due_weekday': '4',
            'role_line_ids': [(0, 0, {'role': 'aprueba', 'target_type': 'job', 'job_id': other_job.id})],
            'reason': 'Evidencia para auditoría',
        })
        self.assertIn('Criterio de terminado', proposal.diff_html)
        self.assertIn('SUPERVISOR PROPUESTA MP', proposal.diff_html)
        opened = proposal.action_submit()
        request = self.env['approval.request'].browse(opened['res_id'])
        self.assertEqual(request.category_id, self.category)
        self.assertEqual(request.request_owner_id, self.user_emp)
        self.assertEqual(request.sgi_activity_id, self.activity)
        self.assertEqual(request.reference, 'ZMPC / 4.1 Revisar la muestra')
        self.assertIn('La muestra tiene etiqueta y foto', request.reason)
        self.assertEqual(self.activity.done_criteria, 'La muestra tiene etiqueta',
                         "Nada cambia antes de aprobarse.")
        self._approve(proposal)
        self.assertEqual(self.activity.done_criteria, 'La muestra tiene etiqueta y foto')
        self.assertEqual((self.activity.measure_cadence, self.activity.due_weekday), ('semanal', '4'))
        self.assertTrue(self.activity.role_ids.filtered(
            lambda r: r.role == 'aprueba' and r.job_id == other_job))
        self.assertTrue(self.activity.role_ids.filtered(
            lambda r: r.role == 'ejecuta' and r.job_id == self.job), "Los roles que siguen se conservan.")
        self.assertEqual(proposal.state, 'aplicada')
        self.assertIn('Criterio de terminado', request.sgi_mp_diff_html,
                      "El cambio aprobado queda congelado en la solicitud.")
        todo = self.env['mail.activity'].search([
            ('res_model', '=', 'sgi.process.activity'), ('res_id', '=', self.activity.id),
            ('user_id', '=', self.manager.id)])
        self.assertEqual(todo.summary, 'Revisar cambio aplicado y republicar')

    def test_03_proponer_nueva_actividad_y_quitar(self):
        screen = self.env['sgi.my.procedure'].with_user(self.user_emp).create({'employee_id': self.emp.id})
        action = screen.action_propose_new_activity()
        proposal = self.env['sgi.activity.change'].browse(action['res_id']).with_user(self.user_emp)
        self.assertEqual(proposal.change_type, 'agregar')
        self.assertEqual(proposal.process_id, self.process, "Un solo proceso: queda elegido.")
        self.assertEqual(proposal.role_line_ids.job_id, self.job, "La ejecuta el puesto de quien propone.")
        # Abre vacío y no deja enviar sin resumen ni motivo.
        with self.assertRaises(UserError):
            proposal.action_submit()
        proposal.write({'name': 'Registrar la merma', 'how_steps': 'Pesar → capturar',
                        'reason': 'No se mide hoy'})
        proposal.action_submit()
        self.assertEqual(proposal.request_id.reference, 'ZMPC / Nueva actividad: Registrar la merma')
        self._approve(proposal)
        new = proposal.activity_id
        self.assertEqual((new.name, new.process_id, new.how_steps),
                         ('Registrar la merma', self.process, 'Pesar → capturar'))
        self.assertEqual(new.role_ids.job_id, self.job)
        # Quitar: se archiva, no se borra.
        action = new.role_ids.with_user(self.user_emp).action_mp_propose_change()
        remove = self.env['sgi.activity.change'].browse(action['res_id']).with_user(self.user_emp)
        remove.write({'change_type': 'quitar', 'reason': 'Ya no aplica'})
        remove.action_submit()
        self._approve(remove)
        self.assertFalse(new.active)
        self.assertTrue(new.exists())

    def test_05_boton_nuevo_de_mis_actividades(self):
        """56.6.1: el «Nuevo» nativo del kanban de Mis actividades abre la
        propuesta de actividad nueva con el puesto y el proceso."""
        screen = self.env['sgi.my.procedure'].with_user(self.user_emp).create({'employee_id': self.emp.id})
        action = screen.action_show_all()
        self.assertTrue(action['context']['create'])
        kanban = self.env['sgi.activity.role'].get_view(
            self.env.ref('quimibond_sgi.sgi_activity_role_view_kanban_mp').id, 'kanban')['arch']
        self.assertIn('on_create="quimibond_sgi.sgi_activity_change_action_new"', kanban)
        listing = self.env['sgi.activity.role'].get_view(
            self.env.ref('quimibond_sgi.sgi_activity_role_view_list_my_procedure').id, 'list')['arch']
        self.assertIn('js_class="sgi_my_activities_list"', listing, "56.6.2: «Nuevo» también en la lista.")
        opened = self.env['sgi.activity.change'].with_user(self.user_emp).with_context(
            **action['context']).action_sgi_new_from_context()
        proposal = self.env['sgi.activity.change'].browse(opened['res_id'])
        self.assertEqual(proposal.change_type, 'agregar')
        self.assertEqual(proposal.job_id, self.job)
        self.assertEqual(proposal.process_id, self.process)
        # Sin contexto (otro camino) toma el puesto del usuario.
        opened = self.env['sgi.activity.change'].with_user(self.user_emp).action_sgi_new_from_context()
        self.assertEqual(self.env['sgi.activity.change'].browse(opened['res_id']).process_id, self.process)

    def test_04_usuario_sin_empleado(self):
        lonely = new_test_user(self.env, login='mpc_lonely',
                               groups='base.group_user,quimibond_sgi.group_sgi_user')
        Wiz = self.env['sgi.my.procedure'].with_user(lonely)
        wiz = Wiz.browse(Wiz.action_open_mine()['res_id'])
        self.assertTrue(wiz.no_employee)
        mine = self.env['sgi.my.procedure'].with_user(self.user_emp)
        self.assertFalse(mine.browse(mine.action_open_mine()['res_id']).no_employee)

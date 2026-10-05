# -*- coding: utf-8 -*-
"""57.108.0: proponer una actividad con seis preguntas en lenguaje normal; el
Jefe MAST completa lo técnico antes de aprobar."""
from odoo.exceptions import AccessError, UserError
from odoo.tests import TransactionCase, new_test_user, tagged

from .common_documents import sgi_hide_real_documents


@tagged('post_install', '-at_install')
class TestPropuestaSencilla(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        cls.job = cls.env['hr.job'].create({'name': 'PLANEADOR PROPUESTA SENCILLA'})
        cls.other_job = cls.env['hr.job'].create({'name': 'ALMACENISTA PROPUESTA SENCILLA'})
        cls.user_emp = new_test_user(cls.env, login='ps_emp',
                                     groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.manager = new_test_user(cls.env, login='ps_mast',
                                    groups='base.group_user,quimibond_sgi.group_sgi_manager')
        cls.emp = cls.env['hr.employee'].create({
            'name': 'Emp Propuesta Sencilla', 'job_id': cls.job.id, 'user_id': cls.user_emp.id})
        cls.process = cls.env['sgi.process'].create({'code': 'ZPS', 'name': 'Proceso propuesta sencilla'})
        cls.env['sgi.process.activity'].create({
            'process_id': cls.process.id, 'name': 'Revisar pedidos',
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': cls.job.id})]})
        cls.env['approval.category'].search([('sgi_is_mp_change', '=', True)]).write(
            {'sgi_is_mp_change': False})
        cls.env['approval.category'].create({
            'name': 'Proponer cambio (prueba sencilla)', 'sgi_is_mp_change': True,
            'has_reference': 'required', 'manager_approval': False, 'approval_minimum': 1,
            'approver_ids': [(0, 0, {'user_id': cls.manager.id, 'required': True})]})

    def _new(self):
        screen = self.env['sgi.my.procedure'].with_user(self.user_emp).create({'employee_id': self.emp.id})
        action = screen.action_propose_new_activity()
        return self.env['sgi.activity.change'].browse(action['res_id']).with_user(self.user_emp)

    def test_01_seis_preguntas_llenan_lo_tecnico(self):
        proposal = self._new()
        self.assertEqual(proposal.q_when, 'evento')
        self.assertEqual(proposal.q_who, 'yo')
        proposal.write({
            'name': 'Publicar el programa semanal', 'q_when': 'semanal', 'due_weekday': '0',
            'q_who': 'yo', 'q_where': 'odoo',
            'done_criteria': 'El programa está publicado antes de las 9 del lunes',
            'reason': 'Hoy se publica cuando se puede'})
        self.assertEqual((proposal.measure_cadence, proposal.due_weekday, proposal.exec_channel),
                         ('semanal', '0', 'odoo'))
        self.assertEqual(proposal._sgi_executor_line().job_id, self.job)
        self.assertIn('Cada lunes', proposal.preview_html)
        self.assertIn('publicar el programa semanal en Odoo', proposal.preview_html)
        self.assertFalse(proposal.hints_html)
        # Cambiar de cadencia limpia el vencimiento que ya no aplica.
        proposal.write({'q_when': 'mensual', 'due_business_day': 5})
        self.assertEqual((proposal.measure_cadence, proposal.due_weekday, proposal.due_business_day),
                         ('mensual', False, 5))
        proposal.action_submit()
        proposal.request_id.with_user(self.manager).action_approve()
        activity = proposal.activity_id
        self.assertEqual((activity.measure_cadence, activity.due_business_day, activity.exec_channel),
                         ('mensual', 5, 'odoo'))
        self.assertEqual(activity.role_ids.filtered(lambda r: r.role == 'ejecuta').job_id, self.job)

    def test_02_por_evento_otro_puesto_y_avisos(self):
        proposal = self._new()
        proposal.write({'name': 'Dar seguimiento a los pedidos', 'q_when': 'evento',
                        'q_who': 'otro', 'q_who_job_id': self.other_job.id, 'q_where': 'papel'})
        self.assertIn('no se puede ver ni contar', proposal.hints_html)
        self.assertIn('qué pasa que la dispara', proposal.hints_html)
        self.assertEqual(proposal._sgi_executor_line().job_id, self.other_job)
        self.assertEqual(len(proposal.role_line_ids.filtered(lambda r: r.role == 'ejecuta')), 1,
                         "Cambia el ejecutor, no agrega otro.")
        proposal.write({'name': 'Avisar a Ventas los pedidos atrasados',
                        'trigger_note': 'Llega un pedido nuevo',
                        'done_criteria': 'Ventas tiene el aviso', 'reason': 'No se avisa'})
        self.assertIn('Cada vez que llega un pedido nuevo', proposal.preview_html)
        proposal.action_submit()
        self.assertIn('Qué la dispara', proposal.request_id.reason)
        proposal.request_id.with_user(self.manager).action_approve()
        self.assertTrue(proposal.activity_id.description.startswith('Se hace cuando llega un pedido nuevo.'))

    def test_03_mast_completa_antes_de_aprobar(self):
        proposal = self._new()
        proposal.write({'name': 'Contar el inventario de químicos', 'q_when': 'mensual',
                        'due_business_day': 3, 'q_where': 'fisico',
                        'done_criteria': 'El conteo cuadra', 'reason': 'Faltan químicos'})
        self.assertIn('Rol «Escala»', proposal.missing_html)
        proposal.action_submit()
        request = proposal.request_id
        with self.assertRaises(AccessError):
            request.with_user(self.user_emp).action_sgi_mp_complete()
        action = request.with_user(self.manager).action_sgi_mp_complete()
        self.assertEqual(action['res_id'], proposal.id)
        full = proposal.with_user(self.manager)
        full.write({'on_fail': 'Avisar al jefe de almacén el mismo día'})
        full.action_sgi_mast_complete()
        self.assertIn('Avisar al jefe de almacén', request.reason)
        with self.assertRaises(UserError):
            proposal.write({'on_fail': 'Otra cosa'})

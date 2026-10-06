# -*- coding: utf-8 -*-
"""57.103.0: registro de cumplimiento por actividad, responsable y periodo
(``sgi.activity.execution``): renglones del periodo en curso, En proceso /
Hecha / No aplica desde Mis pendientes, evidencia en las de registro manual,
cierre solo por el registro de Odoo, medición de las manuales desde el
registro y el faltante «Pantalla que no va con su medición»."""
from datetime import date

from odoo.exceptions import AccessError, UserError
from odoo.tests import TransactionCase, new_test_user, tagged

from .common_calendar import sgi_test_calendar
from .common_documents import sgi_hide_real_documents
from ..models.sgi_calendar import sgi_today


@tagged('post_install', '-at_install')
class TestRegistroCumplimiento(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        sgi_test_calendar(cls.env)
        cls.job = cls.env['hr.job'].create({'name': 'PUESTO REGISTRO CUMPLIMIENTO'})
        cls.boss_user = new_test_user(cls.env, login='rc_boss',
                                      groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.user = new_test_user(cls.env, login='rc_emp',
                                 groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.other_user = new_test_user(cls.env, login='rc_other',
                                       groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.boss = cls.env['hr.employee'].create({
            'name': 'Jefe Registro', 'user_id': cls.boss_user.id})
        cls.emp = cls.env['hr.employee'].create({
            'name': 'Emp Registro', 'job_id': cls.job.id, 'user_id': cls.user.id,
            'parent_id': cls.boss.id})
        cls.process = cls.env['sgi.process'].create({'code': 'ZRC', 'name': 'Proceso registro'})
        Activity = cls.env['sgi.process.activity']
        ejecuta = [(0, 0, {'role': 'ejecuta', 'job_id': cls.job.id})]
        cls.manual = Activity.create({
            'process_id': cls.process.id, 'name': 'Conciliar el auxiliar',
            'role_ids': ejecuta, 'measure_method': 'manual', 'measure_cadence': 'mensual',
            'due_business_day': 5, 'done_criteria': 'El auxiliar cuadra con el banco'})
        cls.partner_model = cls.env['ir.model']._get('res.partner')
        cls.auto = Activity.create({
            'process_id': cls.process.id, 'name': 'Dar de alta al contacto',
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': cls.job.id})],
            'measure_method': 'odoo', 'measure_cadence': 'semanal', 'due_weekday': '4',
            'measure_model_id': cls.partner_model.id,
            'measure_domain': "[('name', '=', 'ZRC evidencia de alta')]"})
        cls.event = Activity.create({
            'process_id': cls.process.id, 'name': 'Atender la queja',
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': cls.job.id})],
            'measure_method': 'manual', 'measure_cadence': 'evento'})
        cls.env.flush_all()
        cls.Exec = cls.env['sgi.activity.execution']
        cls.today = sgi_today(cls.env)
        cls.Exec._sgi_generate(cls.today)

    def _row(self, activity):
        return self.Exec.search([('activity_id', '=', activity.id), ('employee_id', '=', self.emp.id)])

    def test_01_un_renglon_por_actividad_responsable_y_periodo(self):
        manual = self._row(self.manual)
        self.assertEqual(len(manual), 1)
        start, end, due = self.manual._sgi_exec_period(self.today)
        self.assertEqual((manual.period_start, manual.period_end, manual.date_due), (start, end, due))
        self.assertEqual(manual.state, 'pendiente')
        self.assertTrue(manual.needs_evidence)
        self.assertFalse(self._row(self.event), "Las «por evento» no tienen periodo.")
        created, _removed = self.Exec._sgi_generate(self.today)
        self.assertFalse(created.filtered(lambda r: r.employee_id == self.emp),
                         "Correr de nuevo no duplica.")

    def test_02_periodos_sin_vencimiento_capturado(self):
        Activity = self.env['sgi.process.activity']
        quarterly = Activity.create({'process_id': self.process.id, 'name': 'Revisión trimestral ZRC',
                                     'measure_method': 'manual', 'measure_cadence': 'trimestral'})
        start, end, due = quarterly._sgi_exec_period(date(2046, 5, 20))
        self.assertEqual((start, end), (date(2046, 4, 1), date(2046, 6, 30)))
        self.assertEqual(due, date(2046, 6, 29), "Último día hábil: el 30 de junio de 2046 es sábado.")
        daily = Activity.create({'process_id': self.process.id, 'name': 'Diaria ZRC',
                                 'measure_method': 'manual', 'measure_cadence': 'diaria'})
        self.assertIsNone(daily._sgi_exec_period(date(2046, 6, 30)), "Sábado: sin periodo.")
        self.assertEqual(daily._sgi_exec_period(date(2046, 6, 29)),
                         (date(2046, 6, 29), date(2046, 6, 29), date(2046, 6, 29)))

    def test_03_mis_pendientes_en_proceso_y_hecho(self):
        execution = self._row(self.manual)
        Pending = self.env['sgi.my.pending'].with_user(self.user)
        action = Pending.action_open_mine()
        rows = Pending.search(action['domain']).filtered(
            lambda r: r.res_model == 'sgi.activity.execution' and r.res_id == execution.id)
        self.assertEqual(len(rows), 1)
        self.assertTrue(rows.can_mark_exec)
        self.assertTrue(rows.exec_manual)
        # «Ir» abre el registro (criterio, instructivo, evidencia), no un menú.
        opened = rows.action_open()
        self.assertEqual((opened['res_model'], opened['res_id']), ('sgi.activity.execution', execution.id))
        # En proceso: con nota, no quita el atraso.
        wizard_action = rows.action_exec_progress()
        Mark = self.env['sgi.activity.execution.mark'].with_user(self.user)
        with self.assertRaises(UserError):
            Mark.with_context(wizard_action['context']).create({}).action_confirm()
        Mark.with_context(wizard_action['context']).create({
            'progress_note': 'Faltan dos cuentas', 'date_estimated': self.today}).action_confirm()
        self.assertEqual(execution.state, 'en_proceso')
        rows = Pending.search(Pending.action_open_mine()['domain']).filtered(
            lambda r: r.res_id == execution.id and r.res_model == 'sgi.activity.execution')
        self.assertEqual(rows.exec_state, 'en_proceso')
        self.assertIn('Faltan dos cuentas', rows.name)
        if execution.date_due < self.today:
            self.assertEqual(rows.state, 'atrasada', "En proceso no quita el atraso.")
        # Hecho: el registro manual pide evidencia.
        done_action = rows.action_exec_done()
        with self.assertRaises(UserError):
            Mark.with_context(done_action['context']).create({}).action_confirm()
        Mark.with_context(done_action['context']).create({
            'evidence_note': 'Póliza de ajuste 1234'}).action_confirm()
        self.assertEqual(execution.state, 'hecha')
        self.assertEqual(execution.done_by_id, self.user)
        self.assertEqual(self.manual.measure_state, 'verde',
                         "La medición de la manual sale de su registro.")
        rows = Pending.search(Pending.action_open_mine()['domain']).filtered(
            lambda r: r.res_id == execution.id and r.res_model == 'sgi.activity.execution')
        self.assertFalse(rows, "Hecha ya no sale en Mis pendientes.")

    def test_04_quien_puede_marcar(self):
        execution = self._row(self.manual)
        with self.assertRaises(AccessError):
            execution.with_user(self.other_user)._sgi_mark_progress('No es mía')
        execution.with_user(self.boss_user)._sgi_mark_not_applicable('Sin movimientos en el mes')
        self.assertEqual(execution.state, 'no_aplica')
        self.assertEqual(self.manual.measure_state, 'verde', "No aplica cuenta como cumplido.")
        with self.assertRaises(UserError):
            execution.with_user(self.boss_user)._sgi_mark_done('Otra vez')

    def test_05_se_marca_sola_con_su_registro(self):
        execution = self._row(self.auto)
        self.assertEqual(len(execution), 1)
        self.assertFalse(execution.needs_evidence)
        self.assertFalse(self.Exec._sgi_auto_close(self.today) & execution)
        self.env['res.partner'].create({'name': 'ZRC evidencia de alta'})
        closed = self.Exec._sgi_auto_close(self.today)
        self.assertIn(execution, closed)
        self.assertEqual(execution.state, 'hecha')
        self.assertTrue(execution.done_auto)

    def test_06_pantalla_que_no_va_con_su_medicion(self):
        action = self.env['ir.actions.act_window'].create({
            'name': 'Usuarios ZRC', 'res_model': 'res.users'})
        self.auto.odoo_action_id = action
        codes = self.auto.spec_gap_ids.mapped('code')
        self.assertIn('menu_model_mismatch', codes)
        gap = self.auto.spec_gap_ids.filtered(lambda g: g.code == 'menu_model_mismatch')
        self.assertIn('res.users', gap.message)
        self.assertIn('res.partner', gap.message)
        partners = self.env['ir.actions.act_window'].create({
            'name': 'Contactos ZRC', 'res_model': 'res.partner'})
        self.auto.odoo_action_id = partners
        self.assertNotIn('menu_model_mismatch', self.auto.spec_gap_ids.mapped('code'))

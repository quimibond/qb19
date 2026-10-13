# -*- coding: utf-8 -*-
"""57.103.0: registro de cumplimiento por actividad, responsable y periodo
(``sgi.activity.execution``): renglones del periodo en curso, En proceso /
Hecha / No aplica desde Mis pendientes, evidencia en las de registro manual,
cierre solo por el registro de Odoo, medición de las manuales desde el
registro y el faltante «Pantalla que no va con su medición»."""
from datetime import date, timedelta

from odoo.exceptions import AccessError, UserError
from odoo.tests import TransactionCase, new_test_user, tagged

from .common_calendar import sgi_test_calendar
from .common_documents import sgi_hide_real_documents
from .common_exec import sgi_exec_start_open
from ..models.sgi_calendar import sgi_today


@tagged('post_install', '-at_install')
class TestRegistroCumplimiento(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        sgi_test_calendar(cls.env)
        sgi_exec_start_open(cls.env)
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

    # ---- 57.149.0: escalamiento accionable, permisos, arranque, saneamiento ----
    def _late_exec(self, activity=None, employee=None, days=20):
        """Un periodo abierto que venció hace ``days`` días."""
        due = self.today - timedelta(days=days)
        return self.Exec.sudo().create({
            'activity_id': (activity or self.manual).id, 'employee_id': (employee or self.emp).id,
            'period_start': due - timedelta(days=25), 'period_end': due, 'date_due': due})

    def _escalator(self):
        """Usuario cuyo puesto tiene el rol «Escala» de la manual (sin ser jefe
        de nadie ni Jefe MAST)."""
        job = self.env['hr.job'].create({'name': 'ESCALA REGISTRO CUMPLIMIENTO'})
        self.env['sgi.activity.role'].create({'activity_id': self.manual.id, 'role': 'escala',
                                              'job_id': job.id, 'after_days': 0})
        user = new_test_user(self.env, login='rc_escala',
                             groups='base.group_user,quimibond_sgi.group_sgi_user')
        self.env['hr.employee'].create({'name': 'Escala Registro', 'job_id': job.id, 'user_id': user.id})
        self.env.flush_all()
        return user

    def _pending_rows(self, user, execution, kind=None):
        Pending = self.env['sgi.my.pending'].with_user(user)
        return Pending.search(Pending.action_open_mine()['domain']).filtered(
            lambda r: r.res_model == 'sgi.activity.execution' and r.res_id == execution.id
            and (kind is None or r.kind == kind))

    def test_07_escalamiento_apunta_al_periodo_y_se_cierra_al_momento(self):
        esc_user = self._escalator()
        execution = self._late_exec()
        rows = self._pending_rows(esc_user, execution, 'escalamiento')
        self.assertEqual(len(rows), 1, "Un renglón de escalamiento por periodo atrasado.")
        self.assertIn('Emp Registro', rows.name, "Dice de quién es el periodo.")
        self.assertTrue(rows.can_done and rows.can_mark_exec, "Quien escala puede marcarlo.")
        opened = rows.action_open()
        self.assertEqual((opened['res_model'], opened['res_id']), ('sgi.activity.execution', execution.id),
                         "«Ir» abre el periodo, no el catálogo.")
        done = rows.action_done()
        self.assertEqual(done['res_model'], 'sgi.activity.execution.mark')
        self.assertEqual((done['context']['default_mode'], done['context']['sgi_pending_row_id']),
                         ('done', rows.id))
        own = self._pending_rows(self.user, execution)
        boss = self._pending_rows(self.boss_user, execution)
        self.assertTrue(own, "Quien la ejecuta también la tiene.")
        na = rows.action_exec_not_applicable()
        Mark = self.env['sgi.activity.execution.mark'].with_user(esc_user)
        Mark.with_context(na['context']).create({'na_reason': 'La presentó el despacho externo'}).action_confirm()
        self.assertEqual(execution.state, 'no_aplica')
        self.assertFalse(rows.exists(), "El escalamiento sale en la misma transacción.")
        self.assertFalse((own | boss).exists(), "Y los renglones de los demás, sin esperar al cron.")

    def test_08_quien_puede_marcar_desde_57_149(self):
        execution = self._late_exec()
        director = new_test_user(self.env, login='rc_dir',
                                 groups='base.group_user,quimibond_sgi.group_sgi_director')
        admin = new_test_user(self.env, login='rc_admin',
                              groups='base.group_user,quimibond_sgi.group_sgi_admin')
        with self.assertRaises(AccessError):
            execution.with_user(self.other_user)._sgi_mark_not_applicable('No es mía')
        with self.assertRaises(AccessError):
            execution.with_user(self.other_user).action_mark_not_applicable()
        self.assertFalse(execution.with_user(self.other_user).can_mark, "Un tercero sin rol no marca.")
        self.assertTrue(execution.with_user(director).can_mark, "Dirección de Operaciones sí.")
        self.assertTrue(execution.with_user(admin).can_mark, "El Administrador SGI sí.")
        esc_user = self._escalator()
        self.assertTrue(execution.with_user(esc_user).can_mark, "Quien tiene el rol «Escala» sí.")
        self.assertFalse(self.Exec.with_user(esc_user)._sgi_user_can_mark(self.emp),
                         "Sin la actividad, el rol «Escala» no cuenta.")
        execution.with_user(director)._sgi_mark_not_applicable('Lo revisó Dirección')
        self.assertEqual(execution.state, 'no_aplica')

    def test_09_fecha_de_arranque_por_cadencia(self):
        Param = self.env['ir.config_parameter'].sudo()
        Activity = self.env['sgi.process.activity']
        Pending = self.env['sgi.my.pending']
        for cadence in ('mensual', 'trimestral', 'anual'):
            with self.subTest(cadence):
                activity = Activity.create({
                    'process_id': self.process.id, 'name': 'Arranque %s ZRC' % cadence,
                    'measure_method': 'manual', 'measure_cadence': cadence,
                    'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': self.job.id})]})
                self.env.flush_all()
                start, _end, due = activity._sgi_exec_period(self.today)
                prev_start, prev_end, prev_due = activity._sgi_exec_period(start - timedelta(days=1))
                previous = self.Exec.sudo().create({
                    'activity_id': activity.id, 'employee_id': self.emp.id, 'period_start': prev_start,
                    'period_end': prev_end, 'date_due': prev_due})
                # Arranque antes de todo: el periodo anterior sin hacer pone rojo.
                Param.set_param('quimibond_sgi.exec_start_date', '2000-01-01')
                activity._sgi_measure()
                self.assertEqual(activity.measure_state, 'rojo')
                late = Pending._sgi_activity_late_due(activity, self.today)
                # Arranque después del periodo anterior: ya no cuenta.
                Param.set_param('quimibond_sgi.exec_start_date', (prev_due + timedelta(days=1)).isoformat())
                activity._sgi_measure()
                self.assertNotEqual(activity.measure_state, 'rojo', "Antes del arranque no pone rojo.")
                if late and late < prev_due + timedelta(days=1):
                    self.assertFalse(Pending._sgi_activity_late_due(activity, self.today),
                                     "Antes del arranque no escala.")
                self.assertFalse(self._pending_rows(self.user, previous), "Ni sale en Mis pendientes.")
                # Arranque después del vencimiento del periodo en curso: no se crea.
                Param.set_param('quimibond_sgi.exec_start_date', (due + timedelta(days=1)).isoformat())
                created, _removed = self.Exec._sgi_generate(self.today)
                self.assertFalse(created.filtered(lambda r, a=activity: r.activity_id == a))
                row = Pending.sudo().create({
                    'kind': 'actividad', 'name': 'Arranque ZRC', 'employee_id': self.emp.id,
                    'user_id': self.user.id, 'res_model': 'sgi.process.activity', 'res_id': activity.id})
                with self.assertRaisesRegex(UserError, "antes del arranque"):
                    row._sgi_exec_from_activity()
                # Arranque en el vencimiento: sí se crea.
                Param.set_param('quimibond_sgi.exec_start_date', due.isoformat())
                created, _removed = self.Exec._sgi_generate(self.today)
                self.assertEqual(len(created.filtered(lambda r, a=activity: r.activity_id == a)), 1)
                Param.set_param('quimibond_sgi.exec_start_date', '2000-01-01')

    def test_10_saneamiento_y_no_aplica_masivo(self):
        old = self._late_exec(days=60)
        noted = self._late_exec(activity=self.auto, days=60)
        noted.sudo().write({'progress_note': 'Ya empecé'})
        recent = self._late_exec(days=5)
        self.env['ir.config_parameter'].sudo().set_param(
            'quimibond_sgi.exec_start_date', (self.today - timedelta(days=10)).isoformat())
        with self.assertRaises(AccessError):
            self.Exec.with_user(self.user)._sgi_close_prestart()
        manager = new_test_user(self.env, login='rc_mast',
                                groups='base.group_user,quimibond_sgi.group_sgi_manager')
        closed = self.Exec.with_user(manager)._sgi_close_prestart()
        self.assertIn(old, closed)
        self.assertNotIn(noted, closed, "Lo que tiene nota de avance no se toca.")
        self.assertNotIn(recent, closed, "Lo posterior al arranque no se toca.")
        self.assertEqual((old.state, old.na_reason), ('no_aplica', "Periodo vencido antes de la implementación del SGI"))
        self.assertTrue(old.message_ids.filtered(lambda m: 'antes de la implementación' in (m.body or '')))
        self.assertFalse(self.Exec.with_user(manager)._sgi_close_prestart(), "Volver a correrlo no hace nada.")
        # No aplica a varios con un motivo común; «Hecha» es de uno en uno.
        other = self._late_exec(days=3)
        both = (recent | other).with_user(self.boss_user)
        with self.assertRaises(UserError):
            both._sgi_mark_wizard('done')
        action = both.action_mark_not_applicable()
        self.assertEqual(action['context']['default_execution_ids'], [(6, 0, (recent | other).ids)])
        wizard = self.env['sgi.activity.execution.mark'].with_user(self.boss_user).with_context(
            action['context']).create({'na_reason': 'Cierre de planta'})
        self.assertEqual(wizard.execution_count, 2)
        wizard.action_confirm()
        self.assertEqual(set((recent | other).mapped('state')), {'no_aplica'})
        self.assertEqual(set((recent | other).mapped('na_reason')), {'Cierre de planta'})

    def test_11_cerrar_varios_periodos_con_renglones_guardados(self):
        """57.157.0: cerrar de un golpe varios periodos que ya tienen renglones
        en Mis pendientes no lee renglones borrados (tumbó la migración
        57.149.0 en producción)."""
        first = self._late_exec(days=60)
        second = self._late_exec(days=120)
        first_rows = self._pending_rows(self.user, first)
        second_rows = self._pending_rows(self.user, second)
        self.assertTrue(first_rows and second_rows, "Los dos periodos tienen renglón guardado antes de cerrarse.")
        rows = first_rows | second_rows
        self.env['ir.config_parameter'].sudo().set_param(
            'quimibond_sgi.exec_start_date', (self.today - timedelta(days=10)).isoformat())
        closed = self.Exec._sgi_close_prestart()
        self.assertIn(first, closed)
        self.assertIn(second, closed)
        self.assertEqual(set((first | second).mapped('state')), {'no_aplica'})
        self.assertFalse(rows.exists(), "Los renglones de los dos periodos salen en la misma corrida.")

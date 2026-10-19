# -*- coding: utf-8 -*-
"""56.3.0: «Mis pendientes» en una sola lista con semáforo, en la pantalla y
en Mi equipo."""
from datetime import timedelta

from odoo import fields
from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged, new_test_user

from .common_documents import sgi_hide_real_documents
from .common_exec import sgi_exec_start_open
from .common_escalations import sgi_escalations_on


@tagged('post_install', '-at_install')
class TestMyPending(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_escalations_on(cls.env)  # 57.159.0
        sgi_hide_real_documents(cls.env)
        sgi_exec_start_open(cls.env)
        today = fields.Date.context_today(cls.env.user)
        cls.job = cls.env['hr.job'].create({'name': 'PUESTO PENDIENTES MP'})
        cls.boss_user = new_test_user(cls.env, login='mpp_boss',
                                      groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.user = new_test_user(cls.env, login='mpp_emp',
                                 groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.boss = cls.env['hr.employee'].create({
            'name': 'Jefe Pendientes MP', 'job_id': cls.job.id, 'user_id': cls.boss_user.id})
        cls.emp = cls.env['hr.employee'].create({
            'name': 'Emp Pendientes MP', 'job_id': cls.job.id, 'user_id': cls.user.id,
            'parent_id': cls.boss.id})
        objective = cls.env['sgi.objective'].create({'name': 'Objetivo pendientes MP'})
        Action = cls.env['sgi.action.line']
        cls.late = Action.create({
            'name': 'Acción atrasada MP', 'responsible_id': cls.user.id,
            'date_commit': today - timedelta(days=3), 'objective_id': objective.id})
        cls.soon = Action.create({
            'name': 'Acción por vencer MP', 'responsible_id': cls.user.id,
            'date_commit': today + timedelta(days=3), 'objective_id': objective.id})
        cls.doc = cls.env['documents.document'].create({
            'name': 'Procedimiento pendientes MP', 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'procedimiento', 'sgi_code': 'P-A67', 'sgi_state': 'vigente',
            'sgi_owner_id': cls.user.id, 'sgi_next_review_date': today + timedelta(days=30)})

    def _mine(self, rows):
        return rows.filtered(lambda r: r.res_model in ('sgi.action.line', 'documents.document')
                             and r.res_id in (self.late | self.soon).ids + self.doc.ids)

    def test_01_un_boton_una_lista_con_semaforo(self):
        Wiz = self.env['sgi.my.procedure'].with_user(self.user)
        wiz = Wiz.browse(Wiz.action_open_mine()['res_id'])
        self.assertGreaterEqual(wiz.pending_total, 3)
        self.assertGreaterEqual(wiz.pending_late, 1)
        action = wiz.action_show_pending()
        self.assertEqual(action['res_model'], 'sgi.my.pending')
        rows = self._mine(self.env['sgi.my.pending'].with_user(self.user).search(action['domain']))
        by_res = {(r.res_model, r.res_id): r for r in rows}
        self.assertEqual(by_res[('sgi.action.line', self.late.id)].state, 'atrasada')
        self.assertEqual(by_res[('sgi.action.line', self.soon.id)].state, 'por_vencer')
        self.assertEqual(by_res[('documents.document', self.doc.id)].state, 'al_dia')
        self.assertEqual(by_res[('documents.document', self.doc.id)].kind, 'documento')
        # Orden: atrasadas primero y luego por vencimiento.
        self.assertEqual(rows.mapped('state'), ['atrasada', 'por_vencer', 'al_dia'])
        opened = by_res[('sgi.action.line', self.late.id)].action_open()
        self.assertEqual((opened['res_model'], opened['res_id']), ('sgi.action.line', self.late.id))

    def test_02_mi_equipo_semaforo_por_persona(self):
        Public = self.env['hr.employee.public'].with_user(self.boss_user)
        row = Public.browse(self.emp.id)
        self.assertEqual(row.sgi_mp_pending_state, 'atrasada')
        self.assertGreaterEqual(row.sgi_mp_pending_late, 1)
        # 57.95.0 (K-08): los filtros de Mi equipo leen el resumen guardado
        # (cron nocturno o al abrir una lista de pendientes).
        self.env['hr.employee']._sgi_refresh_pending_summary(self.emp)
        self.assertIn(self.emp.id, Public.search([('sgi_mp_pending_late', '>', 0)]).ids)
        self.assertIn(self.emp.id, Public.search([('sgi_mp_pending_state', '=', 'atrasada')]).ids)
        action = Public.browse().action_sgi_team_pending()
        self.assertEqual(action['context'].get('search_default_group_employee'), 1)
        rows = self.env['sgi.my.pending'].with_user(self.boss_user).search(action['domain'])
        self.assertIn(self.emp.id, rows.employee_id.ids)
        single = row.action_sgi_open_pending()
        self.assertTrue(self.env['sgi.my.pending'].with_user(self.boss_user).search(single['domain']))

    # ---- 57.145.0: renglones guardados y «Hecho» por tipo ---------------------
    def _mine_rows(self, user=None):
        Pending = self.env['sgi.my.pending'].with_user(user or self.user)
        return Pending.search(Pending.action_open_mine()['domain'])

    def test_03_renglones_guardados_con_ids_estables(self):
        """El modelo ya no es transitorio: abrir la lista dos veces actualiza
        los mismos renglones en lugar de borrarlos y crearlos (los ids no
        cambian) y lo resuelto se quita."""
        self.assertFalse(self.env['sgi.my.pending']._transient)
        first = self._mine(self._mine_rows())
        ids = {(r.res_model, r.res_id): r.id for r in first}
        self.assertEqual(len(ids), 3)
        self.late.sudo().write({'name': 'Acción atrasada MP (renombrada)'})
        second = self._mine(self._mine_rows())
        self.assertEqual({(r.res_model, r.res_id): r.id for r in second}, ids,
                         "Los ids se conservan: la lista abierta en otra pestaña sigue válida.")
        row = second.filtered(lambda r: r.res_id == self.late.id and r.res_model == 'sgi.action.line')
        self.assertIn('renombrada', row.name)
        self.assertTrue(row.detail and 'Compromiso' in row.detail, "La ficha trae qué hay que hacer.")
        self.assertTrue(row.source_name)
        self.late.sudo().action_mark_done()
        third = self._mine(self._mine_rows())
        self.assertFalse(third.filtered(lambda r: r.res_id == self.late.id and r.res_model == 'sgi.action.line'),
                         "Lo resuelto se quita.")
        self.assertFalse(row.exists())

    def test_04_hecho_termina_la_accion_y_cierra_estandar(self):
        rows = self._mine(self._mine_rows())
        row = rows.filtered(lambda r: r.res_id == self.late.id and r.res_model == 'sgi.action.line')
        self.assertTrue(row.can_done)
        # Un jefe que ve la lista de su gente no tiene «Hecho» en lo ajeno.
        theirs = self.env['hr.employee.public'].with_user(self.boss_user).browse(self.emp.id)
        boss_rows = self.env['sgi.my.pending'].with_user(self.boss_user).search(
            theirs.action_sgi_open_pending()['domain'])
        boss_row = boss_rows.filtered(lambda r: r.res_id == self.late.id and r.res_model == 'sgi.action.line')
        self.assertEqual(boss_row.id, row.id, "Mismo renglón para la persona y su jefe.")
        self.assertFalse(boss_row.can_done)
        result = row.with_user(self.user).action_done()
        self.assertEqual(result['tag'], 'display_notification')
        self.assertEqual(result['params']['next']['type'], 'ir.actions.act_window_close')
        self.assertTrue(self.late.date_done)
        self.assertFalse(row.exists(), "El renglón hecho sale de la lista en el servidor.")

    def test_05_documento_por_revisar_abre_su_revision(self):
        """57.150.0 (auditoría UX, fase 2): antes «se resolvía en su registro»
        (sin «Hecho»); ahora el botón «Revisar» abre la revisión del documento
        (sigue vigente o necesita cambio). «Ir» sigue abriendo el documento."""
        rows = self._mine(self._mine_rows())
        doc_row = rows.filtered(lambda r: r.res_model == 'documents.document')
        self.assertTrue(doc_row.can_done)
        self.assertEqual(doc_row.with_user(self.user).action_done()['res_model'], 'sgi.doc.review')
        self.assertEqual(doc_row.with_user(self.user).action_open()['res_model'], 'documents.document')

    def _activity_with_roles(self, name, **vals):
        """Una actividad atrasada (semáforo en rojo) que ejecuta el puesto de
        la persona y escala al del jefe tras 0 días hábiles (como
        test_bandeja)."""
        if not hasattr(self, '_mp_process'):
            boss_job = self.env['hr.job'].create({'name': 'JEFE PENDIENTES MP'})
            self.boss.sudo().write({'job_id': boss_job.id})
            self._mp_boss_job = boss_job
            self._mp_process = self.env['sgi.process'].create(
                {'code': 'ZMP', 'name': 'Proceso pendientes MP'})
        activity = self.env['sgi.process.activity'].create(dict({
            'process_id': self._mp_process.id, 'name': name,
            'role_ids': [(0, 0, {'job_id': self.job.id, 'role': 'ejecuta'}),
                         (0, 0, {'job_id': self._mp_boss_job.id, 'role': 'escala', 'after_days': 0})],
        }, **vals))
        activity.sudo().write({'measure_state': 'rojo'})
        activity.invalidate_recordset()
        (self.emp | self.boss).invalidate_recordset()
        return activity

    def test_06_escalamiento_tipo_propio_sin_hecho(self):
        # Mensual que vence el día hábil 1 y se mide por su registro en Odoo.
        activity = self._activity_with_roles('Cerrar el mes MP', measure_method='odoo',
                                             measure_cadence='mensual', due_business_day=1)
        boss_rows = self._mine_rows(self.boss_user).filtered(
            lambda r: r.res_model == 'sgi.process.activity' and r.res_id == activity.id)
        self.assertEqual(boss_rows.mapped('kind'), ['escalamiento'])
        self.assertFalse(boss_rows.can_done,
                         "Sin registro del periodo, quien escala no la marca hecha (57.149.0: con registro, "
                         "sí; test_registro_cumplimiento.test_07).")
        self.assertIn('Atrasada desde', boss_rows.detail)
        with self.assertRaises(UserError):
            boss_rows.with_user(self.boss_user).action_done()
        mine = self._mine_rows().filtered(
            lambda r: r.res_model == 'sgi.process.activity' and r.res_id == activity.id)
        self.assertEqual(mine.mapped('kind'), ['actividad'])
        self.assertTrue(mine.can_done, "Lleva registro de cumplimiento: «Hecho» lo crea.")
        # Por evento no lleva registro: sin «Hecho»; se cumple con «Ir».
        event = self._activity_with_roles('Atender la queja MP', measure_method='manual',
                                          measure_cadence='evento')
        row = self._mine_rows().filtered(
            lambda r: r.res_model == 'sgi.process.activity' and r.res_id == event.id)
        self.assertEqual(row.mapped('kind'), ['actividad'])
        self.assertFalse(row.can_done)
        with self.assertRaises(UserError):
            row.with_user(self.user).action_done()

    def test_07_hecho_crea_el_registro_del_periodo_al_vuelo(self):
        """Actividad de registro manual atrasada por semáforo y todavía sin
        renglón del periodo (el cron nocturno no ha corrido): «Hecho» crea el
        registro del periodo en curso, convierte el renglón (mismo id) y abre
        el asistente; al guardar con evidencia el renglón se va."""
        activity = self._activity_with_roles('Conciliar el auxiliar MP',
                                             measure_method='manual', measure_cadence='mensual')
        row = self._mine_rows().filtered(
            lambda r: r.res_model == 'sgi.process.activity' and r.res_id == activity.id)
        if not activity._sgi_exec_period(fields.Date.context_today(self.env.user)):
            self.skipTest("Día inhábil: sin periodo en curso.")
        self.assertEqual(len(row), 1)
        self.assertTrue(row.can_done)
        self.assertFalse(row.can_mark_exec, "Todavía no tiene registro del periodo.")
        row_id = row.id
        action = row.with_user(self.user).action_done()
        self.assertEqual(action['res_model'], 'sgi.activity.execution.mark')
        self.assertEqual(action['context']['default_mode'], 'done')
        self.assertEqual(action['context']['sgi_pending_row_id'], row_id)
        execution = self.env['sgi.activity.execution'].sudo().search(
            [('activity_id', '=', activity.id), ('employee_id', '=', self.emp.id)])
        self.assertEqual(len(execution), 1)
        row = self.env['sgi.my.pending'].with_user(self.user).browse(row_id)
        self.assertEqual((row.res_model, row.res_id), ('sgi.activity.execution', execution.id))
        self.assertTrue(row.can_mark_exec)
        Mark = self.env['sgi.activity.execution.mark'].with_user(self.user)
        result = Mark.with_context(action['context']).create(
            {'evidence_note': 'Póliza 77'}).action_confirm()
        self.assertEqual(result['tag'], 'display_notification')
        self.assertEqual(result['params']['next']['type'], 'ir.actions.act_window_close')
        self.assertEqual(execution.state, 'hecha')
        self.assertFalse(row.exists())
        self.assertFalse(self._mine_rows().filtered(
            lambda r: r.res_id in (activity.id, execution.id)
            and r.res_model in ('sgi.process.activity', 'sgi.activity.execution')),
            "Hecha, ya no vuelve a salir.")

    def test_08_posponer_aviso_propio(self):
        # Sobre un proceso (modelo sgi.*): un registro que no tiene renglón
        # propio del usuario, para que el aviso no quede cubierto.
        process = self.env['sgi.process'].create({'code': 'ZMP8', 'name': 'Proceso aviso MP'})
        notice = process.activity_schedule(
            'mail.mail_activity_data_todo', date_deadline=fields.Date.context_today(self.env.user),
            summary='Revisar aviso MP', user_id=self.user.id)
        notice.sudo().sgi_cron_key = 'aviso_prueba_mp:%d' % process.id
        row = self._mine_rows().filtered(lambda r: r.kind == 'aviso' and r.res_id == notice.id)
        self.assertEqual(len(row), 1)
        self.assertTrue(row.can_done and row.can_snooze)
        self.assertIn('Revisar aviso MP', row.name)
        result = row.with_user(self.user).action_snooze_notice()
        self.assertEqual(result['tag'], 'display_notification')
        self.assertGreater(notice.date_deadline, fields.Date.context_today(self.env.user))
        self.assertEqual(row.date_due, notice.date_deadline)
        self.assertNotEqual(row.state, 'atrasada')
        # El jefe no pospone ni marca hecho el aviso de otro.
        with self.assertRaises(UserError):
            row.with_user(self.boss_user).action_snooze_notice()
        result = row.with_user(self.user).action_done()
        self.assertEqual(result['params']['next']['type'], 'ir.actions.act_window_close')
        self.assertFalse(notice.exists() and notice.active)
        self.assertFalse(row.exists())

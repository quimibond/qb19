# -*- coding: utf-8 -*-
"""56.3.0: «Mis pendientes» en una sola lista con semáforo, en la pantalla y
en Mi equipo."""
from datetime import timedelta

from odoo import fields
from odoo.tests import TransactionCase, tagged, new_test_user

from .common_documents import sgi_hide_real_documents


@tagged('post_install', '-at_install')
class TestMyPending(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
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

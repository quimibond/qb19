# -*- coding: utf-8 -*-
"""PR 2 del plan (19.0.49.0.0): no conformidades que sí se cierran.
NC-1 plazos por etapa con aviso y escalamiento, NC-2 contención obligatoria
en reclamaciones, NC-3 eficacia programada, NC-4 cancelar solo con motivo
aprobado por el Jefe MAST, NC-5 solo las etapas del SGI."""
from datetime import date, datetime, timedelta

from odoo import fields
from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged, new_test_user

from .common_calendar import sgi_test_calendar


@tagged('post_install', '-at_install')
class TestNcDeadlines(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_test_calendar(cls.env)
        cls.team = cls.env.ref('quimibond_sgi.sgi_quality_team_internal')
        cls.stage_open = cls.env.ref('quimibond_sgi.sgi_nc_int_stage_open')
        cls.stage_follow = cls.env.ref('quimibond_sgi.sgi_nc_int_stage_followup')
        cls.stage_closed = cls.env.ref('quimibond_sgi.sgi_nc_int_stage_closed')
        cls.stage_cancel = cls.env.ref('quimibond_sgi.sgi_nc_int_stage_cancel')
        cls.user = new_test_user(cls.env, login='ncd_user',
                                 groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.manager = new_test_user(cls.env, login='ncd_mgr',
                                    groups='base.group_user,quimibond_sgi.group_sgi_manager')
        cls.owner_user = new_test_user(cls.env, login='ncd_owner',
                                       groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.owner = cls.env['hr.employee'].create({'name': 'Dueño NC', 'user_id': cls.owner_user.id})
        cls.process = cls.env['sgi.process'].create({
            'code': 'XNCD', 'name': 'Proceso NC plazos', 'owner_id': cls.owner.id})

    def _nc(self, days_ago=0, **vals):
        alert = self.env['quality.alert'].create(dict({
            'title': 'NC plazos', 'team_id': self.team.id,
            'sgi_process_id': self.process.id,
            'sgi_responsible_ids': [(6, 0, self.user.ids)]}, **vals))
        if days_ago:
            old = datetime.now() - timedelta(days=days_ago)
            self.env.cr.execute("UPDATE quality_alert SET create_date = %s WHERE id = %s",
                                (old, alert.id))
            alert.invalidate_recordset()
            alert._sgi_set_deadlines(force=True)
        return alert

    def _summaries(self, alert, user=None):
        domain = [('res_model', '=', 'quality.alert'), ('res_id', '=', alert.id)]
        if user:
            domain.append(('user_id', '=', user.id))
        return self.env['mail.activity'].search(domain).mapped('summary')

    def test_01_plazos_al_abrir_y_estados_por_hecho(self):
        nc = self._nc()
        self.assertTrue(nc.sgi_due_containment and nc.sgi_due_root_cause and nc.sgi_due_plan)
        self.assertLess(nc.sgi_due_containment, nc.sgi_due_root_cause)
        self.assertLess(nc.sgi_due_root_cause, nc.sgi_due_plan)
        self.assertEqual((nc.sgi_containment_state, nc.sgi_root_cause_state, nc.sgi_plan_state),
                         ('pendiente', 'pendiente', 'pendiente'))
        self.env['sgi.action.line'].create({
            'alert_id': nc.id, 'action_type': 'contencion', 'name': 'Segregar lote',
            'responsible_id': self.user.id, 'date_commit': date.today()})
        self.assertEqual(nc.sgi_containment_state, 'hecha')
        nc.sgi_root_cause = 'Sin instructivo'
        self.assertEqual(nc.sgi_root_cause_state, 'hecha')
        self.env['sgi.action.line'].create({
            'alert_id': nc.id, 'action_type': 'correctiva', 'name': 'Escribir IT',
            'responsible_id': self.user.id, 'date_commit': date.today()})
        self.assertEqual(nc.sgi_plan_state, 'hecha')
        # Una NC sin folio (alerta de piso) no lleva plazos.
        floor = self.env['quality.alert'].create({'title': 'Alerta de piso'})
        self.assertFalse(floor.sgi_due_plan)

    def test_02_aviso_el_dia_y_escalamiento(self):
        nc = self._nc()
        # El día que vence la contención: aviso al responsable, sin escalar.
        nc._sgi_deadline_escalation(nc.sgi_due_containment)
        self.assertTrue(any('contención vence' in s for s in self._summaries(nc, self.user)))
        self.assertFalse(any('escalada' in s for s in self._summaries(nc)))
        # Vencida: escala al dueño del proceso; a los 3+ días, a MAST.
        nc = self._nc(days_ago=5)
        today = fields.Date.context_today(nc)
        nc._sgi_deadline_escalation(today)
        self.assertEqual(nc.sgi_containment_state, 'vencida')
        self.assertTrue(any('dueño del proceso' in s for s in self._summaries(nc, self.owner_user)))
        nc._sgi_deadline_escalation(today + timedelta(days=5))
        mast = self.env['sgi.cron']._sgi_manager_user_id()
        self.assertTrue(any('escalada a MAST' in s for s in self._summaries(
            nc, self.env['res.users'].browse(mast))))
        # Idempotente: una segunda corrida no duplica.
        before = len(self._summaries(nc))
        nc._sgi_deadline_escalation(today + timedelta(days=5))
        self.assertEqual(before, len(self._summaries(nc)))
        # El cron diario también lo corre.
        self.env['sgi.cron'].cron_nonconformities()

    def test_03_reclamacion_no_avanza_sin_contencion(self):
        nc = self._nc(sgi_origin_type='reclamacion')
        with self.assertRaises(UserError):
            nc.with_user(self.manager).write({'stage_id': self.stage_follow.id})
        self.env['sgi.action.line'].create({
            'alert_id': nc.id, 'action_type': 'contencion', 'name': 'Retener embarque',
            'responsible_id': self.user.id, 'date_commit': date.today()})
        nc.with_user(self.manager).write({'stage_id': self.stage_follow.id})
        self.assertEqual(nc.stage_id, self.stage_follow)
        # Una NC de proceso avanza sin contención.
        other = self._nc()
        other.with_user(self.manager).write({'stage_id': self.stage_follow.id})

    def test_04_eficacia_programada_y_cierre(self):
        nc = self._nc(sgi_root_cause='causa')
        line = self.env['sgi.action.line'].create({
            'alert_id': nc.id, 'action_type': 'correctiva', 'name': 'Capacitar',
            'responsible_id': self.user.id, 'date_commit': date.today()})
        self.assertFalse(nc.sgi_effectiveness_due)
        line.action_mark_done()
        self.assertEqual(nc.sgi_effectiveness_due, date.today() + timedelta(days=90))
        mast = self.env['sgi.cron']._sgi_manager_user_id()
        acts = self.env['mail.activity'].search([
            ('res_model', '=', 'quality.alert'), ('res_id', '=', nc.id),
            ('summary', 'ilike', 'eficacia'), ('user_id', '=', mast)])
        self.assertTrue(acts)
        self.assertEqual(acts[0].date_deadline, nc.sgi_effectiveness_due)
        with self.assertRaises(UserError):
            nc.write({'stage_id': self.stage_closed.id})
        nc.write({'sgi_effectiveness_note': 'Sin reincidencia', 'sgi_effectiveness_date': date.today()})
        nc.write({'stage_id': self.stage_closed.id})
        self.assertEqual(nc.stage_id, self.stage_closed)

    def test_05_cancelar_con_motivo_y_aprobacion(self):
        nc = self._nc()
        # Arrastrar a Cancelada no se permite, ni al Jefe MAST.
        with self.assertRaises(UserError):
            nc.with_user(self.user).write({'stage_id': self.stage_cancel.id})
        with self.assertRaises(UserError):
            nc.with_user(self.manager).write({'stage_id': self.stage_cancel.id})
        # El usuario solicita: motivo al historial y actividad a MAST.
        wiz = self.env['sgi.nc.cancel'].with_user(self.user).create({
            'alert_id': nc.id, 'reason': 'Duplicada de NCI-0001'})
        self.assertFalse(wiz.is_manager)
        wiz.action_confirm()
        self.assertNotEqual(nc.stage_id, self.stage_cancel)
        self.assertEqual(nc.sgi_cancel_reason, 'Duplicada de NCI-0001')
        self.assertEqual(nc.sgi_cancel_requested_by, self.user)
        self.assertTrue(any('Aprobar cancelación' in s for s in self._summaries(nc)))
        # El Jefe MAST aprueba con el mismo asistente.
        wiz2 = self.env['sgi.nc.cancel'].with_user(self.manager).create({
            'alert_id': nc.id, 'reason': 'Duplicada de NCI-0001'})
        self.assertTrue(wiz2.is_manager)
        wiz2.action_confirm()
        self.assertEqual(nc.stage_id, self.stage_cancel)
        self.assertFalse(any('Aprobar cancelación' in s for s in self._summaries(nc)))
        self.assertTrue(any('cancelada' in (m.body or '').lower() for m in nc.message_ids))

    def test_06_solo_etapas_del_sgi(self):
        nc = self._nc()
        floor_team = self.env['quality.alert.team'].create({'name': 'Piso prueba'})
        stray = self.env['quality.alert.stage'].create({
            'name': 'Etapa de piso', 'sequence': 99, 'team_ids': [(6, 0, floor_team.ids)]})
        with self.assertRaises(UserError):
            nc.with_user(self.manager).write({'stage_id': stray.id})
        # Etapa sin equipos (compartida) sí se permite: no es ajena.
        Stage = self.env['quality.alert.stage']
        nc_stages = Stage.search([('team_ids', 'in', self.team.ids)])
        self.assertEqual(set(nc_stages.mapped('name')),
                         {'Abierta', 'Seguimiento', 'Cerrada', 'Cancelada'})

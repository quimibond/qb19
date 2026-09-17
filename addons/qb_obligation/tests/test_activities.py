# -*- coding: utf-8 -*-
"""Espejo en actividades nativas: una actividad por obligación abierta en su
documento o contacto; hecha = acuse, cancelada = descarte; el cierre por
evidencia marca la actividad hecha; cambios de dueño y fecha se reflejan."""
from odoo import fields
from odoo.tests import tagged

from .common import ObligationCommon


@tagged('post_install', '-at_install', 'qb_obligation')
class TestActivities(ObligationCommon):

    def _type(self):
        return self.env.ref('qb_obligation.mail_activity_type_obligation')

    def _assert_done(self, act):
        """Odoo conserva las actividades hechas archivadas (keep_done) o las borra."""
        self.assertTrue(not act.exists() or act.date_done or not act.active, 'la actividad quedó hecha')

    def test_activity_on_anchor_document(self):
        self._configure()
        inv = self._invoice(self.cliente, 1000.0)
        ob = self._promise_on(inv, confirm=False)
        act = ob.activity_id
        self.assertTrue(act)
        self.assertEqual((act.res_model, act.res_id), ('account.move', inv.id))
        self.assertEqual(act.activity_type_id, self._type())
        self.assertEqual(act.user_id, self.cxc)
        self.assertEqual(act.date_deadline, inv.invoice_date_due)
        self.assertTrue(act.summary.startswith('Por confirmar: '))
        self.assertIn('Se cierra sola cuando', act.note)
        ob.action_confirm()
        self.assertEqual(ob.activity_id, act, 'misma actividad, resumen sin el prefijo')
        self.assertFalse(act.summary.startswith('Por confirmar'))

    def test_activity_falls_back_when_owner_cannot_read_anchor(self):
        """Dueño sin acceso a la factura: la actividad va al contacto, no revienta."""
        self._configure()
        plain = self.env['res.users'].with_context(no_reset_password=True).create({
            'name': 'Sin contabilidad', 'login': 'plain@test.local', 'email': 'plain@test.local',
            'group_ids': [(6, 0, [self.env.ref('base.group_user').id])]})
        inv = self._invoice(self.cliente, 1000.0)
        ob = self._promise_on(inv, user_id=plain.id)
        self.assertEqual(ob.user_id, plain)
        self.assertEqual((ob.activity_id.res_model, ob.activity_id.res_id), ('res.partner', self.cliente.id))

    def test_activity_on_partner_without_anchor(self):
        self._configure()
        ob = self.Obligation.create_candidate({'obligation_type': 'collection.payment_promise', 'description': 'x',
                                               'partner_id': self.cliente.id, 'source_ref': 'r1',
                                               'date_deadline': '2026-09-20'})
        self.assertEqual((ob.activity_id.res_model, ob.activity_id.res_id), ('res.partner', self.cliente.id))
        self.assertEqual(ob.activity_id.date_deadline, fields.Date.from_string('2026-09-20'))

    def test_activity_on_itself_without_partner(self):
        self._configure()
        self.company.obligation_owner_comercial_id = self.conta
        ob = self.Obligation.create_candidate({'obligation_type': 'comercial.quote', 'description': 'x',
                                               'source_ref': 'r2'})
        self.assertEqual((ob.activity_id.res_model, ob.activity_id.res_id), ('qb.obligation', ob.id))

    def test_mark_done_is_owner_ack(self):
        self._configure()
        ob = self._promise_on(self._invoice(self.cliente, 1000.0))
        act = ob.activity_id
        act.action_feedback(feedback='Ya pagó, lo vi en el banco')
        self.assertEqual((ob.state, ob.close_method), ('done', 'owner_ack'))
        self.assertIn('Ya pagó', ob.evidence_summary)
        self._assert_done(act)

    def test_cancel_activity_discards(self):
        self._configure()
        ob = self._promise_on(self._invoice(self.cliente, 1000.0), confirm=False)
        ob.activity_id.unlink()
        self.assertEqual(ob.state, 'discarded')
        self.assertIn('Actividad cancelada', ob.discard_reason)
        # La descartada no se reutiliza: una nueva con la misma referencia es otra
        again = self.Obligation.create_candidate({
            'obligation_type': 'collection.payment_promise', 'partner_id': self.cliente.id, 'description': 'x',
            'source_ref': ob.source_ref, 'res_model': 'account.move', 'res_id': ob.res_id})
        self.assertNotEqual(again, ob)

    def test_evidence_closes_activity(self):
        self._configure()
        inv = self._invoice(self.cliente, 1000.0)
        ob = self._promise_on(inv)
        act = ob.activity_id
        self._pay(inv)
        self._run()
        self.assertEqual(ob.state, 'done')
        self._assert_done(act)
        done = self.env['mail.message'].search([('model', '=', 'account.move'), ('res_id', '=', inv.id),
                                                ('mail_activity_type_id', '=', self._type().id)])
        self.assertTrue(done, 'queda el mensaje de actividad hecha en el chatter de la factura')

    def test_discard_and_cancel_remove_activity(self):
        self._configure()
        a = self._promise_on(self._invoice(self.cliente, 1000.0))
        b = self._promise_on(self._invoice(self.cliente2, 500.0))
        act_a, act_b = a.activity_id, b.activity_id
        a.action_discard('no aplica')
        b.action_cancel()
        self.assertFalse(act_a.exists())
        self.assertFalse(act_b.exists())

    def test_owner_and_deadline_follow(self):
        self._configure()
        ob = self._promise_on(self._invoice(self.cliente, 1000.0))
        ob.write({'user_id': self.conta.id, 'date_deadline': '2026-12-01'})
        self.assertEqual(ob.activity_id.user_id, self.conta)
        self.assertEqual(ob.activity_id.date_deadline, fields.Date.from_string('2026-12-01'))

    def test_cron_backfills_missing_activities(self):
        self._configure()
        ob = self._promise_on(self._invoice(self.cliente, 1000.0))
        ob.activity_id.with_context(qb_obligation_skip_activity=True).unlink()
        ob.invalidate_recordset(['activity_id'])
        self.assertFalse(ob.activity_id)
        self._run()
        self.assertTrue(ob.activity_id)

# -*- coding: utf-8 -*-
"""Candidatas desde orígenes externos (correo): idempotencia, dueño, fecha
supuesta y el flujo confirmar / descartar / acuse."""
from odoo import fields
from odoo.exceptions import UserError
from odoo.tests import tagged

from .common import ObligationCommon


@tagged('post_install', '-at_install', 'qb_obligation')
class TestCandidates(ObligationCommon):

    def _promise(self, **extra):
        vals = {
            'obligation_type': 'collection.payment_promise',
            'description': 'El cliente prometió pagar el 20',
            'partner_id': self.cliente.id,
            'source': 'email', 'source_ref': 'thread:abc:promise',
            'source_thread_key': 'abc',
            'detection_payload': {'thread_id': 'abc', 'confidence': 0.9},
        }
        vals.update(extra)
        return self.Obligation.create_candidate(vals)

    def test_no_owner_returns_empty(self):
        self.assertFalse(self._promise())

    def test_create_candidate_idempotent(self):
        self._configure()
        a = self._promise(date_deadline='2026-09-20')
        b = self._promise(date_deadline='2026-09-25', description='Ahora dice el 25')
        self.assertEqual(a, b)
        self.assertEqual(a.state, 'candidate')
        self.assertEqual(a.user_id, self.cxc)
        self.assertEqual(a.partner_id, self.cliente)
        self.assertEqual(a.date_deadline, fields.Date.from_string('2026-09-25'))
        self.assertEqual(a.description, 'Ahora dice el 25')
        self.assertEqual(a.evidence_rule_key, 'owner_ack')
        self.assertFalse(a.weak_key)
        self.assertEqual(self.Obligation.search_count([('source_ref', '=', 'thread:abc:promise')]), 1)

    def test_weak_key_when_no_date(self):
        self._configure()
        a = self._promise()
        self.assertTrue(a.weak_key)
        self.assertEqual(a.date_deadline, fields.Date.add(fields.Date.today(), days=3))

    def test_confirm_discard_ack(self):
        self._configure()
        a = self._promise(date_deadline='2026-09-20')
        a.action_confirm()
        self.assertEqual(a.state, 'confirmed')
        self.assertTrue(a.confirmed_at)
        with self.assertRaises(UserError):
            a.action_confirm()
        a.action_ack_done()
        self.assertEqual(a.state, 'done')
        self.assertEqual(a.close_method, 'owner_ack')
        # Descartada: cuenta como ruido y una nueva con la misma referencia crea otra
        b = self._promise(source_ref='thread:xyz:promise', date_deadline='2026-09-20')
        b.action_discard(reason='No es una promesa, es una queja')
        self.assertEqual(b.state, 'discarded')
        self.assertEqual(b.discard_reason, 'No es una promesa, es una queja')
        self.assertTrue(b.discarded_at)
        c = self._promise(source_ref='thread:xyz:promise', date_deadline='2026-09-21')
        self.assertNotEqual(b, c)
        self.assertEqual(c.state, 'candidate')

    def test_candidate_anchored_to_invoice_closes_by_evidence(self):
        self._configure()
        inv = self._invoice(self.cliente, 1000.0)
        a = self._promise(res_model='account.move', res_id=inv.id, date_deadline='2026-09-20')
        self.assertEqual(a.evidence_rule_key, 'invoice_paid_or_credited')
        self.assertAlmostEqual(a.amount_residual, 1000.0, places=2)
        a.action_confirm()
        self._pay(inv)
        self._run()
        self.assertEqual(a.state, 'done')
        self.assertEqual(a.close_method, 'evidence')

    def test_invoice_smart_button_count(self):
        self._configure()
        inv = self._invoice(self.cliente, 1000.0)
        self._promise_on(inv)
        inv.invalidate_recordset()
        self.assertEqual(inv.obligation_count, 1)
        action = inv.action_open_obligations()
        self.assertEqual(action['res_model'], 'qb.obligation')

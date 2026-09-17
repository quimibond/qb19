# -*- coding: utf-8 -*-
"""Sin cobranza: la corrida no crea nada desde facturas vencidas. Cierre por
evidencia y escalación sobre obligaciones ancladas a facturas."""
from odoo import fields
from odoo.tests import tagged

from .common import ObligationCommon


@tagged('post_install', '-at_install', 'qb_obligation')
class TestFlow(ObligationCommon):

    def test_overdue_invoices_do_not_create_obligations(self):
        self._configure()
        self._invoice(self.cliente, 1000.0)
        self._run()
        self.assertFalse(self.Obligation.search([]), 'la cobranza vive en Contabilidad, no aquí')
        self.assertNotIn('collection.overdue_invoice', dict(self.Obligation._fields['obligation_type'].selection))

    def test_close_by_full_payment(self):
        self._configure()
        inv = self._invoice(self.cliente, 1000.0)
        ob = self._promise_on(inv)
        self.assertAlmostEqual(ob.amount_residual, 1000.0, places=2)
        self._pay(inv)
        self._run()
        self.assertEqual((ob.state, ob.close_method, ob.evidence_res_id), ('done', 'evidence', inv.id))
        self.assertIn(inv.name, ob.evidence_summary)
        self.assertAlmostEqual(ob.amount_collected, 1000.0, places=2)

    def test_partial_payment_keeps_open(self):
        self._configure()
        inv = self._invoice(self.cliente, 1000.0)
        ob = self._promise_on(inv)
        self._pay(inv, amount=400.0)
        self._run()
        self.assertEqual(ob.state, 'confirmed')
        self.assertAlmostEqual(ob.amount_residual, 600.0, places=2)
        self._pay(inv, amount=599.50)                     # dentro de la tolerancia de $1
        self._run()
        self.assertEqual(ob.state, 'done')

    def test_credit_note_closes_and_cancelled_invoice_cancels(self):
        self._configure()
        inv = self._invoice(self.cliente, 1000.0)
        ob = self._promise_on(inv)
        inv._reverse_moves(default_values_list=[{'invoice_date': fields.Date.from_string('2026-05-05')}], cancel=True)
        inv.invalidate_recordset()
        self._run()
        self.assertEqual(ob.state, 'done')
        inv2 = self._invoice(self.cliente2, 500.0)
        ob2 = self._promise_on(inv2)
        inv2.button_draft()
        inv2.button_cancel()
        self._run()
        self.assertEqual(ob2.state, 'cancelled')

    def test_escalation_thresholds(self):
        self._configure(escalation=self.direccion, obligation_escalate_days=0,
                        obligation_escalate_amount=500.0, obligation_escalate_overdue_days=10000)
        big = self._promise_on(self._invoice(self.cliente, 1000.0))
        small = self._promise_on(self._invoice(self.cliente2, 100.0))
        self._run()
        self.assertTrue(big.escalated_at)
        self.assertEqual(big.escalated_to, self.direccion)
        self.assertFalse(small.escalated_at, 'saldo chico y no tan vieja: no escala')
        self.company.obligation_escalate_overdue_days = 1
        self._run()
        self.assertTrue(small.escalated_at, 'muy vieja escala aunque sea chica')

    def test_escalation_clock_and_direction_user(self):
        self._configure(escalation=self.direccion, obligation_escalate_days=3, obligation_escalate_amount=1.0)
        ob = self._promise_on(self._invoice(self.cliente, 1000.0))
        self._run()
        self.assertFalse(ob.escalated_at, 'recién confirmada: el reloj corre desde la confirmación')
        self._configure(obligation_escalate_days=0, obligation_escalate_amount=1.0)
        self._run()
        self.assertFalse(ob.escalated_at, 'sin usuario de Dirección no se escala')

    def test_learned_owner_wins(self):
        """El encargado que la memoria aprendió va antes que el mapa de cobranza
        y que el dueño del área."""
        self._configure()
        self.company.obligation_owner_comercial_id = self.conta
        self.cliente.write({'memoria_owner_user_id': self.direccion.id,
                            'memoria_owner_areas': {'finanzas': {'user_id': self.conta.id, 'mailbox': 'cxc@x'}}})
        quote = self.Obligation.create_candidate({'obligation_type': 'comercial.quote', 'description': 'q',
                                                  'partner_id': self.cliente.id, 'source_ref': 'q1'})
        self.assertEqual(quote.user_id, self.direccion, 'general aprendido')
        promise = self.Obligation.create_candidate({'obligation_type': 'collection.payment_promise', 'description': 'p',
                                                    'partner_id': self.cliente.id, 'source_ref': 'p1'})
        self.assertEqual(promise.user_id, self.conta, 'por área aprendido')
        other = self.Obligation.create_candidate({'obligation_type': 'collection.payment_promise', 'description': 'p',
                                                  'partner_id': self.cliente2.id, 'source_ref': 'p2'})
        self.assertEqual(other.user_id, self.cxc, 'sin aprendizaje: mapa de cobranza')

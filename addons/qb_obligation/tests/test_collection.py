# -*- coding: utf-8 -*-
"""Cobranza: generación desde facturas vencidas, cierre por evidencia,
cancelación, escalación y reasignación cuando el SAT ya ve el pago."""
from odoo import fields
from odoo.tests import tagged

from .common import ObligationCommon


@tagged('post_install', '-at_install', 'qb_obligation')
class TestCollection(ObligationCommon):

    def test_no_owner_no_obligation(self):
        self._invoice(self.cliente, 1000.0)
        self._run()
        self.assertFalse(self.Obligation.search([]), 'sin dueño configurado no se crea nada')

    def test_generate_confirmed_and_idempotent(self):
        self._configure()
        inv = self._invoice(self.cliente, 1000.0)
        self._invoice(self.cliente2, 500.0, due='2027-01-01')          # no vencida
        self._invoice(self.cliente2, 700.0, post=False)                # borrador
        self._run()
        self._run()
        obs = self.Obligation.search([])
        self.assertEqual(len(obs), 1)
        ob = obs[0]
        self.assertEqual(ob.state, 'confirmed')
        self.assertEqual(ob.obligation_type, 'collection.overdue_invoice')
        self.assertEqual(ob.source, 'odoo')
        self.assertEqual(ob.user_id, self.cxc)
        self.assertEqual(ob.partner_id, self.cliente)
        self.assertEqual(ob.res_model, 'account.move')
        self.assertEqual(ob.res_id, inv.id)
        self.assertEqual(ob.evidence_rule_key, 'invoice_paid_or_credited')
        self.assertEqual(ob.date_deadline, fields.Date.from_string('2026-04-30'))
        self.assertAlmostEqual(ob.amount_at_creation, 1000.0, places=2)
        self.assertAlmostEqual(ob.amount_residual, 1000.0, places=2)
        self.assertGreater(ob.days_overdue, 0)
        self.assertTrue(ob.confirmed_at)

    def test_partner_owner_overrides_company_default(self):
        self._configure()
        self.cliente.with_company(self.company).collection_user_id = self.conta
        inv = self._invoice(self.cliente, 100.0)
        self._run()
        self.assertEqual(self._open_for(inv).user_id, self.conta)

    def test_only_partner_owner_configured(self):
        self.cliente.with_company(self.company).collection_user_id = self.cxc
        inv = self._invoice(self.cliente, 100.0)
        inv2 = self._invoice(self.cliente2, 100.0)
        self._run()
        self.assertEqual(self._open_for(inv).user_id, self.cxc)
        self.assertFalse(self._open_for(inv2), 'cliente sin dueño y compañía sin default: no se crea')

    def test_discard_is_sticky(self):
        self._configure()
        inv = self._invoice(self.cliente, 100.0)
        self._run()
        ob = self._open_for(inv)
        ob.action_discard('cartera histórica')
        self.assertEqual(ob.state, 'discarded')
        self.assertEqual(ob.discard_reason, 'cartera histórica')
        self._run()
        self.assertFalse(self._open_for(inv), 'una factura descartada no vuelve a generar obligación')
        self.assertEqual(self.Obligation.search_count([('res_id', '=', inv.id)]), 1)

    def test_grace_days(self):
        self._configure(obligation_grace_days=400)
        self._invoice(self.cliente, 100.0)
        self._run()
        self.assertFalse(self.Obligation.search([]))

    def test_close_by_full_payment(self):
        self._configure()
        inv = self._invoice(self.cliente, 1000.0)
        self._run()
        ob = self._open_for(inv)
        self._pay(inv)
        self._run()
        self.assertEqual(ob.state, 'done')
        self.assertEqual(ob.close_method, 'evidence')
        self.assertTrue(ob.closed_by_cron)
        self.assertEqual(ob.evidence_model, 'account.move')
        self.assertEqual(ob.evidence_res_id, inv.id)
        self.assertIn(inv.name, ob.evidence_summary)
        self.assertAlmostEqual(ob.amount_collected, 1000.0, places=2)
        self.assertGreaterEqual(ob.days_to_close, 0)
        # Ya cerrada, la factura no vuelve a generar
        self._run()
        self.assertEqual(len(self.Obligation.search([('res_id', '=', inv.id)])), 1)

    def test_partial_payment_keeps_open(self):
        self._configure()
        inv = self._invoice(self.cliente, 1000.0)
        self._run()
        ob = self._open_for(inv)
        self._pay(inv, amount=400.0)
        self._run()
        self.assertEqual(ob.state, 'confirmed')
        self.assertAlmostEqual(ob.amount_residual, 600.0, places=2)
        self._pay(inv, amount=599.50)                     # dentro de la tolerancia de $1
        self._run()
        self.assertEqual(ob.state, 'done')
        self.assertAlmostEqual(ob.amount_collected, 999.50, places=2)

    def test_credit_note_closes(self):
        self._configure()
        inv = self._invoice(self.cliente, 1000.0)
        self._run()
        ob = self._open_for(inv)
        inv._reverse_moves(default_values_list=[{'invoice_date': fields.Date.from_string('2026-05-05')}], cancel=True)
        inv.invalidate_recordset()
        self._run()
        self.assertEqual(ob.state, 'done')
        self.assertEqual(ob.close_method, 'evidence')

    def test_cancelled_invoice_cancels_obligation(self):
        self._configure()
        inv = self._invoice(self.cliente, 1000.0)
        self._run()
        ob = self._open_for(inv)
        inv.button_draft()
        inv.button_cancel()
        self._run()
        self.assertEqual(ob.state, 'cancelled')

    def test_escalation_thresholds(self):
        self._configure(escalation=self.direccion, obligation_escalate_days=0,
                        obligation_escalate_amount=500.0, obligation_escalate_overdue_days=10000)
        big = self._invoice(self.cliente, 1000.0)
        small = self._invoice(self.cliente2, 100.0)
        self._run()
        self.assertTrue(self._open_for(big).escalated_at)
        self.assertEqual(self._open_for(big).escalated_to, self.direccion)
        self.assertFalse(self._open_for(small).escalated_at, 'saldo chico y no tan vieja: no escala')
        # Muy vieja escala aunque sea chica
        self.company.obligation_escalate_overdue_days = 1
        self._run()
        self.assertTrue(self._open_for(small).escalated_at)

    def test_escalation_clock_starts_at_confirmation(self):
        self._configure(escalation=self.direccion, obligation_escalate_days=3, obligation_escalate_amount=1.0)
        inv = self._invoice(self.cliente, 1000.0)
        self._run()
        self.assertFalse(self._open_for(inv).escalated_at, 'recién confirmada: el reloj no corre desde el vencimiento')

    def test_no_escalation_without_direction_user(self):
        self._configure(obligation_escalate_days=0, obligation_escalate_amount=1.0)
        inv = self._invoice(self.cliente, 1000.0)
        self._run()
        self.assertFalse(self._open_for(inv).escalated_at)

    def test_sat_paid_reassigns_to_accounting(self):
        if 'sat.cfdi.pago' not in self.env:
            self.skipTest('quimibond_sat no instalado')
        self._configure()
        self.company.obligation_accounting_user_id = self.conta
        inv = self._invoice(self.cliente, 1000.0)
        self._run()
        ob = self._open_for(inv)
        uuid = 'eeeeeeee-0000-4000-8000-000000000001'
        cfdi = self.env['sat.cfdi'].create({
            'syntage_id': 'cfdi-test-1', 'uuid': uuid, 'company_id': self.company.id, 'tipo': 'I',
            'estado_sat': 'vigente', 'direction': 'issued', 'total': 1000.0, 'moneda': inv.currency_id.name,
            'fecha_emision': fields.Datetime.now(), 'move_id': inv.id, 'match_status': 'matched',
        })
        self.env['sat.cfdi.pago'].create({
            'syntage_id': 'pago-test-1', 'company_id': self.company.id, 'invoice_uuid': uuid,
            'fecha_pago': fields.Datetime.now(), 'moneda': inv.currency_id.name, 'monto': 1000.0,
            'estado_sat': 'vigente',
        })
        self.assertEqual(cfdi.move_id, inv)
        self._run()
        self.assertEqual(ob.state, 'confirmed')
        self.assertEqual(ob.obligation_type, 'collection.apply_payment')
        self.assertEqual(ob.user_id, self.conta)
        self.assertEqual(ob.evidence_model, 'sat.cfdi.pago')
        # Contabilidad aplica el pago: cierra por evidencia
        self._pay(inv)
        self._run()
        self.assertEqual(ob.state, 'done')

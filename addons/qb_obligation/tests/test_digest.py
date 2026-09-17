# -*- coding: utf-8 -*-
"""Recordatorio diario: un correo por dueño agrupado por cliente, y uno a
Dirección solo con lo escalado."""
from odoo.tests import tagged

from .common import ObligationCommon


@tagged('post_install', '-at_install', 'qb_obligation')
class TestDigest(ObligationCommon):

    def test_owner_digest_grouped_by_partner(self):
        self._configure()
        self._invoice(self.cliente, 1000.0)
        self._invoice(self.cliente, 2000.0, due='2026-05-15')
        self._invoice(self.cliente2, 300.0)
        self._run()
        self.Obligation.create_candidate({
            'obligation_type': 'collection.payment_promise', 'description': 'Promesa', 'partner_id': self.cliente.id,
            'source_ref': 'thread:1', 'date_deadline': '2026-09-20',
        })
        mails = self.Obligation._cron_digest()
        self.assertEqual(len(mails), 1)
        mail = mails[0]
        self.assertEqual(mail.email_to, self.cxc.email)
        self.assertIn('3 vencidas o por vencer', mail.subject)
        self.assertIn('1 por confirmar', mail.subject)
        body = mail.body_html
        self.assertIn('BELSUEÑO', body)
        self.assertIn('BLANCOS MILENIUM', body)
        # Agrupado: Belsueño aparece con 2 obligaciones y 3,000 de saldo, no como dos renglones
        self.assertRegex(body, r'BELSUEÑO</td><td[^>]*>2</td><td[^>]*>3,000')
        self.assertIn('Promesa', body)
        for ob in self.Obligation.search([('state', 'in', ('candidate', 'confirmed'))]):
            self.assertEqual(ob.times_notified, 1)
            self.assertTrue(ob.last_notified_at)

    def test_no_mail_without_open_obligations(self):
        self._configure()
        self.assertFalse(self.Obligation._cron_digest())

    def test_direction_gets_only_escalated(self):
        self._configure(escalation=self.direccion, obligation_escalate_days=0, obligation_escalate_amount=500.0,
                        obligation_escalate_overdue_days=10000)
        self._invoice(self.cliente, 1000.0)
        self._invoice(self.cliente2, 100.0)
        self._run()
        mails = self.Obligation._cron_digest()
        self.assertEqual(len(mails), 2)
        direction_mail = mails.filtered(lambda m: m.email_to == self.direccion.email)
        self.assertEqual(len(direction_mail), 1)
        self.assertIn('1 obligaciones escaladas', direction_mail.subject)
        self.assertIn('BELSUEÑO', direction_mail.body_html)
        self.assertNotIn('BLANCOS MILENIUM', direction_mail.body_html)

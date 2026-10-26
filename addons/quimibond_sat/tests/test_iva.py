# -*- coding: utf-8 -*-
"""IVA del mes: SAT (base de flujo) vs cuentas de IVA de la contabilidad."""
from odoo import Command, fields
from odoo.tests import tagged

from .common import SatCommon, syntage_invoice
from .test_pagos import syntage_payment

UUID_PUE = '55555555-0000-4000-8000-000000000001'
UUID_PPD = '55555555-0000-4000-8000-000000000002'
UUID_E = '55555555-0000-4000-8000-000000000003'


@tagged('post_install', '-at_install', 'quimibond_sat')
class TestSatIva(SatCommon):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        Account = cls.env['account.account']
        cls.iva_acred = Account.create({'code': '118.01.002', 'name': 'IVA ACREDITABLE PAGADO',
                                        'account_type': 'asset_current'})
        cls.journal = cls.company_data['default_journal_misc']
        cls.counter = cls.company_data['default_account_expense']

    def _upsert(self, obj):
        return self.env['sat.cfdi']._upsert_from_syntage(obj, self.company, event_type='pull')

    def _entry(self, day, amount):
        move = self.env['account.move'].create({
            'move_type': 'entry', 'journal_id': self.journal.id, 'date': day,
            'line_ids': [
                Command.create({'account_id': self.iva_acred.id, 'debit': amount, 'credit': 0.0, 'name': 'IVA'}),
                Command.create({'account_id': self.counter.id, 'debit': 0.0, 'credit': amount, 'name': 'IVA'}),
            ]})
        move.action_post()
        return move

    def test_acreditable_cash_basis(self):
        # PUE de abril: 160 de IVA en abril
        self._upsert(syntage_invoice(UUID_PUE, total=1160.0, subtotal=1000.0, paymentType='PUE',
                                     transferredTaxes={'total': 160.0, 'valueAddedTax': 160.0},
                                     issuedAt='2026-04-10 12:00:00'))
        # PPD de abril pagado a la mitad en mayo: 80 de IVA en mayo, nada en abril
        ppd = self._upsert(syntage_invoice(UUID_PPD, id='p', total=1160.0, subtotal=1000.0, paymentType='PPD',
                                           transferredTaxes={'total': 160.0, 'valueAddedTax': 160.0},
                                           issuedAt='2026-04-12 12:00:00'))
        self.env['sat.cfdi.pago']._upsert_from_syntage(
            syntage_payment('pago-iva-1', UUID_PPD, 580.0, date='2026-05-15 12:00:00', previousBalance=1160.0,
                            outstandingBalance=580.0), self.company, event_type='pull')
        self.assertTrue(ppd)
        # Nota de crédito de mayo: resta 16
        self._upsert(syntage_invoice(UUID_E, id='e', type='E', total=116.0, subtotal=100.0, paymentType='PUE',
                                     transferredTaxes={'total': 16.0, 'valueAddedTax': 16.0},
                                     issuedAt='2026-05-20 12:00:00'))
        self._entry('2026-04-10', 160.0)
        self._entry('2026-05-15', 60.0)
        Iva = self.env['sat.iva.month']
        Iva.action_compute('2026-04-01', '2026-06-01', company_id=self.company.id)
        april = Iva.search([('mes', '=', '2026-04-01'), ('concepto', '=', 'acreditable')])
        may = Iva.search([('mes', '=', '2026-05-01'), ('concepto', '=', 'acreditable')])
        self.assertAlmostEqual(april.sat_iva, 160.0, places=2)
        self.assertAlmostEqual(april.odoo_iva, 160.0, places=2)
        self.assertTrue(april.cuadra)
        self.assertAlmostEqual(may.sat_iva, 64.0, places=2)      # 80 del pago − 16 de la nota de crédito
        self.assertAlmostEqual(may.odoo_iva, 60.0, places=2)
        self.assertAlmostEqual(may.delta, 4.0, places=2)
        # Recalcular no duplica filas
        Iva.action_compute('2026-04-01', '2026-06-01', company_id=self.company.id)
        self.assertEqual(Iva.search_count([('mes', '=', '2026-04-01'), ('concepto', '=', 'acreditable')]), 1)
        self.assertEqual(fields.Date.to_string(april.mes), '2026-04-01')

    def test_cierre_mensual_no_cuenta(self):
        # La provisión de impuestos salda acreditable contra trasladado en un
        # solo asiento: no es IVA del mes y no debe dejar el saldo en cero.
        iva_tras = self.env['account.account'].create({'code': '208.01.01', 'name': 'IVA trasladado cobrado',
                                                       'account_type': 'liability_current'})
        self._entry('2026-04-10', 160.0)
        closing = self.env['account.move'].create({
            'move_type': 'entry', 'journal_id': self.journal.id, 'date': '2026-04-30',
            'line_ids': [
                Command.create({'account_id': iva_tras.id, 'debit': 100.0, 'credit': 0.0, 'name': 'IVA COBRADO'}),
                Command.create({'account_id': self.iva_acred.id, 'debit': 0.0, 'credit': 160.0,
                                'name': 'IVA PAGADO'}),
                Command.create({'account_id': self.counter.id, 'debit': 60.0, 'credit': 0.0, 'name': 'IVA a favor'}),
            ]})
        closing.action_post()
        Iva = self.env['sat.iva.month']
        Iva.action_compute('2026-04-01', '2026-05-01', company_id=self.company.id)
        april = Iva.search([('mes', '=', '2026-04-01'), ('concepto', '=', 'acreditable')])
        self.assertAlmostEqual(april.odoo_iva, 160.0, places=2)
        self.assertEqual(Iva._closing_move_ids(self.company, fields.Date.to_date('2026-04-01'),
                                               fields.Date.to_date('2026-05-01')), closing.ids)

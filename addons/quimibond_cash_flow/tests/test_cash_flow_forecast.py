# -*- coding: utf-8 -*-
"""Tests de la proyeccion de flujo de efectivo (13 semanas)."""
from datetime import date, timedelta

from odoo import Command
from odoo.addons.account.tests.common import AccountTestInvoicingCommon
from odoo.tests import tagged

TODAY = date(2026, 9, 7)


@tagged('post_install', '-at_install', 'quimibond_cash_flow')
class TestCashFlowForecast(AccountTestInvoicingCommon):
    chart_template = 'generic_coa'

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.company = cls.env.company
        cls.config = cls.env['cash.flow.config']._get_for_company(cls.company)
        cls.config.rule_ids.unlink()
        cls.config._load_default_rules()
        cls.config.write({'forecast_weeks': 13, 'forecast_overdue_weeks': 2, 'forecast_include_orders': False,
                          'forecast_min_cash': 0.0})
        cls.engine = cls.env['cash.flow.forecast.engine']
        cls.bank = cls.company_data['default_journal_bank']
        cls.bank_account = cls.bank.default_account_id
        cls.partner_late = cls.env['res.partner'].create({'name': 'Cliente tardado'})
        cls.partner_ok = cls.env['res.partner'].create({'name': 'Cliente puntual'})
        cls.vendor = cls.env['res.partner'].create({'name': 'Proveedor'})

    def _invoice(self, partner, amount, invoice_date, due_date, move_type='out_invoice'):
        move = self.env['account.move'].create({
            'move_type': move_type,
            'partner_id': partner.id,
            'invoice_date': invoice_date,
            'date': invoice_date,
            'invoice_date_due': due_date,
            'invoice_line_ids': [Command.create({
                'name': 'x', 'quantity': 1, 'price_unit': amount, 'tax_ids': [Command.clear()],
                'account_id': (self.company_data['default_account_revenue'] if move_type.startswith('out_')
                               else self.company_data['default_account_expense']).id,
            })],
        })
        move.action_post()
        return move

    def _pay(self, invoice, pay_date):
        """Registra el cobro en banco y lo concilia contra la factura."""
        payment = self.env['account.payment.register'].with_context(
            active_model='account.move', active_ids=invoice.ids).create({
                'payment_date': pay_date, 'journal_id': self.bank.id})._create_payments()
        self.assertTrue(payment)
        return payment

    def _week(self, result, day):
        for idx, (start, end) in enumerate(result['weeks']):
            if start <= day <= end:
                return idx
        return None

    def test_10_receivables_by_due_date_and_overdue(self):
        self._invoice(self.partner_ok, 1000, date(2026, 8, 1), TODAY + timedelta(days=20))
        self._invoice(self.partner_ok, 500, date(2026, 7, 1), TODAY - timedelta(days=10))   # vencida
        result = self.engine.compute(self.config, TODAY)
        self.assertEqual(len(result['weeks']), 13)
        idx = self._week(result, TODAY + timedelta(days=20))
        self.assertAlmostEqual(result['rows']['r_due'][idx], 1000.0, 2)
        # Vencida repartida en 2 semanas.
        self.assertAlmostEqual(result['rows']['r_overdue'][0], 250.0, 2)
        self.assertAlmostEqual(result['rows']['r_overdue'][1], 250.0, 2)
        self.assertAlmostEqual(sum(result['net']), 1500.0, 2)
        self.assertAlmostEqual(result['closing'][-1], result['opening_cash'] + 1500.0, 2)

    def test_20_partner_delay_shifts_expected_collection(self):
        # Historial: el cliente tardado pago 21 dias despues del vencimiento.
        old = self._invoice(self.partner_late, 800, date(2026, 5, 1), date(2026, 5, 31))
        self._pay(old, date(2026, 6, 21))
        # Factura abierta que vence en 7 dias: se espera 7 + 21 = 28 dias.
        self._invoice(self.partner_late, 300, date(2026, 8, 20), TODAY + timedelta(days=7))
        result = self.engine.compute(self.config, TODAY)
        self.assertEqual(result['delays'].get(self.partner_late.id), 21)
        self.assertAlmostEqual(result['rows']['r_due'][self._week(result, TODAY + timedelta(days=28))], 300.0, 2)
        self.assertEqual(result['rows']['r_due'].get(self._week(result, TODAY + timedelta(days=7)), 0.0), 0.0)

    def test_30_payables_and_items(self):
        self._invoice(self.vendor, 400, date(2026, 8, 15), TODAY + timedelta(days=14), move_type='in_invoice')
        self._invoice(self.vendor, 100, date(2026, 7, 15), TODAY - timedelta(days=3), move_type='in_invoice')
        Item = self.env['cash.flow.forecast.item']
        Item.create({'config_id': self.config.id, 'name': 'Nómina', 'category': 'payroll', 'amount': -1000,
                     'date_start': TODAY + timedelta(days=8), 'recurrence': 'biweekly'})
        Item.create({'config_id': self.config.id, 'name': 'Préstamo', 'category': 'loans', 'amount': -5000,
                     'date_start': TODAY + timedelta(days=30), 'recurrence': 'once'})
        Item.create({'config_id': self.config.id, 'name': 'Renta', 'category': 'lease', 'amount': -700,
                     'date_start': date(2026, 8, 31), 'recurrence': 'monthly'})
        result = self.engine.compute(self.config, TODAY)
        self.assertAlmostEqual(result['rows']['p_overdue'][0], -100.0, 2)
        self.assertAlmostEqual(result['rows']['p_due'][self._week(result, TODAY + timedelta(days=14))], -400.0, 2)
        # Nomina cada 14 dias dentro de 13 semanas (91 dias) desde el dia 8: dias 8, 22, ..., 92 → 6 pagos.
        self.assertAlmostEqual(sum(result['rows']['i_payroll'].values()), -6000.0, 2)
        self.assertAlmostEqual(sum(result['rows']['i_loans'].values()), -5000.0, 2)
        # Renta mensual el ultimo dia: 30/09, 31/10, 30/11 dentro del horizonte (hasta 06/12).
        self.assertAlmostEqual(sum(result['rows']['i_lease'].values()), -2100.0, 2)
        total = sum(result['net'])
        self.assertAlmostEqual(total, -100 - 400 - 6000 - 5000 - 2100, 2)
        self.assertAlmostEqual(result['closing'][-1], result['opening_cash'] + total, 2)

    def test_40_min_cash_alert_and_summary(self):
        self.config.forecast_min_cash = 1_000_000.0
        Item = self.env['cash.flow.forecast.item']
        Item.create({'config_id': self.config.id, 'name': 'Pago', 'category': 'other', 'amount': -10,
                     'date_start': TODAY, 'recurrence': 'once'})
        result = self.engine.compute(self.config, TODAY)
        self.assertEqual(result['below_min'], list(range(13)))
        summary = self.config.compute_forecast(TODAY)
        self.assertEqual(len(summary['weeks']), 13)
        self.assertEqual(summary['rows']['i_other'][0], -10.0)
        self.assertEqual(summary['closing'][-1], summary['opening_cash'] - 10.0)

    def test_50_items_from_history(self):
        """Los promedios del metodo directo se convierten en compromisos mensuales."""
        misc = self.company_data['default_journal_misc']
        payroll_account = self.env['account.account'].with_company(self.company).create({
            'code': '210.01.01', 'name': 'Sueldos por pagar', 'account_type': 'liability_payable'})
        today = date.today()
        first_day = (today.replace(day=1) - timedelta(days=1)).replace(day=1)
        for _month in range(3):
            first_day = (first_day - timedelta(days=1)).replace(day=1)
        # Tres pagos de nomina (uno por mes) en los tres meses anteriores al actual.
        month_cursor = (today.replace(day=1) - timedelta(days=1)).replace(day=1)
        for _month in range(3):
            move = self.env['account.move'].create({
                'move_type': 'entry', 'date': month_cursor + timedelta(days=10), 'journal_id': misc.id,
                'line_ids': [
                    Command.create({'account_id': payroll_account.id, 'debit': 900, 'credit': 0}),
                    Command.create({'account_id': self.bank_account.id, 'debit': 0, 'credit': 900}),
                ]})
            move.action_post()
            month_cursor = (month_cursor - timedelta(days=1)).replace(day=1)
        self.config.action_load_forecast_items_from_history()
        items = self.config.forecast_item_ids.filtered(lambda i: i.auto and i.category == 'payroll')
        self.assertEqual(len(items), 2)
        self.assertAlmostEqual(sum(items.mapped('amount')), -900.0, 2)
        # Volver a sembrar reemplaza, no duplica.
        self.config.action_load_forecast_items_from_history()
        self.assertEqual(len(self.config.forecast_item_ids.filtered('auto')), 2)

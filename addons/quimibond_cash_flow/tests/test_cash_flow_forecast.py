# -*- coding: utf-8 -*-
"""Tests de la proyeccion de flujo de efectivo (13 semanas)."""
import calendar
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
                          'forecast_min_cash': 0.0, 'forecast_min_item_amount': 0.0, 'forecast_stale_days': 180,
                          'forecast_include_runrate': False})
        cls.engine = cls.env['cash.flow.forecast.engine']
        cls.bank = cls.company_data['default_journal_bank']
        cls.bank_account = cls.bank.default_account_id
        # Los pagos van directo al banco (sin cuentas pendientes) para que
        # las polizas de cobro/pago toquen efectivo.
        (cls.bank.inbound_payment_method_line_ids | cls.bank.outbound_payment_method_line_ids).write(
            {'payment_account_id': cls.bank_account.id})
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
        # Vencido repartido en 2 semanas (forecast_overdue_weeks).
        self.assertAlmostEqual(result['rows']['p_overdue'][0], -50.0, 2)
        self.assertAlmostEqual(result['rows']['p_overdue'][1], -50.0, 2)
        self.assertAlmostEqual(result['rows']['p_due'][self._week(result, TODAY + timedelta(days=14))], -400.0, 2)
        # Nomina cada 14 dias dentro de 13 semanas (91 dias) desde el dia 8: dias 8, 22, ..., 92 → 6 pagos.
        self.assertAlmostEqual(sum(result['rows']['i_payroll'].values()), -6000.0, 2)
        self.assertAlmostEqual(sum(result['rows']['i_loans'].values()), -5000.0, 2)
        # Renta mensual el ultimo dia: 30/09, 31/10, 30/11 dentro del horizonte (hasta 06/12).
        self.assertAlmostEqual(sum(result['rows']['i_lease'].values()), -2100.0, 2)
        total = sum(result['net'])
        self.assertAlmostEqual(total, -100 - 400 - 6000 - 5000 - 2100, 2)
        self.assertAlmostEqual(result['closing'][-1], result['opening_cash'] + total, 2)

    def test_35_stale_items_are_excluded_but_reported(self):
        self._invoice(self.partner_ok, 700, date(2025, 6, 1), date(2025, 7, 1))                      # 14 meses vencida
        self._invoice(self.vendor, 250, date(2025, 8, 1), date(2025, 9, 1), move_type='in_invoice')  # 12 meses vencida
        result = self.engine.compute(self.config, TODAY)
        self.assertAlmostEqual(result['rows']['r_stale'][0], 700.0, 2)
        self.assertAlmostEqual(result['rows']['p_stale'][0], -250.0, 2)
        self.assertAlmostEqual(sum(result['net']), 0.0, 2)
        summary = self.config.compute_forecast(TODAY)
        self.assertEqual(summary['rows']['r_stale'][0], 700.0)

    def test_37_runrate_complements_known_items_after_dso(self):
        """Historial: una factura de 1,000 cobrada cada semana a los 14 dias
        (DSO 2 semanas). Sin facturas abiertas, a partir de la semana 3 se
        estima el promedio semanal de cobros; en las 2 primeras, nada."""
        self.config.forecast_include_runrate = True
        self.config.forecast_history_months = 3
        window_end = TODAY.replace(day=1) - timedelta(days=1)
        window_start = window_end.replace(day=1)
        for _i in range(2):
            window_start = (window_start - timedelta(days=1)).replace(day=1)
        day = window_start
        while day + timedelta(days=14) <= window_end:
            inv = self._invoice(self.partner_ok, 1000, day, day + timedelta(days=14))
            self._pay(inv, day + timedelta(days=14))
            day += timedelta(days=7)
        result = self.engine.compute(self.config, TODAY)
        rr = result['rows']['r_runrate']
        self.assertEqual(rr.get(0, 0.0), 0.0)
        self.assertEqual(rr.get(1, 0.0), 0.0)
        weeks_in_window = (window_end - window_start).days / 7.0
        paid = sum(1 for _ in range(int((window_end - window_start).days // 7) - 1))
        expected_weekly = paid * 1000.0 / weeks_in_window
        self.assertAlmostEqual(rr[2], expected_weekly, 0)
        self.assertAlmostEqual(rr[12], expected_weekly, 0)
        self.assertEqual(result['rows']['p_runrate'], {})

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

    def _cash_payment(self, account, amount, day, journal=None, partner=None):
        journal = journal or self.company_data['default_journal_misc']
        move = self.env['account.move'].create({
            'move_type': 'entry', 'date': day, 'journal_id': journal.id,
            'partner_id': partner.id if partner else False,
            'line_ids': [
                Command.create({'account_id': account.id, 'debit': amount, 'credit': 0, 'partner_id': partner.id if partner else False}),
                Command.create({'account_id': self.bank_account.id, 'debit': 0, 'credit': amount, 'partner_id': partner.id if partner else False}),
            ]})
        move.action_post()
        return move

    def test_50_items_from_history_detect_periodicity(self):
        """Nomina semanal (viernes) + quincenal (15 y fin de mes) + SAT el 17:
        se detectan como series con su dia, importe mediano y contacto."""
        Account = self.env['account.account'].with_company(self.company)
        payroll_account = Account.create({'code': '210.01.01', 'name': 'Sueldos por pagar', 'account_type': 'liability_payable'})
        tax_account = Account.create({'code': '213.01.01', 'name': 'IVA por pagar', 'account_type': 'liability_current'})
        sat = self.env['res.partner'].create({'name': 'SAT test'})
        today = date.today()
        window_end = today.replace(day=1) - timedelta(days=1)
        window_start = window_end.replace(day=1)
        for _i in range(2):
            window_start = (window_start - timedelta(days=1)).replace(day=1)
        # Semanal: cada viernes 900 (con un poco de ruido).
        day = window_start
        while day.weekday() != 4:
            day += timedelta(days=1)
        fridays = 0
        while day <= window_end:
            self._cash_payment(payroll_account, 900 + (fridays % 3) * 10, day)
            day += timedelta(days=7)
            fridays += 1
        # Quincenal: 15 y ultimo dia, 5000 cada uno; SAT el 17: 3000.
        cursor = window_start
        while cursor <= window_end:
            last = cursor.replace(day=calendar.monthrange(cursor.year, cursor.month)[1])
            self._cash_payment(payroll_account, 5000, cursor.replace(day=15))
            self._cash_payment(payroll_account, 5000, last)
            self._cash_payment(tax_account, 3000, cursor.replace(day=17), partner=sat)
            cursor = (last + timedelta(days=1))
        # Arrendador con dos rentas fijas al mes en dias variables + un pago aislado.
        lease_account = Account.create({'code': '701.11.0001', 'name': 'Arrendamiento financiero', 'account_type': 'expense_other'})
        lessor = self.env['res.partner'].create({'name': 'Arrendadora test'})
        cursor = window_start
        for offsets in ((26, 30), (24, 28), (20, 25)):
            for day_no, amount in zip(offsets, (355675.26, 397242.00)):
                self._cash_payment(lease_account, amount, cursor.replace(day=day_no), partner=lessor)
            cursor = (cursor.replace(day=28) + timedelta(days=4)).replace(day=1)
        loan_account = Account.create({'code': '252.01.04', 'name': 'Préstamo', 'account_type': 'liability_non_current'})
        self._cash_payment(loan_account, 1965200.0, window_start.replace(day=17), partner=lessor)   # aislado
        self.config.action_load_forecast_items_from_history()
        items = self.config.forecast_item_ids.filtered('auto')
        lease = items.filtered(lambda i: i.category == 'lease')
        self.assertEqual(len(lease), 1, lease.mapped('name'))
        self.assertAlmostEqual(lease.amount, -(355675.26 + 397242.00), 2)
        self.assertEqual(lease.partner_id, lessor)
        self.assertFalse(items.filtered(lambda i: i.category == 'loans'), 'un pago aislado no es compromiso')
        payroll = items.filtered(lambda i: i.category == 'payroll')
        weekly = payroll.filtered(lambda i: i.recurrence == 'weekly')
        self.assertEqual(len(weekly), 1)
        self.assertEqual(weekly.date_start.weekday(), 4)
        self.assertAlmostEqual(weekly.amount, -910.0, 2)
        monthly = payroll.filtered(lambda i: i.recurrence == 'monthly' and 'irregular' not in i.name)
        days = sorted(monthly.mapped(lambda i: i.date_start.day))
        self.assertEqual(len(days), 2, monthly.mapped('name'))
        self.assertEqual(days[0], 15)
        self.assertGreaterEqual(days[1], 28)
        self.assertTrue(all(abs(i.amount + 5000.0) < 0.01 for i in monthly), monthly.mapped('amount'))
        taxes = items.filtered(lambda i: i.category == 'taxes')
        self.assertEqual(len(taxes), 1)
        self.assertEqual(taxes.partner_id, sat)
        self.assertEqual(taxes.date_start.day, 17)
        self.assertAlmostEqual(taxes.amount, -3000.0, 2)
        self.assertGreaterEqual(taxes.date_start, today)
        # Volver a sembrar reemplaza, no duplica.
        self.config.action_load_forecast_items_from_history()
        self.assertEqual(len(self.config.forecast_item_ids.filtered('auto')), len(items))

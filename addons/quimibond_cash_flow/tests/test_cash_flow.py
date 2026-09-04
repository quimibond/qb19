# -*- coding: utf-8 -*-
"""Tests del motor del flujo de efectivo NIF B-2.

Cada caso reproduce una situacion real de Quimibond sobre un plan de cuentas
minimo con los mismos codigos, y verifica que:

* el metodo indirecto y el directo llegan a la misma cifra;
* esa cifra es la variacion real de las cuentas de efectivo;
* la conciliacion (efectivo final calculado - saldo contable) es cero;
* "Sin clasificar" (indirecto) y "Otros (revisar)" (directo) estan en cero.
"""
from datetime import date

from odoo import Command
from odoo.addons.account.tests.common import AccountTestInvoicingCommon
from odoo.tests import tagged

from ..models import cash_flow_lines as L

# Plan de cuentas minimo: (codigo, nombre, tipo)
ACCOUNTS = [
    ('102.01.002', 'BBVA BANCOMER MXN', 'asset_cash'),
    ('102.01.006', 'BBVA BANCOMER MXN 2', 'asset_cash'),
    ('102.02.02', 'BBVA BANCOMER USD', 'asset_cash'),
    ('102.01.34', 'Recibos pendientes (global)', 'asset_current'),
    ('102.01.35', 'Pagos pendientes (global)', 'asset_current'),
    ('102.09.00', 'Transitoria transferencias', 'asset_current'),
    ('109.23.02', 'Servicio Internacional aduanas', 'asset_cash'),
    ('107.03.001', '5209 BBVA Grupo Quimibond', 'asset_current'),
    ('105.01.01', 'Clientes nacionales', 'asset_receivable'),
    ('108.01.01', 'Estimación incobrables', 'asset_current'),
    ('115.01.01', 'Inventario', 'asset_current'),
    ('119.01.01', 'IVA pendiente de pago', 'asset_current'),
    ('118.01.002', 'IVA acreditable pagado', 'asset_current'),
    ('153.01.01', 'Maquinaria y equipo', 'asset_fixed'),
    ('171.02.01', 'Depre. acum. maquinaria', 'asset_fixed'),
    ('201.01.001', 'Proveedores nacionales', 'liability_payable'),
    ('205.02.01', 'Acreedores nacionales', 'liability_payable'),
    ('205.02.05', 'SAT', 'liability_payable'),
    ('208.01.01', 'IVA trasladado cobrado', 'liability_current'),
    ('209.01.01', 'IVA trasladado no cobrado', 'liability_current'),
    ('210.01.01', 'Sueldos por pagar', 'liability_payable'),
    ('211.01.001', 'IMSS por pagar', 'liability_current'),
    ('213.01.01', 'IVA por pagar', 'liability_current'),
    ('216.01.001', 'ISR retenido sueldos', 'liability_current'),
    ('252.01.04', 'Préstamo Lepezo', 'liability_non_current'),
    ('301.01.01', 'Capital social', 'equity'),
    ('401.01.01', 'Ventas', 'income'),
    ('501.01.01', 'Costo de ventas', 'expense_direct_cost'),
    ('504.01.0034', 'Otros gastos', 'expense'),
    ('504.08.0001', 'Depreciación maquinaria', 'expense_depreciation'),
    ('601.01.01', 'Sueldos y salarios', 'expense'),
    ('701.01.0001', 'Pérdida cambiaria', 'expense_other'),
    ('702.01.0001', 'Utilidad cambiaria', 'income_other'),
    ('701.04.0001', 'Intereses bancarios', 'expense_other'),
    ('701.10.0001', 'Comisiones bancarias', 'expense_other'),
    ('701.11.0001', 'Arrendamiento financiero', 'expense_other'),
    ('704.23.0003', 'Utilidad en venta de activo fijo', 'income_other'),
    ('701.01.0004', 'Pérdida en venta de activo fijo', 'expense_other'),
]

FROM = date(2026, 1, 1)
TO = date(2026, 3, 31)


@tagged('post_install', '-at_install', 'quimibond_cash_flow')
class TestCashFlowNifB2(AccountTestInvoicingCommon):
    # Plan generico a proposito: la compania de prueba se crea en una base
    # que puede ser copia de produccion (Odoo.sh) y con la localizacion
    # mexicana los codigos de cuenta y de diario (CAMBI, TAX, 105.01.01...)
    # ya existirian y chocarian con los del plan minimo de abajo.
    chart_template = 'generic_coa'

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.company = cls.env.company
        cls.currency = cls.company.currency_id
        cls.acc = {}
        for code, name, account_type in ACCOUNTS:
            cls.acc[code] = cls._account(code, name, account_type)
        cls.j_misc = cls.company_data['default_journal_misc']
        cls.j_bank = cls.company_data['default_journal_bank']
        cls.j_fx = cls._journal('Diferencia de cambio', 'CAMBI')
        cls.j_payroll = cls._journal('Nominas', 'NOM')
        cls.j_imss = cls._journal('IMSS', 'IMSS')
        cls.j_tax = cls._journal('Impuestos', 'TAX')
        # Mismo criterio de busqueda que _load_default_rules, para que la
        # regla por contacto apunte al mismo partner que usa el test.
        Partner = cls.env['res.partner']
        cls.partner_sat = Partner.search([('name', 'ilike', 'Servicio de Administración Tributaria'),
                                          ('company_id', 'in', [False, cls.company.id])], limit=1)
        if not cls.partner_sat:
            cls.partner_sat = Partner.create({'name': 'Servicio de Administración Tributaria'})
        cls.config = cls.env['cash.flow.config']._get_for_company(cls.company)
        cls.config.rule_ids.unlink()
        cls.config._load_default_rules()
        cls.engine = cls.env['cash.flow.engine']

    @classmethod
    def _account(cls, code, name, account_type):
        """Cuenta del plan minimo: reutiliza la existente con ese codigo en la
        compania (ajustando tipo/nombre) o la crea."""
        Account = cls.env['account.account'].with_company(cls.company).with_context(active_test=False)
        account = Account.search([('company_ids', 'in', cls.company.ids), ('code', '=', code)], limit=1)
        vals = {'name': name, 'account_type': account_type,
                'reconcile': account_type in ('asset_receivable', 'liability_payable')}
        if account:
            account.write(dict(vals, active=True))
            return account
        return Account.create(dict(vals, code=code))

    @classmethod
    def _journal(cls, name, code):
        """Diario por nombre (como lo buscan los defaults); se crea si falta,
        con un codigo libre en la compania."""
        Journal = cls.env['account.journal'].with_context(active_test=False)
        journal = Journal.search([('company_id', '=', cls.company.id), ('name', '=ilike', name)], limit=1)
        if journal:
            journal.active = True
            return journal
        used = set(Journal.search([('company_id', '=', cls.company.id)]).mapped('code'))
        candidate, n = code, 1
        while candidate in used:
            candidate, n = '%s%d' % (code[:4], n), n + 1
        return Journal.create({'name': name, 'code': candidate, 'type': 'general', 'company_id': cls.company.id})

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _entry(self, lines, day=date(2026, 2, 10), journal=None, partner=None, move_type='entry', closing=False):
        """Crea y registra una poliza. ``lines`` = [(codigo, debit, credit)]."""
        is_invoice = move_type != 'entry'
        sale_sign = 1 if move_type.startswith('out_') else -1

        def line_vals(code, debit, credit):
            vals = {'account_id': self.acc[code].id, 'name': code, 'partner_id': partner.id if partner else False}
            if is_invoice:
                # En facturas el importe sale de price_unit (las lineas son "producto").
                vals.update({'quantity': 1.0, 'price_unit': sale_sign * (credit - debit), 'tax_ids': [Command.clear()]})
            else:
                vals.update({'debit': debit, 'credit': credit})
            return vals

        if journal is None:
            if move_type.startswith('in_'):
                journal = self.company_data['default_journal_purchase']
            elif move_type.startswith('out_'):
                journal = self.company_data['default_journal_sale']
            else:
                journal = self.j_misc
        vals = {
            'move_type': move_type,
            'date': day,
            'journal_id': journal.id,
            'partner_id': partner.id if partner else False,
            'line_ids': [Command.create(line_vals(code, debit, credit)) for code, debit, credit in lines],
        }
        if is_invoice:
            vals['invoice_date'] = day
        if closing:
            # El campo lo aporta la localizacion mexicana Enterprise; sin el,
            # la poliza se crea como una normal (el test que lo usa lo sabe).
            if 'l10n_mx_closing_move' in self.env['account.move']._fields:
                vals['l10n_mx_closing_move'] = True
        move = self.env['account.move'].create(vals)
        move.action_post()
        return move

    def _compute(self, date_from=FROM, date_to=TO):
        result = self.engine.compute(self.config, date_from, date_to)
        sliced = self.engine.slice(result, date_from, date_to)
        totals = self.engine.totals(sliced)
        return result, sliced, totals

    def _cash_delta(self, date_from=FROM, date_to=TO):
        cash_ids = self.config.get_cash_account_ids()
        lines = self.env['account.move.line'].search([
            ('company_id', '=', self.company.id), ('parent_state', '=', 'posted'),
            ('account_id', 'in', cash_ids), ('date', '>=', date_from), ('date', '<=', date_to),
        ])
        return sum(lines.mapped('balance'))

    def assertReconciles(self, sliced, totals, expected_delta=None, date_from=FROM, date_to=TO):
        """Ambos metodos cuadran entre si y contra la variacion real de efectivo."""
        lines = sliced['lines']
        delta = self._cash_delta(date_from, date_to)
        ind_total = totals['ind_net'] + totals['ind_fx']
        dir_total = totals['dir_net'] + totals['dir_fx']
        self.assertAlmostEqual(ind_total, delta, 2, 'El método indirecto no cuadra con la variación de efectivo')
        self.assertAlmostEqual(dir_total, delta, 2, 'El método directo no cuadra con la variación de efectivo')
        self.assertAlmostEqual(totals['methods_difference'], 0.0, 2)
        self.assertAlmostEqual(totals['difference'], 0.0, 2, 'Efectivo final calculado ≠ saldo contable')
        self.assertAlmostEqual(totals['closing_cash_book'], sliced['opening_cash'] + delta, 2)
        self.assertAlmostEqual(lines.get('unclassified', 0.0), 0.0, 2, 'Hay importes sin clasificar: %s' % self._describe(sliced, 'unclassified'))
        self.assertAlmostEqual(lines.get('d_other', 0.0), 0.0, 2, 'Hay importes en "Otros": %s' % self._describe(sliced, 'd_other'))
        if expected_delta is not None:
            self.assertAlmostEqual(delta, expected_delta, 2)

    def _describe(self, sliced, key):
        info = self.config._get_accounts()
        return {info[aid][0]: round(v, 2) for aid, v in sliced['accounts'].get(key, {}).items()}

    def assertLine(self, sliced, key, expected):
        self.assertAlmostEqual(sliced['lines'].get(key, 0.0), expected, 2, 'Línea %s' % L.LINE_LABELS[key])

    # ------------------------------------------------------------------
    # Definicion de efectivo
    # ------------------------------------------------------------------
    def test_00_cash_definition(self):
        cash_ids = set(self.config.get_cash_account_ids())
        for code in ('102.01.002', '102.02.02', '102.01.34', '102.01.35', '102.09.00'):
            self.assertIn(self.acc[code].id, cash_ids, code)
        for code in ('109.23.02', '107.03.001', '105.01.01', '201.01.001'):
            self.assertNotIn(self.acc[code].id, cash_ids, code)
        # Las reglas por defecto no tocan cuentas de otras companias.
        self.assertTrue(all(self.acc[c].id in cash_ids for c in ('102.01.002',)))

    def test_01_empty_period_reconciles(self):
        _result, sliced, totals = self._compute()
        self.assertReconciles(sliced, totals, expected_delta=0.0)

    # ------------------------------------------------------------------
    # Casos
    # ------------------------------------------------------------------
    def test_10_customer_collection_via_outstanding_receipts(self):
        """Factura, cobro a Recibos pendientes y estado de cuenta contra el banco."""
        self._entry([('105.01.01', 116, 0), ('401.01.01', 0, 100), ('209.01.01', 0, 16)], day=date(2026, 1, 15))
        self._entry([('102.01.34', 116, 0), ('105.01.01', 0, 116)], day=date(2026, 2, 3), journal=self.j_bank)
        self._entry([('102.01.002', 116, 0), ('102.01.34', 0, 116)], day=date(2026, 2, 4), journal=self.j_bank)
        _result, sliced, totals = self._compute()
        self.assertReconciles(sliced, totals, expected_delta=116.0)
        # Directo: un solo cobro a clientes, sin IVA ni ventas como flujos.
        self.assertLine(sliced, 'd_customers', 116.0)
        self.assertLine(sliced, 'd_taxes', 0.0)
        self.assertEqual({k for k, v in sliced['lines'].items() if k in L.DIRECT_KEYS and abs(v) > 0.005}, {'d_customers'})
        # Indirecto: resultado + IVA por pagar; clientes netos en cero.
        self.assertLine(sliced, 'result', 100.0)
        self.assertLine(sliced, 'wc_taxes_payable', 16.0)
        self.assertLine(sliced, 'wc_receivables', 0.0)

    def test_11_supplier_payment_with_vat(self):
        """Factura de proveedor con IVA, pago a Pagos pendientes y estado de cuenta."""
        self._entry([('504.01.0034', 100, 0), ('119.01.01', 16, 0), ('201.01.001', 0, 116)], day=date(2026, 1, 20))
        self._entry([('201.01.001', 116, 0), ('102.01.35', 0, 116)], day=date(2026, 2, 5), journal=self.j_bank)
        self._entry([('102.01.35', 116, 0), ('102.01.002', 0, 116)], day=date(2026, 2, 6), journal=self.j_bank)
        _result, sliced, totals = self._compute()
        self.assertReconciles(sliced, totals, expected_delta=-116.0)
        self.assertLine(sliced, 'd_suppliers', -116.0)
        self.assertLine(sliced, 'd_taxes', 0.0)
        self.assertLine(sliced, 'result', -100.0)
        self.assertLine(sliced, 'wc_tax_receivable', -16.0)
        self.assertLine(sliced, 'wc_payables', 0.0)

    def test_12_payment_inside_invoice_is_one_flow(self):
        """Cobro registrado dentro de la factura (diario de venta): toda la
        poliza es un cobro a clientes, no ventas + IVA."""
        self._entry([('102.01.002', 116, 0), ('401.01.01', 0, 100), ('208.01.01', 0, 16)],
                    day=date(2026, 2, 8), journal=self.company_data['default_journal_sale'],
                    move_type='out_invoice', partner=self.partner_a)
        _result, sliced, totals = self._compute()
        self.assertReconciles(sliced, totals, expected_delta=116.0)
        self.assertLine(sliced, 'd_customers', 116.0)
        self.assertLine(sliced, 'd_taxes', 0.0)

    def _pay_straight_to_bank(self):
        """Los pagos van directo a la cuenta del banco (sin cuenta de
        recibos/pagos pendientes) para que la poliza del pago toque efectivo."""
        methods = self.j_bank.inbound_payment_method_line_ids | self.j_bank.outbound_payment_method_line_ids
        methods.write({'payment_account_id': self.j_bank.default_account_id.id})

    def _register_payment(self, invoice, day):
        """Registra el pago/cobro de ``invoice`` en el banco (conciliado)."""
        return self.env['account.payment.register'].with_context(
            active_model='account.move', active_ids=invoice.ids).create({
                'payment_date': day, 'journal_id': self.j_bank.id})._create_payments()

    def test_13_invoice_dominant_account_reclassifies_payments(self):
        """Pagos y cobros de facturas se clasifican por la cuenta dominante de
        la factura conciliada: maquinaria comprada, maquina vendida,
        intereses y arrendamiento, aunque la contraparte del efectivo sea la
        cuenta por pagar/cobrar."""
        self._pay_straight_to_bank()
        self.assertIn(self.j_bank.default_account_id.id, self.config.get_cash_account_ids())
        machine_bill = self._entry([('153.01.01', 1000, 0)], day=date(2026, 1, 10), move_type='in_invoice', partner=self.partner_a)
        interest_bill = self._entry([('701.04.0001', 80, 0)], day=date(2026, 1, 12), move_type='in_invoice', partner=self.partner_a)
        lease_bill = self._entry([('701.11.0001', 300, 0)], day=date(2026, 1, 15), move_type='in_invoice', partner=self.partner_a)
        sale = self._entry([('704.23.0003', 0, 500)], day=date(2026, 2, 1), move_type='out_invoice', partner=self.partner_a)
        for move in (machine_bill, interest_bill, lease_bill, sale):
            self._register_payment(move, date(2026, 2, 20))
        _result, sliced, totals = self._compute()
        self.assertReconciles(sliced, totals, expected_delta=-1000.0 - 80.0 - 300.0 + 500.0)
        self.assertLine(sliced, 'd_assets_bought', -1000.0)
        self.assertLine(sliced, 'd_interest', -80.0)
        self.assertLine(sliced, 'd_lease', -300.0)
        self.assertLine(sliced, 'd_assets_sold', 500.0)
        self.assertLine(sliced, 'd_suppliers', 0.0)
        self.assertLine(sliced, 'd_customers', 0.0)
        # Indirecto: la compra va a inversion aunque se haya pagado via proveedores.
        self.assertLine(sliced, 'inv_acquisitions', -1000.0)
        self.assertLine(sliced, 'wc_payables', 0.0)

    def test_14_partial_payment_is_prorated_by_invoice(self):
        """Un pago parcial reclasifica solo la parte conciliada; el resto de
        la factura sigue en cuentas por pagar (sin flujo)."""
        self._pay_straight_to_bank()
        bill = self._entry([('153.01.01', 1000, 0)], day=date(2026, 1, 10), move_type='in_invoice', partner=self.partner_a)
        self.env['account.payment.register'].with_context(
            active_model='account.move', active_ids=bill.ids).create({
                'payment_date': date(2026, 2, 5), 'journal_id': self.j_bank.id, 'amount': 400.0})._create_payments()
        _result, sliced, totals = self._compute()
        self.assertReconciles(sliced, totals, expected_delta=-400.0)
        self.assertLine(sliced, 'd_assets_bought', -400.0)
        self.assertLine(sliced, 'd_suppliers', 0.0)

    def test_15_invoice_touching_cash_is_not_reclassified(self):
        """Factura de proveedor cuya linea de producto es una cuenta de efectivo
        (venta de USD a casa de cambio contra recibos pendientes) y su pago
        desde el banco USD: es un traspaso, no un pago a proveedor ni "otros"."""
        self._pay_straight_to_bank()
        bill = self._entry([('102.01.34', 17000, 0)], day=date(2026, 2, 10), move_type='in_invoice', partner=self.partner_a)
        self._register_payment(bill, date(2026, 2, 10))
        _result, sliced, totals = self._compute()
        self.assertReconciles(sliced, totals, expected_delta=0.0)
        self.assertLine(sliced, 'd_suppliers', 0.0)
        self.assertLine(sliced, 'd_other', 0.0)

    def test_20_transfer_between_banks_is_not_a_flow(self):
        self._entry([('102.09.00', 1000, 0), ('102.01.002', 0, 1000)], day=date(2026, 2, 10), journal=self.j_bank)
        self._entry([('102.01.006', 1000, 0), ('102.09.00', 0, 1000)], day=date(2026, 2, 11), journal=self.j_bank)
        _result, sliced, totals = self._compute()
        self.assertReconciles(sliced, totals, expected_delta=0.0)
        self.assertTrue(all(abs(v) < 0.005 for v in sliced['lines'].values()), sliced['lines'])

    def test_21_usd_sale_to_exchange_house_with_fx_difference(self):
        """Venta de USD a Mifel: traspaso via 102.09.00 y diferencia a
        perdida cambiaria. Debe verse como traspaso + efecto cambiario, no
        como pago a proveedor."""
        self._entry([('102.09.00', 17000, 0), ('102.02.02', 0, 17000)], day=date(2026, 2, 12), journal=self.j_bank)
        self._entry([('102.01.002', 16900, 0), ('701.01.0001', 100, 0), ('102.09.00', 0, 17000)], day=date(2026, 2, 12), journal=self.j_bank)
        # Revaluacion del banco USD (diario Diferencia de cambio).
        self._entry([('102.02.02', 500, 0), ('702.01.0001', 0, 500)], day=date(2026, 2, 28), journal=self.j_fx)
        _result, sliced, totals = self._compute()
        self.assertReconciles(sliced, totals, expected_delta=400.0)
        self.assertLine(sliced, 'd_fx', 400.0)
        self.assertLine(sliced, 'd_suppliers', 0.0)
        self.assertLine(sliced, 'fx_effect', 400.0)
        self.assertLine(sliced, 'nc_fx_cash', -400.0)
        self.assertLine(sliced, 'result', 400.0)
        self.assertAlmostEqual(totals['ind_net'], 0.0, 2)
        self.assertAlmostEqual(totals['dir_net'], 0.0, 2)

    def test_22_unrealized_fx_on_receivable_stays_in_operations(self):
        """Diferencia cambiaria (diario 4) sobre un cliente: no toca efectivo,
        queda en operacion como partida virtual compensada por clientes."""
        self._entry([('105.01.01', 300, 0), ('702.01.0001', 0, 300)], day=date(2026, 2, 28), journal=self.j_fx)
        _result, sliced, totals = self._compute()
        self.assertReconciles(sliced, totals, expected_delta=0.0)
        self.assertLine(sliced, 'result', 300.0)
        self.assertLine(sliced, 'wc_receivables', -300.0)
        self.assertLine(sliced, 'fx_effect', 0.0)
        self.assertLine(sliced, 'nc_fx_cash', 0.0)

    def test_30_fixed_asset_purchase_sale_and_depreciation(self):
        self._entry([('153.01.01', 1000, 0), ('102.01.002', 0, 1000)], day=date(2026, 1, 10), journal=self.j_bank)
        self._entry([('504.08.0001', 50, 0), ('171.02.01', 0, 50)], day=date(2026, 1, 31))
        # Venta: costo 1000, depreciacion acumulada 600, precio 500 → utilidad 100.
        self._entry([('102.01.002', 500, 0), ('171.02.01', 600, 0), ('153.01.01', 0, 1000), ('704.23.0003', 0, 100)],
                    day=date(2026, 3, 5), journal=self.j_bank)
        _result, sliced, totals = self._compute()
        self.assertReconciles(sliced, totals, expected_delta=-500.0)
        # Directo
        self.assertLine(sliced, 'd_assets_bought', -1000.0)
        self.assertLine(sliced, 'd_assets_sold', 500.0)
        # Indirecto
        self.assertLine(sliced, 'result', 50.0)          # +100 utilidad - 50 depreciacion
        self.assertLine(sliced, 'nc_depreciation', 50.0)
        self.assertLine(sliced, 'nc_depreciation_ctr', 0.0)
        self.assertLine(sliced, 'nc_asset_result', -100.0)
        self.assertLine(sliced, 'inv_acquisitions', -1000.0)
        self.assertLine(sliced, 'inv_disposals', 500.0)
        self.assertAlmostEqual(totals['ind_operating'], 0.0, 2)
        self.assertAlmostEqual(totals['ind_investing'], -500.0, 2)

    def test_31_fixed_asset_sale_at_loss(self):
        self._entry([('153.01.01', 1000, 0), ('301.01.01', 0, 1000)], day=date(2025, 6, 1))
        self._entry([('102.01.002', 350, 0), ('171.02.01', 600, 0), ('701.01.0004', 50, 0), ('153.01.01', 0, 1000)],
                    day=date(2026, 3, 5), journal=self.j_bank)
        _result, sliced, totals = self._compute()
        self.assertReconciles(sliced, totals, expected_delta=350.0)
        self.assertLine(sliced, 'd_assets_sold', 350.0)
        self.assertLine(sliced, 'result', -50.0)
        self.assertLine(sliced, 'nc_asset_result', 50.0)
        self.assertLine(sliced, 'inv_disposals', 350.0)

    def test_40_loan_payment_with_interest(self):
        self._entry([('102.01.002', 10000, 0), ('252.01.04', 0, 10000)], day=date(2026, 1, 5), journal=self.j_bank)
        self._entry([('252.01.04', 1000, 0), ('701.04.0001', 80, 0), ('701.10.0001', 20, 0), ('102.01.002', 0, 1100)],
                    day=date(2026, 2, 5), journal=self.j_bank)
        _result, sliced, totals = self._compute()
        self.assertReconciles(sliced, totals, expected_delta=8900.0)
        self.assertLine(sliced, 'd_loans_received', 10000.0)
        self.assertLine(sliced, 'd_loans_paid', -1000.0)
        self.assertLine(sliced, 'd_interest', -80.0)
        self.assertLine(sliced, 'd_bank_fees', -20.0)
        self.assertLine(sliced, 'result', -100.0)
        self.assertLine(sliced, 'nc_interest', 80.0)
        self.assertLine(sliced, 'fin_interest', -80.0)
        self.assertLine(sliced, 'fin_loans_received', 10000.0)
        self.assertLine(sliced, 'fin_loans_paid', -1000.0)
        self.assertAlmostEqual(totals['ind_operating'], -20.0, 2)
        self.assertAlmostEqual(totals['ind_financing'], 8920.0, 2)

    def test_50_payroll_and_contributions(self):
        # Provision de nomina (diario Nominas): sueldo 1000, ISR ret 100, IMSS 50, neto 850.
        self._entry([('601.01.01', 1000, 0), ('216.01.001', 0, 100), ('211.01.001', 0, 50), ('210.01.01', 0, 850)],
                    day=date(2026, 2, 15), journal=self.j_payroll)
        # Pago de nomina desde el banco.
        self._entry([('210.01.01', 850, 0), ('102.01.002', 0, 850)], day=date(2026, 2, 16), journal=self.j_bank)
        # Pago de IMSS (diario IMSS) y de ISR retenido (diario Impuestos, contacto SAT).
        self._entry([('211.01.001', 50, 0), ('102.01.002', 0, 50)], day=date(2026, 3, 10), journal=self.j_imss)
        self._entry([('216.01.001', 100, 0), ('102.01.002', 0, 100)], day=date(2026, 3, 17), journal=self.j_tax, partner=self.partner_sat)
        _result, sliced, totals = self._compute()
        self.assertReconciles(sliced, totals, expected_delta=-1000.0)
        self.assertLine(sliced, 'd_payroll', -850.0)
        self.assertLine(sliced, 'd_taxes', -150.0)
        self.assertLine(sliced, 'result', -1000.0)
        self.assertLine(sliced, 'wc_payroll', 0.0)
        self.assertLine(sliced, 'wc_taxes_payable', 0.0)

    def test_60_closing_move_is_ignored_and_opening_balance(self):
        """Saldo inicial de 2025 y poliza de cierre (mes 13, fechada el 31 de
        diciembre como exige la localizacion) que traspasa el resultado a
        capital: el periodo diciembre-marzo no la ve ni en resultado ni en
        capital."""
        has_closing_flag = 'l10n_mx_closing_move' in self.env['account.move']._fields
        date_from = date(2025, 12, 1)
        self._entry([('102.01.002', 5000, 0), ('301.01.01', 0, 5000)], day=date(2025, 3, 1), journal=self.j_bank)
        self._entry([('102.01.002', 200, 0), ('401.01.01', 0, 200)], day=date(2025, 7, 1), journal=self.j_bank)
        if has_closing_flag:
            self._entry([('401.01.01', 200, 0), ('301.01.01', 0, 200)], day=date(2025, 12, 31), closing=True)
        self._entry([('102.01.002', 300, 0), ('401.01.01', 0, 300)], day=date(2026, 2, 1), journal=self.j_bank)
        result, sliced, totals = self._compute(date_from, TO)
        self.assertReconciles(sliced, totals, expected_delta=300.0, date_from=date_from, date_to=TO)
        self.assertAlmostEqual(sliced['opening_cash'], 5200.0, 2)
        self.assertAlmostEqual(totals['closing_cash_book'], 5500.0, 2)
        self.assertLine(sliced, 'result', 300.0)
        self.assertLine(sliced, 'fin_equity', 0.0)
        self.assertLine(sliced, 'd_customers', 300.0)
        if not has_closing_flag:
            self.skipTest('l10n_mx_closing_move no existe en esta base (solo Enterprise): exclusión de cierres no probada')

    def test_70_monthly_slices_add_up(self):
        self._entry([('102.01.002', 100, 0), ('401.01.01', 0, 100)], day=date(2026, 1, 10), journal=self.j_bank)
        self._entry([('102.01.002', 200, 0), ('401.01.01', 0, 200)], day=date(2026, 2, 10), journal=self.j_bank)
        self._entry([('102.01.002', 0, 50), ('504.01.0034', 50, 0)], day=date(2026, 3, 10), journal=self.j_bank)
        result, sliced, totals = self._compute()
        self.assertReconciles(sliced, totals, expected_delta=250.0)
        jan = self.engine.slice(result, date(2026, 1, 1), date(2026, 1, 31))
        feb = self.engine.slice(result, date(2026, 2, 1), date(2026, 2, 28))
        mar = self.engine.slice(result, date(2026, 3, 1), date(2026, 3, 31))
        self.assertAlmostEqual(jan['lines']['d_customers'], 100.0, 2)
        self.assertAlmostEqual(feb['lines']['d_customers'], 200.0, 2)
        self.assertAlmostEqual(mar['lines']['d_suppliers'], -50.0, 2)
        self.assertAlmostEqual(feb['opening_cash'], 100.0, 2)
        self.assertAlmostEqual(mar['closing_cash_book'], 250.0, 2)
        self.assertAlmostEqual(sum(s['cash_delta'] for s in (jan, feb, mar)), 250.0, 2)

    def test_80_unclassified_is_reported_not_dropped(self):
        """Una cuenta sin regla cae en "Sin clasificar" y sigue cuadrando."""
        odd = self.env['account.account'].with_company(self.company).create({
            'code': '999.99.99', 'name': 'Cuenta rara', 'account_type': 'asset_current'})
        self.acc['999.99.99'] = odd
        self._entry([('999.99.99', 70, 0), ('102.01.002', 0, 70)], day=date(2026, 2, 20), journal=self.j_bank)
        result, sliced, totals = self._compute()
        delta = self._cash_delta()
        self.assertAlmostEqual(delta, -70.0, 2)
        self.assertLine(sliced, 'unclassified', -70.0)
        self.assertLine(sliced, 'd_other', -70.0)
        self.assertIn(odd.id, sliced['accounts']['unclassified'])
        self.assertAlmostEqual(totals['ind_net'] + totals['ind_fx'], delta, 2)
        self.assertAlmostEqual(totals['dir_net'] + totals['dir_fx'], delta, 2)
        self.assertAlmostEqual(totals['difference'], 0.0, 2)

    def test_90_summary_and_snapshot(self):
        self._entry([('102.01.002', 116, 0), ('401.01.01', 0, 100), ('208.01.01', 0, 16)], day=date(2026, 2, 8), journal=self.j_bank)
        summary = self.config.compute_summary('2026-01-01', '2026-03-31')
        self.assertAlmostEqual(summary['direct']['net_increase'], 116.0, 2)
        self.assertAlmostEqual(summary['indirect']['net_increase'], 116.0, 2)
        self.assertAlmostEqual(summary['difference'], 0.0, 2)
        self.assertEqual(summary['unclassified'], [])
        snap = self.env['cash.flow.snapshot'].generate(self.company, FROM, TO, 'custom')
        self.assertAlmostEqual(snap.closing_cash, snap.closing_cash_book, 2)
        self.assertAlmostEqual(snap.net_increase, 116.0, 2)
        # Poliza manual: la venta va a clientes y el IVA cobrado a impuestos.
        self.assertEqual(snap.data['lines']['d_customers'], 100.0)
        self.assertEqual(snap.data['lines']['d_taxes'], 16.0)
        # Regenerar reemplaza en vez de duplicar.
        snap2 = self.env['cash.flow.snapshot'].generate(self.company, FROM, TO, 'custom')
        self.assertEqual(snap, snap2)


@tagged('post_install', '-at_install', 'quimibond_cash_flow')
class TestCashFlowReportHandler(AccountTestInvoicingCommon):
    """El handler solo existe con account_reports (Enterprise); si no esta
    instalado el test se omite en vez de fallar."""
    chart_template = 'generic_coa'

    def test_report_renders(self):
        if 'account.report.custom.handler' not in self.env or not self.env['ir.module.module'].search(
                [('name', '=', 'account_reports'), ('state', '=', 'installed')]):
            self.skipTest('account_reports no está instalado')
        config = self.env['cash.flow.config']._get_for_company(self.env.company)
        config.rule_ids.unlink()
        config._load_default_rules()
        report = self.env.ref('quimibond_cash_flow.cash_flow_nif_report')
        options = report.get_options({'date': {'date_from': '2026-01-01', 'date_to': '2026-03-31', 'mode': 'range', 'filter': 'custom'}})
        self.assertEqual(len(options['column_groups']), 4)   # 3 meses + acumulado
        lines = report._get_lines(options)
        names = [line['name'] for line in lines]
        self.assertIn('Método indirecto', names)
        self.assertIn('Método directo', names)
        self.assertIn('Diferencia no explicada', names)
        # Variante solo indirecto: sin encabezados de metodo ni lineas del directo.
        variant = self.env.ref('quimibond_cash_flow.cash_flow_nif_report_indirect')
        v_options = variant.get_options({'date': {'date_from': '2026-01-01', 'date_to': '2026-03-31', 'mode': 'range', 'filter': 'custom'}})
        v_names = [line['name'] for line in variant._get_lines(v_options)]
        self.assertNotIn('Método directo', v_names)
        self.assertIn('Incremento neto según método indirecto', v_names)

# -*- coding: utf-8 -*-
"""Presupuesto maestro de ventas — Paso 1: modelo, unidades, divisas y real.

Datos propios (no demo). Periodo 2040 para aislar de la facturación de demo.
"""
from datetime import date

from odoo.tests import TransactionCase, tagged
from odoo.exceptions import UserError, ValidationError


@tagged('post_install', '-at_install')
class TestSalesBudget(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Budget = cls.env['sgi.sales.budget']
        cls.Line = cls.env['sgi.sales.budget.line']
        cls.company = cls.env.company
        cls.team = cls.env['crm.team'].create({'name': 'Mercado Industrial KPI'})
        # Por xmlid (el nombre depende del idioma): m/cm = Longitud, kg = Peso.
        cls.uom_m = cls.env.ref('uom.product_uom_meter')
        cls.uom_cm = cls.env.ref('uom.product_uom_cm')
        cls.uom_kg = cls.env.ref('uom.product_uom_kgm')
        cls.product = cls.env['product.product'].create({
            'name': 'Tela industrial PPV', 'type': 'consu',
            'uom_id': cls.uom_m.id})
        cls.income = cls.env['account.account'].search(
            [('account_type', '=', 'income')], limit=1)
        cls.partner = cls.env['res.partner'].create({'name': 'Cliente PPV'})

    def _budget(self, **vals):
        base = {'year': 2040, 'team_id': self.team.id}
        base.update(vals)
        return self.Budget.create(base)

    def _line(self, budget, when, uom=None, qty=0.0, amount=0.0, product=None):
        return self.Line.create({
            'budget_id': budget.id, 'product_id': (product or self.product).id,
            'date': when, 'uom_id': (uom or self.uom_m).id,
            'qty_budget': qty, 'amount_budget': amount})

    def _invoice(self, when, qty, uom, price, refund=False, currency=None):
        move = self.env['account.move'].create({
            'move_type': 'out_refund' if refund else 'out_invoice',
            'partner_id': self.partner.id, 'invoice_date': when,
            'team_id': self.team.id,
            'currency_id': (currency or self.company.currency_id).id,
            'invoice_line_ids': [(0, 0, {
                'product_id': self.product.id, 'quantity': qty,
                'product_uom_id': uom.id, 'price_unit': price,
                'account_id': self.income.id, 'tax_ids': [(6, 0, [])]})]})
        move.action_post()
        return move

    # ------------------------------------------------------------------
    def test_01_name_and_currency(self):
        budget = self._budget()
        self.assertEqual(budget.name, "Presupuesto Mercado Industrial KPI 2040 Rev.1")
        self.assertTrue(budget.folio.startswith('PPV-'))
        self.assertEqual(budget.currency_id, self.company.currency_id)

    def test_02_unique_active_per_year_team(self):
        self._budget()
        with self.assertRaises(ValidationError):
            self._budget()  # segundo no-obsoleto mismo año+equipo
        # Uno obsoleto sí convive con uno vigente.
        b1 = self.Budget.search([('team_id', '=', self.team.id)], limit=1)
        b1.state = 'obsoleto'
        self._budget()  # ahora sí

    def test_03_line_date_must_be_first_of_month_in_year(self):
        budget = self._budget()
        with self.assertRaises(ValidationError):
            self._line(budget, date(2040, 6, 15))  # no es día 1
        with self.assertRaises(ValidationError):
            self._line(budget, date(2039, 6, 1))  # fuera del año
        line = self._line(budget, date(2040, 6, 1))
        self.assertTrue(line.id)

    def test_04_uom_must_share_category(self):
        budget = self._budget()
        with self.assertRaises(ValidationError):
            self._line(budget, date(2040, 6, 1), uom=self.uom_kg)  # kg vs producto en m
        line = self._line(budget, date(2040, 6, 1), uom=self.uom_cm)  # cm ~ m: ok
        self.assertTrue(line.id)

    def test_05_display_name_includes_unit(self):
        budget = self._budget()
        line = self._line(budget, date(2040, 6, 1), uom=self.uom_m)
        self.assertIn('(m)', line.display_name)

    def test_06_real_facturado_moneda_companiacon_balance(self):
        # Factura en MXN (≠ USD compañía): el real usa balance (ya convertido por
        # contabilidad), NO la cifra facial en MXN.
        mxn = self.env.ref('base.MXN')
        mxn.active = True
        self.env['res.currency.rate'].create({
            'currency_id': mxn.id, 'name': date(2040, 6, 1), 'rate': 20.0,
            'company_id': self.company.id})
        self._invoice(date(2040, 6, 10), 5.0, self.uom_m, 400.0, currency=mxn)
        budget = self._budget()
        line = self._line(budget, date(2040, 6, 1), qty=10.0, amount=150.0)
        # 2000 MXN / 20 = 100 USD (moneda compañía), no 2000.
        self.assertAlmostEqual(line.amount_real, 100.0, places=2)
        self.assertEqual(line.qty_real, 5.0)

    def test_07_real_convierte_unidad_misma_categoria(self):
        # Factura en cm; la línea presupuesta en m → 500 cm = 5 m.
        self._invoice(date(2040, 6, 10), 500.0, self.uom_cm, 1.0)
        budget = self._budget()
        line = self._line(budget, date(2040, 6, 1), uom=self.uom_m)
        self.assertEqual(line.qty_real, 5.0)

    def test_08_real_refund_resta(self):
        self._invoice(date(2040, 6, 10), 10.0, self.uom_m, 100.0)
        self._invoice(date(2040, 6, 12), 3.0, self.uom_m, 100.0, refund=True)
        budget = self._budget()
        line = self._line(budget, date(2040, 6, 1), uom=self.uom_m)
        self.assertEqual(line.qty_real, 7.0, "10 facturados - 3 en nota de crédito.")
        self.assertAlmostEqual(line.amount_real, 700.0, places=2)

    def test_09_unidad_de_otra_categoria_excluida_y_contada(self):
        # Una factura del producto con unidad kg (otra categoría) cuenta en importe
        # pero NO en cantidad, y se marca en unconverted_count.
        move = self.env['account.move'].create({
            'move_type': 'out_invoice', 'partner_id': self.partner.id,
            'invoice_date': date(2040, 6, 10), 'team_id': self.team.id,
            'invoice_line_ids': [(0, 0, {
                'product_id': self.product.id, 'quantity': 8.0,
                'product_uom_id': self.uom_kg.id, 'price_unit': 50.0,
                'account_id': self.income.id, 'tax_ids': [(6, 0, [])]})]})
        move.action_post()
        budget = self._budget()
        line = self._line(budget, date(2040, 6, 1), uom=self.uom_m)
        self.assertEqual(line.qty_real, 0.0, "kg no convertible a m: excluida de cantidad.")
        self.assertAlmostEqual(line.amount_real, 400.0, places=2, msg="El importe sí cuenta.")
        self.assertEqual(line.unconverted_count, 1)
        self.assertEqual(budget.unconverted_count, 1)

    def test_10_qty_text_por_unidad_sin_mezclar(self):
        budget = self._budget()
        p2 = self.env['product.product'].create({
            'name': 'Fibra PPV', 'type': 'consu', 'uom_id': self.uom_kg.id})
        self._line(budget, date(2040, 6, 1), uom=self.uom_m, qty=1200.0)
        self._line(budget, date(2040, 7, 1), uom=self.uom_m, qty=300.0)
        self._line(budget, date(2040, 6, 1), uom=self.uom_kg, qty=80.0, product=p2)
        text = budget.qty_budget_text
        self.assertIn('1,500 m', text)  # 1200 + 300 m
        self.assertIn('80 kg', text)
        self.assertIn('·', text, "Las unidades se listan por separado, no se suman.")

    def test_11_avg_prices_protege_division_cero(self):
        budget = self._budget()
        line = self._line(budget, date(2040, 6, 1), qty=0.0, amount=1000.0)
        self.assertEqual(line.avg_price_budget, 0.0)  # sin cantidad → 0, no ZeroDivision
        line2 = self._line(budget, date(2040, 7, 1), qty=50.0, amount=1000.0)
        self.assertEqual(line2.avg_price_budget, 20.0)

    def test_12_locked_when_approved(self):
        manager = self.env['res.users'].create({
            'name': 'MAST PPV', 'login': 'ppv_mgr',
            'group_ids': [(6, 0, [self.env.ref('quimibond_sgi.group_sgi_manager').id])]})
        raso = self.env['res.users'].create({
            'name': 'Raso PPV', 'login': 'ppv_raso',
            'group_ids': [(6, 0, [self.env.ref('quimibond_sgi.group_sgi_user').id])]})
        budget = self._budget()
        line = self._line(budget, date(2040, 6, 1), qty=10.0)
        budget.with_user(manager).action_approve()
        self.assertEqual(budget.state, 'aprobado')
        # Raso no puede editar ni borrar la línea aprobada.
        with self.assertRaises(UserError):
            line.with_user(raso).write({'qty_budget': 99.0})
        with self.assertRaises(UserError):
            line.with_user(raso).unlink()
        # MAST sí (tras regresar a borrador, o directamente por privilegio).
        line.with_user(manager).write({'qty_budget': 42.0})
        self.assertEqual(line.qty_budget, 42.0)

    def test_13_approve_requires_manager_and_lines(self):
        raso = self.env['res.users'].create({
            'name': 'Raso PPV 2', 'login': 'ppv_raso2',
            'group_ids': [(6, 0, [self.env.ref('quimibond_sgi.group_sgi_user').id])]})
        budget = self._budget()
        # Sin líneas no aprueba.
        with self.assertRaises(UserError):
            budget.action_approve()
        self._line(budget, date(2040, 6, 1), qty=10.0)
        # Raso no aprueba.
        with self.assertRaises(UserError):
            budget.with_user(raso).action_approve()

    def test_14_action_revise_conserva_historia(self):
        manager = self.env['res.users'].create({
            'name': 'MAST PPV rev', 'login': 'ppv_mgr_rev',
            'group_ids': [(6, 0, [self.env.ref('quimibond_sgi.group_sgi_manager').id])]})
        budget = self._budget()
        self._line(budget, date(2040, 6, 1), qty=10.0, amount=500.0)
        budget.with_user(manager).action_approve()
        action = budget.with_user(manager).action_revise()
        new = self.Budget.browse(action['res_id'])
        self.assertEqual(budget.state, 'obsoleto', "La anterior se conserva obsoleta.")
        self.assertEqual(new.state, 'borrador')
        self.assertEqual(new.revision, 2)
        self.assertEqual(new.year, 2040)
        self.assertEqual(len(new.line_ids), 1, "Copia las líneas.")
        self.assertEqual(new.line_ids.qty_budget, 10.0)

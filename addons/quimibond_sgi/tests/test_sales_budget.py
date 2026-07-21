# -*- coding: utf-8 -*-
"""Presupuesto maestro de ventas — Paso 1: modelo, unidades, divisas y real.

Datos propios (no demo). Periodo 2040 para aislar de la facturación de demo.
"""
import base64
import io
from datetime import date

from odoo import fields
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


@tagged('post_install', '-at_install')
class TestSalesBudgetStep2(TransactionCase):
    """Paso 2: grid de captura, comparación, banner de formato e impresión."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Budget = cls.env['sgi.sales.budget']
        cls.Line = cls.env['sgi.sales.budget.line']
        cls.team = cls.env['crm.team'].create({'name': 'Mercado Confección PPV'})
        cls.uom_m = cls.env.ref('uom.product_uom_meter')
        cls.product = cls.env['product.product'].create({
            'name': 'Entretela PPV', 'type': 'consu', 'uom_id': cls.uom_m.id})

    def _budget(self):
        return self.Budget.create({'year': 2040, 'team_id': self.team.id})

    def test_01_grid_update_cell_creates_and_increments(self):
        budget = self._budget()
        Line = self.Line.with_context(default_budget_id=budget.id)
        domain = [('budget_id', '=', budget.id),
                  ('product_id', '=', self.product.id),
                  ('date', '>=', '2040-06-01'), ('date', '<', '2040-07-01')]
        # Celda vacía → crea la línea (producto/mes/unidad de venta).
        Line.grid_update_cell(domain, 'qty_budget', 100.0)
        line = self.Line.search([('budget_id', '=', budget.id)])
        self.assertEqual(len(line), 1)
        self.assertEqual(line.date, date(2040, 6, 1))
        self.assertEqual(line.uom_id, self.uom_m)
        self.assertEqual(line.qty_budget, 100.0)
        # Celda existente → suma.
        Line.grid_update_cell(domain, 'qty_budget', 50.0)
        self.assertEqual(line.qty_budget, 150.0)
        # También sobre importe.
        Line.grid_update_cell(domain, 'amount_budget', 2000.0)
        self.assertEqual(line.amount_budget, 2000.0)

    def test_02_grid_zero_value_noop(self):
        budget = self._budget()
        Line = self.Line.with_context(default_budget_id=budget.id)
        domain = [('budget_id', '=', budget.id),
                  ('product_id', '=', self.product.id),
                  ('date', '>=', '2040-06-01'), ('date', '<', '2040-07-01')]
        Line.grid_update_cell(domain, 'qty_budget', 0.0)
        self.assertFalse(self.Line.search([('budget_id', '=', budget.id)]))

    def test_03_format_banner_a28_18(self):
        budget = self._budget()
        self.assertIn('F-P-A28-18', budget.sgi_format_banner or '')

    def test_04_grid_and_comparison_actions(self):
        budget = self._budget()
        act_qty = budget.action_open_grid_qty()
        self.assertEqual(act_qty['res_model'], 'sgi.sales.budget.line')
        self.assertTrue(any(v[1] == 'grid' for v in act_qty['views']))
        act_cmp = budget.action_open_comparison()
        self.assertEqual(act_cmp['view_mode'], 'pivot,graph,list')

    def test_05_report_renders(self):
        budget = self._budget()
        self.Line.create({
            'budget_id': budget.id, 'product_id': self.product.id,
            'date': date(2040, 6, 1), 'uom_id': self.uom_m.id,
            'qty_budget': 1200.0, 'amount_budget': 48000.0})
        html, ttype = self.env['ir.actions.report']._render_qweb_html(
            'quimibond_sgi.action_report_sales_budget', budget.ids)
        self.assertEqual(ttype, 'html')
        self.assertIn('Presupuesto de Ventas', html.decode())
        self.assertIn('F-P-A28-18', html.decode())


@tagged('post_install', '-at_install')
class TestSalesBudgetStep3(TransactionCase):
    """Paso 3: conexión al SGI — KPI VE-02 y aviso de cierre de mes."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Budget = cls.env['sgi.sales.budget']
        cls.Line = cls.env['sgi.sales.budget.line']
        cls.Indicator = cls.env['sgi.indicator']
        cls.Cron = cls.env['sgi.cron']
        cls.company = cls.env.company
        cls.uom_m = cls.env.ref('uom.product_uom_meter')
        cls.product = cls.env['product.product'].create({
            'name': 'Producto VE02', 'type': 'consu', 'uom_id': cls.uom_m.id})
        cls.income = cls.env['account.account'].search(
            [('account_type', '=', 'income')], limit=1)
        cls.partner = cls.env['res.partner'].create({'name': 'Cliente VE02'})

    def _team(self, name):
        return self.env['crm.team'].create({'name': name})

    def _approved_budget(self, team, when, amount):
        budget = self.Budget.create({'year': when.year, 'team_id': team.id})
        self.Line.create({
            'budget_id': budget.id, 'product_id': self.product.id,
            'date': when, 'uom_id': self.uom_m.id, 'qty_budget': 100.0,
            'amount_budget': amount})
        budget.state = 'aprobado'
        return budget

    def _invoice(self, team, when, amount, refund=False):
        move = self.env['account.move'].create({
            'move_type': 'out_refund' if refund else 'out_invoice',
            'partner_id': self.partner.id, 'invoice_date': when,
            'team_id': team.id,
            'invoice_line_ids': [(0, 0, {
                'product_id': self.product.id, 'quantity': 1,
                'product_uom_id': self.uom_m.id, 'price_unit': amount,
                'account_id': self.income.id, 'tax_ids': [(6, 0, [])]})]})
        move.action_post()
        return move

    def test_01_ve02_lee_presupuesto_aprobado(self):
        team = self._team('Mercado VE02 A')
        self._approved_budget(team, date(2040, 6, 1), 1000.0)
        # Facturación neta junio 2040 = 1000 - 200 = 800 → 80% del presupuesto.
        self._invoice(team, date(2040, 6, 10), 1000.0)
        self._invoice(team, date(2040, 6, 12), 200.0, refund=True)
        ind = self.Indicator.create({
            'code': 'VE02-T', 'name': 'Cumpl. presupuesto',
            'calc_mode': 'presupuesto_ventas', 'direction': 'higher_better'})
        value = ind._calc_presupuesto_ventas(date(2040, 6, 1), date(2040, 6, 30))
        self.assertEqual(value, 80.0)
        # Sin nota: hay presupuesto aprobado.
        self.assertFalse(ind._note_presupuesto_ventas(date(2040, 6, 1), date(2040, 6, 30)))

    def test_02_ve02_fallback_al_parametro_con_nota(self):
        # Sin presupuesto aprobado del año → usa el parámetro de Ajustes + nota.
        self.env['ir.config_parameter'].sudo().set_param(
            'quimibond_sgi.monthly_sales_budget', '1000')
        team = self._team('Mercado VE02 B')
        self._invoice(team, date(2040, 6, 10), 800.0)
        ind = self.Indicator.create({
            'code': 'VE02-F', 'name': 'Cumpl. presupuesto fb',
            'calc_mode': 'presupuesto_ventas', 'direction': 'higher_better'})
        value = ind._calc_presupuesto_ventas(date(2040, 6, 1), date(2040, 6, 30))
        self.assertEqual(value, 80.0)
        self.assertIn('Ajustes',
                      ind._note_presupuesto_ventas(date(2040, 6, 1), date(2040, 6, 30)))

    def test_03_ve02_evidencia_son_las_lineas(self):
        team = self._team('Mercado VE02 C')
        budget = self._approved_budget(team, date(2040, 6, 1), 1000.0)
        ind = self.Indicator.create({
            'code': 'VE02-E', 'name': 'Cumpl. presupuesto ev',
            'calc_mode': 'presupuesto_ventas', 'direction': 'higher_better'})
        measure = self.env['sgi.indicator.measure'].create({
            'indicator_id': ind.id, 'period_date': date(2040, 6, 1),
            'value': 80.0, 'state': 'capturado'})
        action = measure.action_view_evidence()
        self.assertEqual(action['res_model'], 'sgi.sales.budget.line')
        records = self.Line.search(action['domain'])
        self.assertEqual(records, budget.line_ids)

    def test_04_cierre_de_mes_avisa_bajo_umbral(self):
        team = self._team('Mercado cierre bajo')
        team.user_id = self.env.user.id
        self._approved_budget(team, date(2040, 6, 1), 1000.0)
        self._invoice(team, date(2040, 6, 10), 500.0)  # 50% < 80%
        self.Cron._sgi_sales_budget_month_close(date(2040, 6, 1), date(2040, 6, 30))
        acts = self.env['mail.activity'].search([
            ('res_model', '=', 'sgi.sales.budget'),
            ('user_id', '=', self.env.user.id)])
        self.assertTrue(acts, "Equipo bajo umbral debe recibir aviso de cierre.")

    def test_05_cierre_de_mes_no_avisa_si_cumple(self):
        team = self._team('Mercado cierre ok')
        team.user_id = self.env.user.id
        self._approved_budget(team, date(2040, 6, 1), 1000.0)
        self._invoice(team, date(2040, 6, 10), 950.0)  # 95% >= 80%
        before = self.env['mail.activity'].search_count([
            ('res_model', '=', 'sgi.sales.budget')])
        self.Cron._sgi_sales_budget_month_close(date(2040, 6, 1), date(2040, 6, 30))
        after = self.env['mail.activity'].search_count([
            ('res_model', '=', 'sgi.sales.budget')])
        self.assertEqual(before, after, "Equipo que cumple no genera aviso.")


@tagged('post_install', '-at_install')
class TestSalesBudgetClient(TransactionCase):
    """Adición 5.2: dimensión cliente (opcional, sin doble conteo)."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Budget = cls.env['sgi.sales.budget']
        cls.Line = cls.env['sgi.sales.budget.line']
        cls.team = cls.env['crm.team'].create({'name': 'Mercado cliente PPV'})
        cls.uom_m = cls.env.ref('uom.product_uom_meter')
        cls.product = cls.env['product.product'].create({
            'name': 'Tela cliente PPV', 'type': 'consu', 'uom_id': cls.uom_m.id})
        cls.income = cls.env['account.account'].search(
            [('account_type', '=', 'income')], limit=1)
        # Empresa comercial C con un contacto hijo CC, y otra empresa D.
        cls.company_c = cls.env['res.partner'].create(
            {'name': 'Cliente C', 'is_company': True})
        cls.contact_cc = cls.env['res.partner'].create(
            {'name': 'Contacto CC', 'parent_id': cls.company_c.id})
        cls.company_d = cls.env['res.partner'].create(
            {'name': 'Cliente D', 'is_company': True})

    def _budget(self):
        return self.Budget.create({'year': 2040, 'team_id': self.team.id})

    def _line(self, budget, partner=None, amount=0.0, product=None):
        return self.Line.create({
            'budget_id': budget.id, 'product_id': (product or self.product).id,
            'date': date(2040, 6, 1), 'uom_id': self.uom_m.id,
            'partner_id': partner.id if partner else False,
            'qty_budget': 10.0, 'amount_budget': amount})

    def _invoice(self, partner, amount):
        move = self.env['account.move'].create({
            'move_type': 'out_invoice', 'partner_id': partner.id,
            'invoice_date': date(2040, 6, 10), 'team_id': self.team.id,
            'invoice_line_ids': [(0, 0, {
                'product_id': self.product.id, 'quantity': 1,
                'product_uom_id': self.uom_m.id, 'price_unit': amount,
                'account_id': self.income.id, 'tax_ids': [(6, 0, [])]})]})
        move.action_post()
        return move

    def test_01_double_count_blocked(self):
        budget = self._budget()
        self._line(budget, partner=None, amount=1000.0)  # global
        # Mezclar el mismo producto con cliente → bloqueado.
        with self.assertRaises(ValidationError):
            self._line(budget, partner=self.company_c, amount=500.0)

    def test_02_double_count_blocked_reverse(self):
        budget = self._budget()
        self._line(budget, partner=self.company_c, amount=500.0)  # por cliente
        with self.assertRaises(ValidationError):
            self._line(budget, partner=None, amount=1000.0)  # ahora global → bloqueado

    def test_03_two_clients_ok(self):
        budget = self._budget()
        self._line(budget, partner=self.company_c, amount=500.0)
        line_d = self._line(budget, partner=self.company_d, amount=300.0)
        self.assertTrue(line_d.id, "Dos clientes distintos para el mismo producto: OK.")

    def test_04_unique_global_per_product_month(self):
        budget = self._budget()
        self._line(budget, partner=None, amount=1000.0)
        # Segundo global mismo producto+mes → viola el índice único.
        with self.assertRaises(Exception):
            with self.env.cr.savepoint():
                self._line(budget, partner=None, amount=200.0)

    def test_05_real_filtered_by_commercial_partner(self):
        # Factura al CONTACTO hijo de C, y otra a D. La línea por cliente C debe
        # tomar solo la de C (vía commercial_partner_id del documento).
        self._invoice(self.contact_cc, 500.0)
        self._invoice(self.company_d, 300.0)
        budget = self._budget()
        line = self._line(budget, partner=self.company_c, amount=400.0)
        self.assertAlmostEqual(line.amount_real, 500.0, places=2)

    def test_06_amount_real_unbudgeted(self):
        # Equipo factura 800 (500 a C, 300 a D); se presupuesta solo C → 300 sin
        # presupuestar.
        self._invoice(self.contact_cc, 500.0)
        self._invoice(self.company_d, 300.0)
        budget = self._budget()
        self._line(budget, partner=self.company_c, amount=400.0)
        self.assertAlmostEqual(budget.amount_real_unbudgeted, 300.0, places=2)

    def test_07_global_line_takes_all_clients(self):
        # Sin dimensión cliente, el real es de todo el equipo (C + D).
        self._invoice(self.contact_cc, 500.0)
        self._invoice(self.company_d, 300.0)
        budget = self._budget()
        line = self._line(budget, partner=None, amount=1000.0)
        self.assertAlmostEqual(line.amount_real, 800.0, places=2)
        self.assertAlmostEqual(budget.amount_real_unbudgeted, 0.0, places=2)


@tagged('post_install', '-at_install')
class TestSalesBudgetPrice(TransactionCase):
    """Adición 5.2: precio sugerido desde la lista de precios del cliente."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Budget = cls.env['sgi.sales.budget']
        cls.Line = cls.env['sgi.sales.budget.line']
        cls.Param = cls.env['ir.config_parameter'].sudo()
        cls.team = cls.env['crm.team'].create({'name': 'Mercado precio PPV'})
        cls.uom_m = cls.env.ref('uom.product_uom_meter')
        cls.usd = cls.env.ref('base.USD')  # moneda de la compañía en esta BD
        cls.mxn = cls.env.ref('base.MXN')
        cls.mxn.active = True
        cls.product = cls.env['product.product'].create({
            'name': 'Tela precio PPV', 'type': 'consu',
            'uom_id': cls.uom_m.id, 'list_price': 100.0})
        cls.budget = cls.Budget.create({'year': 2040, 'team_id': cls.team.id})

    def _pricelist(self, currency, price):
        pl = self.env['product.pricelist'].create({
            'name': 'Lista %s PPV' % currency.name, 'currency_id': currency.id})
        self.env['product.pricelist.item'].create({
            'pricelist_id': pl.id, 'applied_on': '1_product',
            'product_tmpl_id': self.product.product_tmpl_id.id,
            'compute_price': 'fixed', 'fixed_price': price})
        return pl

    def _partner_with_list(self, pricelist):
        partner = self.env['res.partner'].create(
            {'name': 'Cliente lista PPV', 'is_company': True})
        partner.property_product_pricelist = pricelist
        return partner

    def _new_line(self, partner=None, qty=10.0):
        return self.Line.new({
            'budget_id': self.budget.id, 'product_id': self.product.id,
            'uom_id': self.uom_m.id, 'qty_budget': qty,
            'partner_id': partner.id if partner else False})

    def test_01_suggest_from_client_pricelist_mxn_company_currency(self):
        # Lista en la MISMA moneda de la compañía (USD): sin conversión.
        partner = self._partner_with_list(self._pricelist(self.usd, 37.63))
        line = self._new_line(partner)
        line._onchange_suggest_price()
        self.assertAlmostEqual(line.price_unit_budget, 37.63, places=2)
        self.assertIn("Lista", line.price_source)

    def test_02_suggest_usd_list_with_planning_rate(self):
        # Lista en MXN convertida con el tipo presupuestal (USD→MXN = 10):
        # 350 MXN / 10 = 35 USD (moneda compañía).
        self.Param.set_param('quimibond_sgi.budget_planning_rate', '10')
        partner = self._partner_with_list(self._pricelist(self.mxn, 350.0))
        line = self._new_line(partner)
        line._onchange_suggest_price()
        self.assertAlmostEqual(line.price_unit_budget, 35.0, places=2)
        self.assertIn("MXN", line.price_source)

    def test_03_rate_zero_uses_day_rate(self):
        # Tipo presupuestal 0 → usa el tipo del día. Fijamos MXN=20/USD ese día.
        self.Param.set_param('quimibond_sgi.budget_planning_rate', '0')
        self.env['res.currency.rate'].create({
            'currency_id': self.mxn.id, 'name': fields.Date.context_today(self),
            'rate': 20.0, 'company_id': self.env.company.id})
        partner = self._partner_with_list(self._pricelist(self.mxn, 350.0))
        line = self._new_line(partner)
        line._onchange_suggest_price()
        # 350 MXN a 20/USD = 17.5 USD (no 35, que sería con tipo presupuestal 10).
        self.assertAlmostEqual(line.price_unit_budget, 17.5, places=2)

    def test_04_manual_price_not_overwritten(self):
        partner = self._partner_with_list(self._pricelist(self.usd, 37.63))
        line = self._new_line(partner)
        line.price_unit_budget = 999.0  # capturado a mano
        line._onchange_suggest_price()
        self.assertEqual(line.price_unit_budget, 999.0, "No pisa el precio manual.")

    def test_05_no_client_uses_list_price(self):
        line = self._new_line(partner=None)
        line._onchange_suggest_price()
        self.assertAlmostEqual(line.price_unit_budget, 100.0, places=2)
        self.assertIn("Precio de venta", line.price_source)

    def test_06_amount_is_qty_times_price(self):
        line = self.Line.create({
            'budget_id': self.budget.id, 'product_id': self.product.id,
            'date': date(2040, 6, 1), 'uom_id': self.uom_m.id,
            'qty_budget': 10.0, 'price_unit_budget': 5.0})
        self.assertAlmostEqual(line.amount_budget, 50.0, places=2)
        # Invertible: capturar el importe despeja el precio.
        line2 = self.Line.create({
            'budget_id': self.budget.id, 'product_id': self.product.id,
            'date': date(2040, 7, 1), 'uom_id': self.uom_m.id,
            'qty_budget': 10.0, 'amount_budget': 500.0})
        self.assertAlmostEqual(line2.price_unit_budget, 50.0, places=2)


@tagged('post_install', '-at_install')
class TestSalesBudgetImport(TransactionCase):
    """Adición 5.2: asistente de importación del Excel F-P-A28-18."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Budget = cls.env['sgi.sales.budget']
        cls.Wizard = cls.env['sgi.sales.budget.import']
        cls.team = cls.env['crm.team'].create({'name': 'Mercado import PPV'})
        cls.uom_m = cls.env.ref('uom.product_uom_meter')
        cls.uom_kg = cls.env.ref('uom.product_uom_kgm')
        cls.tela = cls.env['product.product'].create({
            'name': 'Tela import', 'default_code': 'TELA-1', 'type': 'consu',
            'uom_id': cls.uom_m.id, 'list_price': 40.0})
        cls.fibra = cls.env['product.product'].create({
            'name': 'Fibra import', 'default_code': 'FIBRA-1', 'type': 'consu',
            'uom_id': cls.uom_kg.id})

    def _xlsx(self):
        import openpyxl
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = 'Presupuesto'
        ws.append(['PRODUCTO', 'UNIDAD', 'ENERO m', 'ENERO $',
                   'FEBRERO m', 'FEBRERO $', 'MARZO m', 'MARZO $'])
        ws.append(['TELA-1', 'm', 100, 5000, 120, 6000, 0, 0])
        ws.append(['FIBRA-1', 'kg', 50, 2500, 0, 0, 0, 0])
        ws.append(['NOEXISTE', 'm', 10, 500, 0, 0, 0, 0])
        buf = io.BytesIO()
        wb.save(buf)
        return base64.b64encode(buf.getvalue())

    def _wizard(self, budget, mode='replace'):
        return self.Wizard.create({
            'budget_id': budget.id, 'file': self._xlsx(),
            'filename': 'ppv.xlsx', 'sheet_name': 'Presupuesto',
            'conflict_mode': mode})

    def test_01_import_matrix(self):
        budget = self.Budget.create({'year': 2040, 'team_id': self.team.id})
        wiz = self._wizard(budget)
        wiz.action_import()
        # TELA-1: enero + febrero (2 líneas); FIBRA-1: enero (1). Total 3.
        self.assertEqual(len(budget.line_ids), 3)
        tela_ene = budget.line_ids.filtered(
            lambda l: l.product_id == self.tela and l.date == date(2040, 1, 1))
        self.assertEqual(tela_ene.qty_budget, 100.0)
        self.assertAlmostEqual(tela_ene.amount_budget, 5000.0, places=2)
        self.assertEqual(tela_ene.uom_id, self.uom_m)
        fibra = budget.line_ids.filtered(lambda l: l.product_id == self.fibra)
        self.assertEqual(fibra.uom_id, self.uom_kg, "Unidad kg de la columna UNIDAD.")
        self.assertEqual(fibra.qty_budget, 50.0)

    def test_02_unmatched_reported_not_fatal(self):
        budget = self.Budget.create({'year': 2040, 'team_id': self.team.id})
        wiz = self._wizard(budget)
        wiz.action_import()
        self.assertIn('NOEXISTE', wiz.result)
        self.assertIn('sin match', wiz.result)
        # A pesar del no-match, las líneas buenas se importaron.
        self.assertEqual(len(budget.line_ids), 3)

    def test_03_reimport_replace_no_duplicate(self):
        budget = self.Budget.create({'year': 2040, 'team_id': self.team.id})
        self._wizard(budget).action_import()
        self.assertEqual(len(budget.line_ids), 3)
        # Reimportar en modo reemplazar no duplica.
        self._wizard(budget).action_import()
        self.assertEqual(len(budget.line_ids), 3)

    def test_04_no_header_is_structural_error(self):
        import openpyxl
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(['COSA', 'OTRA'])  # sin 'PRODUCTO'
        ws.append(['x', 'y'])
        buf = io.BytesIO()
        wb.save(buf)
        budget = self.Budget.create({'year': 2040, 'team_id': self.team.id})
        wiz = self.Wizard.create({
            'budget_id': budget.id, 'file': base64.b64encode(buf.getvalue()),
            'sheet_name': 'Sheet'})
        with self.assertRaises(UserError):
            wiz.action_import()
        self.assertFalse(budget.line_ids, "Error estructural: se revierte todo.")

    def test_05_approved_budget_blocks_import(self):
        budget = self.Budget.create({'year': 2040, 'team_id': self.team.id})
        self.env['sgi.sales.budget.line'].create({
            'budget_id': budget.id, 'product_id': self.tela.id,
            'date': date(2040, 6, 1), 'uom_id': self.uom_m.id, 'qty_budget': 1.0})
        budget.state = 'aprobado'
        with self.assertRaises(UserError):
            budget.action_open_import()

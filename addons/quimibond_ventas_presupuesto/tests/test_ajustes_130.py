# -*- coding: utf-8 -*-
"""1.3.0 — ajustes del CEO (2026-10-01):

- excepción al «precio de lista mínimo plausible» por producto y por categoría
  (tiras perforadas a menos de $5/m), sin bajar el umbral general;
- «Actualizar real» recalcula el precio de lista también en «Revisado».

Datos propios; año 2047 para aislar."""
from datetime import date

from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestBudgetMinPriceException(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Budget = cls.env['sgi.sales.budget']
        cls.Line = cls.env['sgi.sales.budget.line']
        cls.env['ir.config_parameter'].sudo().set_param(
            'quimibond_sgi.price_min_plausible', '5.0')
        cls.team = cls.env['crm.team'].create({'name': 'Mercado excepción mínimo'})
        cls.uom_m = cls.env.ref('uom.product_uom_meter')
        cls.own = cls.env.company.currency_id
        cls.categ_parent = cls.env['product.category'].create({'name': 'PT prueba'})
        cls.categ_child = cls.env['product.category'].create({
            'name': 'Perfoquim prueba', 'parent_id': cls.categ_parent.id})
        cls.categ_plain = cls.env['product.category'].create({'name': 'Sin excepción'})
        # Un solo presupuesto por clase: hay uno no obsoleto por mercado y año,
        # así que crear uno por línea chocaba en las pruebas que piden dos.
        cls.budget = cls.Budget.create({'year': 2047, 'team_id': cls.team.id})

    def _product(self, categ, own_min=0.0):
        return self.env['product.product'].create({
            'name': 'Tira perforada prueba', 'type': 'consu',
            'uom_id': self.uom_m.id, 'list_price': 1.0, 'categ_id': categ.id,
            'sgi_budget_min_price': own_min})

    def _line(self, product, price):
        pl = self.env['product.pricelist'].create({
            'name': 'PL mínimo', 'currency_id': self.own.id})
        self.env['product.pricelist.item'].create({
            'pricelist_id': pl.id, 'applied_on': '1_product',
            'product_tmpl_id': product.product_tmpl_id.id,
            'compute_price': 'fixed', 'fixed_price': price})
        client = self.env['res.partner'].create({'name': 'Cli mínimo', 'is_company': True})
        client.property_product_pricelist = pl
        return self.Line.create({
            'budget_id': self.budget.id, 'product_id': product.id,
            'date': date(2047, 6, 1), 'uom_id': self.uom_m.id,
            'qty_budget': 10.0, 'partner_id': client.id})

    def test_01_general_threshold_unchanged(self):
        line = self._line(self._product(self.categ_plain), 0.55)
        self.assertFalse(line.has_list_price)
        self.assertIn('implausible', line.price_source)
        self.assertNotIn('mínimo propio', line.price_source)

    def test_02_product_own_minimum(self):
        """AP4032BL10.0/2 I a $1.28: mínimo propio del producto."""
        line = self._line(self._product(self.categ_plain, own_min=0.01), 1.28)
        self.assertTrue(line.has_list_price)
        self.assertAlmostEqual(line.price_unit_budget, 1.28, places=2)
        self.assertIn('mínimo propio del producto', line.price_source)

    def test_03_category_minimum_inherited_by_subcategory(self):
        """KF4032T11BL1.2/0 a $0.55: el mínimo de la categoría padre vale
        para la subcategoría; por debajo de él sigue siendo placebo."""
        self.categ_parent.sgi_budget_min_price = 0.30
        line = self._line(self._product(self.categ_child), 0.55)
        self.assertTrue(line.has_list_price)
        self.assertIn("categoría", line.price_source)
        low = self._line(self._product(self.categ_child), 0.20)
        self.assertFalse(low.has_list_price)
        self.assertIn('implausible', low.price_source)
        self.assertIn("categoría", low.price_source)

    def test_04_nearest_definition_wins(self):
        self.categ_parent.sgi_budget_min_price = 0.01
        self.categ_child.sgi_budget_min_price = 1.00
        self.assertFalse(self._line(self._product(self.categ_child), 0.55).has_list_price,
                         "La subcategoría con mínimo propio manda sobre el padre.")
        self.assertTrue(self._line(self._product(self.categ_child, own_min=0.50), 0.55)
                        .has_list_price, "El mínimo del producto manda sobre la categoría.")

    def test_05_forms_show_the_field(self):
        for model in ('product.template', 'product.category'):
            arch = self.env[model].get_view(view_type='form')['arch']
            self.assertIn('sgi_budget_min_price', arch, model)


@tagged('post_install', '-at_install')
class TestBudgetRefreshReviewedPrice(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Budget = cls.env['sgi.sales.budget']
        cls.Line = cls.env['sgi.sales.budget.line']
        cls.env['ir.config_parameter'].sudo().set_param(
            'quimibond_sgi.price_min_plausible', '5.0')
        cls.team = cls.env['crm.team'].create({'name': 'Mercado revisado precio'})
        cls.uom_m = cls.env.ref('uom.product_uom_meter')
        cls.product = cls.env['product.product'].create({
            'name': 'Tela revisado precio', 'type': 'consu',
            'uom_id': cls.uom_m.id, 'list_price': 1.0})
        cls.pricelist = cls.env['product.pricelist'].create({
            'name': 'PL revisado', 'currency_id': cls.env.company.currency_id.id})
        cls.client = cls.env['res.partner'].create({'name': 'Cli revisado', 'is_company': True})
        cls.client.property_product_pricelist = cls.pricelist
        cls.sale_mgr = cls.env['res.users'].create({
            'name': 'Admin ventas revisado', 'login': 'admrevprecio',
            'group_ids': [(6, 0, [
                cls.env.ref('sales_team.group_sale_manager').id,
                cls.env.ref('account.group_account_invoice').id])]})

    def _reviewed_budget_without_price(self):
        budget = self.Budget.create({'year': 2047, 'team_id': self.team.id})
        line = self.Line.create({
            'budget_id': budget.id, 'product_id': self.product.id,
            'date': date(2047, 6, 1), 'uom_id': self.uom_m.id,
            'qty_budget': 10.0, 'partner_id': self.client.id})
        budget.with_user(self.sale_mgr).action_send_to_review()
        self.assertEqual(budget.state, 'revisado')
        self.assertFalse(line.has_list_price, "La lista aún no tiene regla.")
        # Ventas da de alta el precio en la lista DESPUÉS de la revisión.
        self.env['product.pricelist.item'].create({
            'pricelist_id': self.pricelist.id, 'applied_on': '1_product',
            'product_tmpl_id': self.product.product_tmpl_id.id,
            'compute_price': 'fixed', 'fixed_price': 40.0})
        return budget, line

    def test_01_refresh_recalculates_price_in_reviewed(self):
        """El caso del CEO: antes había que regresarlo a borrador."""
        budget, line = self._reviewed_budget_without_price()
        before = len(budget.message_ids)
        budget.with_user(self.sale_mgr).action_refresh_actuals()
        self.assertEqual(budget.state, 'revisado', "No se reabre: el precio sale de la lista.")
        self.assertTrue(line.has_list_price)
        self.assertAlmostEqual(line.price_unit_budget, 40.0, places=2)
        self.assertEqual(budget.no_price_count, 0)
        self.assertGreater(len(budget.message_ids), before)
        self.assertTrue(any('recalculado en' in (m.body or '') for m in budget.message_ids))

    def test_02_no_change_no_message(self):
        budget, _line = self._reviewed_budget_without_price()
        budget.action_refresh_actuals()
        before = len(budget.message_ids)
        budget.action_refresh_actuals()
        self.assertEqual(len(budget.message_ids), before,
                         "Sin cambio de precio no hay constancia nueva.")

    def test_03_cron_refresh_does_not_touch_reviewed_price(self):
        """El cron y la conciliación siguen refrescando solo borradores."""
        budget, line = self._reviewed_budget_without_price()
        budget._sgi_refresh_actuals()
        self.assertEqual(budget.state, 'revisado')
        self.assertFalse(line.has_list_price)

    def test_04_approved_stays_frozen(self):
        budget, line = self._reviewed_budget_without_price()
        budget.state = 'aprobado'
        budget.action_refresh_actuals()
        self.assertFalse(line.has_list_price, "Lo aprobado no cambia de precio.")

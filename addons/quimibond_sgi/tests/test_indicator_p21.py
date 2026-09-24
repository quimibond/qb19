# -*- coding: utf-8 -*-
"""P-21 y automáticos sin dato: reproceso en kg, diferencia de inventario en
valor y energía en pesos por tonelada."""
from datetime import date

from odoo.tests import TransactionCase, tagged

from .common_accounts import sgi_test_payable


@tagged('post_install', '-at_install')
class TestIndicatorP21(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env = cls.env
        cls.Indicator = env['sgi.indicator']
        cls.Param = env['ir.config_parameter'].sudo()
        cls.kg = env.ref('uom.product_uom_kgm')
        cls.metro = env.ref('uom.product_uom_meter')
        cls.stock = env['stock.location'].search([('usage', '=', 'internal')], limit=1)
        cls.prodloc = env['stock.location'].search([('usage', '=', 'production')], limit=1)
        cls.categ = env['product.category'].create({'name': 'Hilo prueba P-21'})
        cls.Param.set_param('quimibond_sgi.waste_input_categ_ids', str(cls.categ.id))
        cls.hilo = env['product.product'].create({
            'name': 'Hilo P-21', 'type': 'consu', 'is_storable': True,
            'categ_id': cls.categ.id, 'uom_id': cls.kg.id})
        cls.period = date(2045, 3, 1)
        cls.period_end = date(2045, 3, 31)
        cls.when = '2045-03-15 10:00:00'

    def _consume(self, qty, when=None):
        mo = self.env['mrp.production'].create({
            'product_id': self.hilo.id, 'product_qty': qty, 'product_uom_id': self.kg.id})
        move = self.env['stock.move'].create({
            'product_id': self.hilo.id, 'product_uom_qty': qty, 'product_uom': self.kg.id,
            'location_id': self.stock.id, 'location_dest_id': self.prodloc.id,
            'raw_material_production_id': mo.id, 'state': 'done', 'date': when or self.when})
        move.write({'quantity': qty, 'picked': True, 'date': when or self.when})
        return mo

    def _finished(self, picking_type, product, qty, uom):
        mo = self.env['mrp.production'].create({
            'product_id': product.id, 'product_qty': qty, 'product_uom_id': uom.id,
            'picking_type_id': picking_type.id})
        move = self.env['stock.move'].create({
            'product_id': product.id, 'product_uom_qty': qty, 'product_uom': uom.id,
            'location_id': self.prodloc.id, 'location_dest_id': self.stock.id,
            'production_id': mo.id, 'state': 'done', 'date': self.when})
        move.write({'quantity': qty, 'picked': True, 'date': self.when})
        return mo

    def test_01_reproceso_kg(self):
        env = self.env
        wh = self.stock.warehouse_id or env['stock.warehouse'].search([], limit=1)
        rework = env['stock.picking.type'].create({
            'name': 'Re-proceso prueba', 'code': 'mrp_operation',
            'sequence_code': 'RPT', 'warehouse_id': wh.id})
        normal = env['stock.picking.type'].create({
            'name': 'Tejido prueba', 'code': 'mrp_operation',
            'sequence_code': 'TJT', 'warehouse_id': wh.id})
        self.Param.set_param('quimibond_sgi.rework_picking_type_ids', str(rework.id))
        tela = env['product.product'].create({
            'name': 'Tela P-21', 'type': 'consu', 'is_storable': True, 'uom_id': self.kg.id})
        tela_m = env['product.product'].create({
            'name': 'Tela m P-21', 'type': 'consu', 'is_storable': True, 'uom_id': self.metro.id})
        self._consume(400.0)
        mo_rework = self._finished(rework, tela, 20.0, self.kg)
        self._finished(rework, tela_m, 500.0, self.metro)   # metros: no cuenta
        self._finished(normal, tela, 300.0, self.kg)        # no es reproceso
        ind = self.Indicator.new({'calc_mode': 'reproceso'})
        detail = ind._detail_reproceso(self.period, self.period_end)
        self.assertEqual((detail['numerator'], detail['denominator']), (20.0, 400.0))
        self.assertEqual(detail['value'], 5.0)
        self.assertEqual(detail['model'], 'mrp.production')
        self.assertEqual(detail['ids'], mo_rework.ids)
        # Sin tipos configurados: sin dato, con nota.
        self.Param.set_param('quimibond_sgi.rework_picking_type_ids', '')
        self.assertIsNone(ind._calc_reproceso(self.period, self.period_end))
        self.assertIn('reproceso', ind._note_reproceso(self.period, self.period_end))

    def test_02_inventario_diferencia_valor(self):
        env = self.env
        if 'stock.valuation.layer' not in env:
            self.skipTest('stock_account no instalado')
        today = date.today()
        first = today.replace(day=1)
        product = env['product.product'].create({
            'name': 'Producto AL-01', 'type': 'consu', 'is_storable': True,
            'uom_id': self.kg.id, 'standard_price': 10.0})
        ind = self.Indicator.new({'calc_mode': 'inventario_diferencia'})
        base = ind._detail_inventario_diferencia(first, today)
        Quant = env['stock.quant'].with_context(inventory_mode=True)
        quant = Quant.create({'product_id': product.id, 'location_id': self.stock.id,
                              'inventory_quantity': 30.0})
        quant.action_apply_inventory()
        quant.inventory_quantity = 20.0
        quant.action_apply_inventory()
        detail = ind._detail_inventario_diferencia(first, today)
        self.assertAlmostEqual(detail['numerator'] - base['numerator'], 400.0, places=2,
                               msg="|+300| + |−100| en valor.")
        self.assertAlmostEqual(detail['denominator'] - base['denominator'], 200.0, places=2,
                               msg="Valor del inventario al cierre: 20 kg × 10.")
        self.assertEqual(detail['model'], 'stock.move')
        self.assertEqual(len(detail['ids']) - len(base['ids']), 2)

    def test_03_energia_pesos_por_tonelada(self):
        env = self.env
        partner = env['res.partner'].create({'name': 'CFE P-21'})
        sgi_test_payable(env, partner)
        self.Param.set_param('quimibond_sgi.energy_partner_id', partner.id)
        expense = env['account.account'].search([('account_type', '=', 'expense')], limit=1)
        for amount, refund in ((5000.0, False), (500.0, True)):
            move = env['account.move'].create({
                'move_type': 'in_refund' if refund else 'in_invoice',
                'partner_id': partner.id, 'invoice_date': self.period,
                'invoice_line_ids': [(0, 0, {
                    'name': 'Energía', 'quantity': 1, 'price_unit': amount,
                    'account_id': expense.id, 'tax_ids': [(6, 0, [])]})]})
            move.action_post()
        ind = self.Indicator.new({'calc_mode': 'consumo_energia'})
        self.assertIsNone(ind._calc_consumo_energia(self.period, self.period_end),
                          "Sin kg procesados no hay pesos por tonelada.")
        self._consume(3000.0)
        detail = ind._detail_consumo_energia(self.period, self.period_end)
        self.assertEqual((detail['numerator'], detail['denominator']), (4500.0, 3.0))
        self.assertEqual(detail['value'], 1500.0)
        self.assertEqual(detail['model'], 'account.move')
        self.assertEqual(len(detail['ids']), 2)

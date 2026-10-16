# -*- coding: utf-8 -*-
"""Catálogo de partes del cliente: sugerencia desde Odoo, confirmación y
conversión de unidades. Partes y códigos inventados."""
from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestCustomerPart(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Part = cls.env['qb.customer.part']
        cls.uom_m = cls.env.ref('uom.product_uom_meter')
        cls.uom_kg = cls.env.ref('uom.product_uom_kgm')
        cls.customer = cls.env['res.partner'].create(
            {'name': 'Cliente Release Test', 'is_company': True})
        cls.plant = cls.env['res.partner'].create(
            {'name': 'Planta 9000', 'parent_id': cls.customer.id, 'type': 'delivery'})
        cls.fabric_kg = cls.env['product.product'].create({
            'name': 'Tela 45', 'default_code': 'ZZT045Q22JNT160',
            'type': 'consu', 'uom_id': cls.uom_kg.id, 'sale_ok': True})
        cls.fabric_m = cls.env['product.product'].create({
            'name': 'Tela 53', 'default_code': 'ZZT053Q22JNT160',
            'type': 'consu', 'uom_id': cls.uom_m.id, 'sale_ok': True})
        cls.other = cls.env['product.product'].create({
            'name': 'Tela 60', 'default_code': 'ZZT060Q22JNT160',
            'type': 'consu', 'uom_id': cls.uom_m.id, 'sale_ok': True})

    def _order(self, product, ref=None, note_line=None):
        order = self.env['sale.order'].create({
            'partner_id': self.plant.id,
            'client_order_ref': ref,
            'order_line': [(0, 0, {
                'product_id': product.id, 'product_uom_qty': 10,
                'name': note_line or product.name})]})
        order.action_confirm()
        return order

    def _part(self, **vals):
        base = {'partner_id': self.customer.id, 'customer_part': 'LX00000001AA',
                'customer_uom': 'MT'}
        base.update(vals)
        return self.Part.create(base)

    def test_01_po_and_mention_give_high_confidence(self):
        for _i in range(3):
            self._order(self.fabric_kg, ref='PO.7000001')
        self._order(self.fabric_kg, note_line='Tela 45 parte LX00000001AA')
        self._order(self.other)
        part = self._part(customer_po='7000001',
                          customer_description='BACK SCRIM PES IH LAMINATED 63"')
        part.action_suggest_product()
        self.assertEqual(part.product_id, self.fabric_kg)
        self.assertEqual(part.state, 'sugerido')
        self.assertEqual(part.match_confidence, 'alta')
        self.assertIn('7000001', part.match_reasons)

    def test_02_embedded_reference(self):
        part = self._part(customer_part='4000001',
                          customer_description='64" ZZT053Q22JNT160 QUIMIBOND (160 CM)')
        part.action_suggest_product()
        self.assertEqual(part.product_id, self.fabric_m)

    def test_03_no_evidence_leaves_it_empty(self):
        part = self._part(customer_part='9999999', customer_description='MATERIAL')
        part.action_suggest_product()
        self.assertFalse(part.product_id)
        self.assertEqual(part.state, 'sin_producto')
        self.assertEqual(part.match_confidence, 'sin_match')

    def test_04_confirm_needs_product_and_never_overwritten(self):
        part = self._part()
        with self.assertRaises(UserError):
            part.action_confirm()
        part.product_id = self.fabric_m
        part.action_confirm()
        self.assertEqual(part.state, 'confirmado')
        self._order(self.other, ref='PO.X')
        part.action_suggest_product()
        self.assertEqual(part.product_id, self.fabric_m)
        # Cambiar el producto a mano quita la confirmación.
        part.product_id = self.other
        self.assertEqual(part.state, 'sugerido')

    def test_05_conversion(self):
        part = self._part(product_id=self.fabric_m.id, customer_uom='LY')
        self.assertAlmostEqual(part.to_product_qty(100), 91.44)
        # A kilos sin rendimiento: el código ZZT045… no sigue la norma y no hay peso: no se inventa.
        part_kg = self._part(customer_part='LX2', product_id=self.fabric_kg.id)
        self.assertIsNone(part_kg.to_product_qty(100))
        part_kg.write({'conversion': 'fija', 'factor': 0.25})
        self.assertAlmostEqual(part_kg.to_product_qty(100), 25.0)

    def test_05b_rendimiento_del_codigo(self):
        """1.4.0: sin el módulo anterior, gramaje, ancho y rendimiento salen del código y del costeo."""
        if 'qb.producto.kg' not in self.env:
            return
        tela = self.env['product.product'].create({
            'name': 'Tela 45 kg', 'default_code': 'WJ045Q22JNT160', 'type': 'consu',
            'uom_id': self.uom_kg.id, 'sale_ok': True})
        gramaje, ancho, rendimiento = self.env['qb.customer.part']._qb_ficha_by_product(tela)[tela.id]
        self.assertEqual((gramaje, ancho), (45.0, 1.6))
        self.assertAlmostEqual(rendimiento, 1000.0 / (45 * 1.6), 6)
        part = self._part(customer_part='LX3', product_id=tela.id, customer_uom='MT')
        self.assertAlmostEqual(part.to_product_qty(100), 100 / rendimiento, 4)

    def test_06_unique_per_customer_and_plant(self):
        self._part()
        self._part(ship_to_id=self.plant.id)  # otra planta: se vale
        with self.assertRaises(Exception), self.cr.savepoint():
            self._part()

    def test_07_invoice_note_links_part(self):
        income = self.env['account.account'].search([('account_type', '=', 'income')], limit=1)
        for _i in range(2):
            move = self.env['account.move'].create({
                'move_type': 'out_invoice', 'partner_id': self.plant.id,
                'invoice_line_ids': [
                    (0, 0, {'product_id': self.fabric_kg.id, 'quantity': 10, 'price_unit': 5,
                            'account_id': income.id, 'tax_ids': [(6, 0, [])]}),
                    (0, 0, {'display_type': 'line_note',
                            'name': 'ZZT045Q22JNT160 / NUMERO DE PARTE LX00000009AA'}),
                    (0, 0, {'product_id': self.other.id, 'quantity': 1, 'price_unit': 5,
                            'account_id': income.id, 'tax_ids': [(6, 0, [])]}),
                ]})
            move.action_post()
        part = self._part(customer_part='LX00000009AA', customer_description='SCRIM')
        part.action_suggest_product()
        self.assertEqual(part.product_id, self.fabric_kg)
        self.assertEqual(part.match_confidence, 'alta')
        self.assertIn('nota', part.match_reasons)

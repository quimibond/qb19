# -*- coding: utf-8 -*-
"""3.4.0: comparación de la hoja PRECIOS del forecast contra las listas de Odoo. Datos inventados."""
import base64
import io

import openpyxl

from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestPriceCheck(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        uom_m = cls.env.ref('uom.product_uom_meter')
        uom_kg = cls.env.ref('uom.product_uom_kgm')
        cls.pricelist = cls.env['product.pricelist'].create({'name': 'Lista ZZ precios'})
        cls.customer = cls.env['res.partner'].create({
            'name': 'ZZCLIENTE PRECIOS', 'is_company': True, 'customer_rank': 1,
            'property_product_pricelist': cls.pricelist.id})
        cls.env['qb.forecast.sheet'].create({'name': 'ZZCLIENTE PRECIOS', 'partner_id': cls.customer.id})
        cls.tela_m = cls.env['product.product'].create({
            'name': 'Tela precio m', 'default_code': 'ZK045Q22JNT160', 'type': 'consu', 'uom_id': uom_m.id})
        cls.tela_kg = cls.env['product.product'].create({
            'name': 'Tela precio kg', 'default_code': 'IZK045Q22JNT160', 'type': 'consu', 'uom_id': uom_kg.id})
        cls.otra_kg = cls.env['product.product'].create({
            'name': 'Otra kg', 'default_code': 'IZK060Q21JNT160', 'type': 'consu', 'uom_id': uom_kg.id})
        (cls.tela_m | cls.tela_kg | cls.otra_kg).action_qb_actualizar_articulo()
        Item = cls.env['product.pricelist.item']
        Item.create({'pricelist_id': cls.pricelist.id, 'applied_on': '0_product_variant',
                     'product_id': cls.tela_m.id, 'compute_price': 'fixed', 'fixed_price': 0.66})
        Item.create({'pricelist_id': cls.pricelist.id, 'applied_on': '0_product_variant',
                     'product_id': cls.tela_kg.id, 'compute_price': 'fixed', 'fixed_price': 9.00})
        Item.create({'pricelist_id': cls.pricelist.id, 'applied_on': '0_product_variant',
                     'product_id': cls.otra_kg.id, 'compute_price': 'fixed', 'fixed_price': 8.00})

    def _xlsx(self, rows):
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = 'PRECIOS'
        ws.append(['Columna 1', 'CLIENTE', 'ZZ', 'precio / m', 'Precio / kg', 'Precio con descuento', 'RENDIMIENTO'])
        for row in rows:
            ws.append(row)
        buffer = io.BytesIO()
        wb.save(buffer)
        return base64.b64encode(buffer.getvalue())

    def _compare(self, rows):
        wizard = self.env['qb.price.check.wizard'].create({'file': self._xlsx(rows), 'filename': 'forecast.xlsx'})
        wizard.action_compare()
        return {(c.code, c.basis): c for c in self.env['qb.price.check'].search([])}

    def test_01_estados(self):
        checks = self._compare([
            ['a', 'ZZCLIENTE PRECIOS', 'ZK045Q22JNT160', 0.66, None, None, 13.9],     # igual por metro
            ['b', 'ZZCLIENTE PRECIOS', 'IZK045Q22JNT160', None, 9.54, None, None],   # +6 % por kilo
            ['c', 'ZZCLIENTE PRECIOS', 'ZK060Q21JNT160', 0.70, None, None, 10.0],    # solo hay el de kilos
            ['d', 'ZZCLIENTE PRECIOS', 'ZK099Q22JNT160', 0.50, None, None, None],    # no existe
            ['e', 'CLIENTE QUE NO EXISTE', 'ZK045Q22JNT160', 0.66, None, None, None],
        ])
        self.assertEqual(checks[('ZK045Q22JNT160', 'm')].state, 'sin_cliente')  # el último gana la llave
        kg = checks[('IZK045Q22JNT160', 'kg')]
        self.assertEqual((kg.state, kg.product_id, kg.price_odoo), ('diferente', self.tela_kg, 9.0))
        self.assertAlmostEqual(kg.diff_pct, 6.0, places=2)
        derived = checks[('ZK060Q21JNT160', 'm')]
        self.assertEqual(derived.product_id, self.otra_kg, "Sin el código en metros se compara el de kilos.")
        self.assertIn('m/kg', derived.note)
        self.assertEqual(checks[('ZK099Q22JNT160', 'm')].state, 'sin_producto')
        states = set(self.env['qb.price.check'].search([]).mapped('state'))
        self.assertIn('igual', states)

    def test_02_sin_regla_y_reemplaza_la_anterior(self):
        self._compare([['a', 'ZZCLIENTE PRECIOS', 'ZK045Q22JNT160', 0.66, None, None, None]])
        self.env['product.pricelist.item'].search([('product_id', '=', self.tela_m.id)]).unlink()
        checks = self._compare([['a', 'ZZCLIENTE PRECIOS', 'ZK045Q22JNT160', 0.66, None, None, None]])
        self.assertEqual(len(self.env['qb.price.check'].search([])), 1, "Cada comparación reemplaza la anterior.")
        self.assertEqual(checks[('ZK045Q22JNT160', 'm')].state, 'sin_regla')

    def test_03_codigo_con_i_en_columna_de_metro_y_metro_sin_regla(self):
        self.env['product.pricelist.item'].search([('product_id', '=', self.tela_m.id)]).unlink()
        checks = self._compare([
            ['a', 'ZZCLIENTE PRECIOS', 'IZK045Q22JNT160', 9.00, None, None, None],   # «precio / m» pero es kilo
            ['b', 'ZZCLIENTE PRECIOS', 'ZK045Q22JNT160', 0.5, None, None, None],     # el metro sin regla
        ])
        kilo = checks[('IZK045Q22JNT160', 'kg')]
        self.assertEqual((kilo.state, kilo.product_id), ('igual', self.tela_kg))
        metro = checks[('ZK045Q22JNT160', 'm')]
        self.assertEqual(metro.product_id, self.tela_kg, "Sin regla en metros se compara el kilo ÷ rendimiento.")
        self.assertIn('m/kg', metro.note)

    def test_04_cliente_por_prefijo_de_hoja(self):
        self.env['qb.forecast.sheet'].create({'name': 'ZZFXI CD JUAREZ', 'partner_id': self.customer.id})
        checks = self._compare([['a', 'ZZFXI', 'ZK045Q22JNT160', 0.66, None, None, None]])
        self.assertEqual(checks[('ZK045Q22JNT160', 'm')].partner_id, self.customer)

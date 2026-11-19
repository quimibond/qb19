# -*- coding: utf-8 -*-
from odoo.tests import tagged
from odoo.tests.common import TransactionCase


@tagged('post_install', '-at_install')
class TestCierreInventario(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.company = cls.env.company
        Account = cls.env['account.account']
        cls.cta_refacciones = Account.create({'code': '115991', 'name': 'Prueba refacciones', 'account_type': 'asset_current'})
        cls.cta_mp = Account.create({'code': '115992', 'name': 'Prueba materia prima', 'account_type': 'asset_current'})
        Categ = cls.env['product.category']
        cls.refacciones = Categ.create({
            'name': 'Prueba Refacciones',
            'property_valuation': 'periodic',
            'property_stock_valuation_account_id': cls.cta_refacciones.id,
        })
        cls.refacciones_sub = Categ.create({'name': 'Prueba General', 'parent_id': cls.refacciones.id})
        cls.mp = Categ.create({
            'name': 'Prueba Materia Prima',
            'property_valuation': 'real_time',
            'property_stock_valuation_account_id': cls.cta_mp.id,
        })
        # La subcategoría comparte la cuenta de materia prima: esa cuenta no se puede excluir.
        cls.maquila = Categ.create({
            'name': 'Prueba Maquila',
            'property_valuation': 'periodic',
            'property_stock_valuation_account_id': cls.cta_mp.id,
        })
        Product = cls.env['product.product']
        cls.broca = Product.create({'name': 'Broca prueba', 'is_storable': True, 'categ_id': cls.refacciones_sub.id, 'standard_price': 10})
        cls.hilo = Product.create({'name': 'Hilo prueba', 'is_storable': True, 'categ_id': cls.mp.id, 'standard_price': 20})
        stock = cls.env['stock.warehouse'].search([('company_id', '=', cls.company.id)], limit=1).lot_stock_id
        Quant = cls.env['stock.quant']
        Quant._update_available_quantity(cls.broca, stock, 5)
        Quant._update_available_quantity(cls.hilo, stock, 7)

    def _productos_del_cierre(self):
        return self.env['product.product'].search(self.company._get_valuation_product_domain())

    def test_sin_marcar_entra_todo(self):
        productos = self._productos_del_cierre()
        self.assertIn(self.broca, productos)
        self.assertIn(self.hilo, productos)
        self.assertFalse(self.company._qb_cuentas_excluidas())

    def test_marcada_excluye_subcategorias(self):
        self.refacciones.qb_excluir_cierre = True
        productos = self._productos_del_cierre()
        self.assertNotIn(self.broca, productos, 'La subcategoría de una categoría marcada no se valúa')
        self.assertIn(self.hilo, productos)
        valor = self.company.stock_value()
        self.assertNotIn(self.cta_refacciones, valor)
        self.assertAlmostEqual(valor.get(self.cta_mp, 0.0), 140.0)

    def test_cuentas_excluidas(self):
        self.refacciones.qb_excluir_cierre = True
        self.maquila.qb_excluir_cierre = True
        cuentas = self.company._qb_cuentas_excluidas()
        self.assertIn(self.cta_refacciones, cuentas)
        self.assertNotIn(self.cta_mp, cuentas, 'Una cuenta que usa una categoría valuada sigue en el cierre')

    def test_read_group_con_contexto(self):
        Move = self.env['stock.move']
        domain = [('product_id', 'in', (self.broca | self.hilo).ids)]
        normal = {p for p, in Move._read_group(domain, ['product_id'])}
        filtrado = {p for p, in Move.with_context(qb_cierre_excluir_categ_ids=self.refacciones_sub.ids)._read_group(domain, ['product_id'])}
        self.assertIn(self.broca, normal)
        self.assertNotIn(self.broca, filtrado)
        self.assertIn(self.hilo, filtrado)

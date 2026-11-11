# -*- coding: utf-8 -*-
from datetime import datetime

from odoo.exceptions import UserError
from odoo.tests import tagged

from .common import CosteoCase


@tagged('post_install', '-at_install')
class TestCapaAbsorcion(CosteoCase):
    """1.11.0: conversión que le falta al inventario a fin de mes."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        Cat = cls.env['product.category']
        raiz = Cat.create({'name': 'Producto Terminado'})
        cls.cat = Cat.create({'name': 'Prueba capa', 'parent_id': raiz.id,
                              'property_cost_method': 'average'})
        cls.cat_importado = Cat.create({
            'name': 'Importado', 'parent_id': raiz.id,
            'property_cost_method': 'average'})
        cls.stock = cls.env['stock.warehouse'].search(
            [('company_id', '=', cls.company.id)], limit=1).lot_stock_id
        en_sep = datetime(2026, 9, 15, 12)

        def tela(nombre, costo_odoo, categ=None):
            p = cls.env['product.product'].create({
                'name': nombre, 'type': 'consu', 'is_storable': True,
                'categ_id': (categ or cls.cat).id,
                'standard_price': costo_odoo})
            cls.env['stock.quant'].with_context(inventory_mode=True).create({
                'product_id': p.id, 'location_id': cls.stock.id,
                'inventory_quantity': 100.0}).action_apply_inventory()
            moves = cls.env['stock.move'].search([('product_id', '=', p.id)])
            moves.write({'date': en_sep})
            moves.move_line_ids.write({'date': en_sep})
            return p

        cls.a = tela('Tela A', 10.0)      # le falta conversión
        cls.b = tela('Tela B', 10.0)      # la diferencia pasa del tope
        cls.c = tela('Tela C', 20.0)      # Odoo ya vale más que el módulo
        cls.d = tela('Tela D', 10.0)      # sin costo del módulo
        cls.importada = tela('Tela importada', 10.0, cls.cat_importado)
        Costo = cls.env['qb.costo.unitario']
        for p, mp, fab in ((cls.a, 8.0, 5.0), (cls.b, 12.0, 1.0),
                           (cls.c, 8.0, 5.0), (cls.importada, 8.0, 5.0)):
            Costo.create({'periodo_id': cls.periodo.id, 'product_id': p.id,
                          'company_id': cls.company.id, 'mp_unit': mp,
                          'fabricacion_unit': fab,
                          'costo_produccion': mp + fab, 'calidad': 'alta'})
        cls.periodo.costos_calculado_el = datetime(2026, 10, 1, 8)

    def _fila(self, product):
        return self.periodo.capa_ids.filtered(lambda f: f.product_id == product)

    def test_capa_por_producto(self):
        self.periodo.action_calcular_capa()
        a, b, c, d = (self._fila(p) for p in (self.a, self.b, self.c, self.d))
        # A: 100 × 13 − 1000 = 300, dentro del tope de 100 × 5.
        self.assertAlmostEqual(a.qty, 100.0, 4)
        self.assertAlmostEqual(a.valor_odoo, 1000.0, 2)
        self.assertAlmostEqual(a.valor_completo, 1300.0, 2)
        self.assertAlmostEqual(a.faltante, 300.0, 2)
        self.assertAlmostEqual(a.capitalizable, 300.0, 2)
        self.assertEqual(a.estado, 'capitaliza')
        # B: faltante 300 pero solo 100 × 1 es conversión; el resto es precio
        # de la materia prima y no se capitaliza.
        self.assertAlmostEqual(b.faltante, 300.0, 2)
        self.assertAlmostEqual(b.tope_conversion, 100.0, 2)
        self.assertAlmostEqual(b.capitalizable, 100.0, 2)
        # C: Odoo vale 2000 contra 1300 del módulo: no se capitaliza.
        self.assertAlmostEqual(c.faltante, -700.0, 2)
        self.assertAlmostEqual(c.capitalizable, 0.0, 2)
        self.assertEqual(c.estado, 'cubierto')
        # D: sin costo del módulo, se lista pero no se mide.
        self.assertEqual(d.estado, 'sin_costo')
        self.assertAlmostEqual(d.capitalizable, 0.0, 2)
        # Lo importado no entra a la capa.
        self.assertFalse(self._fila(self.importada))
        # Totales del período.
        self.assertAlmostEqual(self.periodo.capa_total, 400.0, 2)
        self.assertAlmostEqual(self.periodo.capa_valor_odoo, 5000.0, 2)
        self.assertAlmostEqual(self.periodo.capa_valor_completo, 3900.0, 2)
        self.assertAlmostEqual(self.periodo.capa_sin_costo_valor, 1000.0, 2)

    def test_capa_al_cierre_del_periodo(self):
        """Lo que entra después del último día no cuenta, y recalcular
        reemplaza las filas."""
        self.env['stock.quant'].with_context(inventory_mode=True).create({
            'product_id': self.a.id, 'location_id': self.stock.id,
            'inventory_quantity': 150.0}).action_apply_inventory()
        tarde = self.env['stock.move'].search(
            [('product_id', '=', self.a.id)], order='id desc', limit=1)
        tarde.write({'date': datetime(2026, 10, 2, 12)})
        tarde.move_line_ids.write({'date': datetime(2026, 10, 2, 12)})
        self.periodo.action_calcular_capa()
        self.periodo.action_calcular_capa()
        filas = self.periodo.capa_ids.filtered(lambda f: f.product_id == self.a)
        self.assertEqual(len(filas), 1)
        self.assertAlmostEqual(filas.qty, 100.0, 4)

    def test_sin_costos_no_calcula(self):
        periodo = self.env['qb.periodo'].create({'period': datetime(2026, 8, 1).date()})
        with self.assertRaises(UserError):
            periodo.action_calcular_capa()

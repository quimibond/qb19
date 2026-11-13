# -*- coding: utf-8 -*-
from datetime import datetime

from odoo.exceptions import UserError
from odoo.tests import tagged

from .common import CosteoCase


@tagged('post_install', '-at_install')
class TestCapaAbsorcion(CosteoCase):
    """1.11.0: conversión que le falta al inventario a fin de mes.
    1.11.1: en neto por categoría y con merma (costo vendible)."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        Cat = cls.env['product.category']
        raiz = Cat.create({'name': 'Producto Terminado'})

        def cat(nombre):
            return Cat.create({'name': nombre, 'parent_id': raiz.id,
                               'property_cost_method': 'average'})

        cls.cat_neto = cat('Prueba neto')
        cls.cat_tope = cat('Prueba tope')
        cls.cat_merma = cat('Prueba merma')
        cls.cat_importado = cat('Importado')
        cls.stock = cls.env['stock.warehouse'].search(
            [('company_id', '=', cls.company.id)], limit=1).lot_stock_id
        en_sep = datetime(2026, 9, 15, 12)

        def tela(nombre, costo_odoo, categ):
            p = cls.env['product.product'].create({
                'name': nombre, 'type': 'consu', 'is_storable': True,
                'categ_id': categ.id, 'standard_price': costo_odoo})
            cls.env['stock.quant'].with_context(inventory_mode=True).create({
                'product_id': p.id, 'location_id': cls.stock.id,
                'inventory_quantity': 100.0}).action_apply_inventory()
            moves = cls.env['stock.move'].search([('product_id', '=', p.id)])
            moves.write({'date': en_sep})
            moves.move_line_ids.write({'date': en_sep})
            return p

        # Categoría neto: A le faltan 300, C trae 100 de más → neto 200.
        cls.a = tela('Tela A', 10.0, cls.cat_neto)
        cls.c = tela('Tela C', 14.0, cls.cat_neto)
        cls.d = tela('Tela D', 10.0, cls.cat_neto)      # sin costo
        cls.f = tela('Tela F', 10.0, cls.cat_neto)      # sin fabricación
        # Categoría tope: faltan 300 pero solo 100 son conversión.
        cls.b = tela('Tela B', 10.0, cls.cat_tope)
        # Categoría merma: producción 9.9, rendimiento 0.9 → vendible 11.
        cls.g = tela('Tela G', 10.0, cls.cat_merma)
        cls.importada = tela('Tela importada', 10.0, cls.cat_importado)
        Costo = cls.env['qb.costo.unitario']
        for p, mp, fab, rend in ((cls.a, 8.0, 5.0, 1.0), (cls.c, 8.0, 5.0, 1.0),
                                 (cls.f, 8.0, 0.0, 1.0), (cls.b, 12.0, 1.0, 1.0),
                                 (cls.g, 9.0, 0.9, 0.9),
                                 (cls.importada, 8.0, 5.0, 1.0)):
            Costo.create({'periodo_id': cls.periodo.id, 'product_id': p.id,
                          'company_id': cls.company.id, 'mp_unit': mp,
                          'fabricacion_unit': fab, 'rendimiento': rend,
                          'costo_produccion': mp + fab,
                          'costo_vendible': (mp + fab) / rend,
                          'calidad': 'alta'})
        cls.periodo.costos_calculado_el = datetime(2026, 10, 1, 8)

    def _fila(self, product):
        return self.periodo.capa_ids.filtered(lambda f: f.product_id == product)

    def _cat(self, categ):
        return self.periodo.capa_categoria_ids.filtered(
            lambda r: r.categ_id == categ)

    def test_capa_por_producto(self):
        self.periodo.action_calcular_capa()
        a, c, d, f = (self._fila(p) for p in (self.a, self.c, self.d, self.f))
        self.assertAlmostEqual(a.qty, 100.0, 4)
        self.assertAlmostEqual(a.valor_odoo, 1000.0, 2)
        self.assertAlmostEqual(a.valor_completo, 1300.0, 2)
        self.assertAlmostEqual(a.faltante, 300.0, 2)
        self.assertAlmostEqual(a.tope_conversion, 500.0, 2)
        self.assertEqual(a.estado, 'falta')
        self.assertAlmostEqual(c.faltante, -100.0, 2)
        self.assertEqual(c.estado, 'cubierto')
        self.assertEqual(d.estado, 'sin_costo')
        self.assertEqual(f.estado, 'sin_fabricacion')
        self.assertAlmostEqual(f.faltante, 0.0, 2)
        g = self._fila(self.g)
        self.assertAlmostEqual(g.costo_vendible, 11.0, 4)
        self.assertAlmostEqual(g.valor_completo, 1100.0, 2)
        self.assertAlmostEqual(g.tope_conversion, 100.0, 2)
        self.assertFalse(self._fila(self.importada))

    def test_capa_neta_por_categoria(self):
        self.periodo.action_calcular_capa()
        neto = self._cat(self.cat_neto)
        # Lo que le sobra a C se resta de lo que le falta a A.
        self.assertAlmostEqual(neto.faltante_neto, 200.0, 2)
        self.assertAlmostEqual(neto.tope_conversion, 1000.0, 2)
        self.assertAlmostEqual(neto.capitalizable, 200.0, 2)
        self.assertEqual(neto.n_medidos, 2)
        self.assertEqual(neto.n_sin_medir, 2)
        self.assertAlmostEqual(neto.valor_sin_medir, 2000.0, 2)
        # El tope de conversión manda sobre la diferencia de precio de MP.
        self.assertAlmostEqual(self._cat(self.cat_tope).capitalizable, 100.0, 2)
        # Con merma: vendible 11 × 100 − 1000 = 100, tope 100.
        self.assertAlmostEqual(self._cat(self.cat_merma).capitalizable, 100.0, 2)
        self.assertAlmostEqual(self.periodo.capa_total, 400.0, 2)
        self.assertAlmostEqual(self.periodo.capa_valor_odoo, 6400.0, 2)
        self.assertAlmostEqual(self.periodo.capa_valor_completo, 5000.0, 2)
        self.assertAlmostEqual(self.periodo.capa_sin_costo_valor, 2000.0, 2)

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
        self.assertEqual(len(self._cat(self.cat_neto)), 1)

    def test_sin_costos_no_calcula(self):
        periodo = self.env['qb.periodo'].create(
            {'period': datetime(2026, 8, 1).date()})
        with self.assertRaises(UserError):
            periodo.action_calcular_capa()

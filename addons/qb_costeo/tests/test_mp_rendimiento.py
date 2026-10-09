# -*- coding: utf-8 -*-
from datetime import date, datetime

from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestMp(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        kg = cls.env.ref('uom.product_uom_kgm')
        m = cls.env.ref('uom.product_uom_meter')
        cls.kg, cls.m = kg, m

        def prod(code, uom, price=0.0):
            return cls.env['product.product'].create({
                'name': code, 'default_code': code, 'type': 'consu',
                'is_storable': True, 'uom_id': uom.id, 'standard_price': price})
        cls.hilo = prod('HILO', kg, 40.0)
        cls.negro = prod('NEGRO016', kg, 55.0)
        cls.crudo = prod('WJ080Q21HNT165', kg)
        cls.tenido = prod('WJ080Q21INT165', kg)
        cls.terminado = prod('WJ080Q21JNT165', m)
        Bom = cls.env['mrp.bom']
        Bom.create({'product_tmpl_id': cls.crudo.product_tmpl_id.id, 'product_qty': 1,
                    'product_uom_id': kg.id,
                    'bom_line_ids': [(0, 0, {'product_id': cls.hilo.id, 'product_qty': 1.02,
                                             'product_uom_id': kg.id})]})
        Bom.create({'product_tmpl_id': cls.tenido.product_tmpl_id.id, 'product_qty': 1,
                    'product_uom_id': kg.id,
                    'bom_line_ids': [(0, 0, {'product_id': cls.crudo.id, 'product_qty': 1,
                                             'product_uom_id': kg.id}),
                                     (0, 0, {'product_id': cls.negro.id, 'product_qty': 0.08,
                                             'product_uom_id': kg.id})]})
        Bom.create({'product_tmpl_id': cls.terminado.product_tmpl_id.id, 'product_qty': 100,
                    'product_uom_id': m.id,
                    'bom_line_ids': [(0, 0, {'product_id': cls.tenido.id, 'product_qty': 13.76,
                                             'product_uom_id': kg.id})]})
        cls.MP = cls.env['qb.producto.mp']

    def _compra(self, product, precio, fecha, uom=None):
        partner = self.env['res.partner'].create({'name': 'Prov'})
        po = self.env['purchase.order'].create({
            'partner_id': partner.id, 'date_order': fecha,
            'order_line': [(0, 0, {
                'product_id': product.id, 'product_qty': 100, 'price_unit': precio,
                'product_uom_id': (uom or product.uom_id).id, 'name': product.name,
                'date_planned': fecha})]})
        po.button_confirm()
        po.write({'date_order': fecha})
        return po

    def test_explosion_a_costo_promedio(self):
        mp, hojas = self.MP.explotar(self.terminado)
        # 13.76 kg teñido / 100 m; teñido = 1.02 kg hilo × 40 + 0.08 × 55 = 45.2
        self.assertAlmostEqual(mp, 13.76 / 100 * 45.2, 4)
        self.assertEqual(set(hojas), {self.hilo.id, self.negro.id})
        self.assertAlmostEqual(hojas[self.hilo.id][0], 0.1376 * 1.02, 6)

    def test_ultima_compra_manda_y_corte(self):
        self._compra(self.hilo, 50.0, datetime(2026, 8, 10))
        self._compra(self.hilo, 60.0, datetime(2026, 9, 20))
        hoy = self.MP.mp_para(self.crudo)
        self.assertAlmostEqual(hoy, 1.02 * 60.0, 4)
        agosto = self.MP.mp_para(self.crudo, cutoff=date(2026, 9, 1))
        self.assertAlmostEqual(agosto, 1.02 * 50.0, 4)
        # Antes de cualquier compra: la primera conocida, no el costo promedio.
        julio = self.MP.mp_para(self.crudo, cutoff=date(2026, 7, 1))
        self.assertAlmostEqual(julio, 1.02 * 50.0, 4)

    def test_compra_en_otra_unidad(self):
        gramo = self.env.ref('uom.product_uom_gram')
        self._compra(self.negro, 0.06, datetime(2026, 9, 1), uom=gramo)  # 60/kg
        precios = self.MP.precios_compra([self.negro.id])
        self.assertAlmostEqual(precios[self.negro.id], 60.0, 4)

    def test_recalcular_marca_dudosas(self):
        self.hilo.standard_price = 0
        self.env['qb.producto.validacion'].revisar(self.crudo)
        filas = self.MP.recalcular(self.crudo | self.terminado)
        f = filas.filtered(lambda r: r.product_id == self.terminado)
        self.assertEqual(f.calidad, 'dudosa')
        self.assertGreaterEqual(f.hojas_dudosas, 1)
        self.assertIn('NEGRO016', f.detalle)
        # Producto sin receta: sin fila.
        self.assertFalse(self.MP.recalcular(self.hilo))


@tagged('post_install', '-at_install')
class TestRendimiento(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        Loc = cls.env['stock.location']
        wh = cls.env['stock.warehouse'].search([], limit=1)
        cls.origen = Loc.create({'name': 'Produccion', 'usage': 'production'})
        cls.pq = Loc.create({'name': 'PT PQ', 'usage': 'internal',
                             'location_id': wh.lot_stock_id.id})
        cls.fe = Loc.create({'name': 'PT FE', 'usage': 'internal',
                             'location_id': wh.lot_stock_id.id})
        P = cls.env['qb.parametro']
        for key, val in (('calidad_locs_vendible', str(cls.pq.id)),
                         ('calidad_locs_merma', str(cls.fe.id)),
                         ('calidad_locs_origen', str(cls.origen.id))):
            P._rec(key).value_text = val
        P.set_float('rendimiento_min_unidades', 1000)
        m = cls.env.ref('uom.product_uom_meter')
        cls.grande = cls.env['product.product'].create({
            'name': 'GRANDE', 'default_code': 'WJ053Q22JNT160', 'type': 'consu',
            'is_storable': True, 'uom_id': m.id})
        cls.chico = cls.env['product.product'].create({
            'name': 'CHICO', 'default_code': 'WD038Q46JNG163', 'type': 'consu',
            'is_storable': True, 'uom_id': m.id})
        cls.R = cls.env['qb.producto.rendimiento']

    def _mov(self, product, qty, dest):
        move = self.env['stock.move'].create({
            'product_id': product.id, 'product_uom_qty': qty,
            'product_uom': product.uom_id.id, 'location_id': self.origen.id,
            'location_dest_id': dest.id})
        move._action_confirm()
        move.quantity = qty
        move.picked = True
        move._action_done()
        move.move_line_ids.write({'date': datetime(2026, 9, 15)})
        return move

    def test_producto_planta_y_manual(self):
        self._mov(self.grande, 9000, self.pq)
        self._mov(self.grande, 1000, self.fe)     # 90 %, arriba del umbral
        self._mov(self.chico, 50, self.pq)
        self._mov(self.chico, 50, self.fe)        # 50 %, pero chico → planta
        filas = self.R.recalcular()
        g = filas.filtered(lambda r: r.product_id == self.grande)
        c = filas.filtered(lambda r: r.product_id == self.chico)
        self.assertEqual(g.fuente, 'producto')
        self.assertAlmostEqual(g.rendimiento, 0.9, 4)
        self.assertEqual(c.fuente, 'planta')
        self.assertAlmostEqual(c.rendimiento, 9050 / 10100, 4)
        # Manual con vigencia manda; vencido, vuelve al calculado.
        with self.assertRaises(UserError):
            g.write({'rendimiento_manual': 0.88})
        g.write({'rendimiento_manual': 0.88, 'manual_motivo': 'arranque',
                 'manual_vigente_hasta': date(2099, 1, 1)})
        self.R.recalcular()
        self.assertEqual(g.fuente, 'manual')
        self.assertAlmostEqual(g.rendimiento, 0.88, 4)
        g.manual_vigente_hasta = date(2020, 1, 1)
        self.R.recalcular()
        self.assertEqual(g.fuente, 'producto')
        self.assertAlmostEqual(g.rendimiento, 0.9, 4)
        # `para` de un producto sin fila: planta.
        otro = self.env['product.product'].create({'name': 'OTRO', 'type': 'consu'})
        rend, fuente = self.R.para(otro)
        self.assertEqual(fuente, 'planta')
        self.assertAlmostEqual(rend, 9050 / 10100, 4)

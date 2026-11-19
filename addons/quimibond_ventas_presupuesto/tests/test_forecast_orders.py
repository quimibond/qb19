# -*- coding: utf-8 -*-
"""19.0.3.29.0 — pedido y pronóstico ligados (Jose, 19-nov): «Crear pedido» desde el pronóstico, mover la
fecha o la cantidad del pedido mueve la semana, y un pedido a mano se liga al confirmarse. Semanas de 2073."""
from datetime import date, datetime, time, timedelta

from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestForecastOrders(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.uom_m = cls.env.ref('uom.product_uom_meter')
        cls.product = cls.env['product.product'].create({
            'name': 'Tela liga', 'default_code': 'WLIGA160', 'type': 'consu', 'uom_id': cls.uom_m.id,
            'sale_ok': True, 'list_price': 10.0})
        cls.client = cls.env['res.partner'].create({'name': 'Cliente liga', 'is_company': True})
        cls.week = date(2073, 3, 6)  # lunes
        cls.Forecast = cls.env['qb.forecast.line']
        cls.env['qb.order.rule'].create({'partner_id': cls.client.id, 'po_mode': 'abierta',
                                         'open_po': 'OC-LIGA', 'so_weeks': 4})

    def _fl(self, week, qty):
        return self.Forecast.create({'partner_id': self.client.id, 'product_id': self.product.id,
                                     'week': week, 'qty_origen': qty})

    def _weeks(self):
        lines = self.Forecast.search([('partner_id', '=', self.client.id)])
        return {ln.week: ln.qty_venta for ln in lines}

    def test_01_crear_pedido_desde_el_pronostico(self):
        fl = self._fl(self.week, 1000.0)
        action = fl.action_qb_create_order()
        order = self.env['sale.order'].browse(action['res_id'])
        self.assertEqual((order.state, order.client_order_ref, order.qb_forecast_week),
                         ('draft', 'OC-LIGA', self.week))
        self.assertEqual(order.order_line.product_uom_qty, 1000.0)
        self.assertEqual(order.order_line.qb_forecast_line_id, fl)
        self.assertEqual((fl.qty_ligada, fl.qty_por_pedir, fl.order_count), (1000.0, 0.0, 1))
        with self.assertRaises(UserError):
            fl.action_qb_create_order()

    def test_02_mover_la_fecha_mueve_la_semana(self):
        fl = self._fl(self.week, 1000.0)
        order = self.env['sale.order'].browse(fl.action_qb_create_order()['res_id'])
        order.action_confirm()
        order.commitment_date = datetime.combine(self.week + timedelta(weeks=2, days=1), time(12, 0))
        new_week = self.week + timedelta(weeks=2)
        self.assertEqual(self._weeks(), {new_week: 1000.0}, "La semana vieja sin pedido se quita")
        self.assertEqual(order.order_line.qb_forecast_line_id.week, new_week)
        self.assertEqual(order.qb_forecast_week, new_week)
        change = self.env['qb.forecast.change'].search(
            [('partner_id', '=', self.client.id), ('week', '=', new_week)], limit=1)
        self.assertIn("cambió de fecha", change.note)

    def test_03_cambiar_la_cantidad_cambia_la_semana(self):
        fl = self._fl(self.week, 1000.0)
        order = self.env['sale.order'].browse(fl.action_qb_create_order()['res_id'])
        order.action_confirm()
        order.order_line.product_uom_qty = 1200.0
        self.assertEqual(fl.qty_venta, 1200.0)
        order.order_line.product_uom_qty = 900.0
        self.assertEqual(fl.qty_venta, 900.0, "El pedido cubría la semana: la semana sigue al pedido")

    def test_04_pedido_a_mano_se_liga_y_mueve_la_semana(self):
        self._fl(self.week, 500.0)
        order = self.env['sale.order'].create({
            'partner_id': self.client.id, 'client_order_ref': 'OC-LIGA',
            'commitment_date': datetime.combine(self.week + timedelta(weeks=1, days=2), time(12, 0)),
            'order_line': [(0, 0, {'product_id': self.product.id, 'product_uom_qty': 500.0})]})
        order.action_confirm()
        new_week = self.week + timedelta(weeks=1)
        self.assertEqual(self._weeks(), {new_week: 500.0})
        self.assertEqual(order.order_line.qb_forecast_line_id.week, new_week)

    def test_05_pedido_a_mano_en_su_semana_solo_se_liga(self):
        fl = self._fl(self.week, 800.0)
        order = self.env['sale.order'].create({
            'partner_id': self.client.id,
            'commitment_date': datetime.combine(self.week + timedelta(days=3), time(12, 0)),
            'order_line': [(0, 0, {'product_id': self.product.id, 'product_uom_qty': 300.0})]})
        order.action_confirm()
        self.assertEqual(order.order_line.qb_forecast_line_id, fl)
        self.assertEqual((fl.qty_venta, fl.qty_por_pedir), (800.0, 500.0))

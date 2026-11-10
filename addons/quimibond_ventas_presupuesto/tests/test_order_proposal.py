# -*- coding: utf-8 -*-
"""19.0.3.15.0 — pedidos desde el pronóstico (E3): un pedido (cotización) por OC y semana, con la
OC según la regla del cliente. Semanas de 2072 para aislar."""
from datetime import date, timedelta

from odoo import fields
from odoo.exceptions import UserError, ValidationError
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestOrderProposal(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        Param = cls.env['ir.config_parameter'].sudo()
        Param.set_param('quimibond_sgi.price_min_plausible', '0')
        cls.uom_m = cls.env.ref('uom.product_uom_meter')
        cls.product = cls.env['product.product'].create({
            'name': 'Tela pedido', 'default_code': 'WPED160', 'type': 'consu', 'uom_id': cls.uom_m.id,
            'sale_ok': True})
        cls.pricelist = cls.env['product.pricelist'].create({
            'name': 'Lista pedido', 'currency_id': cls.env.company.currency_id.id,
            'item_ids': [(0, 0, {'applied_on': '0_product_variant', 'product_id': cls.product.id,
                                 'compute_price': 'fixed', 'fixed_price': 12.0})]})
        cls.client = cls.env['res.partner'].create({
            'name': 'Cliente pedido', 'is_company': True, 'property_product_pricelist': cls.pricelist.id})
        cls.week = date(2072, 3, 7)  # lunes
        cls.Forecast = cls.env['qb.forecast.line']
        cls.Rule = cls.env['qb.order.rule']

    def _fl(self, week, qty, po=False):
        return self.Forecast.create({'partner_id': self.client.id, 'product_id': self.product.id,
                                     'week': week, 'qty_origen': qty, 'customer_po': po})

    def _propose(self):
        proposal = self.env['qb.order.proposal'].create({'partner_ids': [(6, 0, self.client.ids)],
                                                         'date_from': self.week})
        proposal.action_build()
        return proposal

    def test_01_oc_en_el_release(self):
        self.Rule.create({'partner_id': self.client.id, 'po_mode': 'release', 'so_weeks': 2})
        self._fl(self.week, 100.0, '39712/PV17113')
        self._fl(self.week + timedelta(weeks=1), 50.0, 'PO')  # sin OC de verdad
        self._fl(self.week + timedelta(weeks=3), 70.0, '40000')  # fuera de las semanas a pedido
        proposal = self._propose()
        nuevo = proposal.line_ids.filtered(lambda l: l.action == 'nuevo')
        self.assertEqual((nuevo.customer_po, nuevo.week, nuevo.qty_forecast), ('39712', self.week, 100.0))
        self.assertAlmostEqual(nuevo.price_unit, 12.0)
        sin_oc = proposal.line_ids.filtered(lambda l: l.action == 'no_entra')
        self.assertEqual(sin_oc.reason, "La semana no trae OC")
        self.assertEqual(len(proposal.line_ids), 2, "La semana 4 no se propone")
        action = proposal.action_apply()
        order = self.env['sale.order'].browse(action['domain'][0][2])
        self.assertEqual(order.state, 'draft', "Se crea como cotización")
        self.assertEqual(order.client_order_ref, '39712')
        self.assertEqual(order.commitment_date.date(), self.week)
        self.assertEqual(order.order_line.product_uom_qty, 100.0)
        self.assertTrue(order.qb_from_forecast)
        # El cliente sube la semana: se actualiza la misma cotización.
        self.Forecast.search([('partner_id', '=', self.client.id), ('week', '=', self.week)]).qty_origen = 130.0
        again = self._propose()
        aumenta = again.line_ids.filtered(lambda l: l.action == 'aumenta')
        self.assertEqual((aumenta.qty_existing, aumenta.qty_forecast), (100.0, 130.0))
        again.action_apply()
        self.assertEqual(order.order_line.product_uom_qty, 130.0)
        self.assertEqual(len(order.order_line), 1)

    def test_02_oc_abierta(self):
        self.Rule.create({'partner_id': self.client.id, 'po_mode': 'abierta', 'open_po': '1030782',
                          'so_weeks': 2})
        self._fl(self.week, 10.0)
        self._fl(self.week + timedelta(weeks=1), 20.0)
        proposal = self._propose()
        self.assertEqual(set(proposal.line_ids.mapped('customer_po')), {'1030782'})
        action = proposal.action_apply()
        orders = self.env['sale.order'].browse(action['domain'][0][2])
        self.assertEqual(len(orders), 2, "Un pedido por OC y semana")
        with self.assertRaises(ValidationError):
            self.Rule.create({'partner_id': self.client.id, 'plant': 'Otra', 'po_mode': 'abierta'})

    def _manager(self):
        return self.env['res.users'].create({
            'name': 'Admin ventas pedido', 'login': 'admin_ventas_pedido',
            'group_ids': [(6, 0, [self.env.ref('sales_team.group_sale_manager').id,
                                  self.env.ref('quimibond_sgi.group_sgi_user').id])]})

    def test_03_release_sin_oc_y_baja_confirmada(self):
        self.Rule.create({'partner_id': self.client.id, 'po_mode': 'release_sin_oc', 'so_weeks': 1})
        self._fl(self.week, 40.0)
        proposal = self._propose()
        line = proposal.line_ids
        self.assertTrue(line.customer_po.startswith('REL-'))
        action = proposal.action_apply()
        order = self.env['sale.order'].browse(action['domain'][0][2])
        order.action_confirm()
        self.Forecast.search([('partner_id', '=', self.client.id), ('week', '=', self.week)]).qty_origen = 25.0
        manager = self._manager()
        again = self.env['qb.order.proposal'].with_user(manager).create({
            'partner_ids': [(6, 0, self.client.ids)], 'date_from': self.week})
        again.action_build()
        self.assertEqual((again.line_ids.action, again.line_ids.adjust_confirmed), ('baja', True))
        self.assertFalse(again.line_ids.apply, "Una baja en un pedido confirmado viene desmarcada")

    def test_04_sin_regla_o_manual_no_propone(self):
        self.Rule.create({'partner_id': self.client.id, 'po_mode': 'manual', 'so_weeks': 2})
        self._fl(self.week, 10.0, '123')
        proposal = self._propose()
        self.assertEqual(proposal.line_ids.action, 'no_entra')

    def test_05_ajustar_pedido_confirmado_y_bloqueado(self):
        """3.15.2 (opción B): el administrador de ventas ajusta el pedido confirmado y bloqueado como a
        mano: se desbloquea, se cambia, se vuelve a bloquear y queda nota. Otro usuario: por autorizar."""
        self.Rule.create({'partner_id': self.client.id, 'po_mode': 'release', 'so_weeks': 1})
        fl = self._fl(self.week, 100.0, '555')
        action = self._propose().action_apply()
        order = self.env['sale.order'].browse(action['domain'][0][2])
        order.action_confirm()
        has_lock = 'locked' in order._fields
        if has_lock:
            order.action_lock()
        fl.qty_origen = 160.0
        manager = self._manager()
        proposal = self.env['qb.order.proposal'].with_user(manager).create({
            'partner_ids': [(6, 0, self.client.ids)], 'date_from': self.week})
        proposal.action_build()
        line = proposal.line_ids
        self.assertEqual((line.action, line.apply, line.adjust_confirmed), ('aumenta', True, True))
        proposal.action_apply()
        self.assertEqual(order.order_line.product_uom_qty, 160.0, "Un solo pedido por OC y semana")
        if has_lock:
            self.assertTrue(order.locked, "Queda bloqueado otra vez")
        self.assertTrue(order.message_ids.filtered(lambda m: '160' in (m.body or '')))
        salesman = self.env['res.users'].create({
            'name': 'Vendedor pedido', 'login': 'vendedor_pedido',
            'group_ids': [(6, 0, [self.env.ref('sales_team.group_sale_salesman_all_leads').id,
                                  self.env.ref('quimibond_sgi.group_sgi_user').id])]})
        fl.qty_origen = 200.0
        # Sin derechos sobre el asistente, con sudo: lo que cuenta es que no es administrador de ventas.
        other = self.env['qb.order.proposal'].with_user(salesman).sudo().create({
            'partner_ids': [(6, 0, self.client.ids)], 'date_from': self.week})
        other.action_build()
        self.assertEqual((other.line_ids.action, other.line_ids.apply), ('por_autorizar', False))

    def test_06_entrega_otra_semana_release_nuevo_y_doble_clic(self):
        """3.16.1: el pedido se reconoce por la semana del pronóstico aunque su fecha compromiso sea la
        de entrega (otra semana); un release sin OC se reconoce aunque cambie su número; aplicar dos
        veces no duplica."""
        self.Rule.create({'partner_id': self.client.id, 'po_mode': 'release_sin_oc', 'so_weeks': 1})
        fl = self._fl(self.week, 40.0)
        fl.delivery_date = self.week + timedelta(days=9)  # se entrega la semana siguiente
        proposal = self._propose()
        action = proposal.action_apply()
        order = self.env['sale.order'].browse(action['domain'][0][2])
        self.assertEqual(order.commitment_date.date(), self.week + timedelta(days=9))
        with self.assertRaises(UserError):
            proposal.action_apply()  # clic doble: ya se aplicó
        self.assertEqual(order.order_line.product_uom_qty, 40.0)
        # El release de la semana siguiente trae otro número: la referencia del pedido no cambia.
        order.client_order_ref = 'REL-2072-OTRO'
        again = self._propose()
        self.assertEqual(again.line_ids.action, 'sin_cambio', "Ya está pedido: no se propone otra vez")
        # Dos asistentes armados antes de aplicar: el segundo no vuelve a pedir lo mismo.
        fl.qty_origen = 60.0
        first, second = self._propose(), self._propose()
        first.action_apply()
        second.action_apply()
        self.assertEqual(len(order.order_line), 1)
        self.assertEqual(order.order_line.product_uom_qty, 60.0)

    def test_07_plantas_no_se_mezclan(self):
        """3.16.1: con OC abierta para dos plantas, el pedido de una no cuenta para la otra."""
        self.Rule.create({'partner_id': self.client.id, 'po_mode': 'abierta', 'open_po': '777', 'so_weeks': 1})
        plant_a = self.env['res.partner'].create({'name': 'Planta A', 'type': 'delivery',
                                                  'parent_id': self.client.id})
        plant_b = self.env['res.partner'].create({'name': 'Planta B', 'type': 'delivery',
                                                  'parent_id': self.client.id})
        for plant, qty in ((plant_a, 100.0), (plant_b, 50.0)):
            self.Forecast.create({'partner_id': self.client.id, 'product_id': self.product.id,
                                  'ship_to_id': plant.id, 'week': self.week, 'qty_origen': qty})
        action = self._propose().action_apply()
        orders = self.env['sale.order'].browse(action['domain'][0][2])
        self.assertEqual(len(orders), 2, "Un pedido por planta")
        self.assertEqual(orders.filtered(lambda o: o.partner_shipping_id == plant_a).order_line.product_uom_qty, 100.0)
        self.assertEqual(orders.filtered(lambda o: o.partner_shipping_id == plant_b).order_line.product_uom_qty, 50.0)
        again = self._propose()
        self.assertEqual(set(again.line_ids.mapped('action')), {'sin_cambio'})

    def test_08_misma_tela_en_otro_codigo(self):
        """3.16.1: si la semana ya tiene pedido de la tela en metros y el pronóstico la trae en kilos,
        no se pide otra vez: queda «no entra» con el código del pedido."""
        uom_kg = self.env.ref('uom.product_uom_kgm')
        kilos = self.env['product.product'].create({
            'name': 'Tela pedido kg', 'default_code': 'IWPED160', 'type': 'consu', 'uom_id': uom_kg.id})
        kilos.qb_articulo_id = self.product
        self.Rule.create({'partner_id': self.client.id, 'po_mode': 'release', 'so_weeks': 1})
        fl = self._fl(self.week, 100.0, '901')
        self._propose().action_apply()
        fl.unlink()
        self.Forecast.create({'partner_id': self.client.id, 'product_id': kilos.id, 'week': self.week,
                              'qty_origen': 20.0, 'customer_po': '901'})
        line = self._propose().line_ids
        self.assertEqual(line.action, 'no_entra')
        self.assertIn('WPED160', line.reason)

    def test_09_baja_a_cero_y_cotizacion_vacia(self):
        """3.16.6: si el release deja de pedir el producto (la semana se quita del pronóstico), la propuesta
        lo baja a cero en el pedido del pronóstico y la cotización que se queda vacía se cancela."""
        self.Rule.create({'partner_id': self.client.id, 'po_mode': 'release', 'so_weeks': 1})
        fl = self._fl(self.week, 100.0, '6701')
        fl.delivery_date = self.week + timedelta(days=9)
        proposal = self._propose()
        self.assertEqual(proposal.line_ids.delivery_date, self.week + timedelta(days=9))
        order = self.env['sale.order'].browse(proposal.action_apply()['domain'][0][2])
        self.assertEqual(order.commitment_date.date(), self.week + timedelta(days=9))
        fl.unlink()  # el release ya no trae el producto
        dropped = self._propose()
        self.assertEqual((dropped.line_ids.action, dropped.line_ids.qty_forecast, dropped.line_ids.apply),
                         ('baja', 0.0, True))
        dropped.action_apply()
        self.assertEqual(order.state, 'cancel', "La cotización que se quedó sin productos se cancela")

    def test_10_semana_atrasada_se_pide_para_hoy(self):
        """3.16.6: una semana que ya pasó no deja la fecha de entrega en el pasado."""
        today = fields.Date.context_today(self.env.user)
        past = today - timedelta(days=today.weekday() + 14)
        self.Rule.create({'partner_id': self.client.id, 'po_mode': 'release', 'so_weeks': 4})
        self._fl(past, 30.0, '777')
        proposal = self.env['qb.order.proposal'].create({'partner_ids': [(6, 0, self.client.ids)],
                                                         'date_from': past})
        proposal.action_build()
        self.assertEqual(proposal.line_ids.delivery_date, today)
        order = self.env['sale.order'].browse(proposal.action_apply()['domain'][0][2])
        self.assertEqual(order.commitment_date.date(), today)

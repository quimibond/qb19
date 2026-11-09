# -*- coding: utf-8 -*-
"""19.0.3.15.0 — pedidos desde el pronóstico (E3): un pedido (cotización) por OC y semana, con la
OC según la regla del cliente. Semanas de 2072 para aislar."""
from datetime import date, timedelta

from odoo.exceptions import ValidationError
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

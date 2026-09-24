# -*- coding: utf-8 -*-
"""P-7: no surtir lotes sin liberar; liga del traslado interno con su entrega."""
from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestRelease(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env = cls.env
        cls.Param = env['ir.config_parameter'].sudo()
        cls.wh = env['stock.warehouse'].search([('company_id', '=', env.company.id)], limit=1)
        cls.stock = cls.wh.lot_stock_id
        cls.waiting = env['stock.location'].create({
            'name': 'Liberación prueba', 'location_id': cls.stock.id, 'usage': 'internal'})
        cls.production = env['stock.location'].create({
            'name': 'Producción prueba', 'location_id': cls.stock.id, 'usage': 'internal'})
        cls.req_type = env['stock.picking.type'].create({
            'name': 'Requisición prueba', 'code': 'internal', 'sequence_code': 'REQT',
            'warehouse_id': cls.wh.id, 'default_location_src_id': cls.stock.id,
            'default_location_dest_id': cls.production.id})
        cls.Param.set_param('quimibond_sgi.release_block_enabled', 'True')
        cls.Param.set_param('quimibond_sgi.release_block_picking_type_ids', str(cls.req_type.id))
        cls.Param.set_param('quimibond_sgi.unreleased_location_ids', str(cls.waiting.id))
        cls.product = env['product.product'].create({
            'name': 'Tela liberación', 'type': 'consu', 'is_storable': True, 'tracking': 'lot'})
        cls.Quant = env['stock.quant']

    def _lot(self, name, location, qty=10.0):
        lot = self.env['stock.lot'].create({'name': name, 'product_id': self.product.id})
        self.Quant._update_available_quantity(self.product, location, qty, lot_id=lot)
        return lot

    def _requisition(self, lot, qty=5.0):
        picking = self.env['stock.picking'].create({
            'picking_type_id': self.req_type.id,
            'location_id': self.stock.id, 'location_dest_id': self.production.id,
            'move_ids': [(0, 0, {
                'product_id': self.product.id, 'product_uom_qty': qty,
                'product_uom': self.product.uom_id.id,
                'location_id': self.stock.id, 'location_dest_id': self.production.id})],
        })
        picking.action_confirm()
        picking.action_assign()
        picking.move_line_ids.write({'lot_id': lot.id, 'quantity': qty, 'picked': True})
        return picking

    def test_01_lot_in_waiting_location_blocks(self):
        lot = self._lot('SIN-LIBERAR', self.waiting)
        picking = self._requisition(lot)
        self.assertEqual(picking.move_line_ids.location_id, self.waiting)
        with self.assertRaises(UserError):
            picking.button_validate()
        self.assertNotEqual(picking.state, 'done')

    def test_02_released_lot_passes(self):
        lot = self._lot('LIBERADO', self.stock)
        picking = self._requisition(lot)
        picking.button_validate()
        self.assertEqual(picking.state, 'done')

    def test_03_failed_quality_check_blocks_until_pass(self):
        lot = self._lot('FALLIDO', self.stock)
        point = self.env['quality.point'].create({
            'title': 'Control liberación prueba',
            'test_type_id': self.env.ref('quality_control.test_type_passfail').id,
            'picking_type_ids': [(4, self.wh.in_type_id.id)]})
        check = self.env['quality.check'].create({
            'point_id': point.id, 'product_id': self.product.id, 'lot_ids': [(6, 0, lot.ids)]})
        check.quality_state = 'fail'
        picking = self._requisition(lot)
        with self.assertRaises(UserError):
            picking.button_validate()
        later = self.env['quality.check'].create({
            'point_id': point.id, 'product_id': self.product.id, 'lot_ids': [(6, 0, lot.ids)]})
        later.quality_state = 'pass'
        picking.button_validate()
        self.assertEqual(picking.state, 'done', "El último control manda.")

    def test_04_other_picking_types_and_switch_off(self):
        lot = self._lot('OTRO-TIPO', self.waiting)
        self.Param.set_param('quimibond_sgi.release_block_picking_type_ids', '0')
        picking = self._requisition(lot)
        picking.button_validate()
        self.assertEqual(picking.state, 'done', "Solo bloquea los tipos configurados.")
        self.Param.set_param('quimibond_sgi.release_block_picking_type_ids', str(self.req_type.id))
        self.Param.set_param('quimibond_sgi.release_block_enabled', 'False')
        picking = self._requisition(self._lot('APAGADO', self.waiting))
        picking.button_validate()
        self.assertEqual(picking.state, 'done', "Con el parámetro apagado no bloquea.")

    def test_05_internal_transfer_links_delivery(self):
        customer = self.env['res.partner'].create({'name': 'Cliente embarque'})
        order = self.env['sale.order'].create({'partner_id': customer.id})
        customers = self.env.ref('stock.stock_location_customers')
        delivery = self.env['stock.picking'].create({
            'picking_type_id': self.wh.out_type_id.id, 'sale_id': order.id,
            'partner_id': customer.id,
            'location_id': self.stock.id, 'location_dest_id': customers.id,
            'move_ids': [(0, 0, {
                'product_id': self.product.id, 'product_uom_qty': 5.0,
                'product_uom': self.product.uom_id.id,
                'location_id': self.stock.id, 'location_dest_id': customers.id})]})
        transfer = self.env['stock.picking'].create({
            'picking_type_id': self.req_type.id, 'sale_id': order.id,
            'location_id': self.stock.id, 'location_dest_id': self.production.id,
            'move_ids': [(0, 0, {
                'product_id': self.product.id, 'product_uom_qty': 5.0,
                'product_uom': self.product.uom_id.id,
                'location_id': self.stock.id, 'location_dest_id': self.production.id})]})
        self.assertEqual(transfer.sgi_delivery_picking_id, delivery[:1])
        self.assertFalse(delivery[:1].sgi_delivery_picking_id, "Solo los traslados internos.")

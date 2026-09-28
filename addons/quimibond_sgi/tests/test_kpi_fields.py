# -*- coding: utf-8 -*-
"""Campos nuevos para los 28 indicadores por fórmula (55.0.0)."""
from datetime import date, datetime

from odoo import fields
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestKpiFields(TransactionCase):

    def test_01_bitacora_de_bloqueo(self):
        company = self.env['res.company'].create({'name': 'ZK Bloqueo'})
        Log = self.env['sgi.lock.date.log']
        before = Log.search_count([('company_id', '=', company.id)])
        company.write({'fiscalyear_lock_date': date(2046, 2, 28)})
        logs = Log.search([('company_id', '=', company.id)])
        self.assertEqual(len(logs), before + 1)
        log = logs.sorted('id')[-1]
        self.assertEqual((log.lock_field, log.date_before, log.date_after), ('fiscalyear_lock_date', False, date(2046, 2, 28)))
        self.assertGreaterEqual(log.business_day, 0)
        company.write({'fiscalyear_lock_date': date(2046, 2, 28)})
        self.assertEqual(Log.search_count([('company_id', '=', company.id)]), before + 1, "Sin cambio no hay fila.")

    def test_02_foto_del_inventario(self):
        Value = self.env['sgi.inventory.value']
        snapshot = Value.sgi_snapshot(date(2046, 3, 31))
        if not snapshot:
            self.skipTest("Sin valuación de inventario en esta base.")
        self.assertEqual(snapshot.date, date(2046, 3, 31))
        again = Value.sgi_snapshot(date(2046, 3, 31))
        self.assertEqual(snapshot, again, "Idempotente por compañía y mes.")

    def test_03_obligacion_patronal(self):
        Obl = self.env['sgi.employer.obligation']
        late = Obl.create({'name': 'IMSS marzo', 'period_date': date(2046, 3, 1),
                           'due_date': date(2046, 4, 17), 'filed_date': date(2046, 4, 20)})
        self.assertEqual((late.state, late.on_time), ('tarde', False))
        pending = Obl.create({'name': 'IMSS abril', 'period_date': date(2046, 4, 1), 'due_date': date(2046, 5, 17)})
        self.assertEqual(pending.state, 'pendiente')
        old = Obl.create({'name': 'IMSS 2020', 'period_date': date(2020, 1, 1), 'due_date': date(2020, 2, 17)})
        self.assertEqual(old.state, 'vencida')
        old.action_mark_filed()
        self.assertEqual(old.state, 'tarde')

    def test_04_usuario_desactivado_y_baja(self):
        user = self.env['res.users'].create({'name': 'ZK Usuario', 'login': 'zk_usuario'})
        self.assertFalse(user.sgi_deactivated_date)
        user.write({'active': False})
        self.assertEqual(user.sgi_deactivated_date, fields.Date.context_today(user))
        user.write({'active': True})
        self.assertFalse(user.sgi_deactivated_date)
        employee = self.env['hr.employee'].create({'name': 'ZK Empleado'})
        version = employee.version_id
        self.assertFalse(version.sgi_departure_registered_at)
        version.write({'departure_date': date(2046, 3, 15)})
        self.assertTrue(version.sgi_departure_registered_at)
        reason = self.env['hr.departure.reason'].search([], limit=1)
        if reason:
            employee.sudo().write({'departure_reason_id': reason.id})
            self.assertEqual(version.sgi_departure_reason_id, reason)
        version.write({'trial_date_end': date(2046, 4, 1)})
        self.assertEqual(employee.sgi_trial_date_end, date(2046, 4, 1))

    def test_05_primer_pedido_y_liberacion(self):
        product = self.env['product.product'].create({'name': 'ZK Producto', 'type': 'consu', 'list_price': 10})
        self.assertFalse(product.product_tmpl_id.sgi_first_sale_date)
        partner = self.env['res.partner'].create({'name': 'ZK Cliente'})
        order = self.env['sale.order'].create({
            'partner_id': partner.id, 'date_order': datetime(2046, 3, 10, 12, 0),
            'order_line': [(0, 0, {'product_id': product.id, 'product_uom_qty': 1})]})
        order.action_confirm()
        self.assertEqual(product.product_tmpl_id.sgi_first_sale_date, date(2046, 3, 10))
        picking_type = self.env['stock.picking.type'].search([('code', '=', 'internal')], limit=1)
        if picking_type:
            picking = self.env['stock.picking'].create({
                'picking_type_id': picking_type.id,
                'location_id': picking_type.default_location_src_id.id or self.env.ref('stock.stock_location_stock').id,
                'location_dest_id': picking_type.default_location_dest_id.id or self.env.ref('stock.stock_location_stock').id,
            })
            self.assertEqual(picking.sgi_release_hours, 0.0)
            self.assertFalse(picking.sgi_quality_approved_at)

    def test_06_acuerdo_cumplido_y_pago(self):
        review = self.env['sgi.management.review'].create({
            'date': date(2046, 3, 1), 'period_from': date(2045, 9, 1), 'period_to': date(2046, 2, 28)})
        agreement = self.env['sgi.management.review.agreement'].create({
            'review_id': review.id, 'name': 'ZK acuerdo', 'deadline': date(2046, 4, 1)})
        self.assertFalse(agreement.done_date)
        agreement.write({'done_date': date(2046, 3, 20)})
        self.assertEqual(agreement.done_date, date(2046, 3, 20), "A mano cuando no hay acción.")
        self.assertIn('sgi_payment_date', self.env['account.move']._fields)

# -*- coding: utf-8 -*-
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestCustomerReturnNc(TransactionCase):

    def test_01_customer_return_creates_nc(self):
        warehouse = self.env['stock.warehouse'].search(
            [('company_id', '=', self.env.company.id)], limit=1)
        customer_loc = self.env.ref('stock.stock_location_customers')
        partner = self.env['res.partner'].create({
            'name': 'Cliente Devolución', 'is_company': True})
        product = self.env['product.product'].create({
            'name': 'Tela devuelta', 'is_storable': False})
        # Entrega a cliente validada.
        out = self.env['stock.picking'].create({
            'partner_id': partner.id,
            'picking_type_id': warehouse.out_type_id.id,
            'location_id': warehouse.lot_stock_id.id,
            'location_dest_id': customer_loc.id,
            'move_ids': [(0, 0, {
                'product_id': product.id,
                'product_uom_qty': 5,
                'location_id': warehouse.lot_stock_id.id,
                'location_dest_id': customer_loc.id,
            })],
        })
        out.action_confirm()
        out.move_ids.write({'quantity': 5, 'picked': True})
        out._action_done()
        self.assertEqual(out.state, 'done')
        self.assertFalse(out.sgi_return_alert_id,
                         "Una entrega normal no genera NC.")
        # Devolución del cliente (recepción cuyos movimientos retornan la entrega).
        ret = self.env['stock.picking'].create({
            'partner_id': partner.id,
            'picking_type_id': warehouse.in_type_id.id,
            'location_id': customer_loc.id,
            'location_dest_id': warehouse.lot_stock_id.id,
            'move_ids': [(0, 0, {
                'product_id': product.id,
                'product_uom_qty': 2,
                'location_id': customer_loc.id,
                'location_dest_id': warehouse.lot_stock_id.id,
                'origin_returned_move_id': out.move_ids[0].id,
            })],
        })
        ret.action_confirm()
        ret.move_ids.write({'quantity': 2, 'picked': True})
        ret._action_done()
        self.assertEqual(ret.state, 'done')
        alert = ret.sgi_return_alert_id
        self.assertTrue(alert, "La devolución de cliente debe levantar una NC.")
        self.assertEqual(alert.sgi_origin_type, 'reclamacion')
        self.assertEqual(alert.partner_id, partner)
        self.assertEqual(alert.sgi_source_id.code, 'devolucion_cliente')
        # Idempotente: revalidar no duplica.
        ret._sgi_create_return_alert()
        self.assertEqual(ret.sgi_return_alert_id, alert)


@tagged('post_install', '-at_install')
class TestOperationalSignals(TransactionCase):

    def test_01_repetitive_failure_schedules_activity(self):
        equipment = self.env['maintenance.equipment'].create({
            'name': 'Telar señal'})
        for i in range(3):
            self.env['maintenance.request'].create({
                'name': 'Falla %d' % i,
                'equipment_id': equipment.id,
                'maintenance_type': 'corrective',
            })
        Cron = self.env['sgi.cron']
        Cron.cron_operational_signals()
        Cron.cron_operational_signals()
        acts = self.env['mail.activity'].search([
            ('res_model', '=', 'maintenance.equipment'),
            ('res_id', '=', equipment.id),
            ('summary', 'like', 'Falla repetitiva%'),
        ])
        self.assertEqual(len(acts), 1,
                         "3 correctivas en 90 días → una sola actividad (sin duplicar).")

    def test_02_two_failures_no_activity(self):
        equipment = self.env['maintenance.equipment'].create({
            'name': 'Rama señal'})
        for i in range(2):
            self.env['maintenance.request'].create({
                'name': 'Falla %d' % i,
                'equipment_id': equipment.id,
                'maintenance_type': 'corrective',
            })
        self.env['sgi.cron'].cron_operational_signals()
        acts = self.env['mail.activity'].search([
            ('res_model', '=', 'maintenance.equipment'),
            ('res_id', '=', equipment.id),
        ])
        self.assertFalse(acts, "Con menos de 3 correctivas no se escala.")


@tagged('post_install', '-at_install')
class TestQualityMenuRelocation(TransactionCase):

    def test_01_quality_menus_hang_from_quality_app(self):
        """E-008 (57.5.0): los menús operativos cuelgan con padre fijo del
        raíz de la app Calidad, antes de su Configuración (secuencias 23 y
        24, las de producción); ya no hay función que los recuelgue."""
        quality_root = self.env.ref('quality_control.menu_quality_root')
        self.assertFalse(hasattr(self.env['ir.ui.menu'], '_sgi_attach_quality_menus'))
        for xmlid, sequence in (('quimibond_sgi.menu_sgi_automotive', 23),
                                ('quimibond_sgi.menu_sgi_dashboards', 24)):
            menu = self.env.ref(xmlid)
            self.assertEqual(menu.parent_id, quality_root,
                             "%s debe colgar del raíz de la app Calidad." % xmlid)
            self.assertEqual(menu.sequence, sequence)
            config = self.env['ir.ui.menu'].search([
                ('parent_id', '=', quality_root.id),
                ('name', 'ilike', 'onfig')], limit=1)
            if config:
                self.assertLess(
                    menu.sequence, config.sequence,
                    "%s debe aparecer antes de Configuración." % xmlid)

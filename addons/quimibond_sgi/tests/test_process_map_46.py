# -*- coding: utf-8 -*-
from odoo.tests import TransactionCase, tagged
from odoo.exceptions import UserError


@tagged('post_install', '-at_install')
class TestProcessMap46(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # Procesos y flujos propios: los flow_* del mapa viejo ya no son del
        # módulo (57.4.0, A-002).
        Process = cls.env['sgi.process']
        cls.p_a = Process.create({'code': 'XPM-A', 'name': 'Proceso XPM A'})
        cls.p_b = Process.create({'code': 'XPM-B', 'name': 'Proceso XPM B'})

    def _flow(self, name, model=None):
        return self.env['sgi.process.flow'].create({
            'name': name,
            'from_process_id': self.p_a.id,
            'to_process_id': self.p_b.id,
            'odoo_model_id': self.env['ir.model']._get(model).id if model else False,
        })

    def test_01_flow_with_model_opens_records(self):
        flow = self._flow('Factura emitida', 'account.move')
        self.assertEqual(flow.odoo_model_id.model, 'account.move')
        action = flow.action_view_records()
        self.assertEqual(action['res_model'], 'account.move')

    def test_02_flow_without_model_blocks(self):
        flow = self._flow('Pronóstico (sin modelo)')
        self.assertFalse(flow.odoo_model_id)
        with self.assertRaises(UserError):
            flow.action_view_records()

    def test_03_maintenance_raise_nc(self):
        equipment = self.env['maintenance.equipment'].create({'name': 'Circular 9'})
        request = self.env['maintenance.request'].create({
            'name': 'Falla de banda',
            'maintenance_type': 'corrective',
            'equipment_id': equipment.id,
            'description': 'Banda rota',
        })
        request.action_sgi_raise_nc()
        self.assertTrue(request.sgi_alert_id, "Debe crearse la NC ligada.")
        self.assertEqual(request.sgi_alert_id.team_id,
                         self.env.ref('quimibond_sgi.sgi_quality_team_internal'))
        # Idempotente: no crea una segunda.
        first = request.sgi_alert_id
        request.action_sgi_raise_nc()
        self.assertEqual(request.sgi_alert_id, first)

    def test_04_coa_published_attaches_pdf(self):
        prod = self.env['product.product'].create({
            'name': 'Tela CoA test', 'type': 'consu', 'tracking': 'lot'})
        lot = self.env['stock.lot'].create({'name': 'LOTE-CoA-T', 'product_id': prod.id})
        point = self.env['quality.point'].create({
            'title': 'Gramaje CoA test',
            'test_type_id': self.env.ref('quality_control.test_type_measure').id,
            'picking_type_ids': [(4, self.env.ref('stock.picking_type_in').id)],
            'sgi_in_coa': True,
        })
        self.env['quality.check'].create({
            'point_id': point.id, 'product_id': prod.id,
            'lot_ids': [(4, lot.id)], 'measure': 10.0, 'quality_state': 'pass'})
        out_type = self.env['stock.picking.type'].search([('code', '=', 'outgoing')], limit=1)
        src = out_type.default_location_src_id or self.env['stock.location'].search(
            [('usage', '=', 'internal')], limit=1)
        dest = self.env['stock.location'].search([('usage', '=', 'customer')], limit=1)
        picking = self.env['stock.picking'].create({
            'picking_type_id': out_type.id,
            'location_id': src.id, 'location_dest_id': dest.id})
        self.env['stock.move.line'].create({
            'picking_id': picking.id, 'product_id': prod.id, 'lot_id': lot.id,
            'quantity': 5.0, 'product_uom_id': prod.uom_id.id,
            'location_id': src.id, 'location_dest_id': dest.id})
        lot.action_sgi_publish_coa()
        att = self.env['ir.attachment'].search([
            ('res_model', '=', 'stock.picking'), ('res_id', '=', picking.id),
            ('name', '=', 'CoA-%s.pdf' % lot.name)])
        self.assertTrue(att, "El CoA debe adjuntarse a la entrega para el portal.")

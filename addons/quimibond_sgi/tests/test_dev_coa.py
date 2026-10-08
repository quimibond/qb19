# -*- coding: utf-8 -*-
"""57.130.0 (C1, Jose 3.4): reporte de conformidad impreso desde la tabla («En certificado»)."""
from odoo import fields
from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged

from ..models.sgi_dev_shipment import PARAM_OUT_PICKING_TYPE


@tagged('post_install', '-at_install')
class TestDevCoa(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        m = cls.env.ref('uom.product_uom_meter')
        cls.partner = cls.env['res.partner'].create({'name': 'CLIENTE COA PRUEBA', 'is_company': True})
        cls.tela = cls.env['product.product'].create({'name': 'tela coa', 'default_code': 'WJ150Q28JNT172X',
                                                      'type': 'consu', 'is_storable': True, 'uom_id': m.id})
        wh = cls.env['stock.warehouse'].search([('company_id', '=', cls.env.company.id)], limit=1)
        cls.location = cls.env['stock.location'].create({'name': '31 DESARROLLOS coa', 'usage': 'internal',
                                                         'location_id': wh.lot_stock_id.id})
        cls.out_type = cls.env['stock.picking.type'].create({
            'name': 'Baja de Muestras coa', 'code': 'outgoing', 'sequence_code': 'BMC',
            'warehouse_id': wh.id, 'company_id': cls.env.company.id,
            'default_location_src_id': cls.location.id,
            'default_location_dest_id': cls.env.ref('stock.stock_location_customers').id})
        cls.env['ir.config_parameter'].sudo().set_param(PARAM_OUT_PICKING_TYPE, str(cls.out_type.id))
        Car = cls.env['ficha.tecnica.caracteristica']
        cls.car_masa = Car.search([('code', '=', 'masa')], limit=1)
        cls.car_ancho = Car.search([('code', '=', 'ancho')], limit=1)
        cls.car_text = Car.create({'code': 'tacto_coa', 'name': 'Tacto', 'name_en': 'Hand feel', 'kind': 'text'})
        cls.lot = cls.env['stock.lot'].create({'name': 'LOTE-COA-1', 'product_id': cls.tela.id,
                                               'company_id': cls.env.company.id})

    def _dev(self):
        dev = self.env['project.project'].create({'name': 'x', 'sgi_is_ft': True, 'partner_id': self.partner.id,
                                                  'sgi_dev_product_name': 'Jersey coa', 'sgi_dev_product_id': self.tela.id})
        Line = self.env['sgi.dev.characteristic']
        # masa: nominal 150 ± 5 % del cliente, control interno ± 2 %; corrida 156 (fuera del interno, dentro del cliente)
        Line.create({'project_id': dev.id, 'caracteristica_id': self.car_masa.id, 'spec_nominal': 150.0,
                     'spec_tol_minus': 5.0, 'spec_tol_plus': 5.0, 'spec_tol_pct': True,
                     'ctrl_tol_minus': 2.0, 'ctrl_tol_plus': 2.0, 'in_coa': True,
                     'run_1': 156.0, 'run_2': 156.0, 'run_3': 156.0})
        # ancho: no va al certificado
        Line.create({'project_id': dev.id, 'caracteristica_id': self.car_ancho.id, 'spec_nominal': 1.7,
                     'spec_tol_minus': 0.02, 'spec_tol_plus': 0.02, 'in_coa': False, 'run_1': 1.71})
        # tacto: cualitativa, va al certificado
        Line.create({'project_id': dev.id, 'caracteristica_id': self.car_text.id, 'spec_text': 'Suave',
                     'in_coa': True, 'run_text': 'Suave'})
        muestra = self.env.ref('quimibond_sgi.sgi_dev_stage_muestra')
        dev.with_context(sgi_dev_migration=True).write({'stage_id': muestra.id})
        return dev

    def _verdict(self, dev):
        req = self.env['sgi.dev.lab.request'].create({'project_id': dev.id, 'kind': 'corrida'})
        req.write({'state': 'medida', 'date_measured': fields.Datetime.now(),
                   'verdict_by_id': self.env.uid, 'date_verdict': fields.Datetime.now()})

    def test_01_renglones_en_certificado_contra_el_cliente(self):
        dev = self._dev()
        coa = self.env['sgi.dev.coa'].create({'project_id': dev.id, 'lot_id': self.lot.id})
        self.assertEqual(coa.product_id, self.tela)
        self.assertEqual(len(coa.line_ids), 2, "Solo los renglones «En certificado»")
        masa = coa.line_ids.filtered(lambda l: l.characteristic_id.caracteristica_id == self.car_masa)
        tacto = coa.line_ids - masa
        self.assertAlmostEqual(masa.value, 156.0, msg="Precargado con el promedio de la corrida")
        self.assertEqual(masa.result, 'cumple', "156 está dentro del ±5 % del cliente aunque salga del interno")
        self.assertEqual(masa.characteristic_id.run_result, 'desviacion', "La tabla sí marca la desviación interna")
        self.assertEqual(tacto.text, 'Suave')
        self.assertFalse(tacto.result, "Las cualitativas no se dictaminan solas")
        masa.write({'value': 160.0})
        self.assertEqual(masa.result, 'no_conforme')
        self.assertEqual(coa.nonconforming_count, 1)
        coa.action_load_lines()
        self.assertEqual(len(coa.line_ids), 2, "Cargar otra vez no duplica")
        html = self.env['ir.actions.report']._render_qweb_html('quimibond_sgi.report_dev_coa_document', coa.ids)[0]
        html = html.decode()
        self.assertIn('150 g/m² ± 5%', html.replace('&#177;', '±').replace('&#178;', '²'))
        self.assertIn('Hand feel', html)
        self.assertNotIn('Control interno', html)
        self.assertNotIn(masa.characteristic_id.ctrl_label, html, "El control interno nunca sale al cliente")

    def test_02_emitir_exige_valores_y_adjunta_al_envio_y_a_la_baja(self):
        dev = self._dev()
        self._verdict(dev)
        ship = self.env['sgi.dev.shipment'].create({
            'project_id': dev.id, 'medium': 'recoge_cliente',
            'roll_ids': [(0, 0, {'lot_id': self.lot.id, 'length_m': 35.0}),
                         (0, 0, {'lot_id': self.lot.id, 'length_m': 25.0})]})
        action = ship.action_sgi_dev_coa()
        coa = self.env['sgi.dev.coa'].browse(action['res_id'])
        self.assertEqual(coa.shipment_id, ship)
        self.assertEqual(coa.lot_id, self.lot)
        self.assertAlmostEqual(coa.quantity_m, 60.0)
        self.assertEqual(coa.roll_count, 2)
        self.assertEqual(ship.action_sgi_dev_coa()['res_id'], coa.id, "Un certificado por lote, no se repite")
        tacto = coa.line_ids.filtered(lambda l: l.kind != 'num')
        tacto.write({'text': False})
        with self.assertRaises(UserError, msg="Sin valor obtenido no se emite"):
            coa.action_emit()
        tacto.write({'text': 'Suave'})
        picking_action = ship.action_create_picking()
        picking = self.env['stock.picking'].browse(picking_action['res_id'])
        coa.action_emit()
        self.assertEqual(coa.state, 'emitido')
        self.assertEqual(coa.issued_by_id, self.env.user)
        self.assertTrue(coa.attachment_id)
        self.assertEqual(coa.attachment_id.res_model, 'sgi.dev.coa')
        self.assertTrue(ship.attachment_ids, "El PDF viaja con el envío")
        self.assertEqual(ship.attachment_ids.res_model, 'sgi.dev.shipment')
        self.assertTrue(picking.sgi_coa_attachment_ids, "Y queda como CoA de la baja")
        self.assertEqual(picking.sgi_coa_uid, self.env.user)
        with self.assertRaises(UserError, msg="Emitido no se borra"):
            coa.unlink()
        with self.assertRaises(UserError, msg="Ni se vuelve a emitir"):
            coa.action_emit()

    def test_03_sin_renglones_en_certificado_no_hay_coa(self):
        dev = self._dev()
        dev.sgi_dev_line_ids.write({'in_coa': False})
        with self.assertRaises(UserError):
            dev.action_sgi_dev_new_coa()
        coa = self.env['sgi.dev.coa'].create({'project_id': dev.id})
        with self.assertRaises(UserError, msg="Sin renglones no se emite"):
            coa.action_emit()

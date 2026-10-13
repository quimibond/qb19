# -*- coding: utf-8 -*-
"""57.125.0 (C1, bloque F): la corrida de muestra se pide desde el proyecto.

57.154.0: la corrida es la cadena crudo → teñido → acabado, una orden por artículo, con cantidades de
atrás hacia adelante, solo listas del propio artículo y con ruta."""
from datetime import timedelta

from odoo import fields
from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged

from ..models.sgi_dev_sample import (
    PARAM_EXPECTED_YIELD, PARAM_LOCATION, PARAM_MIN_BATH_KG, PARAM_PICKING_TYPE, PARAM_PICKING_TYPE_ACABADO,
    PARAM_PICKING_TYPE_TENIDO, PARAM_PLANNING_JOB, PARAM_PQ_M)


@tagged('post_install', '-at_install')
class TestDevSample(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Project = cls.env['project.project']
        kg = cls.env.ref('uom.product_uom_kgm')
        m = cls.env.ref('uom.product_uom_meter')
        cls.partner = cls.env['res.partner'].create({'name': 'CLIENTE MUESTRA PRUEBA', 'is_company': True})
        cls.hilo = cls.env['product.product'].create({'name': 'HILO MUESTRA', 'type': 'consu', 'is_storable': True,
                                                      'uom_id': kg.id})
        cls.crudo = cls.env['product.product'].create({'name': 'crudo muestra', 'default_code': 'WJ150Q28HNT170X',
                                                       'type': 'consu', 'is_storable': True, 'uom_id': kg.id})
        cls.tela = cls.env['product.product'].create({'name': 'tela muestra', 'default_code': 'WJ150Q28JNT170X',
                                                      'type': 'consu', 'is_storable': True, 'uom_id': m.id})
        cls.wc = wc = cls.env['mrp.workcenter'].create({'name': 'Tejido prueba muestra'})
        cls.wc_rama = cls.env['mrp.workcenter'].create({'name': 'Rama prueba muestra'})
        Bom = cls.env['mrp.bom']
        cls.bom_crudo = Bom.create({
            'product_tmpl_id': cls.crudo.product_tmpl_id.id, 'product_qty': 1, 'product_uom_id': kg.id,
            'bom_line_ids': [(0, 0, {'product_id': cls.hilo.id, 'product_qty': 1.05, 'product_uom_id': kg.id})],
            'operation_ids': [(0, 0, {'name': 'Tejer', 'workcenter_id': wc.id, 'time_cycle_manual': 10})]})
        cls.bom_tela = Bom.create({
            'product_tmpl_id': cls.tela.product_tmpl_id.id, 'product_qty': 100, 'product_uom_id': m.id,
            'bom_line_ids': [(0, 0, {'product_id': cls.crudo.id, 'product_qty': 25, 'product_uom_id': kg.id})],
            'operation_ids': [(0, 0, {'name': 'Ramear', 'workcenter_id': cls.wc_rama.id, 'time_cycle_manual': 5})]})
        wh = cls.env['stock.warehouse'].search([('company_id', '=', cls.env.company.id)], limit=1)
        PType = cls.env['stock.picking.type']
        cls.picking_type = PType.create({
            'name': 'Tejido Desarrollo prueba', 'code': 'mrp_operation', 'sequence_code': 'OPDP',
            'warehouse_id': wh.id, 'company_id': cls.env.company.id,
            'default_location_src_id': wh.lot_stock_id.id, 'default_location_dest_id': wh.lot_stock_id.id})
        cls.picking_tenido = PType.create({
            'name': 'Tintorería prueba', 'code': 'mrp_operation', 'sequence_code': 'OPTP',
            'warehouse_id': wh.id, 'company_id': cls.env.company.id,
            'default_location_src_id': wh.lot_stock_id.id, 'default_location_dest_id': wh.lot_stock_id.id})
        cls.picking_acabado = PType.create({
            'name': 'Acabado prueba', 'code': 'mrp_operation', 'sequence_code': 'OPAP',
            'warehouse_id': wh.id, 'company_id': cls.env.company.id,
            'default_location_src_id': wh.lot_stock_id.id, 'default_location_dest_id': wh.lot_stock_id.id})
        cls.location = cls.env['stock.location'].create({'name': '31 DESARROLLOS prueba', 'usage': 'internal',
                                                         'location_id': wh.lot_stock_id.id})
        cls.job = cls.env['hr.job'].create({'name': 'PLANEADOR DE PRODUCCION (prueba)'})
        cls.planner = cls.env['res.users'].with_context(no_reset_password=True).create({
            'name': 'Planeador prueba', 'login': 'sgi_dev_sample_plan',
            'group_ids': [(6, 0, [cls.env.ref('base.group_user').id])]})
        cls.env['hr.employee'].create({'name': 'Planeador prueba', 'job_id': cls.job.id, 'user_id': cls.planner.id})
        Param = cls.env['ir.config_parameter'].sudo()
        Param.set_param(PARAM_PICKING_TYPE, str(cls.picking_type.id))
        Param.set_param(PARAM_PICKING_TYPE_TENIDO, str(cls.picking_tenido.id))
        Param.set_param(PARAM_PICKING_TYPE_ACABADO, str(cls.picking_acabado.id))
        Param.set_param(PARAM_LOCATION, str(cls.location.id))
        Param.set_param(PARAM_PLANNING_JOB, str(cls.job.id))
        Param.set_param(PARAM_PQ_M, '50')
        Param.set_param(PARAM_EXPECTED_YIELD, '')
        Param.set_param(PARAM_MIN_BATH_KG, '')
        Car = cls.env['ficha.tecnica.caracteristica']
        cls.car_masa = Car.search([('code', '=', 'masa')], limit=1)
        cls.car_ancho = Car.search([('code', '=', 'ancho')], limit=1)

    def _dev(self, approved=True, **vals):
        dev = self.Project.create(dict({
            'name': 'x', 'sgi_is_ft': True, 'partner_id': self.partner.id, 'sgi_dev_product_name': 'Jersey muestra',
            'sgi_dev_product_id': self.tela.id, 'sgi_dev_product_crudo_id': self.crudo.id,
            'sgi_dev_sample_m': 20.0}, **vals))
        (self.crudo | self.tela).product_tmpl_id.write({'sgi_dev_project_id': dev.id, 'sgi_dev_state': 'desarrollo'})
        for car, val in ((self.car_masa, 150.0), (self.car_ancho, 1.7)):
            self.env['sgi.dev.characteristic'].create({'project_id': dev.id, 'caracteristica_id': car.id,
                                                       'spec_nominal': val})
        if approved:
            dev.with_context(sgi_dev_migration=True).write({'sgi_dev_approved_by_id': self.env.uid})
        return dev

    def _wizard(self, dev):
        return self.env['sgi.dev.sample.wizard'].with_context(default_project_id=dev.id).create({})

    def test_01_sugerencia_con_motivo(self):
        dev = self._dev()
        rend = 1000.0 / (150.0 * 1.7)
        self.assertAlmostEqual(dev._sgi_dev_yield_m_per_kg(), rend, places=4)
        qty, note = dev._sgi_dev_sample_suggestion(self.tela)
        self.assertAlmostEqual(qty, 50.0, msg="50 m PQ ganan a los 20 m del cliente")
        self.assertIn('50 m PQ', note)
        self.assertIn('100 %', note, "Sin rendimiento esperado se asume 100 % y se dice")
        qty_kg, note_kg = dev._sgi_dev_sample_suggestion(self.crudo)
        self.assertAlmostEqual(qty_kg, 50.0 / rend, places=3)
        self.assertIn('kg', note_kg)
        Param = self.env['ir.config_parameter'].sudo()
        Param.set_param(PARAM_EXPECTED_YIELD, '80')
        qty, note = dev._sgi_dev_sample_suggestion(self.tela)
        self.assertAlmostEqual(qty, 62.5, msg="50 m al 80 % de primera")
        dev.write({'sgi_dev_sample_m': 200.0})
        qty, note = dev._sgi_dev_sample_suggestion(self.tela)
        self.assertAlmostEqual(qty, 200.0, msg="Lo que pide el cliente gana")
        self.assertIn('lo que pide el cliente', note)
        dev.write({'sgi_dev_product_tenido_id': self.crudo.id})
        Param.set_param(PARAM_MIN_BATH_KG, '100')
        qty, note = dev._sgi_dev_sample_suggestion(self.tela)
        self.assertAlmostEqual(qty, 100.0 * rend, places=3, msg="El mínimo de baño gana si lleva teñido")

    def test_02_cadena_crudo_y_acabado_de_atras_hacia_adelante(self):
        dev = self._dev()
        wiz = self._wizard(dev)
        lines = wiz.line_ids.sorted('sequence')
        self.assertEqual(lines.mapped('product_id'), self.crudo | self.tela, "Crudo y luego acabado")
        crudo, tela = lines
        self.assertEqual((crudo.stage, tela.stage), ('crudo', 'acabado'))
        self.assertAlmostEqual(tela.product_qty, 50.0, msg="El último artículo lleva la cantidad sugerida")
        self.assertAlmostEqual(crudo.product_qty, 12.5, places=3,
                               msg="25 kg de crudo por cada 100 m de acabado: 12.5 kg para 50 m")
        self.assertIn('lista de materiales', crudo.qty_note)
        self.assertEqual((crudo.bom_id, tela.bom_id), (self.bom_crudo, self.bom_tela))
        self.assertEqual((crudo.picking_type_id, tela.picking_type_id), (self.picking_type, self.picking_acabado))
        self.assertEqual(tela.location_dest_id, self.location, "El sobrante del último va a desarrollos")
        self.assertFalse(wiz.warning)
        fecha = fields.Datetime.now() + timedelta(days=3)
        wiz.write({'date_planned': fecha, 'note': 'urge'})
        action = wiz.action_confirm()
        mos = self.env['mrp.production'].search(action['domain'])
        self.assertEqual(len(mos), 2)
        self.assertEqual(dev.sgi_dev_mo_count, 2)
        for mo in mos:
            self.assertEqual(mo.origin, dev.sgi_ft_folio or dev.name)
            self.assertEqual(mo.sgi_dev_project_id, dev)
            self.assertEqual(mo.state, 'draft', "Planeación la emite")
            self.assertTrue(mo.activity_ids.filtered(lambda a: a.user_id == self.planner), "Actividad a Planeación")
            self.assertEqual(mo.sgi_dev_task_id.date_deadline, fecha)
        mo_crudo = mos.filtered(lambda m: m.product_id == self.crudo)
        mo_tela = mos.filtered(lambda m: m.product_id == self.tela)
        self.assertEqual(mo_crudo.picking_type_id, self.picking_type)
        self.assertEqual(mo_tela.picking_type_id, self.picking_acabado)
        self.assertAlmostEqual(mo_crudo.product_qty, 12.5, places=3)
        nueva = fecha + timedelta(days=2)
        mo_crudo.write({'date_start': nueva})
        self.assertEqual(mo_crudo.sgi_dev_task_id.date_deadline, nueva, "La fecha de máquina se refleja en la tarea")
        mo_crudo.action_confirm()
        self.assertTrue(mo_crudo.workorder_ids, "La ruta de la lista de materiales viaja con la orden")

    def test_03_cadena_de_tres_con_tenido(self):
        kg = self.env.ref('uom.product_uom_kgm')
        m = self.env.ref('uom.product_uom_meter')
        tenido = self.env['product.product'].create({'name': 'tenido muestra', 'default_code': 'WJ150Q28ING170X',
                                                     'type': 'consu', 'is_storable': True, 'uom_id': kg.id})
        acabado = self.env['product.product'].create({'name': 'acabado muestra', 'default_code': 'WJ150Q28JNG170X',
                                                      'type': 'consu', 'is_storable': True, 'uom_id': m.id})
        Bom = self.env['mrp.bom']
        Bom.create({'product_tmpl_id': tenido.product_tmpl_id.id, 'product_qty': 1, 'product_uom_id': kg.id,
                    'bom_line_ids': [(0, 0, {'product_id': self.crudo.id, 'product_qty': 1.02, 'product_uom_id': kg.id})],
                    'operation_ids': [(0, 0, {'name': 'Teñir', 'workcenter_id': self.wc_rama.id})]})
        Bom.create({'product_tmpl_id': acabado.product_tmpl_id.id, 'product_qty': 100, 'product_uom_id': m.id,
                    'bom_line_ids': [(0, 0, {'product_id': tenido.id, 'product_qty': 25, 'product_uom_id': kg.id})],
                    'operation_ids': [(0, 0, {'name': 'Ramear', 'workcenter_id': self.wc_rama.id})]})
        dev = self._dev(sgi_dev_product_id=acabado.id, sgi_dev_product_tenido_id=tenido.id)
        lines = self._wizard(dev).line_ids.sorted('sequence')
        self.assertEqual(lines.mapped('stage'), ['crudo', 'tenido', 'acabado'])
        self.assertAlmostEqual(lines[2].product_qty, 50.0)
        self.assertAlmostEqual(lines[1].product_qty, 12.5, places=3, msg="25 kg de teñido por 100 m")
        self.assertAlmostEqual(lines[0].product_qty, 12.75, places=3, msg="1.02 kg de crudo por kg de teñido")
        self.assertEqual(lines[1].picking_type_id, self.picking_tenido)

    def test_04_solo_listas_del_propio_articulo(self):
        dev = self._dev()
        wiz = self._wizard(dev)
        crudo = wiz.line_ids.filtered(lambda l: l.product_id == self.crudo)
        crudo.bom_id = self.bom_tela
        self.assertIn('no es de', wiz.warning)
        before = self.env['mrp.production'].search_count([('sgi_dev_project_id', '=', dev.id)])
        with self.assertRaises(UserError, msg="Con la lista de otro artículo no se pide nada"):
            wiz.action_confirm()
        self.assertEqual(self.env['mrp.production'].search_count([('sgi_dev_project_id', '=', dev.id)]), before)
        sin = self.env['product.product'].create({'name': 'sin receta muestra', 'type': 'consu'})
        dev2 = self._dev(sgi_dev_product_id=sin.id)
        wiz2 = self._wizard(dev2)
        self.assertIn('no tiene lista de materiales propia', wiz2.warning)
        with self.assertRaises(UserError):
            wiz2.action_confirm()

    def test_05_sin_ruta_no_se_confirma(self):
        self.bom_tela.operation_ids.unlink()
        dev = self._dev()
        wiz = self._wizard(dev)
        self.assertIn('no tiene operaciones (ruta)', wiz.warning)
        with self.assertRaises(UserError):
            wiz.action_confirm()
        self.assertFalse(dev.sgi_dev_mo_ids, "Ni el crudo se pide si una orden de la cadena no tiene ruta")

    def test_06_sin_aprobacion_no_se_pide(self):
        dev = self._dev(approved=False)
        with self.assertRaises(UserError):
            dev.action_sgi_dev_request_sample()
        with self.assertRaises(UserError):
            self._wizard(dev).action_confirm()
        dev.with_context(sgi_dev_migration=True).write({'sgi_dev_approved_by_id': self.env.uid})
        self.assertEqual(dev.action_sgi_dev_request_sample()['res_model'], 'sgi.dev.sample.wizard')

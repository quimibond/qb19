# -*- coding: utf-8 -*-
"""57.125.0 (C1, bloque F): la corrida de muestra se pide desde el proyecto."""
from datetime import timedelta

from odoo import fields
from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged

from ..models.sgi_dev_sample import (
    PARAM_EXPECTED_YIELD, PARAM_LOCATION, PARAM_MIN_BATH_KG, PARAM_PICKING_TYPE, PARAM_PLANNING_JOB, PARAM_PQ_M)


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
        wc = cls.env['mrp.workcenter'].create({'name': 'Tejido prueba muestra'})
        Bom = cls.env['mrp.bom']
        Bom.create({'product_tmpl_id': cls.crudo.product_tmpl_id.id, 'product_qty': 1, 'product_uom_id': kg.id,
                    'bom_line_ids': [(0, 0, {'product_id': cls.hilo.id, 'product_qty': 1.05, 'product_uom_id': kg.id})],
                    'operation_ids': [(0, 0, {'name': 'Tejer', 'workcenter_id': wc.id, 'time_cycle_manual': 10})]})
        Bom.create({'product_tmpl_id': cls.tela.product_tmpl_id.id, 'product_qty': 100, 'product_uom_id': m.id,
                    'bom_line_ids': [(0, 0, {'product_id': cls.crudo.id, 'product_qty': 25, 'product_uom_id': kg.id})]})
        wh = cls.env['stock.warehouse'].search([('company_id', '=', cls.env.company.id)], limit=1)
        cls.picking_type = cls.env['stock.picking.type'].create({
            'name': 'Tejido Desarrollo prueba', 'code': 'mrp_operation', 'sequence_code': 'OPDP',
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

    def test_02_wizard_crea_la_orden_armada(self):
        dev = self._dev()
        wiz = self._wizard(dev)
        self.assertEqual(wiz.product_id, self.crudo, "Por omisión el crudo: la corrida arranca en Tejido")
        self.assertEqual(wiz.picking_type_id, self.picking_type)
        self.assertEqual(wiz.location_dest_id, self.location)
        self.assertTrue(wiz.bom_id)
        self.assertAlmostEqual(wiz.product_qty, wiz.suggested_qty)
        fecha = fields.Datetime.now() + timedelta(days=3)
        wiz.write({'product_qty': 15.0, 'date_planned': fecha, 'note': 'urge'})
        action = wiz.action_confirm()
        mo = self.env['mrp.production'].browse(action['res_id'])
        self.assertEqual(mo.product_id, self.crudo)
        self.assertEqual(mo.product_qty, 15.0)
        self.assertEqual(mo.picking_type_id, self.picking_type)
        self.assertEqual(mo.location_dest_id, self.location)
        self.assertEqual(mo.origin, dev.sgi_ft_folio or dev.name)
        self.assertEqual(mo.sgi_dev_project_id, dev)
        self.assertTrue(mo.bom_id)
        self.assertEqual(mo.state, 'draft', "Planeación la emite")
        self.assertTrue(mo.activity_ids.filtered(lambda a: a.user_id == self.planner), "Actividad a Planeación")
        task = mo.sgi_dev_task_id
        self.assertEqual(task.project_id, dev)
        self.assertEqual(task.date_deadline, fecha)
        nueva = fecha + timedelta(days=2)
        mo.write({'date_start': nueva})
        self.assertEqual(task.date_deadline, nueva, "La fecha de máquina se refleja sola en la tarea")
        self.assertEqual(dev.sgi_dev_mo_count, 1)
        mo.action_confirm()
        self.assertTrue(mo.workorder_ids, "La ruta de la lista de materiales viaja con la orden")

    def test_03_sin_aprobacion_o_sin_receta_no_se_pide(self):
        dev = self._dev(approved=False)
        with self.assertRaises(UserError):
            dev.action_sgi_dev_request_sample()
        with self.assertRaises(UserError):
            self._wizard(dev).action_confirm()
        dev.with_context(sgi_dev_migration=True).write({'sgi_dev_approved_by_id': self.env.uid})
        self.assertEqual(dev.action_sgi_dev_request_sample()['res_model'], 'sgi.dev.sample.wizard')
        sin = self.env['product.product'].create({'name': 'sin receta muestra', 'type': 'consu'})
        wiz = self._wizard(dev)
        wiz.write({'product_id': sin.id, 'product_qty': 5})
        with self.assertRaises(UserError, msg="Sin lista de materiales no hay orden"):
            wiz.action_confirm()

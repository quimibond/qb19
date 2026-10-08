# -*- coding: utf-8 -*-
"""57.124.0 (C1, bloque E): compuerta de la solicitud aprobada, aviso, existencias y requisición."""
from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged

from ..models.sgi_dev_start import PARAM_NOTIFY_JOBS, PARAM_NOTIFY_USERS


@tagged('post_install', '-at_install')
class TestDevStart(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Project = cls.env['project.project']
        kg = cls.env.ref('uom.product_uom_kgm')
        m = cls.env.ref('uom.product_uom_meter')
        cls.partner = cls.env['res.partner'].create({'name': 'CLIENTE ARRANQUE PRUEBA', 'is_company': True})
        cls.hilo = cls.env['product.product'].create({
            'name': 'HILO PRUEBA ARRANQUE', 'default_code': 'HILO-ARR', 'type': 'consu', 'is_storable': True,
            'uom_id': kg.id})
        cls.crudo = cls.env['product.product'].create({
            'name': 'crudo arranque', 'default_code': 'WJ080Q21HNT165X', 'type': 'consu', 'is_storable': True,
            'uom_id': kg.id})
        cls.tela = cls.env['product.product'].create({
            'name': 'tela arranque', 'default_code': 'WJ080Q21JNT165X', 'type': 'consu', 'is_storable': True,
            'uom_id': m.id})
        Bom = cls.env['mrp.bom']
        Bom.create({'product_tmpl_id': cls.crudo.product_tmpl_id.id, 'product_qty': 1, 'product_uom_id': kg.id,
                    'bom_line_ids': [(0, 0, {'product_id': cls.hilo.id, 'product_qty': 1.1, 'product_uom_id': kg.id})]})
        Bom.create({'product_tmpl_id': cls.tela.product_tmpl_id.id, 'product_qty': 100, 'product_uom_id': m.id,
                    'bom_line_ids': [(0, 0, {'product_id': cls.crudo.id, 'product_qty': 10, 'product_uom_id': kg.id})]})
        cls.tela.product_tmpl_id.write({'sgi_dev_state': 'desarrollo'})
        cls.job = cls.env['hr.job'].create({'name': 'JEFE DE MANUFACTURA (prueba arranque)'})
        cls.user_job = cls.env['res.users'].with_context(no_reset_password=True).create({
            'name': 'Parte interesada prueba', 'login': 'sgi_dev_start_pi',
            'group_ids': [(6, 0, [cls.env.ref('base.group_user').id])]})
        cls.env['hr.employee'].create({'name': 'Parte interesada prueba', 'job_id': cls.job.id,
                                       'user_id': cls.user_job.id})
        Param = cls.env['ir.config_parameter'].sudo()
        Param.set_param(PARAM_NOTIFY_JOBS, str(cls.job.id))
        Param.set_param(PARAM_NOTIFY_USERS, '')
        cls.category = cls.env['approval.category'].search([('approval_type', '=', 'purchase')], limit=1) or \
            cls.env['approval.category'].create({'name': 'Requisición prueba', 'approval_type': 'purchase',
                                                 'has_product': 'required'})

    def _dev(self, **vals):
        return self.Project.create(dict({
            'name': 'x', 'sgi_is_ft': True, 'partner_id': self.partner.id, 'sgi_dev_product_name': 'Jersey arranque',
            'sgi_dev_product_id': self.tela.id, 'sgi_dev_sample_m': 200.0}, **vals))

    def test_01_sin_aprobacion_no_hay_corrida(self):
        dev = self._dev()
        self.tela.product_tmpl_id.write({'sgi_dev_project_id': dev.id})
        muestra = self.env.ref('quimibond_sgi.sgi_dev_stage_muestra')
        with self.assertRaises(UserError, msg="Sin aprobación el proyecto no pasa a Muestra"):
            dev.write({'stage_id': muestra.id})
        mo = self.env['mrp.production'].create({'product_id': self.tela.id, 'product_qty': 100,
                                                'product_uom_id': self.tela.uom_id.id})
        with self.assertRaises(UserError, msg="Sin aprobación no se confirma la orden de muestra"):
            mo.action_confirm()
        dev.action_sgi_dev_approve_request()
        self.assertTrue(dev.sgi_dev_approved_by_id)
        mo.action_confirm()
        self.assertEqual(mo.state, 'confirmed')
        dev.write({'stage_id': muestra.id})
        self.assertEqual(dev.sgi_dev_stage_key, 'muestra')
        cerrado = self.env.ref('quimibond_sgi.sgi_dev_stage_cerrado_sin_producto')
        otro = self._dev()
        otro.write({'stage_id': cerrado.id})
        self.assertEqual(otro.sgi_dev_stage_key, 'cerrado_sin_producto', "Cerrar sin producto no pide aprobación.")

    def test_02_aprobar_avisa_y_revisa_existencias(self):
        dev = self._dev()
        before = len(dev.message_ids)
        dev.action_sgi_dev_approve_request()
        lines = dev.sgi_dev_mp_line_ids
        self.assertEqual(lines.product_id, self.hilo, "La explosión llega hasta la hoja comprada")
        self.assertAlmostEqual(lines.qty_needed, 200 / 100 * 10 * 1.1, places=3)
        self.assertEqual(lines.qty_available, 0.0)
        self.assertAlmostEqual(lines.qty_missing, 22.0, places=3)
        self.assertEqual(dev.sgi_dev_mp_missing_count, 1)
        new_msgs = dev.message_ids[:len(dev.message_ids) - before]
        aviso = new_msgs.filtered(lambda m: m.attachment_ids)
        self.assertTrue(aviso, "El aviso lleva el PDF de la solicitud")
        self.assertIn(self.user_job.partner_id, aviso.partner_ids, "Las partes interesadas reciben el aviso")
        self.assertTrue(aviso.attachment_ids[:1].name.endswith('.pdf'))

    def test_03_existencias_y_requisicion(self):
        dev = self._dev()
        quant = self.env['stock.quant'].with_context(inventory_mode=True)
        wh = self.env['stock.warehouse'].search([('company_id', '=', self.env.company.id)], limit=1)
        quant.create({'product_id': self.hilo.id, 'location_id': wh.lot_stock_id.id, 'inventory_quantity': 5.0}).action_apply_inventory()
        dev.action_sgi_dev_mp_check()
        line = dev.sgi_dev_mp_line_ids
        self.assertAlmostEqual(line.qty_available, 5.0, places=3)
        self.assertAlmostEqual(line.qty_missing, 17.0, places=3)
        action = dev.action_sgi_dev_mp_requisition()
        req = self.env['approval.request'].browse(action['res_id'])
        self.assertEqual(req.sgi_dev_project_id, dev)
        self.assertEqual(req.category_id, self.category)
        self.assertEqual(len(req.product_line_ids), 1)
        self.assertAlmostEqual(req.product_line_ids.quantity, 17.0, places=3)
        self.assertEqual(req.product_line_ids.product_id, self.hilo)
        self.assertEqual(line.requisition_id, req)
        self.assertTrue(dev.sgi_dev_mp_pending, "La requisición arranca el reloj de materia prima")
        self.assertEqual(dev.sgi_dev_requisition_count, 1)
        with self.assertRaises(UserError, msg="Nada pendiente sin requisición"):
            dev.action_sgi_dev_mp_requisition()
        req.action_cancel()
        self.assertFalse(dev.sgi_dev_mp_pending, "Una requisición cancelada ya no detiene el desarrollo")
        dev.action_sgi_dev_mp_check()
        self.assertEqual(dev.sgi_dev_mp_line_ids.requisition_id, req, "La línea con requisición se conserva")

    def test_04_sin_articulo_o_sin_receta(self):
        dev = self._dev(sgi_dev_product_id=False)
        with self.assertRaises(UserError):
            dev.action_sgi_dev_mp_check()
        sin_bom = self.env['product.product'].create({'name': 'sin receta', 'type': 'consu'})
        dev.write({'sgi_dev_product_id': sin_bom.id})
        with self.assertRaises(UserError):
            dev.action_sgi_dev_mp_check()
        dev.action_sgi_dev_approve_request()
        self.assertTrue(dev.sgi_dev_approved_by_id, "Sin receta la aprobación pasa igual; el chatter lo dice")
        self.assertTrue(any('No se revisaron existencias' in (m.body or '') for m in dev.message_ids))

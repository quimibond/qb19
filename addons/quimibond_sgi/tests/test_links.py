# -*- coding: utf-8 -*-
"""Campos de liga entrada ↔ salida (19.0.53.5.0) y reglas de datos."""
import base64

from odoo.tests import TransactionCase, tagged

from .common_documents import sgi_hide_real_documents


@tagged('post_install', '-at_install')
class TestLinks(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        cls.env = cls.env(context=dict(cls.env.context, sgi_skip_role_check=True))
        cls.process = cls.env['sgi.process'].create({'code': 'XLK', 'name': 'Proceso ligas'})
        cls.project = cls.env['project.project'].create({'name': 'FT-LIGAS'})
        cls.task = cls.env['project.task'].create({'name': 'Desarrollo ligas', 'project_id': cls.project.id})
        cls.product = cls.env['product.template'].create({'name': 'Artículo ligas', 'type': 'consu'})

    def test_01_c1_tarea_del_desarrollo_en_toda_la_cadena(self):
        fmea = self.env['sgi.fmea'].create({
            'name': 'AMEF ligas', 'fmea_type': 'proceso', 'product_tmpl_id': self.product.id,
            'sgi_dyd_task_id': self.task.id})
        plan = self.env['sgi.control.plan'].create({
            'name': 'Plan ligas', 'product_tmpl_ids': [(6, 0, self.product.ids)], 'sgi_dyd_task_id': self.task.id})
        customer = self.env['res.partner'].create({'name': 'Cliente ligas'})
        ppap = self.env['sgi.ppap'].create({
            'partner_id': customer.id, 'product_tmpl_id': self.product.id, 'sgi_dyd_task_id': self.task.id})
        bom = self.env['mrp.bom'].create({'product_tmpl_id': self.product.id, 'sgi_dyd_task_id': self.task.id})
        self.assertEqual(self.task.sgi_fmea_ids, fmea)
        self.assertEqual(self.task.sgi_control_plan_ids, plan)
        self.assertEqual(self.task.sgi_ppap_ids, ppap)
        self.assertEqual(self.task.sgi_bom_ids, bom)
        self.assertEqual(self.task.sgi_dyd_link_count, 4)
        # match_path con punto: la LdM llega al AMEF por la tarea.
        self.assertEqual(bom.mapped('sgi_dyd_task_id.sgi_fmea_ids'), fmea)
        self.assertIn(bom, self.env['mrp.bom'].search([('sgi_dyd_task_id.sgi_fmea_ids', '=', fmea.id)]))
        # El producto propone el plan de control que lo incluye.
        self.product.invalidate_recordset(['sgi_control_plan_id'])
        self.assertEqual(self.product.sgi_control_plan_id, plan)

    def test_02_s5_06_nc_desde_mantenimiento(self):
        equipment = self.env['maintenance.equipment'].create({'name': 'Telar ligas'})
        request = self.env['maintenance.request'].create({
            'name': 'Falla ligas', 'equipment_id': equipment.id, 'maintenance_type': 'corrective'})
        request.action_sgi_raise_nc()
        self.assertTrue(request.sgi_alert_id)
        self.assertEqual(request.sgi_alert_id.sgi_maintenance_request_id, request)

    def test_03_s2_08_y_c4_19_y_c2_34(self):
        Picking = self.env['stock.picking']
        picking_type = self.env.ref('stock.picking_type_out')
        picking = Picking.create({'picking_type_id': picking_type.id,
                                  'location_id': picking_type.default_location_src_id.id,
                                  'location_dest_id': self.env.ref('stock.stock_location_customers').id})
        # Sin factura ligada, la factura propone vacío; a mano se liga.
        move = self.env['account.move'].create({'move_type': 'out_invoice'})
        self.assertFalse(move.sgi_picking_ids)
        move.sgi_picking_ids = picking
        self.assertEqual(move.sgi_picking_ids, picking)
        # C4.19: el traspaso propone la orden por el documento origen.
        production = self.env['mrp.production'].create({
            'product_id': self.product.product_variant_id.id, 'product_qty': 1})
        internal_type = self.env.ref('stock.picking_type_internal')
        transfer = Picking.create({'picking_type_id': internal_type.id, 'origin': production.name,
                                   'location_id': internal_type.default_location_src_id.id,
                                   'location_dest_id': internal_type.default_location_dest_id.id})
        self.assertEqual(transfer.sgi_production_id, production)
        self.assertIn(transfer, production.sgi_release_picking_ids)
        # C2.34: el acuse queda ligado a su entrega por campo.
        wiz = self.env['sgi.acuse.attach.wizard'].create({
            'picking_id': picking.id, 'file': base64.b64encode(b'%PDF-1.4 acuse'), 'file_name': 'acuse.pdf'})
        wiz.action_attach()
        self.assertEqual(len(picking.sgi_acuse_attachment_ids), 1)
        att = picking.sgi_acuse_attachment_ids
        self.assertTrue(att.name.startswith('ACUSE-'))
        self.assertEqual(att.sgi_picking_id, picking)
        # Y uno que llega por el chatter con nombre ACUSE también.
        chat = self.env['ir.attachment'].create({
            'name': 'ACUSE firmado.pdf', 'res_model': 'stock.picking', 'res_id': picking.id,
            'datas': base64.b64encode(b'%PDF-1.4')})
        self.assertEqual(chat.sgi_picking_id, picking)
        self.assertEqual(picking.sgi_acuse_count, 2)

    def test_04_documentos_fuera_del_sgi_sin_estado(self):
        plain = self.env['documents.document'].create({'name': 'Foto de la planta.jpg', 'type': 'binary'})
        self.assertFalse(plain.sgi_state, "Un archivo que no es del SGI no lleva estado.")
        controlled = self.env['documents.document'].create({
            'name': 'P-L91 Procedimiento ligas', 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'procedimiento', 'sgi_code': 'P-L91', 'sgi_process_id': self.process.id})
        self.assertEqual(controlled.sgi_state, 'borrador')
        plain.write({'sgi_is_controlled': True, 'sgi_doc_type': 'anexo', 'sgi_code': 'AN-L91'})
        self.assertEqual(plain.sgi_state, 'borrador', "Al volverse controlado arranca en borrador.")

    def test_05_faltantes_no_viven_en_procesos_archivados(self):
        activity = self.env['sgi.process.activity'].create({'process_id': self.process.id, 'name': 'Sin nada'})
        self.assertTrue(activity.spec_gap_ids, "Una actividad vacía tiene faltantes.")
        self.process.active = False
        activity._sgi_refresh_spec_gaps()
        self.assertFalse(activity.spec_gap_ids, "Archivado el proceso, sus faltantes se van.")

    def test_06_objetivo_sin_indicador_sin_dato(self):
        objective = self.env['sgi.objective'].create({'name': 'Objetivo sin indicador'})
        self.assertEqual(objective.health, 'sin_dato')

    def test_07_solo_proveedores_criticos(self):
        Eval = self.env['sgi.supplier.eval']
        categ = self.env['product.category'].create({'name': 'Maquila ligas'})
        self.env['ir.config_parameter'].sudo().set_param(
            'quimibond_sgi.supplier_critical_categ_ids', str(categ.id))
        self.assertEqual(Eval._sgi_critical_categ_ids(), {categ.id})
        self.env['ir.config_parameter'].sudo().set_param('quimibond_sgi.supplier_critical_categ_ids', '')
        found = self.env['product.category'].search([('name', 'ilike', 'maquila')])
        self.assertTrue(set(found.ids) <= Eval._sgi_critical_categ_ids(),
                        "Sin configuración, cuentan las categorías «maquila».")
        partner = self.env['res.partner'].create({'name': 'Proveedor ligas', 'sgi_supplier_critical': True})
        self.assertTrue(partner.sgi_supplier_critical)

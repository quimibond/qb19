# -*- coding: utf-8 -*-
"""57.127.0: correcciones de Jose a las mediciones de C1."""
from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestDevCorrections(TransactionCase):

    def test_01_ruta_atribuida_a_quien_la_asigna(self):
        kg = self.env.ref('uom.product_uom_kgm')
        wc = self.env['mrp.workcenter'].create({'name': 'CIRCULAR 97 prueba'})
        product = self.env['product.product'].create({'name': 'crudo ruta', 'type': 'consu', 'uom_id': kg.id})
        user = self.env['res.users'].with_context(no_reset_password=True).create({
            'name': 'Procesos ruta', 'login': 'sgi_ruta_yet',
            'group_ids': [(6, 0, [self.env.ref('base.group_user').id, self.env.ref('mrp.group_mrp_user').id])]})
        bom = self.env['mrp.bom'].create({'product_tmpl_id': product.product_tmpl_id.id, 'product_qty': 1,
                                          'product_uom_id': kg.id})
        self.assertFalse(bom.sgi_route_assigned_by_id, "Sin operaciones no hay ruta asignada")
        bom.with_user(user).write({'operation_ids': [(0, 0, {'name': 'Tejer', 'workcenter_id': wc.id,
                                                             'time_cycle_manual': 5})]})
        self.assertEqual(bom.sgi_route_assigned_by_id, user)
        self.assertTrue(bom.sgi_route_assigned_date)
        bom.write({'code': 'otra referencia'})
        self.assertEqual(bom.sgi_route_assigned_by_id, user, "Editar otra cosa no cambia quién asignó la ruta")
        bom.operation_ids.write({'time_cycle_manual': 7})
        self.assertEqual(bom.sgi_route_assigned_by_id, self.env.user)
        bom.operation_ids.unlink()
        self.assertFalse(bom.sgi_route_assigned_by_id, "Sin operaciones se quita la atribución")

    def test_02_reloj_por_etapa_guarda_quien_movio(self):
        dev = self.env['project.project'].create({'name': 'reloj', 'sgi_is_ft': True, 'sgi_dev_product_name': 'x'})
        log = dev.sgi_dev_stage_log_ids.filtered(lambda l: not l.date_end)
        self.assertEqual(log.user_id, self.env.user)
        analisis = self.env.ref('quimibond_sgi.sgi_dev_stage_analisis')
        dev.write({'stage_id': analisis.id})
        nuevo = dev.sgi_dev_stage_log_ids.filtered(lambda l: not l.date_end)
        self.assertEqual(nuevo.stage_id, analisis)
        self.assertEqual(nuevo.user_id, self.env.user)

    def test_03_mediciones_c1_apuntan_a_campos_de_usuario(self):
        report = self.env['project.project']._sgi_dev_apply_c1_measures()
        Deliverable = self.env['sgi.deliverable']
        ruta = Deliverable.search([('code', '=', 'C1-RUTA')], limit=1)
        if ruta:
            self.assertEqual(ruta.measure_user_field, 'sgi_route_assigned_by_id')
            self.assertEqual(ruta.measure_date_field, 'sgi_route_assigned_date')
        for code in ('C1-RESPUESTA', 'C1-APROBADO'):
            d = Deliverable.search([('code', '=', code)], limit=1)
            if d:
                self.assertEqual(d.measure_user_field, 'user_id')
                if 'measure_user_history' in d._fields:
                    self.assertFalse(d.measure_user_history)
        for code, field in (('C1-OP-MUESTRA', 'sgi_dev_issued_by_id'), ('C1-MUESTRA', 'sgi_dev_run_validated_by_id')):
            d = Deliverable.search([('code', '=', code)], limit=1)
            if d:
                self.assertEqual(d.odoo_model_id.model, 'mrp.production')
                self.assertEqual(d.measure_user_field, field)
                self.assertIn('sgi_dev_project_id', d.measure_domain)
        self.assertIsInstance(report, dict)

    def test_04_validar_corrida(self):
        product = self.env['product.product'].create({'name': 'tela corrida', 'type': 'consu'})
        mo = self.env['mrp.production'].create({'product_id': product.id, 'product_qty': 1,
                                                'product_uom_id': product.uom_id.id})
        with self.assertRaises(UserError, msg="Solo órdenes de muestra de un desarrollo"):
            mo.action_sgi_dev_validate_run()
        dev = self.env['project.project'].create({'name': 'corrida', 'sgi_is_ft': True, 'sgi_dev_product_name': 'x'})
        mo.write({'sgi_dev_project_id': dev.id})
        with self.assertRaises(UserError, msg="La orden debe estar terminada"):
            mo.action_sgi_dev_validate_run()
        dev.with_context(sgi_dev_migration=True).write({'sgi_dev_approved_by_id': self.env.uid})
        mo.action_confirm()
        self.assertEqual(mo.sgi_dev_issued_by_id, self.env.user, "Planeación emite = confirma")
        mo.write({'state': 'done'})
        mo.action_sgi_dev_validate_run()
        self.assertEqual(mo.sgi_dev_run_validated_by_id, self.env.user)
        with self.assertRaises(UserError, msg="No se valida dos veces"):
            mo.action_sgi_dev_validate_run()

# -*- coding: utf-8 -*-
"""57.126.0 (C1, bloque G): diagrama de flujo desde la ruta y ficha de tejido con firmas vigentes."""
from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged

from ..models.sgi_dev_process_sheet import PARAM_VALIDATOR_JOB_TEJIDO


@tagged('post_install', '-at_install')
class TestDevProcessSheet(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.product = cls.env['product.product'].create({'name': 'tela ficha proceso', 'type': 'consu'})
        Users = cls.env['res.users'].with_context(no_reset_password=True)
        groups = [cls.env.ref('base.group_user').id, cls.env.ref('mrp.group_mrp_user').id]
        cls.yet = Users.create({'name': 'Procesos prueba', 'login': 'sgi_ps_yet', 'group_ids': [(6, 0, groups)]})
        cls.jefe = Users.create({'name': 'Jefe de Manufactura prueba', 'login': 'sgi_ps_jefe', 'group_ids': [(6, 0, groups)]})
        cls.job = cls.env['hr.job'].create({'name': 'JEFE DE MANUFACTURA (prueba ficha)'})
        cls.env['hr.employee'].create({'name': 'Jefe', 'job_id': cls.job.id, 'user_id': cls.jefe.id})
        cls.env['ir.config_parameter'].sudo().set_param(PARAM_VALIDATOR_JOB_TEJIDO, str(cls.job.id))

    def test_01_ficha_de_tejido_firmas_vigentes(self):
        wc = self.env['mrp.workcenter'].create({'name': 'CIRCULAR 99 prueba'})
        sheet = self.env['sgi.machine.sheet'].create({'product_id': self.product.id, 'workcenter_id': wc.id})
        with self.assertRaises(UserError, msg="Primero propone Diseño de Procesos"):
            sheet.with_user(self.jefe).action_validate()
        sheet.with_user(self.yet).action_propose()
        self.assertEqual(sheet.engineering_by_id, self.yet)
        self.assertTrue(sheet.proposed_date)
        sheet.param_line_ids[:1].write({'real': '12', 'adjustment': 2})
        with self.assertRaises(UserError, msg="Diseño de Procesos no valida"):
            sheet.with_user(self.yet).action_validate()
        sheet.with_user(self.jefe).action_validate()
        self.assertEqual(sheet.approved_by_id, self.jefe)
        self.assertTrue(sheet.validated_date)
        self.assertEqual(sheet._fields['approved_by_id'].string, "Validó (Jefe de Manufactura)")

    def test_02_flujo_desde_la_ruta(self):
        kg = self.env.ref('uom.product_uom_kgm')
        wc = self.env['mrp.workcenter'].create({'name': 'CIRCULAR 98 prueba'})
        crudo = self.env['product.product'].create({'name': 'crudo flujo', 'type': 'consu', 'uom_id': kg.id})
        self.env['mrp.bom'].create({'product_tmpl_id': crudo.product_tmpl_id.id, 'product_qty': 1, 'product_uom_id': kg.id,
                                    'operation_ids': [(0, 0, {'name': 'Tejer', 'workcenter_id': wc.id, 'time_cycle_manual': 5})]})
        dev = self.env['project.project'].create({'name': 'flujo', 'sgi_is_ft': True, 'sgi_dev_product_name': 'x',
                                                  'sgi_dev_product_crudo_id': crudo.id})
        ops = dev._sgi_dev_route_operations()
        self.assertEqual([(p, o.name) for p, o in ops], [(crudo, 'Tejer')])
        self.assertEqual(dev.action_sgi_dev_print_flow()['type'], 'ir.actions.report')
        sin = self.env['project.project'].create({'name': 'sin ruta', 'sgi_is_ft': True, 'sgi_dev_product_name': 'y'})
        with self.assertRaises(UserError):
            sin.action_sgi_dev_print_flow()

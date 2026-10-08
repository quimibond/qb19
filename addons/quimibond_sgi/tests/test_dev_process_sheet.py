# -*- coding: utf-8 -*-
"""57.126.0 (C1, bloque G): fichas de proceso de tintorería y acabado, gráfica, pases y flujo."""
from odoo.exceptions import UserError, ValidationError
from odoo.tests import TransactionCase, tagged

from ..models.sgi_dev_process_sheet import PARAM_PRODUCT_DESIGN_JOB, PARAM_VALIDATOR_JOB, _workcenter_area


@tagged('post_install', '-at_install')
class TestDevProcessSheet(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.product = cls.env['product.product'].create({'name': 'tela ficha proceso', 'type': 'consu'})
        cls.wc_dye = cls.env['mrp.workcenter'].create({'name': 'TINTORERIA 9 prueba', 'code': 'HTJ9P'})
        cls.wc_rama = cls.env['mrp.workcenter'].create({'name': 'RAMA 9 prueba'})
        Users = cls.env['res.users'].with_context(no_reset_password=True)
        grp = cls.env.ref('base.group_user')
        cls.yet = Users.create({'name': 'Procesos prueba', 'login': 'sgi_ps_yet', 'group_ids': [(6, 0, [grp.id])]})
        cls.sup = Users.create({'name': 'Supervisor tintorería prueba', 'login': 'sgi_ps_sup', 'group_ids': [(6, 0, [grp.id])]})
        cls.selena = Users.create({'name': 'Diseño de producto prueba', 'login': 'sgi_ps_selena', 'group_ids': [(6, 0, [grp.id])]})
        Job = cls.env['hr.job']
        cls.job_sup = Job.create({'name': 'SUPERVISOR TINTORERIA (prueba)'})
        cls.job_design = Job.create({'name': 'DISEÑO DE PRODUCTO (prueba)'})
        cls.env['hr.employee'].create({'name': 'Supervisor', 'job_id': cls.job_sup.id, 'user_id': cls.sup.id})
        cls.env['hr.employee'].create({'name': 'Selena', 'job_id': cls.job_design.id, 'user_id': cls.selena.id})
        Param = cls.env['ir.config_parameter'].sudo()
        Param.set_param(PARAM_VALIDATOR_JOB['tintoreria'], str(cls.job_sup.id))
        Param.set_param(PARAM_VALIDATOR_JOB['acabado'], str(cls.job_sup.id))
        Param.set_param(PARAM_PRODUCT_DESIGN_JOB, str(cls.job_design.id))

    def test_01_area_por_centro_y_parametros_sembrados(self):
        self.assertEqual(_workcenter_area(self.wc_dye), 'tintoreria')
        self.assertEqual(_workcenter_area(self.wc_rama), 'acabado')
        self.assertEqual(_workcenter_area(self.env['mrp.workcenter'].new({'name': 'CIRCULAR 40'})), 'tejido')
        dye = self.env['sgi.dev.process.sheet'].create({'area': 'tintoreria', 'product_id': self.product.id,
                                                        'workcenter_id': self.wc_dye.id})
        self.assertTrue(dye.name.startswith('FPR-'))
        self.assertTrue(dye.param_line_ids.filtered(lambda p: p.name == 'Relación de baño'))
        fin = self.env['sgi.dev.process.sheet'].create({'area': 'acabado', 'product_id': self.product.id,
                                                        'workcenter_id': self.wc_rama.id})
        self.assertEqual(fin.pass_count, 1)
        self.assertTrue(fin.param_line_ids.filtered(lambda p: p.name == 'Temperatura campo 8 lado B'))
        fin.action_add_pass()
        fin.action_add_pass()
        self.assertEqual(fin.pass_count, 3)
        with self.assertRaises(UserError, msg="Hasta tres pases"):
            fin.action_add_pass()
        with self.assertRaises(ValidationError, msg="Hasta diez pasos de ruta"):
            fin.write({'route_line_ids': [(0, 0, {'name': 'paso %d' % i}) for i in range(11)]})

    def test_02_grafica_de_tintoreria(self):
        dye = self.env['sgi.dev.process.sheet'].create({
            'area': 'tintoreria', 'product_id': self.product.id, 'workcenter_id': self.wc_dye.id,
            'dye_step_ids': [(0, 0, {'name': 'Calentar', 'temp_start': 30, 'temp_end': 130, 'gradient': 2, 'hold_min': 30}),
                             (0, 0, {'name': 'Enfriar', 'temp_start': 130, 'temp_end': 60, 'gradient': 3.5, 'hold_min': 0})]})
        self.assertAlmostEqual(dye.dye_step_ids[0].minutes, 80.0)
        self.assertAlmostEqual(dye.dye_total_min, 80.0 + 20.0, places=3)
        self.assertIn('<svg', dye.dye_chart_svg)
        self.assertIn('polyline', dye.dye_chart_svg)

    def test_03_firmas_por_puesto_y_vigencia(self):
        Sheet = self.env['sgi.dev.process.sheet']
        dye = Sheet.create({'area': 'tintoreria', 'product_id': self.product.id, 'workcenter_id': self.wc_dye.id})
        line = dye.param_line_ids.filtered(lambda p: p.name == 'Relación de baño')
        line.write({'proposed_num': 8.0})
        dye.with_user(self.yet).action_propose()
        self.assertEqual(dye.state, 'propuesta')
        self.assertEqual(dye.proposed_by_id, self.yet)
        line.write({'real_num': 9.0, 'adjustment': 1})
        with self.assertRaises(UserError, msg="Diseño de Procesos no valida"):
            dye.with_user(self.yet).action_validate()
        dye.with_user(self.sup).action_validate()
        self.assertEqual(dye.state, 'validada')
        self.assertEqual(dye.validated_by_id, self.sup)
        self.assertEqual(line.real_label, '9 L/kg')
        dye.action_set_current()
        self.assertEqual(dye.state, 'vigente')
        otra = Sheet.create({'area': 'tintoreria', 'product_id': self.product.id, 'workcenter_id': self.wc_dye.id})
        otra.action_set_current()
        self.assertEqual(otra.state, 'vigente')
        self.assertEqual(dye.state, 'obsoleta', "Una vigente por artículo y área")
        self.assertEqual(otra.revision, 1)

    def test_04_tercer_pase_avisa_a_diseno_de_producto(self):
        fin = self.env['sgi.dev.process.sheet'].create({'area': 'acabado', 'product_id': self.product.id,
                                                        'workcenter_id': self.wc_rama.id})
        fin.action_add_pass()
        fin.action_add_pass()
        fin.write({'pass_result_3': 'no_cumple'})
        self.assertTrue(fin.third_pass_alerted)
        self.assertTrue(fin.activity_ids.filtered(lambda a: a.user_id == self.selena))

    def test_05_ficha_de_tejido_firmas_vigentes(self):
        Param = self.env['ir.config_parameter'].sudo()
        Param.set_param(PARAM_VALIDATOR_JOB['tejido'], str(self.job_sup.id))
        wc = self.env['mrp.workcenter'].create({'name': 'CIRCULAR 99 prueba'})
        sheet = self.env['sgi.machine.sheet'].create({'product_id': self.product.id, 'workcenter_id': wc.id})
        with self.assertRaises(UserError, msg="Primero propone Diseño de Procesos"):
            sheet.with_user(self.sup).action_validate()
        sheet.with_user(self.yet).action_propose()
        self.assertEqual(sheet.engineering_by_id, self.yet)
        sheet.param_line_ids[:1].write({'real': '12', 'adjustment': 2})
        sheet.with_user(self.sup).action_validate()
        self.assertEqual(sheet.approved_by_id, self.sup)
        self.assertTrue(sheet.validated_date)

    def test_06_flujo_desde_la_ruta(self):
        kg = self.env.ref('uom.product_uom_kgm')
        crudo = self.env['product.product'].create({'name': 'crudo flujo', 'type': 'consu', 'uom_id': kg.id})
        self.env['mrp.bom'].create({'product_tmpl_id': crudo.product_tmpl_id.id, 'product_qty': 1, 'product_uom_id': kg.id,
                                    'operation_ids': [(0, 0, {'name': 'Tejer', 'workcenter_id': self.wc_dye.id, 'time_cycle_manual': 5})]})
        dev = self.env['project.project'].create({'name': 'flujo', 'sgi_is_ft': True, 'sgi_dev_product_name': 'x',
                                                  'sgi_dev_product_crudo_id': crudo.id})
        ops = dev._sgi_dev_route_operations()
        self.assertEqual([(p, o.name) for p, o in ops], [(crudo, 'Tejer')])
        self.assertEqual(dev.action_sgi_dev_print_flow()['type'], 'ir.actions.report')
        sin = self.env['project.project'].create({'name': 'sin ruta', 'sgi_is_ft': True, 'sgi_dev_product_name': 'y'})
        with self.assertRaises(UserError):
            sin.action_sgi_dev_print_flow()

# -*- coding: utf-8 -*-
"""57.155.0: una sola ficha por proceso (la de quimibond_ficha_tecnica_tela) con la capa de control del SGI."""
from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged

from ..models.sgi_dev_fabric_sheet import TEJIDO_PARAMS
from ..models.sgi_dev_process_sheet import PARAM_VALIDATOR_JOB_TEJIDO


@tagged('post_install', '-at_install')
class TestDevFabricSheet(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        kg = cls.env.ref('uom.product_uom_kgm')
        m = cls.env.ref('uom.product_uom_meter')
        cls.crudo = cls.env['product.product'].create({'name': 'crudo ficha FS', 'default_code': 'WJFS01HNT170',
                                                       'type': 'consu', 'uom_id': kg.id})
        cls.tenido = cls.env['product.product'].create({'name': 'teñido ficha FS', 'default_code': 'WJFS01ING170',
                                                        'type': 'consu', 'uom_id': kg.id})
        cls.acabado = cls.env['product.product'].create({'name': 'acabado ficha FS', 'default_code': 'WJFS01JNG170',
                                                         'type': 'consu', 'uom_id': m.id})
        cls.dev = cls.env['project.project'].create({
            'name': 'x', 'sgi_is_ft': True, 'sgi_dev_product_name': 'Jersey FS',
            'sgi_dev_product_crudo_id': cls.crudo.id, 'sgi_dev_product_tenido_id': cls.tenido.id,
            'sgi_dev_product_id': cls.acabado.id})
        cls.wc = cls.env['mrp.workcenter'].create({'name': 'CIRCULAR FS prueba'})
        Users = cls.env['res.users'].with_context(no_reset_password=True)
        groups = [cls.env.ref('base.group_user').id, cls.env.ref('mrp.group_mrp_user').id]
        cls.yet = Users.create({'name': 'Procesos FS', 'login': 'sgi_fs_yet', 'group_ids': [(6, 0, groups)]})
        cls.jefe = Users.create({'name': 'Jefe de Manufactura FS', 'login': 'sgi_fs_jefe', 'group_ids': [(6, 0, groups)]})
        job = cls.env['hr.job'].create({'name': 'JEFE DE MANUFACTURA (prueba FS)'})
        cls.env['hr.employee'].create({'name': 'Jefe FS', 'job_id': job.id, 'user_id': cls.jefe.id})
        cls.env['ir.config_parameter'].sudo().set_param(PARAM_VALIDATOR_JOB_TEJIDO, str(job.id))
        cls.reason = cls.env['sgi.dev.option'].create({'kind': 'motivo_ajuste', 'name': 'Ajuste FS prueba'})

    def _tejido(self, **vals):
        return self.env['ficha.tecnica.tejido'].create(dict({
            'articulo': 'WJFS01HNT170', 'product_proceso_id': self.crudo.id, 'project_id': self.dev.id,
            'maquina_tejido': self.wc.id, 'longitud_malla_polea1': 30.0, 'longitud_malla_tol': 0.5,
            'peso_acondicionado': 150.0, 'peso_acondicionado_tol': 12.0, 'peso_acondicionado_tol_minus': 6.0}, **vals))

    def test_01_parametros_en_vivo_y_tolerancia(self):
        sheet = self._tejido()
        self.assertEqual(len(sheet.sgi_reading_ids), len(TEJIDO_PARAMS), "Un renglón por parámetro al crearla")
        malla = sheet.sgi_reading_ids.filtered(lambda r: r.param_key == 'longitud_malla_polea1')
        peso = sheet.sgi_reading_ids.filtered(lambda r: r.param_key == 'peso_acondicionado')
        self.assertEqual((malla.nominal, malla.tolerance), ('30', '± 0.5'))
        self.assertEqual(peso.tolerance, '+12 / −6')
        self.assertEqual(malla.result, 'pendiente')
        malla.real = 30.4
        self.assertEqual(malla.result, 'ok')
        malla.real = 31.0
        self.assertEqual(malla.result, 'fuera')
        peso.real = 145.0
        self.assertEqual(peso.result, 'ok', "150 − 6 = 144: dentro")
        peso.real = 143.0
        self.assertEqual(peso.result, 'fuera')
        sheet.longitud_malla_polea1 = 31.0
        self.assertEqual(malla.result, 'ok', "El nominal se lee en vivo de la ficha")

    def test_02_proponer_medir_validar(self):
        sheet = self._tejido()
        with self.assertRaises(UserError, msg="Primero propone Diseño de Procesos"):
            sheet.with_user(self.jefe).action_sgi_validate()
        sheet.with_user(self.yet).action_sgi_propose()
        self.assertEqual(sheet.sgi_proposed_by_id, self.yet)
        with self.assertRaises(UserError, msg="Diseño de Procesos no valida"):
            sheet.with_user(self.yet).action_sgi_validate()
        with self.assertRaises(UserError, msg="Sin valores reales no se valida"):
            sheet.with_user(self.jefe).action_sgi_validate()
        for reading in sheet.sgi_reading_ids:
            nominal, _up, _down, _unit = reading._sgi_spec()
            reading.real = float(nominal or 0.0)
        malla = sheet.sgi_reading_ids.filtered(lambda r: r.param_key == 'longitud_malla_polea1')
        malla.write({'real': 32.0, 'adjustment': 2})
        with self.assertRaises(UserError, msg="Fuera de tolerancia exige motivo"):
            sheet.with_user(self.jefe).action_sgi_validate()
        malla.reason_id = self.reason
        sheet.with_user(self.yet).action_sgi_measure()
        self.assertEqual(sheet.sgi_measured_by_id, self.yet)
        sheet.with_user(self.jefe).action_sgi_validate()
        self.assertEqual(sheet.sgi_validated_by_id, self.jefe)
        with self.assertRaises(UserError, msg="Validada no se vuelve a proponer"):
            sheet.with_user(self.yet).action_sgi_propose()

    def test_03_fichas_de_proceso_del_proyecto(self):
        tejido = self._tejido()
        action = self.dev.action_sgi_dev_process_sheets()
        wiz = self.env[action['res_model']].with_context(action['context']).create({})
        lines = wiz.line_ids.sorted('sequence')
        self.assertEqual(lines.mapped('stage'), ['crudo', 'tenido', 'acabado'])
        crudo, tenido, acabado = lines
        self.assertEqual(crudo.tejido_id, tejido)
        self.assertEqual(crudo.action_open()['res_id'], tejido.id)
        self.assertFalse(tenido.can_create)
        self.assertIn('tintorería', tenido.status)
        with self.assertRaises(UserError):
            tenido.action_open()
        self.assertTrue(acabado.can_create)
        ctx = acabado.action_open()['context']
        self.assertEqual(ctx['default_product_acabado_id'], self.acabado.id)
        self.assertEqual(ctx['default_tejido_id'], tejido.id, "La de acabado nace sobre la ficha de tejido del crudo")
        self.assertEqual(ctx['default_project_id'], self.dev.id)

    def test_04_validar_corrida_contra_la_ficha(self):
        def mo(product):
            return self.env['mrp.production'].new({'product_id': product.id, 'sgi_dev_project_id': self.dev.id})
        self.assertIn('no tiene ficha de proceso', mo(self.crudo)._sgi_dev_run_sheet_problems()[0])
        sheet = self._tejido()
        self.assertIn('no está validada', mo(self.crudo)._sgi_dev_run_sheet_problems()[0])
        sheet.write({'sgi_proposed_by_id': self.yet.id, 'sgi_validated_by_id': self.jefe.id})
        self.assertEqual(mo(self.crudo)._sgi_dev_run_sheet_problems(), [])
        self.assertEqual(mo(self.tenido)._sgi_dev_run_sheet_problems(), [],
                         "Teñido pasa sin ficha mientras no exista la de tintorería")

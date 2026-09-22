# -*- coding: utf-8 -*-
"""Los valores del CFDI que el módulo de Odoo saca mal: registro patronal del
contrato, SDI/SBC de las líneas del recibo, ClaveEntFed y NumEmpleado, y que
se escriben sobre las llaves del diccionario del módulo estén donde estén.

No corre en el CI (depende de Enterprise); se corre en el shell de Odoo.sh:
``odoo-bin ... --test-tags /quimibond_nomina --stop-after-init``."""
from datetime import date

from odoo.exceptions import UserError
from odoo.tests.common import TransactionCase
from odoo.tools import mute_logger

LOG = 'odoo.addons.quimibond_nomina.models.hr_payslip'


class TestCfdiValues(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env = cls.env
        stype = env['hr.payroll.structure.type'].create({'name': 'QB CFDI prueba'})
        cls.struct = env['hr.payroll.structure'].create({'name': 'QB CFDI prueba', 'code': 'QB_CFDI', 'type_id': stype.id})
        cat = env.ref('hr_payroll.BASIC')
        cls.rules = {}
        for code in ('INT_DAY_WAGE_BASE', 'INT_DAY_WAGE'):
            cls.rules[code] = env['hr.salary.rule'].create({
                'name': code, 'code': code, 'struct_id': cls.struct.id, 'category_id': cat.id,
                'condition_select': 'none', 'amount_select': 'code', 'amount_python_compute': 'result = 0',
            })
        mex = env['res.country.state'].search([('code', '=', 'MEX'), ('country_id.code', '=', 'MX')], limit=1)
        cls.mex = mex or env['res.country.state'].create({
            'name': 'México', 'code': 'MEX', 'country_id': env.ref('base.mx').id})
        cls.work = env['res.partner'].create({'name': 'Planta Toluca', 'state_id': cls.mex.id,
                                              'country_id': env.ref('base.mx').id})
        cls.employee = env['hr.employee'].create({'name': 'Prueba CFDI', 'address_id': cls.work.id,
                                                  'registration_number': 'S-1'})
        cls.company = env.company

    def _recibo(self, lineas=None, employee=None):
        slip = self.env['hr.payslip'].create({
            'name': 'Recibo de prueba', 'employee_id': (employee or self.employee).id,
            'struct_id': self.struct.id, 'date_from': date(2026, 9, 14), 'date_to': date(2026, 9, 20),
        })
        for code, amount in (lineas or {}).items():
            self.env['hr.payslip.line'].create({
                'slip_id': slip.id, 'salary_rule_id': self.rules[code].id, 'name': code,
                'amount': amount, 'quantity': 1.0, 'rate': 100.0, 'sequence': 5,
            })
        return slip

    def test_sdi_y_sbc_salen_de_las_lineas(self):
        slip = self._recibo({'INT_DAY_WAGE_BASE': 889.43, 'INT_DAY_WAGE': 889.43})
        vals = slip._qb_nomina_cfdi_values()
        self.assertEqual(vals['salario_diario_integrado'], '889.43')
        self.assertEqual(vals['salario_base_cot_apor'], '889.43')
        self.assertEqual((vals['sdi'], vals['sbc']), (889.43, 889.43))

    def test_registro_patronal_del_contrato_o_de_la_compania(self):
        slip = self._recibo()
        self.company.l10n_mx_imss_id = 'C-675994510-1'
        self.assertEqual(slip._qb_nomina_cfdi_values()['registro_patronal'], 'C-675994510-1')
        slip.version_id.l10n_mx_employer_registration = 'Y6087828106'
        self.assertEqual(slip._qb_nomina_cfdi_values()['registro_patronal'], 'Y6087828106')

    def test_clave_ent_fed_de_la_direccion_laboral(self):
        slip = self._recibo()
        self.assertEqual(slip._qb_nomina_cfdi_values()['clave_ent_fed'], 'MEX')

    def test_clave_ent_fed_cae_a_la_compania_y_al_parametro(self):
        sin_direccion = self.env['hr.employee'].create({'name': 'Sin dirección', 'address_id': False,
                                                        'registration_number': 'S-2'})
        slip = self._recibo(employee=sin_direccion)
        self.company.state_id = self.mex
        self.assertEqual(slip._qb_nomina_cfdi_values()['clave_ent_fed'], 'MEX')
        self.company.state_id = False
        self.env['ir.config_parameter'].sudo().set_param('quimibond_nomina.clave_ent_fed', 'cmx')
        self.assertEqual(slip._qb_nomina_cfdi_values()['clave_ent_fed'], 'CMX')

    @mute_logger(LOG)
    def test_num_empleado_solo_la_referencia(self):
        slip = self._recibo()
        self.employee.registration_number = 'C-32'
        self.assertEqual(slip._qb_nomina_cfdi_values()['num_empleado'], 'C-32')
        # Sin referencia se detiene el CFDI (NumEmpleado es requerido): no se
        # inventa con la credencial (la de Ricardo es 041460744711 y NOI manda
        # 32) ni con el id de Odoo.
        self.employee.barcode = '041460744711'
        self.employee.registration_number = False
        with self.assertRaises(UserError):
            slip._qb_nomina_cfdi_values()
        self.employee.registration_number = 'S|1'
        with self.assertRaises(UserError):
            slip._qb_nomina_cfdi_values()
        self.employee.registration_number = 'S-' + '1' * 14
        with self.assertRaises(UserError):
            slip._qb_nomina_cfdi_values()

    @mute_logger(LOG)
    def test_parche_sobrescribe_num_empleado_del_modulo(self):
        slip = self._recibo({'INT_DAY_WAGE_BASE': 411.32, 'INT_DAY_WAGE': 411.32})
        self.employee.registration_number = 'C-32'
        cv = {'nomina_receptor': {'salario_diario_integrado': 0, 'salario_base_cot_apor': 0,
                                  'num_empleado': '041460744711'}}
        slip._qb_nomina_patch_cfdi_values(cv, slip._qb_nomina_cfdi_values())
        self.assertEqual(cv['nomina_receptor']['num_empleado'], 'C-32')
        self.assertEqual(cv['nomina_receptor']['salario_diario_integrado'], 411.32)

    def test_parche_sobre_el_diccionario_del_modulo(self):
        slip = self._recibo({'INT_DAY_WAGE_BASE': 889.43, 'INT_DAY_WAGE': 889.43})
        self.company.l10n_mx_imss_id = 'C-675994510-1'
        cv = {
            'nomina_emisor': {'registro_patronal': 'C-675994510-1', 'curp': 'X'},
            'nomina_receptor': {'salario_diario_integrado': 517.13, 'salario_base_cot_apor': 0,
                                'clave_ent_fed': False, 'num_empleado': False, 'curp': 'Y'},
            'otra': 'cosa',
        }
        vals = slip._qb_nomina_cfdi_values()
        tocadas = slip._qb_nomina_patch_cfdi_values(cv, vals)
        self.assertEqual(sorted(tocadas), ['clave_ent_fed', 'num_empleado', 'registro_patronal',
                                           'salario_base_cot_apor', 'salario_diario_integrado'])
        self.assertEqual(cv['nomina_receptor']['salario_diario_integrado'], 889.43)
        self.assertEqual(cv['nomina_receptor']['salario_base_cot_apor'], 889.43)
        self.assertEqual(cv['nomina_receptor']['clave_ent_fed'], 'MEX')
        self.assertEqual(cv['nomina_receptor']['num_empleado'], 'S-1')
        self.assertEqual(cv['nomina_receptor']['curp'], 'Y')          # lo demás no se toca
        self.assertEqual(cv['otra'], 'cosa')

    def test_parche_respeta_el_tipo_texto(self):
        slip = self._recibo({'INT_DAY_WAGE_BASE': 889.43, 'INT_DAY_WAGE': 889.43})
        cv = {'salario_diario_integrado': '517.13', 'salario_base_cot_apor': '0.00'}
        slip._qb_nomina_patch_cfdi_values(cv, slip._qb_nomina_cfdi_values())
        self.assertEqual(cv, {'salario_diario_integrado': '889.43', 'salario_base_cot_apor': '889.43'})

    @mute_logger(LOG)
    def test_parche_no_inventa_llaves(self):
        slip = self._recibo({'INT_DAY_WAGE_BASE': 889.43, 'INT_DAY_WAGE': 889.43})
        cv = {'receptor': {'rfc': 'X'}}
        self.assertEqual(slip._qb_nomina_patch_cfdi_values(cv, slip._qb_nomina_cfdi_values()), [])
        self.assertEqual(cv, {'receptor': {'rfc': 'X'}})

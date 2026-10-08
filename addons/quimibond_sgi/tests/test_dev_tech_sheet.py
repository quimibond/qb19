# -*- coding: utf-8 -*-
"""57.132.0 (C1, Jose 5.3): ficha técnica interna y especificaciones del producto desde la tabla."""
from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged

from ..models.sgi_dev_tech_sheet import PARAM_SIGN_JOBS


@tagged('post_install', '-at_install')
class TestDevTechSheet(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        m = cls.env.ref('uom.product_uom_meter')
        cls.partner = cls.env['res.partner'].create({'name': 'CLIENTE FICHA PRUEBA', 'is_company': True})
        cls.tela = cls.env['product.product'].create({'name': 'tela ficha', 'default_code': 'WJ150Q28JNT173X',
                                                      'type': 'consu', 'is_storable': True, 'uom_id': m.id})
        Job = cls.env['hr.job']
        cls.job_calidad = Job.create({'name': 'JEFE DE CALIDAD (prueba ficha)'})
        cls.job_ventas = Job.create({'name': 'ADMINISTRADOR DE VENTAS Y MARKETING (prueba ficha)'})
        Users = cls.env['res.users'].with_context(no_reset_password=True)
        grp = cls.env.ref('base.group_user').id
        cls.calidad = Users.create({'name': 'Calidad ficha', 'login': 'sgi_dev_ficha_calidad', 'group_ids': [(6, 0, [grp])]})
        cls.ventas = Users.create({'name': 'Ventas ficha', 'login': 'sgi_dev_ficha_ventas', 'group_ids': [(6, 0, [grp])]})
        cls.otro = Users.create({'name': 'Otro ficha', 'login': 'sgi_dev_ficha_otro', 'group_ids': [(6, 0, [grp])]})
        Emp = cls.env['hr.employee']
        Emp.create({'name': 'Calidad ficha', 'job_id': cls.job_calidad.id, 'user_id': cls.calidad.id})
        Emp.create({'name': 'Ventas ficha', 'job_id': cls.job_ventas.id, 'user_id': cls.ventas.id})
        cls.env['ir.config_parameter'].sudo().set_param(PARAM_SIGN_JOBS, "%d,%d" % (cls.job_calidad.id, cls.job_ventas.id))
        Car = cls.env['ficha.tecnica.caracteristica']
        cls.car_masa = Car.search([('code', '=', 'masa')], limit=1)
        cls.car_ancho = Car.search([('code', '=', 'ancho')], limit=1)
        cls.cuidado = cls.env['sgi.dev.option'].create({'kind': 'cuidado', 'name': 'No planchar', 'name_en': 'Do not iron'})

    def _dev(self):
        dev = self.env['project.project'].create({'name': 'x', 'sgi_is_ft': True, 'partner_id': self.partner.id,
                                                  'sgi_dev_product_name': 'Jersey ficha', 'sgi_dev_product_id': self.tela.id,
                                                  'sgi_dev_use': 'Forro de calzado'})
        Line = self.env['sgi.dev.characteristic']
        Line.create({'project_id': dev.id, 'caracteristica_id': self.car_masa.id, 'spec_nominal': 150.0,
                     'spec_tol_minus': 5.0, 'spec_tol_plus': 5.0, 'spec_tol_pct': True,
                     'ctrl_tol_minus': 2.0, 'ctrl_tol_plus': 2.0, 'in_customer_spec': True})
        Line.create({'project_id': dev.id, 'caracteristica_id': self.car_ancho.id, 'spec_nominal': 1.7,
                     'spec_tol_minus': 0.02, 'spec_tol_plus': 0.02, 'in_customer_spec': False})
        return dev

    def test_01_ficha_interna_firmas_y_aprobacion(self):
        dev = self._dev()
        sheet = self.env['sgi.dev.tech.sheet'].create({'project_id': dev.id})
        self.assertEqual(len(sheet.line_ids), 2, "La ficha interna lleva toda la tabla")
        masa = sheet.line_ids.filtered(lambda l: l.caracteristica_id == self.car_masa)
        self.assertEqual(masa.spec_label, dev.sgi_dev_line_ids[0].spec_label)
        self.assertTrue(masa.ctrl_label, "Con control interno")
        self.assertEqual(len(sheet.sign_ids), 2, "Un renglón de firma por puesto del parámetro")
        with self.assertRaises(UserError, msg="Un usuario sin puesto en la lista no firma"):
            sheet.with_user(self.otro).action_sign()
        sheet.with_user(self.calidad).action_sign()
        self.assertEqual(sheet.sign_ids.filtered(lambda s: s.job_id == self.job_calidad).user_id, self.calidad)
        self.assertEqual(sheet.signed_count, 1)
        self.assertTrue(sheet.with_user(self.ventas).can_sign)
        self.assertFalse(sheet.with_user(self.calidad).can_sign, "Ya firmó")
        html = self.env['ir.actions.report']._render_qweb_html('quimibond_sgi.report_dev_tech_sheet_document', sheet.ids)[0].decode()
        self.assertIn(masa.ctrl_label, html, "La ficha interna sí imprime el control interno")
        self.assertIn('Calidad ficha', html)
        sheet.with_user(self.ventas).action_approve()
        self.assertEqual(sheet.state, 'vigente')
        self.assertEqual(sheet.approved_by_id, self.ventas)
        self.assertTrue(sheet.attachment_id)
        with self.assertRaises(UserError, msg="Emitida no se borra"):
            sheet.unlink()
        with self.assertRaises(UserError, msg="Emitida no se recarga"):
            sheet.action_load_lines()
        otra = self.env['sgi.dev.tech.sheet'].create({'project_id': dev.id})
        otra.action_approve()
        self.assertEqual(sheet.state, 'sustituida', "La nueva vigente sustituye a la anterior")
        self.assertEqual(dev.sgi_dev_tech_sheet_count, 2)

    def test_02_especificaciones_solo_tolerancia_del_cliente(self):
        dev = self._dev()
        spec = self.env['sgi.dev.customer.spec'].create({'project_id': dev.id, 'care_ids': [(6, 0, self.cuidado.ids)]})
        self.assertEqual(len(spec.line_ids), 1, "Solo los renglones «En especificación del cliente»")
        line = spec.line_ids
        self.assertEqual(line.caracteristica_id, self.car_masa)
        self.assertFalse(line.ctrl_defined, "El control interno no se copia")
        self.assertFalse(line.ctrl_label)
        self.assertEqual(spec.main_use, 'Forro de calzado')
        html = self.env['ir.actions.report']._render_qweb_html('quimibond_sgi.report_dev_customer_spec_document', spec.ids)[0].decode()
        self.assertIn(line.spec_label, html.replace('&#177;', '±').replace('&#178;', '²'))
        self.assertIn('Do not iron', html)
        self.assertIn('Forro de calzado', html)
        self.assertNotIn('Control interno', html)
        spec.with_user(self.ventas).action_emit()
        self.assertEqual(spec.state, 'vigente')
        self.assertEqual(spec.emitted_by_id, self.ventas)
        self.assertTrue(spec.attachment_id)
        self.assertEqual(dev.sgi_dev_customer_spec_count, 1)
        dev.sgi_dev_line_ids.write({'in_customer_spec': False})
        with self.assertRaises(UserError, msg="Sin renglones para el cliente no hay documento"):
            dev.action_sgi_dev_new_customer_spec()

    def test_03_puestos_que_firman_por_nombre(self):
        Param = self.env['ir.config_parameter'].sudo()
        Param.set_param(PARAM_SIGN_JOBS, False)
        found, missing = self.env['sgi.dev.tech.sheet']._sgi_dev_set_sign_jobs_default()
        self.assertIn(self.job_calidad, found)
        self.assertIn(self.job_ventas, found)
        self.assertEqual(len(found) + len(missing), 6, "Seis puestos del brief: encontrados más faltantes")
        self.assertTrue(Param.get_param(PARAM_SIGN_JOBS))
        found2, missing2 = self.env['sgi.dev.tech.sheet']._sgi_dev_set_sign_jobs_default()
        self.assertEqual(found2, found, "Con el parámetro lleno no se toca")
        self.assertEqual(missing2, [])

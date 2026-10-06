# -*- coding: utf-8 -*-
"""Entrega 1 de la auditoría 2026-09, seguridad.

- F-003: el asistente «Firmar desde el registro» ya no manda plantillas de
  Sign como superusuario.
- F-004: los exámenes médicos solo los ve el grupo «Salud ocupacional (SGI)»
  (vacío al instalar) y el Jefe MAST; ya no «Empleados / Encargado»."""
import io

from odoo.exceptions import AccessError, UserError
from odoo.tests import TransactionCase, new_test_user, tagged
from odoo.tools.pdf import PdfFileWriter


def _blank_pdf():
    writer = PdfFileWriter()
    writer.add_blank_page(612, 792)
    out = io.BytesIO()
    writer.write(out)
    return out.getvalue()


@tagged('post_install', '-at_install')
class TestSecurityEntrega1(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env = cls.env
        cls.sgi_user = new_test_user(env, login='e1_sgi_user', email='e1.user@example.com',
                                     groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.hr_user = new_test_user(env, login='e1_hr_user', email='e1.hr@example.com',
                                    groups='base.group_user,hr.group_hr_user')
        cls.health_user = new_test_user(env, login='e1_health', email='e1.health@example.com',
                                        groups='base.group_user,quimibond_sgi.group_sgi_health')
        cls.partner = env['res.partner'].create({'name': 'Firmante E1', 'email': 'firmante.e1@example.com'})
        cls.product = env['product.template'].create({'name': 'Tela E1'})

    # ---- F-003 -------------------------------------------------------------
    def _template(self):
        role = self.env['sign.item.role'].search([], limit=1)
        if not role:
            self.skipTest("Base sin papeles de Sign.")
        return self.env['sgi.sign.builder']._sgi_template('Plantilla E1', _blank_pdf(), 1, [(0, role)])

    def test_01_sign_wizard_rejects_models_outside_sgi(self):
        wizard = self.env['sgi.sign.request.wizard'].create({
            'res_model': 'res.partner', 'res_id': self.partner.id,
            'template_id': self._template().id, 'partner_id': self.partner.id})
        with self.assertRaises(UserError):
            wizard.action_confirm()

    def test_02_sign_wizard_needs_access_to_the_template(self):
        """Un usuario del SGI sin acceso a la plantilla ya no la puede mandar
        (antes se leía y se mandaba con sudo)."""
        template = self._template()
        wizard = self.env['sgi.sign.request.wizard'].with_user(self.sgi_user).create({
            'res_model': 'product.template', 'res_id': self.product.id,
            'template_id': template.id, 'partner_id': self.partner.id})
        with self.assertRaises(AccessError):
            wizard.action_confirm()

    def test_03_sign_wizard_still_works_with_sign_rights(self):
        template = self._template()
        wizard = self.env['sgi.sign.request.wizard'].create({
            'res_model': 'product.template', 'res_id': self.product.id,
            'template_id': template.id, 'partner_id': self.partner.id})
        action = wizard.action_confirm()
        request = self.env['sign.request'].browse(action['res_id'])
        self.assertEqual(request.reference_doc, self.product)

    def test_04_sign_wizard_is_not_for_every_employee(self):
        """El ACL del asistente es del Usuario SGI, no de cualquier interno."""
        groups = self.env['ir.model.access'].search([
            ('model_id.model', '=', 'sgi.sign.request.wizard')]).mapped('group_id')
        self.assertEqual(groups, self.env.ref('quimibond_sgi.group_sgi_user'))

    def test_04b_sign_button_only_for_who_can_sign(self):
        """I-014: el botón «Firmar» solo se ve si además hay acceso a Firma
        electrónica; antes lo veía cualquier Usuario SGI y fallaba al confirmar."""
        self.assertFalse(self.product.with_user(self.sgi_user).sgi_can_sign)
        signer = new_test_user(self.env, login='e1_signer', email='e1.signer@example.com',
                               groups='base.group_user,quimibond_sgi.group_sgi_user,sign.group_sign_user')
        self.assertTrue(self.product.with_user(signer).sgi_can_sign)
        variant = self.product.product_variant_id
        self.assertTrue(variant.with_user(signer).sgi_can_sign)

    # ---- F-004 -------------------------------------------------------------
    def test_05_health_group_is_empty_on_install(self):
        group = self.env.ref('quimibond_sgi.group_sgi_health')
        real_users = group.user_ids - self.health_user
        self.assertFalse(real_users, "El grupo nace vacío: Dirección decide quién entra.")

    def test_06_hr_officer_no_longer_reads_health_records(self):
        employee = self.env['hr.employee'].create({'name': 'Trabajador E1'})
        record = self.env['sgi.health.record'].create({
            'employee_id': employee.id, 'name': 'Audiometría', 'kind': 'examen_medico'})
        with self.assertRaises(AccessError):
            record.with_user(self.hr_user).read(['result'])
        with self.assertRaises(AccessError):
            self.env['sgi.health.record'].with_user(self.hr_user).search([])
        self.assertEqual(record.with_user(self.health_user).read(['name'])[0]['name'], 'Audiometría')
        mast = new_test_user(self.env, login='e1_mast', groups='base.group_user,quimibond_sgi.group_sgi_manager')
        self.assertTrue(record.with_user(mast).read(['name']))

    def test_07_health_menu_and_tab_follow_the_group(self):
        groups = self.env.ref('quimibond_sgi.menu_sgi_health_records').group_ids
        self.assertIn(self.env.ref('quimibond_sgi.group_sgi_health'), groups)
        self.assertNotIn(self.env.ref('hr.group_hr_user'), groups)

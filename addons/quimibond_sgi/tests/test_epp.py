# -*- coding: utf-8 -*-
"""PER-2 (19.0.48.0.0): EPP del puesto con responsiva de entrega firmada por
el empleado, visible en Mi procedimiento y en el PDF."""
from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged, new_test_user

from .common_documents import sgi_hide_real_documents


@tagged('post_install', '-at_install')
class TestEpp(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        cls.job = cls.env['hr.job'].create({
            'name': 'OPERADOR EPP PRUEBA',
            'sgi_epp_required': "Casco\nLentes de seguridad\nGuantes de nitrilo"})
        cls.user_emp = new_test_user(cls.env, login='epp_emp',
                                     groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.user_rh = new_test_user(cls.env, login='epp_rh',
                                    groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.emp = cls.env['hr.employee'].create({
            'name': 'Emp EPP', 'job_id': cls.job.id, 'user_id': cls.user_emp.id})
        cls.other = cls.env['hr.employee'].create({
            'name': 'Otro EPP', 'job_id': cls.job.id, 'user_id': cls.user_rh.id})

    def _deliver(self):
        action = self.emp.with_user(self.user_rh).action_sgi_deliver_epp()
        self.assertEqual(action['res_model'], 'sgi.epp.delivery')
        self.assertEqual(action['context']['default_employee_id'], self.emp.id)
        return self.env['sgi.epp.delivery'].with_user(self.user_rh).with_context(
            action['context']).create({})

    def test_01_entrega_propone_el_epp_del_puesto_y_solo_firma_el_empleado(self):
        delivery = self._deliver()
        self.assertTrue(delivery.name.startswith('EPP-'))
        self.assertEqual(delivery.job_id, self.job)
        self.assertIn('Casco', delivery.items)
        self.assertEqual(delivery.state, 'entregada')
        self.assertEqual(self.emp.sgi_epp_pending_count, 1)
        # Otro usuario no firma por él.
        with self.assertRaises(UserError):
            delivery.with_user(self.user_rh).action_sign()
        with self.assertRaises(UserError):
            delivery.with_user(self.user_rh).write({'state': 'firmada'})
        self.assertFalse(delivery.with_user(self.user_rh).can_sign)
        # El propio empleado sí.
        mine = delivery.with_user(self.user_emp)
        self.assertTrue(mine.can_sign)
        mine.action_sign()
        self.assertEqual(delivery.state, 'firmada')
        self.assertTrue(delivery.signed_date)
        self.assertEqual(self.emp.sgi_epp_pending_count, 0)
        # Firmada no se edita: entrega nueva.
        with self.assertRaises(UserError):
            delivery.with_user(self.user_rh).write({'items': 'Otra cosa'})
        self.assertTrue(any('firmada' in (m.body or '').lower() for m in delivery.message_ids))

    def test_02_mi_procedimiento_y_pdf_traen_el_epp(self):
        wiz = self.env['sgi.my.procedure'].with_user(self.user_emp).create({'employee_id': self.emp.id})
        self.assertIn('Lentes de seguridad', wiz.epp_required)
        self.assertFalse(wiz.epp_pending_sign)
        delivery = self._deliver()
        wiz = self.env['sgi.my.procedure'].with_user(self.user_emp).create({'employee_id': self.emp.id})
        self.assertIn(delivery, wiz.epp_delivery_ids)
        self.assertTrue(wiz.epp_pending_sign)
        wiz.epp_delivery_ids.action_sign()
        self.assertEqual(delivery.state, 'firmada')
        # PDF: bloque de EPP con el texto del puesto y la responsiva firmada.
        html = self.env['ir.actions.report'].with_context(sgi_mp_employee_id=self.emp.id)._render_qweb_html(
            'quimibond_sgi.report_my_procedure_document', self.job.ids)[0].decode()
        for text in ('Equipo de protección personal', 'Guantes de nitrilo', delivery.name, 'firmada el'):
            self.assertIn(text, html)

    def test_03_el_epp_es_contenido_del_procedimiento(self):
        first = self.job._sgi_my_procedure_data()
        self.assertEqual(first['epp'], ['Casco', 'Lentes de seguridad', 'Guantes de nitrilo'])
        self.job.sgi_epp_required = "Casco\nLentes de seguridad\nGuantes de nitrilo\nProtección auditiva"
        self.assertNotEqual(first['hash'], self.job._sgi_my_procedure_data()['hash'],
                            "Cambiar el EPP obliga a releer y firmar.")

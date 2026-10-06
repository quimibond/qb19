# -*- coding: utf-8 -*-
"""57.62.0 — laboratorio de Calidad sobre lo que ya existe: equipo marcado de
laboratorio, verificación como tipo de calibración, préstamo en el
seguimiento del equipo y su formato controlado."""
from odoo import fields
from odoo.tests import TransactionCase, tagged

from .common_documents import sgi_hide_real_documents


@tagged('post_install', '-at_install')
class TestLaboratorio(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        Map = cls.env['sgi.format.map']
        # Los mapeos son noupdate y MAST los edita: la prueba fija los suyos.
        cls.env.ref('quimibond_sgi.format_map_lab_verification').write({
            'sgi_code': 'F-P-C05-11', 'active': True,
            'record_domain': "[('calibration_type', '=', 'verificacion')]"})
        cls.env.ref('quimibond_sgi.format_map_lab_equipment').write({
            'sgi_code': 'F-IT-P-C05-06-07', 'active': True,
            'record_domain': "[('sgi_is_lab', '=', True)]"})
        cls.env.ref('quimibond_sgi.format_ref_lab_loan').write(
            {'sgi_code': 'F-P-C05-07', 'active': True})
        # Otros mapeos de estos modelos que existan en la base no deben ganar.
        Map.search([('model_name', 'in', ('sgi.calibration', 'maintenance.equipment')),
                    ('id', 'not in', (cls.env.ref('quimibond_sgi.format_map_lab_verification').id,
                                      cls.env.ref('quimibond_sgi.format_map_lab_equipment').id))]
                   ).write({'active': False})
        cls.equipment = cls.env['maintenance.equipment'].create({
            'name': 'ZL Balanza de laboratorio', 'sgi_is_measuring': True, 'sgi_is_lab': True,
            'sgi_calibration_interval_months': 12,
            'sgi_last_calibration_date': '2026-01-15'})
        cls.employee = cls.env['hr.employee'].create({'name': 'ZL Analista prueba'})

    def _verify(self, result):
        return self.env['sgi.calibration'].create({
            'equipment_id': self.equipment.id, 'calibration_type': 'verificacion',
            'result': result, 'date': '2026-09-30'})

    def test_01_verificacion_conforme_no_mueve_fechas(self):
        next_before = self.equipment.sgi_next_calibration_date
        ver = self._verify('conforme')
        self.assertFalse(ver.next_date)
        self.assertEqual(str(self.equipment.sgi_last_calibration_date), '2026-01-15')
        self.assertEqual(self.equipment.sgi_next_calibration_date, next_before)
        self.assertFalse(self.equipment.sgi_do_not_use)

    def test_02_verificacion_fuera_bloquea(self):
        self._verify('fuera_tolerancia')
        self.assertTrue(self.equipment.sgi_do_not_use)
        self.assertEqual(str(self.equipment.sgi_last_calibration_date), '2026-01-15')
        # Una verificación conforme posterior no lo desbloquea: hace falta calibrar.
        self._verify('conforme')
        self.assertTrue(self.equipment.sgi_do_not_use)

    def test_03_formatos_del_laboratorio(self):
        self.assertEqual(self._verify('conforme').sgi_format_info(), "F-P-C05-11")
        external = self.env['sgi.calibration'].create({
            'equipment_id': self.equipment.id, 'calibration_type': 'externa',
            'certificate_ref': 'CERT-ZL', 'date': '2026-09-30'})
        self.assertFalse(external.sgi_format_info(), "La calibración no lleva formato general.")
        self.assertEqual(self.equipment.sgi_format_info(), "F-IT-P-C05-06-07")
        other = self.env['maintenance.equipment'].create({'name': 'ZL Montacargas'})
        self.assertFalse(other.sgi_format_info())
        self.assertEqual(self.equipment.sgi_loan_format_label, "F-P-C05-07")

    def test_04_prestamo(self):
        self.equipment.sgi_loan_employee_id = self.employee
        self.assertEqual(self.equipment.sgi_loan_date, fields.Date.context_today(self.equipment))
        self.equipment.sgi_loan_employee_id = False
        self.assertFalse(self.equipment.sgi_loan_date)

    def test_05_menus_y_acciones(self):
        for xmlid, action in (
                ('quimibond_sgi.menu_sgi_lab_equipment', 'quimibond_sgi.sgi_equipment_action_lab'),
                ('quimibond_sgi.menu_sgi_lab_verifications',
                 'quimibond_sgi.sgi_calibration_action_lab')):
            menu = self.env.ref(xmlid)
            self.assertEqual(menu.action, self.env.ref(action))
            self.assertEqual(menu.parent_id, self.env.ref('quimibond_sgi.menu_sgi_metrology'))
        lab = self.env['maintenance.equipment'].search([('sgi_is_lab', '=', True)])
        self.assertIn(self.equipment, lab)

# -*- coding: utf-8 -*-
from datetime import date

from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestCalibration(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Equipment = cls.env['maintenance.equipment']
        cls.Calibration = cls.env['sgi.calibration']
        cls.equipment = cls.Equipment.create({
            'name': 'Vernier digital 0-150mm',
            'sgi_is_measuring': True,
            'sgi_calibration_interval_months': 12,
        })

    def test_01_out_of_tolerance_locks_equipment(self):
        cal = self.Calibration.create({
            'equipment_id': self.equipment.id,
            'date': date.today(),
            'result': 'fuera_tolerancia',
        })
        self.assertTrue(self.equipment.sgi_do_not_use,
                        "El equipo fuera de tolerancia debe quedar bloqueado.")
        self.assertTrue(cal.sgi_alert_id,
                        "Debe generarse una NC de evaluación de impacto (IATF 7.1.5).")
        self.assertEqual(cal.sgi_alert_id.sgi_classification, 'mayor')

    def test_02_conforme_clears_lock(self):
        self.Calibration.create({
            'equipment_id': self.equipment.id,
            'date': date.today(),
            'result': 'fuera_tolerancia',
        })
        self.assertTrue(self.equipment.sgi_do_not_use)
        # Una calibración conforme posterior libera el candado.
        self.Calibration.create({
            'equipment_id': self.equipment.id,
            'date': date.today(),
            'result': 'conforme',
        })
        self.assertFalse(self.equipment.sgi_do_not_use,
                         "La calibración conforme debe liberar el candado.")

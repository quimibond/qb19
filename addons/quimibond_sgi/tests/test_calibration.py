# -*- coding: utf-8 -*-
from datetime import date, timedelta

from odoo.exceptions import ValidationError
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

    def test_03_lab_date_persists_over_interval(self):
        # El laboratorio fija una fecha distinta a "última + intervalo": debe persistir.
        lab_date = date(2027, 3, 15)
        self.Calibration.create({
            'equipment_id': self.equipment.id,
            'date': date(2026, 1, 10),
            'result': 'conforme',
            'next_date': lab_date,
        })
        self.equipment.invalidate_recordset(['sgi_next_calibration_date'])
        self.assertEqual(
            self.equipment.sgi_next_calibration_date, lab_date,
            "La fecha fijada por el laboratorio debe prevalecer sobre el recálculo.")

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


@tagged('post_install', '-at_install')
class TestCalibrationBlocksMeasurement(TransactionCase):
    """OLA A paso 2 — bloqueo real: un gauge vencido o fuera de tolerancia no
    puede usarse para dictaminar una inspección de calidad (IATF 7.1.5.2.1)."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.product = cls.env['product.product'].create({
            'name': 'Tela bloqueo test', 'type': 'consu'})
        cls.point = cls.env['quality.point'].create({
            'title': 'Gramaje bloqueo test',
            'test_type_id': cls.env.ref('quality_control.test_type_measure').id,
            'picking_type_ids': [(4, cls.env.ref('stock.picking_type_in').id)],
        })

    def _equipment(self, **vals):
        base = {'name': 'Balanza', 'sgi_is_measuring': True,
                'sgi_calibration_interval_months': 12}
        base.update(vals)
        return self.env['maintenance.equipment'].create(base)

    def _new_check(self, equipment):
        return self.env['quality.check'].create({
            'point_id': self.point.id, 'product_id': self.product.id,
            'sgi_equipment_id': equipment.id, 'measure': 10.0})

    def test_01_point_equipment_inherited_by_check(self):
        eq = self._equipment(sgi_last_calibration_date=date.today())
        self.point.sgi_equipment_id = eq.id
        check = self.env['quality.check'].create({
            'point_id': self.point.id, 'product_id': self.product.id,
            'measure': 10.0})
        self.assertEqual(check.sgi_equipment_id, eq,
                         "La inspección hereda el equipo del punto de control.")

    def test_02_blocked_equipment_cannot_pass(self):
        eq = self._equipment(sgi_last_calibration_date=date.today(),
                             sgi_do_not_use=True)
        check = self._new_check(eq)
        with self.assertRaises(ValidationError):
            check.quality_state = 'pass'

    def test_03_expired_equipment_cannot_pass_even_without_cron(self):
        # Vencida ayer; el cron NO ha corrido (sgi_do_not_use sigue False).
        eq = self._equipment(
            sgi_last_calibration_date=date.today() - timedelta(days=400))
        self.assertFalse(eq.sgi_do_not_use, "Sin cron, la bandera sigue apagada.")
        self.assertTrue(eq.sgi_next_calibration_date < date.today())
        check = self._new_check(eq)
        with self.assertRaises(ValidationError):
            check.quality_state = 'fail'

    def test_04_calibrated_equipment_passes(self):
        eq = self._equipment(sgi_last_calibration_date=date.today())
        self.assertEqual(eq.sgi_calibration_state, 'vigente')
        check = self._new_check(eq)
        check.quality_state = 'pass'
        self.assertEqual(check.quality_state, 'pass',
                         "Un equipo vigente sí dictamina la inspección.")

    def test_05_no_equipment_no_enforcement(self):
        # Retro-compatibilidad: inspección sin equipo configurado no se bloquea.
        check = self.env['quality.check'].create({
            'point_id': self.point.id, 'product_id': self.product.id,
            'measure': 10.0})
        check.quality_state = 'pass'
        self.assertEqual(check.quality_state, 'pass')

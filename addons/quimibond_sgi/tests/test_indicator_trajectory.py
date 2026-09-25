# -*- coding: utf-8 -*-
"""I-7: escalones trimestrales interpolados, corrección a mano con motivo,
metas por periodo y sentido «dentro de un rango»."""
from datetime import date

from odoo.exceptions import UserError, ValidationError
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestIndicatorTrajectory(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Indicator = cls.env['sgi.indicator']
        cls.Measure = cls.env['sgi.indicator.measure']

    def _indicator(self, code, **vals):
        base = {'code': code, 'name': 'KPI %s' % code, 'direction': 'higher_better',
                'baseline_value': 10.0, 'baseline_date': date(2027, 1, 1),
                'target_objective': 22.0, 'target_acceptable': 19.0,
                'target_date': date(2027, 12, 31)}
        base.update(vals)
        return self.Indicator.create(base)

    def test_01_escalones_interpolados(self):
        ind = self._indicator('ZT-01')
        ind.action_generate_trajectory()
        steps = ind.step_ids.sorted('date_from')
        self.assertEqual(steps.mapped('name'), ['T1 2027', 'T2 2027', 'T3 2027', 'T4 2027'])
        objectives = steps.mapped('objective')
        self.assertEqual(objectives, sorted(objectives), "Crece hacia la meta.")
        self.assertGreater(objectives[0], 10.0)
        self.assertEqual(objectives[-1], 22.0, "El último trimestre llega a la meta final.")
        for step in steps:
            self.assertAlmostEqual(step.objective - step.acceptable, 3.0, places=2,
                                   msg="Misma distancia que la meta final (22 − 19).")
        self.assertFalse(any(steps.mapped('manual')))
        # Regenerar es idempotente.
        ind.action_generate_trajectory()
        self.assertEqual(len(ind.step_ids), 4)

    def test_02_medicion_contra_su_trimestre(self):
        ind = self._indicator('ZT-02')
        ind.action_generate_trajectory()
        q2 = ind.step_ids.filtered(lambda s: s.name == 'T2 2027')
        measure = self.Measure.create({'indicator_id': ind.id, 'period_date': date(2027, 5, 1),
                                       'value': q2.objective, 'state': 'capturado'})
        self.assertEqual(measure.target_objective, q2.objective)
        self.assertEqual(measure.semaphore, 'verde')
        measure.value = q2.acceptable - 0.01
        self.assertEqual(measure.semaphore, 'rojo')
        measure.value = 22.0
        self.assertEqual(measure.semaphore, 'verde')
        # Después de la fecha de la meta: la meta final.
        later = self.Measure.create({'indicator_id': ind.id, 'period_date': date(2028, 3, 1),
                                     'value': 20.0, 'state': 'capturado'})
        self.assertEqual((later.target_objective, later.target_acceptable), (22.0, 19.0))
        self.assertEqual(later.semaphore, 'amarillo')
        # Sin escalones: las metas del indicador.
        plain = self._indicator('ZT-02b')
        m = self.Measure.create({'indicator_id': plain.id, 'period_date': date(2027, 5, 1),
                                 'value': 12.0, 'state': 'capturado'})
        self.assertEqual(m.target_objective, 22.0)
        self.assertEqual(m.semaphore, 'rojo')

    def test_03_correccion_a_mano(self):
        ind = self._indicator('ZT-03')
        ind.action_generate_trajectory()
        q3 = ind.step_ids.filtered(lambda s: s.name == 'T3 2027')
        with self.assertRaises(UserError):
            q3.write({'objective': 15.0})
        q3.write({'objective': 15.0, 'acceptable': 12.0, 'reason': 'Paro de planta en agosto'})
        self.assertTrue(q3.manual)
        bodies = ind.message_ids.mapped('body')
        self.assertTrue(any('corregido a mano' in b and 'Paro de planta' in b for b in bodies))
        ind.action_generate_trajectory()
        q3_after = ind.step_ids.filtered(lambda s: s.name == 'T3 2027')
        self.assertEqual((q3_after.objective, q3_after.acceptable), (15.0, 12.0),
                         "Regenerar respeta el escalón corregido.")
        measure = self.Measure.create({'indicator_id': ind.id, 'period_date': date(2027, 8, 1),
                                       'value': 15.0, 'state': 'capturado'})
        self.assertEqual(measure.semaphore, 'verde')

    def test_04_validaciones(self):
        ind = self._indicator('ZT-04', target_date=date(2026, 12, 31))
        with self.assertRaises(UserError):
            ind.action_generate_trajectory()
        ind.write({'target_date': False})
        with self.assertRaises(UserError):
            ind.action_generate_trajectory()
        with self.assertRaises(ValidationError):
            self.env['sgi.indicator.step'].create({
                'indicator_id': ind.id, 'date_from': date(2027, 2, 1), 'objective': 1.0})

    def test_05_dentro_de_un_rango(self):
        ind = self.Indicator.create({'code': 'ZT-05', 'name': 'pH', 'direction': 'range',
                                     'range_min': 6.5, 'range_max': 7.5, 'range_tolerance': 0.3})
        self.assertNotIn('sin meta', ind._sgi_spec_problems())
        def sem(v):
            m = self.Measure.create({'indicator_id': ind.id, 'period_date': date(2047, sem.n, 1),
                                     'value': v, 'state': 'capturado'})
            sem.n += 1
            return m.semaphore
        sem.n = 1
        self.assertEqual(sem(7.0), 'verde')
        self.assertEqual(sem(6.5), 'verde')
        self.assertEqual(sem(7.7), 'amarillo')
        self.assertEqual(sem(6.3), 'amarillo')
        self.assertEqual(sem(7.9), 'rojo')
        with self.assertRaises(ValidationError):
            ind.write({'range_min': 8.0})
        with self.assertRaises(UserError):
            ind.action_generate_trajectory()

    def test_06_trayectoria_automatica(self):
        """55.0.0: con arranque, fecha de arranque y fecha de meta los escalones
        se generan solos (al guardar y en el cron)."""
        ind = self.env['sgi.indicator'].create({
            'code': 'ZT-AUTO', 'name': 'Auto', 'target_objective': 100.0, 'target_acceptable': 90.0,
            'baseline_value': 40.0, 'baseline_date': date(2046, 1, 1), 'target_date': date(2046, 12, 31)})
        self.assertEqual(len(ind.step_ids), 4, "Cuatro trimestres al crear.")
        ind.step_ids.unlink()
        self.env['sgi.indicator'].cron_missing_trajectories()
        self.assertEqual(len(ind.step_ids), 4, "El cron repone los que falten.")
        ind.write({'target_date': date(2046, 6, 30)})
        self.assertEqual(len(ind.step_ids), 2, "Cambiar la meta regenera.")
        ind.write({'direction': 'range', 'range_min': 1, 'range_max': 2})
        self.assertEqual(len(ind.step_ids), 2, "Con rango no se toca (la trayectoria no aplica).")

# -*- coding: utf-8 -*-
from datetime import date

from odoo.tests import TransactionCase, tagged
from odoo.exceptions import UserError


@tagged('post_install', '-at_install')
class TestIndicators(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Indicator = cls.env['sgi.indicator']
        cls.Measure = cls.env['sgi.indicator.measure']
        cls.Cron = cls.env['sgi.cron']

    def _indicator(self, direction, obj, acc, **vals):
        base = {
            'code': vals.pop('code', 'T-%s-%s' % (direction[:2], obj)),
            'name': 'Indicador prueba',
            'direction': direction,
            'target_objective': obj,
            'target_acceptable': acc,
            'calc_mode': 'manual',
        }
        base.update(vals)
        return self.Indicator.create(base)

    def _measure(self, ind, day, value):
        return self.Measure.create({
            'indicator_id': ind.id, 'period_date': date(2026, day, 1),
            'value': value, 'state': 'capturado'})

    def test_01_semaphore_higher_better(self):
        ind = self._indicator('higher_better', 90.0, 80.0, code='HB-1')
        self.assertEqual(self._measure(ind, 1, 95.0).semaphore, 'verde')
        self.assertEqual(self._measure(ind, 2, 85.0).semaphore, 'amarillo')
        self.assertEqual(self._measure(ind, 3, 70.0).semaphore, 'rojo')

    def test_02_semaphore_lower_better(self):
        ind = self._indicator('lower_better', 5.0, 8.0, code='LB-1')
        self.assertEqual(self._measure(ind, 1, 3.0).semaphore, 'verde')
        self.assertEqual(self._measure(ind, 2, 7.0).semaphore, 'amarillo')
        self.assertEqual(self._measure(ind, 3, 12.0).semaphore, 'rojo')

    def test_02b_pending_has_no_semaphore(self):
        # Una medición pendiente (value=0 por defecto) no debe mostrarse en rojo.
        ind = self._indicator('higher_better', 90.0, 80.0, code='PEND-1')
        measure = self.Measure.create({'indicator_id': ind.id, 'period_date': date(2026, 6, 1)})
        self.assertEqual(measure.state, 'pendiente')
        self.assertFalse(measure.semaphore)
        measure.action_capture()
        self.assertEqual(measure.semaphore, 'rojo')

    def test_03_cron_idempotent(self):
        # Un indicador manual: el cron crea 1 medición del mes anterior y no duplica.
        self._indicator('higher_better', 90.0, 80.0, code='CR-1')
        self.Cron.cron_indicators()
        count1 = self.Measure.search_count([('indicator_id.code', '=', 'CR-1')])
        self.Cron.cron_indicators()
        count2 = self.Measure.search_count([('indicator_id.code', '=', 'CR-1')])
        self.assertEqual(count1, 1)
        self.assertEqual(count2, 1)

    def test_04_nc_on_red_creates_one_alert(self):
        # El validador debe ser manager o responsable; damos manager al usuario de test.
        self.env.user.group_ids = [(4, self.env.ref('quimibond_sgi.group_sgi_manager').id)]
        ind = self._indicator('higher_better', 90.0, 80.0, code='NC-1', nc_on_red=True)
        measure = self.Measure.create({
            'indicator_id': ind.id, 'period_date': date(2026, 4, 1),
            'value': 50.0, 'state': 'capturado'})
        self.assertEqual(measure.semaphore, 'rojo')
        # Validado -> crea NC ligada
        measure.action_validate()
        self.assertTrue(measure.alert_id)
        self.assertEqual(measure.alert_id.sgi_origin_type, 'indicador')
        alert = measure.alert_id
        # Idempotente: revalidar / correr NC de nuevo no duplica
        measure._sgi_maybe_create_nc()
        self.assertEqual(measure.alert_id, alert)
        self.assertEqual(self.env['quality.alert'].search_count([
            ('sgi_indicator_measure_id', '=', measure.id)]), 1)

    def test_05_no_nc_without_flag(self):
        self.env.user.group_ids = [(4, self.env.ref('quimibond_sgi.group_sgi_manager').id)]
        ind = self._indicator('higher_better', 90.0, 80.0, code='NC-2', nc_on_red=False)
        measure = self.Measure.create({
            'indicator_id': ind.id, 'period_date': date(2026, 5, 1),
            'value': 50.0, 'state': 'capturado'})
        measure.action_validate()
        self.assertFalse(measure.alert_id)

    def test_06_validate_requires_manager_or_responsible(self):
        ind = self._indicator('higher_better', 90.0, 80.0, code='VAL-1')
        measure = self.Measure.create({
            'indicator_id': ind.id, 'period_date': date(2026, 5, 1),
            'value': 50.0, 'state': 'capturado'})
        other = self.env['res.users'].create({
            'name': 'Operador', 'login': 'sgi_val_test',
            'group_ids': [(6, 0, [self.env.ref('quimibond_sgi.group_sgi_user').id])]})
        with self.assertRaises(UserError):
            measure.with_user(other).action_validate()

# -*- coding: utf-8 -*-
from datetime import date

from odoo.tests import TransactionCase, tagged


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

    def test_01_semaphore_higher_better(self):
        ind = self._indicator('higher_better', 90.0, 80.0, code='HB-1')
        m_green = self.Measure.create({'indicator_id': ind.id, 'period_date': date(2026, 1, 1), 'value': 95.0})
        m_yellow = self.Measure.create({'indicator_id': ind.id, 'period_date': date(2026, 2, 1), 'value': 85.0})
        m_red = self.Measure.create({'indicator_id': ind.id, 'period_date': date(2026, 3, 1), 'value': 70.0})
        self.assertEqual(m_green.semaphore, 'verde')
        self.assertEqual(m_yellow.semaphore, 'amarillo')
        self.assertEqual(m_red.semaphore, 'rojo')

    def test_02_semaphore_lower_better(self):
        ind = self._indicator('lower_better', 5.0, 8.0, code='LB-1')
        m_green = self.Measure.create({'indicator_id': ind.id, 'period_date': date(2026, 1, 1), 'value': 3.0})
        m_yellow = self.Measure.create({'indicator_id': ind.id, 'period_date': date(2026, 2, 1), 'value': 7.0})
        m_red = self.Measure.create({'indicator_id': ind.id, 'period_date': date(2026, 3, 1), 'value': 12.0})
        self.assertEqual(m_green.semaphore, 'verde')
        self.assertEqual(m_yellow.semaphore, 'amarillo')
        self.assertEqual(m_red.semaphore, 'rojo')

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
        ind = self._indicator('higher_better', 90.0, 80.0, code='NC-1', nc_on_red=True)
        measure = self.Measure.create({
            'indicator_id': ind.id, 'period_date': date(2026, 4, 1), 'value': 50.0})
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
        ind = self._indicator('higher_better', 90.0, 80.0, code='NC-2', nc_on_red=False)
        measure = self.Measure.create({
            'indicator_id': ind.id, 'period_date': date(2026, 5, 1), 'value': 50.0})
        measure.action_validate()
        self.assertFalse(measure.alert_id)

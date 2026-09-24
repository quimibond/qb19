# -*- coding: utf-8 -*-
"""Lógica de indicadores I-1 (detalle por medición) e I-2 (prueba/oficial,
sin dato, muestra chica, medir desde)."""
from datetime import date, timedelta

from odoo import fields
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestIndicatorDetail(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env = cls.env
        cls.Indicator = env['sgi.indicator']
        cls.Measure = env['sgi.indicator.measure']
        cls.Cron = env['sgi.cron']
        env['ir.config_parameter'].sudo().set_param('quimibond_sgi.indicator_min_sample', '5')
        env.user.group_ids = [(4, env.ref('quimibond_sgi.group_sgi_manager').id)]
        cls.model = env['ir.model']._get_id('res.partner')
        cls.deliverable = env['sgi.deliverable'].create({
            'code': 'X-DET-OUT', 'name': 'Contacto con correo', 'odoo_model_id': cls.model,
            'measure_domain': "[('ref', '=', 'X-DET')]", 'measure_date_field': 'create_date',
            'complete_domain': "[('email', '!=', False)]"})
        cls.today = fields.Date.context_today(env['res.partner'])
        cls.monday = cls.today - timedelta(days=cls.today.weekday())

    def _indicator(self, code, **vals):
        return self.Indicator.create(dict({
            'code': code, 'name': 'Detalle %s' % code, 'uom': '%',
            'direction': 'higher_better', 'target_objective': 90, 'target_acceptable': 80,
            'calc_mode': 'entregable_completo', 'deliverable_id': self.deliverable.id,
            'frequency': 'weekly'}, **vals))

    def _partners(self, n_complete, n_total):
        return self.env['res.partner'].create([
            {'name': 'D%d' % i, 'ref': 'X-DET',
             'email': 'd%d@x.test' % i if i < n_complete else False}
            for i in range(n_total)])

    def _measure(self, ind, period=None):
        self.Cron._sgi_generate_measures(
            ind, period or self.monday, period or self.monday,
            (period or self.monday) + timedelta(days=6), self.today, 'semana')
        return self.Measure.search([('indicator_id', '=', ind.id)], limit=1)

    def test_01_measure_stores_detail_and_records(self):
        partners = self._partners(3, 8)
        measure = self._measure(self._indicator('X-D1'))
        self.assertEqual(measure.state, 'capturado')
        self.assertEqual(measure.value, 37.5)
        self.assertEqual((measure.numerator, measure.denominator, measure.sample_size), (3, 8, 8))
        self.assertEqual(measure.detail_model, 'res.partner')
        self.assertFalse(measure.small_sample)
        action = measure.action_view_records()
        self.assertEqual(action['res_model'], 'res.partner')
        self.assertEqual(set(action['domain'][0][2]), set(partners.ids))

    def test_02_no_records_is_sin_dato_not_zero(self):
        ind = self._indicator('X-D2', status='oficial', nc_on_red=True)
        measure = self._measure(ind)
        self.assertEqual(measure.state, 'sin_dato')
        self.assertFalse(measure.semaphore, "Gris: sin semáforo.")
        self.assertFalse(ind.last_semaphore)
        measure.action_validate()
        self.assertEqual(measure.state, 'sin_dato', "Sin dato no se valida.")
        measure._sgi_maybe_create_nc()
        self.assertFalse(measure.alert_id)

    def test_03_small_sample_never_opens_nc(self):
        self._partners(0, 3)
        ind = self._indicator('X-D3', status='oficial', nc_on_red=True)
        measure = self._measure(ind)
        self.assertEqual(measure.semaphore, 'rojo')
        self.assertTrue(measure.small_sample)
        measure.action_validate()
        self.assertFalse(measure.alert_id, "Muestra chica: sin NC.")

    def test_04_only_official_opens_nc(self):
        self._partners(0, 6)
        trial = self._indicator('X-D4A', nc_on_red=True)
        self.assertEqual(trial.status, 'prueba', "Nace en prueba.")
        measure = self._measure(trial)
        measure.action_validate()
        self.assertFalse(measure.alert_id, "En prueba no abre NC.")
        trial.action_set_official()
        measure._sgi_maybe_create_nc()
        self.assertTrue(measure.alert_id, "Oficial, rojo, 6 casos: NC.")

    def test_05_measure_from_skips_earlier_periods(self):
        ind = self._indicator('X-D5', measure_from=self.monday)
        earlier = self.monday - timedelta(days=7)
        self.Cron._sgi_generate_measures(
            ind, earlier, earlier, earlier + timedelta(days=6), self.today, 'semana')
        self.assertFalse(self.Measure.search([('indicator_id', '=', ind.id)]),
                         "Antes de «medir desde» no hay medición.")
        self._partners(1, 6)
        self.assertTrue(self._measure(ind))

    def test_06_aggregate_sums_not_averages(self):
        ind = self._indicator('X-D6')
        a = self.Measure.create({'indicator_id': ind.id, 'period_date': date(2040, 1, 1),
                                 'value': 100.0, 'numerator': 2, 'denominator': 2,
                                 'state': 'capturado'})
        b = self.Measure.create({'indicator_id': ind.id, 'period_date': date(2040, 1, 8),
                                 'value': 0.0, 'numerator': 0, 'denominator': 18,
                                 'state': 'capturado'})
        self.assertEqual(self.Indicator._sgi_aggregate(a | b), 10.0,
                         "2 de 20, no el promedio de 100 y 0.")
        c = self.Measure.create({'indicator_id': ind.id, 'period_date': date(2040, 1, 15),
                                 'value': 50.0, 'state': 'capturado'})
        self.assertIsNone(self.Indicator._sgi_aggregate(a | b | c), "Sin detalle no se suma.")

    def test_07_otif_detail_and_recompute(self):
        ind = self.Indicator.create({
            'code': 'X-D7', 'name': 'OTIF detalle', 'uom': '%', 'calc_mode': 'otif_ventas',
            'target_objective': 90, 'target_acceptable': 80})
        detail = ind._sgi_compute_detail(date(2040, 1, 1), date(2040, 1, 31))
        self.assertEqual(detail['model'], 'stock.picking')
        self.assertEqual(detail['denominator'], len(detail['ids']))
        measure = self.Measure.create({'indicator_id': ind.id, 'period_date': date(2040, 1, 1),
                                       'state': 'pendiente'})
        measure.action_recompute_value()
        self.assertIn(measure.state, ('sin_dato', 'capturado'))
        self.assertEqual(measure.sample_size, len(detail['ids']))

    def test_08_load_status_and_measure_from(self):
        result = self.env['sgi.process'].load_payload({'indicators': [
            {'code': 'X-D8', 'name': 'Cargado', 'calc_mode': 'manual', 'target': 90,
             'unit': '%', 'formula': 'f', 'source': 's', 'frequency': 'monthly',
             'status': 'oficial', 'measure_from': '2026-09-01'}]})
        self.assertTrue(result['ok'], result['errors'])
        ind = self.Indicator.search([('code', '=', 'X-D8')])
        self.assertEqual((ind.status, ind.measure_from), ('oficial', date(2026, 9, 1)))

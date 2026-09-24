# -*- coding: utf-8 -*-
"""I-4 plan de acción en rojo, I-6 calendario de cálculo, I-8 ventana y P-40
validación masiva en la Revisión por la Dirección."""
from datetime import date

from odoo.exceptions import UserError, ValidationError
from odoo.tests import TransactionCase, tagged
from odoo.tests.common import new_test_user

from ..models.sgi_calendar import sgi_nth_business_day


@tagged('post_install', '-at_install')
class TestIndicatorPlan(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env = cls.env
        cls.Indicator = env['sgi.indicator']
        cls.Measure = env['sgi.indicator.measure']
        cls.Activity = env['mail.activity']
        cls.Cron = env['sgi.cron']
        cls.owner = new_test_user(env, login='plan_owner',
                                  groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.manager = new_test_user(env, login='plan_manager',
                                    groups='base.group_user,quimibond_sgi.group_sgi_manager')

    def _red(self, code, period=date(2047, 5, 1), **vals):
        base = {'code': code, 'name': 'KPI %s' % code, 'direction': 'higher_better',
                'target_objective': 90.0, 'target_acceptable': 80.0,
                'responsible_id': self.owner.id, 'status': 'oficial'}
        base.update(vals)
        indicator = self.Indicator.create(base)
        measure = self.Measure.create({'indicator_id': indicator.id, 'period_date': period,
                                       'value': 10.0, 'state': 'capturado'})
        return indicator, measure

    def _plan_activities(self, indicator):
        return self.Activity.search([('res_model', '=', 'sgi.indicator'),
                                     ('res_id', '=', indicator.id),
                                     ('summary', 'ilike', 'Causa y acción')])

    def test_01_ventana(self):
        ind = self.Indicator.create({'code': 'ZW-01', 'name': 'Ventana'})
        self.assertEqual(ind.window_label, 'Mes')
        ind.frequency = 'weekly'
        self.assertEqual(ind.window_label, 'Semana')
        ind.write({'frequency': 'monthly', 'calc_mode': 'desperdicio_kg'})
        self.assertEqual(ind.window_label, '3 meses móviles')
        ind.calc_mode = 'configurable'
        model = self.env['ir.model']._get('sgi.indicator.measure')
        Term = self.env['sgi.indicator.term']
        Term.create({'indicator_id': ind.id, 'role': 'numerator', 'model_id': model.id,
                     'domain': '[]', 'date_field': 'period_date'})
        Term.create({'indicator_id': ind.id, 'role': 'denominator', 'model_id': model.id,
                     'domain': '[]', 'date_field': False, 'window': 'to_date'})
        self.assertEqual(ind.window_label, 'Mes / Acumulado al cierre')
        measure = self.Measure.create({'indicator_id': ind.id, 'period_date': date(2047, 1, 1)})
        self.assertEqual(measure.window_label, ind.window_label)

    def test_02_rojo_pide_plan_y_lo_cierra(self):
        indicator, measure = self._red('ZP-02')
        self.assertTrue(measure.plan_required)
        self.assertEqual(measure.plan_due, date(2047, 6, 10), "Día 10 del mes siguiente.")
        activities = self._plan_activities(indicator)
        self.assertEqual(len(activities), 1)
        self.assertEqual(activities.user_id, self.owner)
        self.assertEqual(activities.date_deadline, date(2047, 6, 10))
        measure.write({'value': 5.0})  # sigue rojo: no duplica
        self.assertEqual(len(self._plan_activities(indicator)), 1)
        measure.cause = 'Paro de máquina'
        self.assertFalse(measure.plan_done, "Causa sin acción no cierra el plan.")
        line = self.env['sgi.action.line'].create({
            'measure_id': measure.id, 'name': 'Reparar', 'responsible_id': self.owner.id,
            'date_commit': date(2047, 6, 5)})
        self.assertTrue(measure.plan_done)
        self.assertFalse(self._plan_activities(indicator), "Con causa y acción se da por hecha.")
        self.assertEqual(line._sgi_origin(), indicator)
        self.assertEqual(line.origin_display, measure.display_name)
        self.assertTrue(line.activity_id, "La acción cuelga su actividad del indicador.")

    def test_02b_en_prueba_pide_plan_sin_actividad(self):
        indicator, measure = self._red('ZP-02B', status='prueba')
        self.assertTrue(measure.plan_required, "La ficha sí pide causa y acción.")
        self.assertFalse(self._plan_activities(indicator), "En prueba no hay actividad.")
        self.Measure._sgi_escalate_red_plans(date(2047, 7, 1))
        self.assertFalse(self.Activity.search([('res_model', '=', 'sgi.indicator'),
                                               ('res_id', '=', indicator.id)]))
        indicator.action_set_official()
        measure.write({'value': 6.0})
        self.assertEqual(len(self._plan_activities(indicator)), 1,
                         "Al pasar a oficial, el siguiente dato rojo agenda.")

    def test_03_sin_plan_escala_a_direccion(self):
        director = new_test_user(self.env, login='plan_director',
                                 groups='base.group_user,quimibond_sgi.group_sgi_director')
        indicator, measure = self._red('ZP-03', period=date(2030, 1, 1))
        self.Measure._sgi_escalate_red_plans(date(2030, 2, 9))
        escalations = self.Activity.search([('res_model', '=', 'sgi.indicator'),
                                            ('res_id', '=', indicator.id),
                                            ('summary', 'ilike', 'escalado a Dirección')])
        self.assertFalse(escalations, "Antes del día 10 no escala.")
        self.Measure._sgi_escalate_red_plans(date(2030, 2, 11))
        escalations = self.Activity.search([('res_model', '=', 'sgi.indicator'),
                                            ('res_id', '=', indicator.id),
                                            ('summary', 'ilike', 'escalado a Dirección')])
        self.assertEqual(len(escalations), 1)
        self.assertTrue(escalations.user_id.has_group('quimibond_sgi.group_sgi_director')
                        or escalations.user_id.has_group('quimibond_sgi.group_sgi_manager'))
        self.Measure._sgi_escalate_red_plans(date(2030, 2, 12))
        self.assertEqual(len(self.Activity.search([('res_model', '=', 'sgi.indicator'),
                                                   ('res_id', '=', indicator.id),
                                                   ('summary', 'ilike', 'escalado a Dirección')])), 1)
        self.assertTrue(director)

    def test_04_xor_origen(self):
        indicator, measure = self._red('ZP-04')
        alert = self.env['quality.alert'].create({'name': 'NC prueba plan'})
        with self.assertRaises(ValidationError):
            self.env['sgi.action.line'].create({
                'measure_id': measure.id, 'alert_id': alert.id, 'name': 'Doble origen',
                'responsible_id': self.owner.id, 'date_commit': date(2047, 6, 5)})

    def test_05_calendario_mensual(self):
        self.env['ir.config_parameter'].sudo().set_param(
            'quimibond_sgi.monthly_measure_business_day', '3')
        run_day = sgi_nth_business_day(self.env, 2047, 8, 3)
        before = run_day.replace(day=1)
        self.assertFalse(self.Cron._sgi_monthly_run_due(before))
        self.assertTrue(self.Cron._sgi_monthly_run_due(run_day))
        after = date(2047, 8, 25)
        self.assertTrue(self.Cron._sgi_monthly_run_due(after), "Julio 2047 sigue sin medir.")
        ind = self.Indicator.create({'code': 'ZC-05', 'name': 'Mensual'})
        self.Measure.create({'indicator_id': ind.id, 'period_date': date(2047, 7, 1)})
        self.assertFalse(self.Cron._sgi_monthly_run_due(after), "Ya se midió julio.")

    def test_06_calendario_semanal(self):
        monday = date(2047, 8, 5)
        self.assertEqual(monday.weekday(), 0)
        self.assertTrue(self.Cron._sgi_weekly_run_due(monday))
        has_weekly = self.Indicator.search_count([('frequency', '=', 'weekly')])
        ind = self.Indicator.create({'code': 'ZC-06', 'name': 'Semanal', 'frequency': 'weekly'})
        tuesday = date(2047, 8, 6)
        self.assertTrue(self.Cron._sgi_weekly_run_due(tuesday), "Semana previa sin medir.")
        self.Measure.create({'indicator_id': ind.id, 'period_date': date(2047, 7, 29)})
        self.assertFalse(self.Cron._sgi_weekly_run_due(tuesday))
        self.assertGreaterEqual(has_weekly, 0)

    def test_07_revision_valida_y_abre_rojos(self):
        indicator, measure = self._red('ZP-07', period=date(2047, 9, 1))
        green = self.Indicator.create({'code': 'ZP-07b', 'name': 'Verde', 'status': 'oficial',
                                       'target_objective': 1.0, 'target_acceptable': 0.5})
        green_measure = self.Measure.create({'indicator_id': green.id, 'period_date': date(2047, 9, 1),
                                             'value': 2.0, 'state': 'capturado'})
        review = self.env['sgi.management.review'].create({
            'period_from': date(2047, 9, 1), 'period_to': date(2047, 9, 30)})
        with self.assertRaises(UserError, msg="Solo el Jefe MAST valida desde la revisión."):
            review.with_user(self.owner).action_validate_measures()
        review = review.with_user(self.manager)
        action = review.action_validate_measures()
        self.assertEqual((measure.state, green_measure.state), ('validado', 'validado'))
        reds = self.Measure.search(action['domain'])
        self.assertIn(measure, reds)
        self.assertNotIn(green_measure, reds)
        measure.cause = 'x'
        self.env['sgi.action.line'].create({
            'measure_id': measure.id, 'name': 'Acción', 'responsible_id': self.owner.id,
            'date_commit': date(2047, 10, 5)})
        action = review.action_validate_measures()
        self.assertNotIn(measure, self.Measure.search(action['domain']))

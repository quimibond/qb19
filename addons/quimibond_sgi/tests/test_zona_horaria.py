# -*- coding: utf-8 -*-
"""57.15.0 (entrega 8, zona horaria y días hábiles; G-007, G-008, G-009,
G-020, G-022, J-011). Datos propios: un calendario de lunes a viernes en
hora de México con los festivos de la LFT de 2027."""
from datetime import date, datetime

from odoo.tests import TransactionCase, tagged

from ..models.sgi_calendar import (
    sgi_add_business_days, sgi_business_days, sgi_lft_holidays, sgi_local_date)
from .common_users import sgi_set_mast


@tagged('post_install', '-at_install')
class TestZonaHorariaDiasHabiles(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.calendar = cls.env['resource.calendar'].create({
            'name': 'ZS SGI México L-V', 'tz': 'America/Mexico_City',
            'attendance_ids': [(5, 0, 0)] + [
                (0, 0, {'name': 'Día %s' % day, 'dayofweek': day, 'hour_from': 8.0,
                        'hour_to': 17.0, 'day_period': 'morning'})
                for day in '01234'],
        })
        cls.env['ir.config_parameter'].sudo().set_param(
            'quimibond_sgi.business_calendar_id', str(cls.calendar.id))
        cls.Config = cls.env['sgi.config']
        cls.Config._sgi_load_holidays([2027], cls.calendar)
        cls.process = cls.env['sgi.process'].create({'code': 'ZZH', 'name': 'ZS Zona horaria'})
        cls.job = cls.env['hr.job'].create({'name': 'ZS PUESTO ZONA HORARIA'})

    def _act(self, **vals):
        return self.env['sgi.process.activity'].create(dict({
            'process_id': self.process.id, 'name': 'Revisar la bitácora',
            'done_criteria': 'La bitácora quedó revisada', 'on_fail': 'Avisar a MAST',
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': self.job.id})]}, **vals))

    def test_01_festivos_lft(self):
        self.assertEqual([d for d, _label in sgi_lft_holidays(2027)], [
            date(2027, 1, 1), date(2027, 2, 1), date(2027, 3, 15), date(2027, 5, 1),
            date(2027, 9, 16), date(2027, 11, 15), date(2027, 12, 25)])
        self.assertIn(date(2030, 10, 1), [d for d, _l in sgi_lft_holidays(2030)],
                      "Cambio del Ejecutivo Federal cada seis años.")
        self.assertNotIn(date(2027, 10, 1), [d for d, _l in sgi_lft_holidays(2027)])

    def test_02_carga_idempotente(self):
        Leave = self.env['resource.calendar.leaves']
        before = Leave.search_count([('calendar_id', '=', self.calendar.id)])
        self.assertEqual(before, 7)
        self.assertEqual(self.Config._sgi_load_holidays([2027], self.calendar), [],
                         "La segunda carga no duplica.")
        self.assertEqual(Leave.search_count([('calendar_id', '=', self.calendar.id)]), 7)
        # Sin parámetro no se carga en el calendario de la compañía.
        self.env['ir.config_parameter'].sudo().set_param('quimibond_sgi.business_calendar_id', '')
        self.assertEqual(self.Config._sgi_load_holidays([2028]), [])

    def test_03_dias_habiles_saltan_festivos(self):
        self.assertEqual(sgi_add_business_days(self.env, date(2027, 1, 29), 1), date(2027, 2, 2),
                         "Viernes + 1 día hábil: el lunes 1 de febrero es festivo.")
        self.assertEqual(sgi_business_days(self.env, date(2027, 3, 12), date(2027, 3, 16)), 1,
                         "El lunes 15 de marzo no cuenta.")

    def test_04_datetime_en_hora_local(self):
        # 02:00 UTC del 1 de octubre = 20:00 del 30 de septiembre en México.
        self.assertEqual(sgi_local_date(self.env, datetime(2026, 10, 1, 2, 0)), date(2026, 9, 30))
        # Jueves 19:00 en México (viernes 01:00 UTC) + 1 día hábil = viernes.
        self.assertEqual(sgi_add_business_days(self.env, datetime(2026, 10, 2, 1, 0), 1),
                         date(2026, 10, 2))
        self.assertEqual(
            sgi_business_days(self.env, datetime(2026, 10, 2, 1, 0), datetime(2026, 10, 3, 1, 0)), 1,
            "Del jueves al viernes (hora local) pasó un día hábil.")

    def test_05_vencimiento_en_sabado_y_festivo(self):
        """J-011: el vencimiento que cae en inhábil se adelanta."""
        saturday = self._act(measure_cadence='semanal', due_weekday='5')
        self.assertEqual(saturday._sgi_periodic_due(date(2027, 3, 10)), date(2027, 3, 12),
                         "Sábado → viernes.")
        monday = self._act(measure_cadence='semanal', due_weekday='0')
        self.assertEqual(monday._sgi_periodic_due(date(2027, 2, 3)), date(2027, 2, 2),
                         "Lunes festivo sin hábil antes en la semana: el siguiente hábil.")
        self.assertEqual(monday._sgi_periodic_due(date(2027, 2, 10)), date(2027, 2, 8))
        yearly = self._act(measure_cadence='anual', due_month='9', due_day=16)
        self.assertEqual(yearly._sgi_periodic_due(date(2027, 5, 1)), date(2027, 9, 15),
                         "16 de septiembre (festivo) → 15.")
        new_year = self._act(measure_cadence='anual', due_month='1', due_day=1)
        self.assertEqual(new_year._sgi_periodic_due(date(2027, 6, 1)), date(2027, 1, 4),
                         "No se sale del año: el 1 de enero pasa al primer hábil.")

    def test_06_cadencia_larga_pide_mes_y_dia(self):
        act = self._act(measure_cadence='trimestral')
        act.input_ids = [(0, 0, {
            'deliverable_id': self.env['sgi.deliverable'].create(
                {'code': 'ZZH-IN', 'name': 'Entrada ZZH'}).id, 'max_days': 3})]
        codes = [code for code, _m in act._sgi_spec_problems()]
        self.assertIn('no_timing', codes, "Trimestral sin mes ni día, aunque la entrada tenga plazo.")
        act.write({'due_month': '1', 'due_day': 15})
        self.assertNotIn('no_timing', [code for code, _m in act._sgi_spec_problems()])

    def test_07_corrida_mensual_una_vez(self):
        Cron = self.env['sgi.cron']
        after = date(2047, 8, 25)
        self.assertTrue(Cron._sgi_monthly_run_due(after), "Julio 2047 sin medir: recupera.")
        self.env['ir.config_parameter'].sudo().set_param('quimibond_sgi.monthly_run_done', '2047-08')
        self.assertFalse(Cron._sgi_monthly_run_due(after), "G-020: ya corrió en agosto.")
        self.assertTrue(Cron._sgi_monthly_run_due(date(2047, 9, 28)), "Otro mes vuelve a correr.")

    def _template(self, frequency, equipment=True):
        vals = {'name': 'ZS Checklist %s' % frequency, 'frequency': frequency,
                'item_ids': [(0, 0, {'name': 'Presión'})]}
        if equipment:
            vals['equipment_ids'] = [(6, 0, self.env['maintenance.equipment'].create(
                {'name': 'ZS Compresor %s' % frequency}).ids)]
        return self.env['sgi.checklist.template'].create(vals)

    def test_08_checklist_festivo_semana_y_hora_local(self):
        daily = self._template('diaria')
        self.assertFalse(daily._sgi_due_today(date(2027, 2, 1)), "Festivo: no hay hoja.")
        self.assertTrue(daily._sgi_due_today(date(2027, 2, 2)))
        weekly = self._template('semanal')
        self.assertTrue(weekly._sgi_due_today(date(2027, 2, 2)),
                        "Lunes festivo: la semanal sale el martes.")
        request = weekly._sgi_generate(date(2027, 2, 2))
        self.assertEqual(request.schedule_date, datetime(2027, 2, 2, 14, 0),
                         "08:00 en México = 14:00 UTC.")
        self.assertFalse(weekly._sgi_due_today(date(2027, 2, 3)), "Una vez por semana.")
        self.assertTrue(weekly._sgi_due_today(date(2027, 2, 8)))

    def test_09_checklist_sin_equipos_avisa(self):
        mast = sgi_set_mast(self.env, login='zzh_mast')
        template = self._template('diaria', equipment=False)
        Template = self.env['sgi.checklist.template']
        Template._sgi_warn_without_equipment(template)
        Template._sgi_warn_without_equipment(template)
        Activity = self.env['mail.activity'].with_context(active_test=False)
        notices = Activity.search([('res_model', '=', template._name), ('res_id', '=', template.id),
                                   ('sgi_cron_key', '=', 'checklist_sin_equipos')])
        self.assertEqual(len(notices), 1, "Un aviso por plantilla, sin duplicar.")
        self.assertEqual(notices.user_id, mast)
        template.equipment_ids = [(6, 0, self.env['maintenance.equipment'].create(
            {'name': 'ZS Montacargas'}).ids)]
        Template._sgi_warn_without_equipment(template)
        self.assertFalse(notices.active, "Con equipos, el aviso se cierra solo.")

    def test_10_crons_a_hora_de_mexico(self):
        checklist = self.env.ref('quimibond_sgi.sgi_cron_checklists')
        legal = self.env.ref('quimibond_sgi.sgi_cron_legal_requirements')
        checklist.nextcall = datetime(2026, 10, 1, 22, 44, 9)
        legal.nextcall = datetime(2026, 10, 1, 2, 39, 27)
        moved = self.Config._sgi_move_cron_hours(now=datetime(2026, 10, 1, 15, 0))
        self.assertEqual(checklist.nextcall, datetime(2026, 10, 2, 11, 30), "05:30 en México.")
        self.assertEqual(legal.nextcall, datetime(2026, 10, 1, 12, 39),
                         "Mismo día UTC, 06:39 en México.")
        self.assertIn('quimibond_sgi.sgi_cron_legal_requirements', moved)
        again = self.Config._sgi_move_cron_hours(now=datetime(2026, 10, 1, 15, 0))
        self.assertNotIn('quimibond_sgi.sgi_cron_checklists', again)
        self.assertNotIn('quimibond_sgi.sgi_cron_legal_requirements', again, "Idempotente.")

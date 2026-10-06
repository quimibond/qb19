# -*- coding: utf-8 -*-
"""57.17.0 (entrega 8, rendimiento: G-015, G-016, G-023). Datos propios.

No mide tiempos (eso se hace en staging con ``--log-level=debug_sql``):
prueba que lo guardado da lo mismo que el cálculo al vuelo."""
from datetime import date, datetime, timedelta

from odoo.tests import TransactionCase, tagged

from ..models.sgi_calendar import SgiWorkdays, sgi_add_business_days
from .common_documents import sgi_hide_real_documents


@tagged('post_install', '-at_install')
class TestRendimiento(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        cls.calendar = cls.env['resource.calendar'].create({
            'name': 'ZS SGI rendimiento L-V', 'tz': 'America/Mexico_City',
            'attendance_ids': [(5, 0, 0)] + [
                (0, 0, {'name': 'Día %s' % day, 'dayofweek': day, 'hour_from': 8.0,
                        'hour_to': 17.0, 'day_period': 'morning'})
                for day in '01234'],
        })
        cls.env['ir.config_parameter'].sudo().set_param(
            'quimibond_sgi.business_calendar_id', str(cls.calendar.id))
        cls.env['sgi.config']._sgi_load_holidays([2027], cls.calendar)
        cls.job = cls.env['hr.job'].create({'name': 'ZS PUESTO RENDIMIENTO'})
        cls.employee = cls.env['hr.employee'].create({'name': 'ZS Rendimiento', 'job_id': cls.job.id})
        cls.process = cls.env['sgi.process'].create({'code': 'ZRD', 'name': 'ZS Rendimiento'})
        cls.activity = cls.env['sgi.process.activity'].create({
            'process_id': cls.process.id, 'name': 'Revisar el tablero',
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': cls.job.id})],
            'measure_cadence': 'semanal', 'due_weekday': '4',
            'done_criteria': 'El tablero quedó revisado', 'on_fail': 'Avisar a MAST'})

    def test_01_dias_habiles_por_corrida_igual_que_uno_por_uno(self):
        """G-016: el cálculo en memoria da lo mismo que el calendario."""
        workdays = SgiWorkdays(self.env, date(2027, 1, 1), date(2027, 4, 30))
        day = date(2027, 1, 5)
        while day <= date(2027, 3, 31):
            for days in (-5, -1, 0, 1, 2, 5, 10):
                self.assertEqual(workdays.add(day, days), sgi_add_business_days(self.env, day, days),
                                 "%s %+d" % (day, days))
            day += timedelta(days=3)
        # Datetime UTC: fecha local (viernes 29-ene 19:00 en México).
        self.assertEqual(workdays.add(datetime(2027, 1, 30, 1, 0), 1), date(2027, 2, 2))
        # Fuera del rango usa el calendario.
        self.assertEqual(workdays.add(date(2027, 6, 1), 3),
                         sgi_add_business_days(self.env, date(2027, 6, 1), 3))

    def test_02_cifras_guardadas_en_el_puesto(self):
        """G-015: Mi equipo lee las cifras guardadas; un cambio marca el puesto
        «por recalcular» y mientras tanto se calcula al vuelo."""
        job = self.job
        job._sgi_mp_refresh_stats(force=True)
        self.assertTrue(job.sgi_mp_stats_at)
        self.assertEqual(job.sgi_mp_job_total, 1)
        self.assertEqual(job.sgi_mp_hash_current, job._sgi_my_procedure_data()['hash'])
        stats = self.env['hr.employee.public']._sgi_mp_job_stats(job)
        self.assertEqual(stats[job.id][3], 1)
        old_hash = job.sgi_mp_hash_current
        self.activity.write({'name': 'Revisar el tablero semanal'})
        self.assertFalse(job.sgi_mp_stats_at, "El texto cambió: por recalcular.")
        live = job._sgi_mp_stats_map()[job.id]
        self.assertNotEqual(live['sgi_mp_hash_current'], old_hash, "Al vuelo ve el cambio.")
        self.assertEqual(job.sgi_mp_hash_current, old_hash, "Leer no escribe.")
        job._sgi_mp_refresh_stats()
        self.assertEqual(job.sgi_mp_hash_current, live['sgi_mp_hash_current'])
        self.assertTrue(job.sgi_mp_stats_at)
        # Un rol nuevo también lo marca.
        self.env['sgi.activity.role'].create({
            'activity_id': self.activity.id, 'role': 'informa', 'job_id': job.id})
        self.assertFalse(job.sgi_mp_stats_at)
        # La medición (cron) no lo marca: no cambia el procedimiento.
        job._sgi_mp_refresh_stats()
        self.activity.write({'measure_state': 'rojo'})
        self.assertTrue(job.sgi_mp_stats_at)

    def test_03_fusion_de_puestos_recalcula(self):
        """G-023: tras mover el puesto por SQL (como la fusión), los campos
        guardados que salen del puesto se recalculan y el puesto que se queda
        queda «por recalcular»."""
        keep = self.env['hr.job'].create({'name': 'ZS PUESTO QUE SE QUEDA'})
        other = self.env['hr.job'].create({'name': 'ZS PUESTO QUE SE VA'})
        emp = self.env['hr.employee'].create({'name': 'ZS Se mueve', 'job_id': other.id})
        keep._sgi_mp_refresh_stats(force=True)
        self.assertEqual(emp.sgi_mp_job_id, other)
        self.env.flush_all()
        self.env.cr.execute("UPDATE hr_version SET job_id = %s WHERE job_id = %s",
                            (keep.id, other.id))
        self.env.invalidate_all()
        self.assertEqual(emp.sgi_mp_job_id, other, "El guardado sigue viejo tras el SQL.")
        self.env['hr.job']._sgi_merge_recompute(keep, other)
        self.assertEqual(emp.sgi_mp_job_id, keep)
        self.assertFalse(keep.sgi_mp_stats_at)

# -*- coding: utf-8 -*-
"""56.38.0 (G-006, decisión 2 de la tanda 2): el cron de calibraciones avisa
sin bloquear, al Coordinador de Laboratorio y al Jefe de Calidad (por
puesto), con un resumen diario en lugar de un aviso por equipo."""
from datetime import timedelta

from odoo import fields
from odoo.tests import TransactionCase, tagged, new_test_user


@tagged('post_install', '-at_install')
class TestCalibracionAvisos(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Cron = cls.env['sgi.cron']
        cls.Activity = cls.env['mail.activity'].with_context(active_test=False)
        cls.today = fields.Date.context_today(cls.Cron)
        Param = cls.env['ir.config_parameter'].sudo()
        # Puestos propios de la prueba (en la copia de producción ya existen
        # los reales; el parámetro apunta a estos).
        Param.set_param('quimibond_sgi.calibration_job_names',
                        'ZCAL COORDINADOR DE LABORATORIO;ZCAL JEFE DE CALIDAD')
        Param.set_param('quimibond_sgi.calibration_block_expired', False)
        company = cls.env['sgi.config']._sgi_company()
        cls.lab = new_test_user(cls.env, login='zcal_lab', groups='base.group_user')
        cls.quality = new_test_user(cls.env, login='zcal_calidad', groups='base.group_user')
        for name, user in (('ZCAL COORDINADOR DE LABORATORIO', cls.lab),
                           ('ZCAL JEFE DE CALIDAD', cls.quality)):
            job = cls.env['hr.job'].create({'name': name, 'company_id': company.id})
            cls.env['hr.employee'].create({'name': name.title(), 'job_id': job.id,
                                           'user_id': user.id, 'company_id': company.id})
        Equipment = cls.env['maintenance.equipment']
        cls.expired = Equipment.create({
            'name': 'ZCAL Vernier vencido', 'sgi_is_measuring': True, 'company_id': company.id,
            'sgi_next_calibration_date': cls.today - timedelta(days=20)})
        cls.soon = Equipment.create({
            'name': 'ZCAL Balanza por vencer', 'sgi_is_measuring': True, 'company_id': company.id,
            'sgi_next_calibration_date': cls.today + timedelta(days=10)})

    def _summaries(self):
        return self.Activity.search([('active', '=', True),
                                     ('sgi_cron_key', '=like', 'calibracion_resumen:%')])

    def test_01_avisa_sin_bloquear(self):
        self.Cron.cron_calibrations()
        self.assertEqual(self.expired.sgi_calibration_state, 'vencido')
        self.assertFalse(self.expired.sgi_do_not_use, "Con el parámetro apagado no bloquea.")
        summaries = self._summaries()
        self.assertEqual(set(summaries.user_id.ids), {self.lab.id, self.quality.id},
                         "Van al Coordinador de Laboratorio y al Jefe de Calidad, por puesto.")
        self.assertEqual(len(summaries), 2, "Un resumen por destinatario, no uno por equipo.")
        self.assertIn('ZCAL Vernier vencido', summaries[0].note)
        self.assertIn('ZCAL Balanza por vencer', summaries[0].note)
        self.assertFalse(self.Activity.search([
            ('res_model', '=', 'maintenance.equipment'), ('res_id', 'in', (self.expired | self.soon).ids),
            ('summary', '=like', 'Calibración VENCIDA%')]), "Ya no hay aviso por equipo.")
        # Segunda corrida el mismo día: sin duplicar.
        self.Cron.cron_calibrations()
        self.assertEqual(self._summaries(), summaries)

    def test_02_con_parametro_bloquea(self):
        self.env['ir.config_parameter'].sudo().set_param('quimibond_sgi.calibration_block_expired', 'True')
        self.Cron.cron_calibrations()
        self.assertTrue(self.expired.sgi_do_not_use, "Encendido, el vencido queda «No usar».")
        self.assertFalse(self.soon.sgi_do_not_use)

    def test_03_resumen_de_ayer_se_cierra(self):
        self.Cron.cron_calibrations()
        today_notice = self._summaries()
        today_notice.write({'sgi_cron_key': 'calibracion_resumen:x:2000-01-01'})
        self.Cron.cron_calibrations()
        self.assertFalse(today_notice.filtered('active'), "El resumen viejo lo reemplaza el de hoy.")
        self.assertEqual(len(self._summaries()), 2)

    def test_04_migracion_cierra_los_avisos_por_equipo(self):
        legacy = self.expired.activity_schedule(
            'mail.mail_activity_data_todo', summary="Calibración VENCIDA: ZCAL Vernier vencido",
            user_id=self.lab.id)
        self.expired.sgi_do_not_use = True
        closed = self.Cron._sgi_migrate_close_calibration_notices('sgi_mail_activity_bak_prueba_cal')
        self.assertIn(legacy.id, closed)
        self.assertFalse(legacy.active, "Se archiva con nota, no se borra.")
        self.assertIn("resumen diario", legacy.feedback or '')
        self.assertTrue(self.expired.sgi_do_not_use, "La migración no toca el «No usar».")
        self.env.cr.execute("SELECT id FROM sgi_mail_activity_bak_prueba_cal")
        self.assertIn(legacy.id, {row[0] for row in self.env.cr.fetchall()})
        self.assertEqual(self.Cron._sgi_migrate_close_calibration_notices(
            'sgi_mail_activity_bak_prueba_cal'), [], "Idempotente.")

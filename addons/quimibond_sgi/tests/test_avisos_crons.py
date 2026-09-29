# -*- coding: utf-8 -*-
"""56.37.0 (auditoría 2026-09, entrega 8a, línea «avisos»): avisos de los
crons con clave estable y cierre por episodio (G-004, G-005, G-011, G-025),
Jefe MAST por parámetro o por login (I-003) y la migración que reasigna los
avisos de MAST que quedaron en otras bandejas. J-008: los crons corren dos
veces sin duplicar."""
from datetime import timedelta

from odoo import fields
from odoo.tests import TransactionCase, tagged, new_test_user

# Crons de avisos (no los de medición ni evaluación de proveedores, que
# crean registros de negocio). Se corren dos veces seguidas.
AVISO_CRONS = (
    'cron_nonconformities', 'cron_overdue_actions', 'cron_documents', 'cron_news',
    'cron_audit_program', 'cron_risk_review', 'cron_calibrations', 'cron_competences',
    'cron_satisfaction_survey', 'cron_dnc', 'cron_emergency_drills',
    'cron_operational_signals', 'cron_legal_requirements', 'cron_worker_participation',
    'cron_context_review', 'cron_my_procedure_stale',
)
# 57.11.0 (A-016): cron_forecast_coverage se corre dos veces en
# quimibond_ventas_presupuesto/tests/test_sales_budget_sgi.py.


@tagged('post_install', '-at_install')
class TestAvisosCrons(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Cron = cls.env['sgi.cron']
        cls.Activity = cls.env['mail.activity'].with_context(active_test=False)
        cls.mast = new_test_user(cls.env, login='avisos_mast',
                                 groups='base.group_user,quimibond_sgi.group_sgi_manager')
        cls.other = new_test_user(cls.env, login='avisos_otro', groups='base.group_user')
        cls.other2 = new_test_user(cls.env, login='avisos_otro2', groups='base.group_user')
        cls.third = new_test_user(cls.env, login='avisos_tercero', groups='base.group_user')
        cls.env['ir.config_parameter'].sudo().set_param('quimibond_sgi.mast_user_id', cls.mast.id)
        cls.today = fields.Date.context_today(cls.env['sgi.cron'])
        cls.party = cls.env['sgi.interested.party'].create({
            'name': 'Parte de prueba avisos', 'needs': 'Prueba',
            'next_review_date': cls.today - timedelta(days=3)})

    def _notices(self, record, key=None):
        domain = [('res_model', '=', record._name), ('res_id', '=', record.id)]
        if key:
            domain.append(('sgi_cron_key', '=', key))
        return self.Activity.search(domain)

    # --- _sgi_schedule con clave ------------------------------------------
    def test_01_misma_clave_no_duplica_y_sigue_al_destinatario(self):
        first = self.Cron._sgi_schedule(self.party, "Aviso 1 de 3", "nota", self.other.id,
                                         date_deadline=self.today, key='prueba')
        again = self.Cron._sgi_schedule(self.party, "Aviso 2 de 3", "otra nota", self.mast.id,
                                         date_deadline=self.today + timedelta(days=5), key='prueba')
        self.assertEqual(first, again, "Misma clave: el mismo aviso, no otro (G-005).")
        notices = self._notices(self.party, 'prueba').filtered('active')
        self.assertEqual(len(notices), 1)
        self.assertEqual(notices.user_id, self.mast, "Cambió quien recibe: se reasigna (G-005 a).")
        self.assertEqual(notices.summary, "Aviso 2 de 3", "El dato del resumen se actualiza (G-005 b).")
        self.assertIn("otra nota", notices.note, "La nota sigue al dato de hoy (G-025).")
        self.assertEqual(notices.date_deadline, self.today + timedelta(days=5), "G-011.")

    def test_02_hecha_no_se_recrea_en_el_mismo_episodio(self):
        run = self.Cron._sgi_new_run()
        notice = run._sgi_schedule(self.party, "Aviso episodio", "", self.mast.id, key='episodio')
        notice.action_feedback(feedback="Visto")
        self.assertFalse(run._sgi_schedule(self.party, "Aviso episodio", "", self.mast.id, key='episodio'),
                         "Hecha sin resolver la causa: no se vuelve a crear (decisión 5).")
        self.assertFalse(self._notices(self.party, 'episodio').filtered('active'))
        # Una corrida que ya no la ve cierra el episodio...
        self.Cron._sgi_new_run()._sgi_sweep(['episodio'], "ya no aplica")
        self.assertTrue(self._notices(self.party, 'episodio').sgi_episode_closed)
        # ...y si la causa reaparece, nace un aviso nuevo.
        reborn = self.Cron._sgi_schedule(self.party, "Aviso episodio", "", self.mast.id, key='episodio')
        self.assertTrue(reborn and reborn.active)
        self.assertEqual(len(self._notices(self.party, 'episodio')), 2)

    def test_03_adopta_la_actividad_vieja_sin_clave(self):
        legacy = self.party.activity_schedule(
            'mail.mail_activity_data_todo', summary="Aviso viejo", user_id=self.other.id)
        adopted = self.Cron._sgi_schedule(self.party, "Aviso viejo", "", self.mast.id, key='viejo')
        self.assertEqual(adopted, legacy, "La de antes de 56.37.0 se adopta, no se duplica.")
        self.assertEqual(legacy.sgi_cron_key, 'viejo')
        self.assertEqual(legacy.user_id, self.mast)

    def test_04_sin_clave_se_conserva_el_comportamiento(self):
        self.Cron._sgi_schedule(self.party, "Aviso sin clave", "", self.other.id)
        self.Cron._sgi_schedule(self.party, "Aviso sin clave", "", self.mast.id)
        self.assertEqual(len(self.Activity.search([
            ('res_model', '=', 'sgi.interested.party'), ('res_id', '=', self.party.id),
            ('summary', '=', "Aviso sin clave")])), 2, "Sin clave: uno por resumen y usuario.")

    # --- cierre por episodio en un cron real ------------------------------
    def test_05_cron_cierra_lo_que_ya_no_aplica(self):
        self.Cron.cron_context_review()
        notice = self._notices(self.party, 'revisar_parte_interesada').filtered('active')
        self.assertEqual(len(notice), 1)
        self.assertEqual(notice.user_id, self.mast)
        self.assertEqual(notice.date_deadline, self.party.next_review_date,
                         "Vence el día de la revisión, no nace vencido hoy (G-011).")
        self.party.action_mark_reviewed()
        self.Cron.cron_context_review()
        self.assertFalse(notice.active, "Revisada la parte, el cron cierra su aviso.")
        self.assertTrue(notice.sgi_episode_closed)
        self.assertIn("Cerrada automáticamente", notice.feedback or '')

    def test_06_mast_por_parametro_o_por_login(self):
        Param = self.env['ir.config_parameter'].sudo()
        self.assertEqual(self.Cron._sgi_manager_user_id(), self.mast.id)
        Param.set_param('quimibond_sgi.mast_user_id', 0)
        by_login = self.env['res.users'].sudo().search([('login', '=', 'mas@quimibond.com')], limit=1)
        if not by_login:
            by_login = new_test_user(self.env, login='mas@quimibond.com', groups='base.group_user')
        self.assertEqual(self.Cron._sgi_manager_user_id(), by_login.id,
                         "Sin parámetro, MAST se resuelve por login y no por id fijo.")

    def test_07_calibracion_escribe_solo_lo_que_cambia(self):
        equipment = self.env['maintenance.equipment'].create({
            'name': 'Vernier avisos', 'sgi_is_measuring': True,
            'sgi_last_calibration_date': self.today - timedelta(days=100)})
        self.assertEqual(equipment.sgi_calibration_state, 'vigente')
        self.assertEqual(self.Cron._sgi_refresh_calibration_states(equipment), 0,
                         "Sin cambio de estado no se escribe (G-024).")

    # --- J-008: dos corridas seguidas no duplican -------------------------
    def test_08_crons_dos_veces_sin_duplicar(self):
        self.env['maintenance.equipment'].create({
            'name': 'Balanza por vencer avisos', 'sgi_is_measuring': True,
            'sgi_next_calibration_date': self.today + timedelta(days=10)})
        for name in AVISO_CRONS:
            getattr(self.Cron, name)()
        first = self.Activity.search([('active', '=', True)])
        attachments = self.env['ir.attachment'].sudo().search_count([])
        for name in AVISO_CRONS:
            getattr(self.Cron, name)()
        second = self.Activity.search([('active', '=', True)])
        self.assertFalse(second - first, "La segunda corrida no crea avisos: %s" % (
            (second - first).mapped('summary')))
        self.assertEqual(self.env['ir.attachment'].sudo().search_count([]), attachments,
                         "La segunda corrida no crea adjuntos (NEWS, G-012).")

    # --- migración 56.37.0 ------------------------------------------------
    def test_09_migracion_reasigna_y_cierra_repetidos(self):
        summary = "Revisar parte interesada: %s" % self.party.name
        mine = self.party.activity_schedule('mail.mail_activity_data_todo', summary=summary,
                                            user_id=self.other.id)
        dup = self.party.activity_schedule('mail.mail_activity_data_todo', summary=summary,
                                           user_id=self.other2.id)
        foreign = self.party.activity_schedule('mail.mail_activity_data_todo', summary=summary,
                                               user_id=self.third.id)
        result = self.Cron._sgi_migrate_mast_notices(
            [self.other.id, self.other2.id], 'sgi_mail_activity_bak_prueba')
        self.assertEqual(mine.user_id, self.mast, "La más vieja queda y pasa a MAST.")
        self.assertEqual(mine.sgi_cron_key, 'revisar_parte_interesada')
        self.assertFalse(dup.active, "La repetida se cierra (archivada, no borrada).")
        self.assertIn("aviso repetido", dup.feedback or '')
        self.assertTrue(foreign.active, "La de un tercero no se toca.")
        self.assertEqual(foreign.user_id, self.third)
        self.assertIn(dup.id, result['closed'])
        self.assertIn(mine.id, result['moved'][self.mast.id])
        self.env.cr.execute("SELECT id FROM sgi_mail_activity_bak_prueba")
        self.assertEqual({row[0] for row in self.env.cr.fetchall()}, {mine.id, dup.id},
                         "Respaldo previo de lo que se toca.")
        again = self.Cron._sgi_migrate_mast_notices(
            [self.other.id, self.other2.id], 'sgi_mail_activity_bak_prueba')
        self.assertEqual((again['closed'], again['moved']), ([], {}), "Idempotente.")

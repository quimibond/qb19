# -*- coding: utf-8 -*-
"""57.7.0 (D-14 y D-15)."""
from datetime import date
from unittest.mock import patch

from odoo.tests import TransactionCase, new_test_user, tagged


@tagged('post_install', '-at_install')
class TestWeeklyOverdueMail(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        groups = 'base.group_user,quimibond_sgi.group_sgi_user'
        cls.late_on = new_test_user(cls.env, login='d14_late_on', email='d14.a@example.com',
                                    groups=groups)
        cls.late_off = new_test_user(cls.env, login='d14_late_off', email='d14.b@example.com',
                                     groups=groups)
        cls.late_off.sgi_weekly_overdue_mail = False
        cls.on_time = new_test_user(cls.env, login='d14_on_time', email='d14.c@example.com',
                                    groups=groups)
        cls.Pending = type(cls.env['sgi.my.pending'])

    def _fake_values(self, users):
        late = {'kind': 'accion', 'name': 'Acción atrasada D14', 'state': 'atrasada',
                'date_due': date(2026, 9, 1), 'process_id': False}
        ok = dict(late, name='Acción al día D14', state='al_dia')
        return {user.id: ([late, ok] if user in (self.late_on | self.late_off) else [ok])
                for user in users}

    def _run(self):
        fake = self._fake_values
        with patch.object(self.Pending, '_sgi_pending_values', autospec=True,
                          side_effect=lambda _self, users: fake(users)):
            return self.env['sgi.cron'].cron_weekly_overdue_mail()

    def test_01_only_opted_in_with_late_rows(self):
        self.assertTrue(self.on_time.sgi_weekly_overdue_mail, "Encendido por default.")
        sent = self._run()
        self.assertIn(self.late_on.id, sent)
        self.assertNotIn(self.late_off.id, sent, "Quien lo apagó no recibe correo.")
        self.assertNotIn(self.on_time.id, sent, "Sin atrasos no hay correo.")
        mails = self.env['mail.mail'].sudo().search([('subject', 'ilike', 'atrasado')])

        # Odoo 19 convierte el «Para» de la plantilla en destinatarios
        # (recipient_ids, el contacto del usuario) y deja email_to vacío: el
        # correo sí sale, a la dirección del contacto. Se revisan ambos.
        def addresses(mail):
            return ' '.join([mail.email_to or ''] + mail.recipient_ids.mapped('email'))
        recipients = ' '.join(addresses(mail) for mail in mails)
        self.assertIn('d14.a@example.com', recipients)
        self.assertNotIn('d14.b@example.com', recipients)
        self.assertNotIn('d14.c@example.com', recipients)
        body = ' '.join(mails.filtered(lambda m: 'd14.a@' in addresses(m)).mapped('body_html'))
        self.assertIn('Acción atrasada D14', body)
        self.assertNotIn('Acción al día D14', body)

    def test_02_user_can_turn_it_off(self):
        self.late_on.with_user(self.late_on).write({'sgi_weekly_overdue_mail': False})
        self.assertFalse(self.late_on.sgi_weekly_overdue_mail)
        self.assertNotIn(self.late_on.id, self._run())

    def test_03_cron_ships_off(self):
        cron = self.env.ref('quimibond_sgi.sgi_cron_weekly_digest')
        self.assertIn('cron_weekly_overdue_mail', cron.code)
        self.assertFalse(hasattr(self.env['sgi.cron'], 'cron_weekly_digest'))


@tagged('post_install', '-at_install')
class TestArchiveUnusedCatalogs(TransactionCase):

    def test_01_archives_only_unused(self):
        Config = self.env['sgi.config']
        purchase = self.env.ref('quimibond_sgi.sgi_approval_category_purchase')
        moc = self.env.ref('quimibond_sgi.sgi_approval_category_moc')
        (purchase | moc).write({'active': True})
        self.env['approval.request'].create({
            'name': 'MOC en uso', 'category_id': moc.id,
            'request_owner_id': self.env.user.id})
        report = Config._sgi_archive_unused_catalogs()
        self.assertFalse(purchase.active, "Sin solicitudes: se archiva.")
        self.assertTrue(moc.active, "Con una solicitud: no se archiva.")
        self.assertTrue(purchase.exists(), "Se archiva, nunca se borra.")
        self.assertTrue(self.env['ir.attachment'].sudo().search_count([
            ('res_model', '=', 'approval.category'), ('res_id', '=', purchase.id),
            ('name', 'like', 'respaldo_d15_')]))
        self.assertTrue(any('MOC' in k or 'infraestructura' in k for k in report['kept']))
        # Idempotente.
        self.assertFalse(Config._sgi_archive_unused_catalogs()['archived'])

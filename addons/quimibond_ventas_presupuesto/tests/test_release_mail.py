# -*- coding: utf-8 -*-
"""Release por correo con aprobación de Ventas (3.30.0). Datos inventados."""
from odoo.exceptions import UserError
from odoo.tests import TransactionCase, new_test_user, tagged

from .test_release import LEAR_TEXT


@tagged('post_install', '-at_install')
class TestReleaseMail(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.team = cls.env['crm.team'].create({'name': 'Industrial correo test'})
        cls.customer = cls.env['res.partner'].create({'name': 'Cliente correo', 'is_company': True})
        cls.jessica = new_test_user(cls.env, login='qb_release_aprueba', groups='sales_team.group_sale_manager')
        cls.otro = new_test_user(cls.env, login='qb_release_otro', groups='sales_team.group_sale_manager')
        cls.profile = cls.env['qb.release.profile'].create({
            'partner_id': cls.customer.id, 'team_id': cls.team.id, 'reader': 'lear_aiag',
            'firm_rule': 'fab_auth', 'sender_domains': 'clientecorreo.test', 'email_subject': 'release',
            'email_file': '*.eml, *.txt', 'sales_user_id': cls.jessica.id})

    def _mail(self, files, sender='Diana <diana@clientecorreo.test>', subject='Release 11-19-40', body=''):
        return self.env['qb.release'].message_new({
            'email_from': sender, 'subject': subject, 'body': body,
            'attachments': [(name, content, {}) for name, content in files]})

    def _approval(self, release):
        return release.activity_ids.filtered(lambda a: (a.summary or '').startswith('Aprobar release'))

    def test_01_se_lee_y_espera_la_aprobacion(self):
        release = self._mail([('image001.png', b'\x89PNG'), ('QUIMIBOND.txt', LEAR_TEXT)])
        self.assertEqual(len(release), 1)
        self.assertEqual(release.profile_id, self.profile)
        self.assertTrue(release.by_email)
        self.assertEqual(release.state, 'leido')  # leído, nunca aplicado
        self.assertEqual(self._approval(release).user_id, self.jessica)

    def test_02_reenviado_y_duplicado(self):
        body = '<p>De: Diana &lt;diana@clientecorreo.test&gt;</p>'
        first = self._mail([('QUIMIBOND.txt', LEAR_TEXT)], sender='Ventas <ventas@quimibond.test>',
                           subject='Fwd: Release', body=body)
        self.assertEqual(first.profile_id, self.profile)
        again = self._mail([('QUIMIBOND.txt', LEAR_TEXT)])
        self.assertEqual(again, first)
        self.assertEqual(self.env['qb.release'].search_count([('profile_id', '=', self.profile.id)]), 1)

    def test_03_sin_reconocer_y_sin_archivo(self):
        unknown = self._mail([('programa.xlsx', b'xx')], sender='x@otro.test', subject='Programa')
        self.assertFalse(unknown.profile_id)
        self.assertEqual(unknown.state, 'por_revisar')
        self.assertTrue(self._approval(unknown))
        nothing = self._mail([('image001.png', b'\x89PNG')], subject='Release gracias')
        self.assertEqual(nothing.state, 'descartado')
        self.assertFalse(self._approval(nothing))

    def test_04_ilegible_queda_por_revisar(self):
        release = self._mail([('QUIMIBOND.txt', 'no es un release')])
        self.assertEqual(release.state, 'por_revisar')
        self.assertTrue(release.error_reason)

    def test_05_solo_aprueba_el_responsable(self):
        release = self._mail([('QUIMIBOND.txt', LEAR_TEXT)])
        with self.assertRaises(UserError):
            release.with_user(self.otro).action_mark_reviewed()
        release.with_user(self.jessica).action_mark_reviewed()
        self.assertEqual(release.state, 'revisado')

    def test_06_semanas_por_pedir(self):
        release = self._mail([('QUIMIBOND.txt', LEAR_TEXT)])
        action = release.action_qb_weeks_to_order()
        self.assertEqual(action['res_model'], 'qb.forecast.line')
        self.assertIn(('qty_neta', '>', 0), action['domain'])

# -*- coding: utf-8 -*-
"""Dueños aprendidos: buzón → persona (catálogo o login), cron nocturno con la
vista simulada, umbrales, fijado a mano y memoria_owner_for por área."""
from unittest.mock import patch

from odoo.tests import TransactionCase, tagged

from .test_memoria import fake_get


@tagged('post_install', '-at_install', 'qb_memoria')
class TestOwners(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        icp = cls.env['ir.config_parameter'].sudo()
        icp.set_param('quimibond_intelligence.supabase_url', 'https://memoria.test')
        icp.set_param('quimibond_intelligence.supabase_service_key', 'k')
        Users = cls.env['res.users'].with_context(no_reset_password=True)
        cls.dario = Users.create({'name': 'Dario', 'login': 'logistica@test.local', 'email': 'logistica@test.local'})
        cls.sandra = Users.create({'name': 'Sandra', 'login': 'sandra@test.local', 'email': 'sandra@test.local'})
        cls.jessica = Users.create({'name': 'Jessica', 'login': 'jessica@test.local', 'email': 'jessica@test.local'})
        cls.env['qb.memoria.mailbox'].create({'email': 'CxCobrar@test.local', 'user_id': cls.sandra.id, 'area': 'finanzas'})
        cls.company = cls.env['res.partner'].create({'name': 'BLANCOS MILENIUM', 'is_company': True})
        cls.contact = cls.env['res.partner'].create({'name': 'Ana', 'parent_id': cls.company.id})
        cls.other = cls.env['res.partner'].create({'name': 'SHAWMUT', 'is_company': True})
        cls.Client = type(cls.env['qb.memoria.client'])
        cls.rows = [
            {'odoo_partner_id': cls.contact.id, 'area': None, 'mailbox': 'logistica@test.local', 'n': 469, 'share': 88,
             'last_at': '2026-09-10T10:00:00+00:00'},
            {'odoo_partner_id': cls.contact.id, 'area': 'finanzas', 'mailbox': 'cxcobrar@test.local', 'n': 5, 'share': 70,
             'last_at': '2026-09-01T10:00:00+00:00'},
            {'odoo_partner_id': cls.contact.id, 'area': 'comercial', 'mailbox': 'gilberto@test.local', 'n': 2, 'share': 50,
             'last_at': '2026-09-01T10:00:00+00:00'},                       # bajo el umbral
            {'odoo_partner_id': cls.other.id, 'area': None, 'mailbox': 'innovacion@test.local', 'n': 40, 'share': 60,
             'last_at': '2026-09-10T10:00:00+00:00'},                       # buzón sin persona
            {'odoo_partner_id': 999999, 'area': None, 'mailbox': 'logistica@test.local', 'n': 40, 'share': 60,
             'last_at': '2026-09-10T10:00:00+00:00'},                       # partner inexistente
        ]

    def _run(self):
        with patch.object(self.Client, 'get', fake_get({'memoria_encargados': self.rows})):
            return self.env['res.partner']._cron_memoria_owners()

    def test_mailbox_resolution(self):
        Mailbox = self.env['qb.memoria.mailbox']
        self.assertEqual(Mailbox.user_for('cxcobrar@test.local'), self.sandra, 'catálogo (case-insensitive)')
        self.assertEqual(Mailbox.user_for('LOGISTICA@test.local'), self.dario, 'login del usuario')
        self.assertFalse(Mailbox.user_for('innovacion@test.local'))
        self.assertFalse(Mailbox.user_for(''))

    def test_cron_learns_owner_and_areas(self):
        self.assertEqual(self._run(), 2)
        self.assertEqual(self.company.memoria_owner_user_id, self.dario, 'se escribe en el partner comercial')
        self.assertIn('469 correos', self.company.memoria_owner_evidence)
        self.assertTrue(self.company.memoria_owner_at)
        areas = self.company.memoria_owner_areas
        self.assertEqual(areas['finanzas']['user_id'], self.sandra.id)
        self.assertNotIn('comercial', areas, 'n=2 no alcanza el umbral')
        # memoria_owner_for: área con señal, si no el general; desde el contacto hijo también
        self.assertEqual(self.contact.memoria_owner_for('finanzas'), self.sandra)
        self.assertEqual(self.contact.memoria_owner_for('comercial'), self.dario)
        self.assertEqual(self.company.memoria_owner_for(), self.dario)
        # Buzón sin persona: evidencia pero sin dueño
        self.assertFalse(self.other.memoria_owner_user_id)
        self.assertIn('sin persona', self.other.memoria_owner_evidence)
        self.assertFalse(self.other.memoria_owner_for())

    def test_locked_owner_is_kept(self):
        self.company.write({'memoria_owner_user_id': self.jessica.id, 'memoria_owner_locked': True})
        self._run()
        self.assertEqual(self.company.memoria_owner_user_id, self.jessica)
        self.assertIn('logistica', self.company.memoria_owner_evidence, 'la evidencia sí se actualiza')
        self.assertEqual(self.company.memoria_owner_for('finanzas'), self.sandra, 'las áreas siguen aprendiéndose')

    def test_inactive_user_is_not_proposed(self):
        self._run()
        self.dario.active = False
        self.assertFalse(self.company.memoria_owner_for('comercial'))

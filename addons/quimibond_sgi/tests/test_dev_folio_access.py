# -*- coding: utf-8 -*-
"""57.151.0: el folio FT no se repite y el Usuario SGI escribe los proyectos de desarrollo.

Dirección General 2026-10-13: la secuencia ``sgi.dev.ft`` creó el rango 2026 en 1 y asignó
FT-001-2026 al proyecto 499, que ya era del 380; y Jessica (Desarrollo, sin grupo de Proyecto con
escritura) no podía mover de etapa ni guardar el proyecto."""
from unittest.mock import patch

from odoo import fields
from odoo.exceptions import AccessError, UserError, ValidationError
from odoo.tests import TransactionCase, tagged
from odoo.tests.common import new_test_user


@tagged('post_install', '-at_install')
class TestDevFolioAccess(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Project = cls.env['project.project']
        cls.company = cls.env.ref('base.main_company')
        cls.partner = cls.env['res.partner'].create({'name': 'CLIENTE FOLIO PRUEBA', 'is_company': True})
        cls.year = fields.Date.today().year

    def _dev(self, **vals):
        return self.Project.create(dict({'name': 'x', 'sgi_is_ft': True, 'partner_id': self.partner.id,
                                         'sgi_dev_product_name': 'Jersey de prueba',
                                         'company_id': self.company.id}, **vals))

    def _folio(self, number):
        return 'FT-%03d-%d' % (number, self.year)

    def _stage(self, key):
        return self.env.ref('quimibond_sgi.sgi_dev_stage_%s' % key)

    # ------------------------------------------------------------------
    # Folio FT
    # ------------------------------------------------------------------
    def test_01_sequence_behind_manual_folios_jumps_ahead(self):
        """El rango del año va en N y alguien capturó a mano N+3: el siguiente folio es N+4."""
        nxt = self.Project._sgi_dev_ft_sync_range(self.company)
        self.assertTrue(nxt, "La secuencia sgi.dev.ft usa rangos por año.")
        self._dev(sgi_ft_folio=self._folio(nxt + 3))
        dev = self._dev()
        dev.action_sgi_dev_assign_folio()
        self.assertEqual(dev.sgi_ft_folio, self._folio(nxt + 4))

    def test_02_archived_folio_counts(self):
        nxt = self.Project._sgi_dev_ft_sync_range(self.company)
        self._dev(sgi_ft_folio=self._folio(nxt)).action_archive()
        dev = self._dev()
        dev.action_sgi_dev_assign_folio()
        self.assertEqual(dev.sgi_ft_folio, self._folio(nxt + 1), "Un folio archivado también está ocupado.")

    def test_03_taken_folio_is_skipped_even_without_sync(self):
        """Si la secuencia diera un folio ocupado (sin el ajuste previo), se pide el siguiente."""
        nxt = self.Project._sgi_dev_ft_sync_range(self.company)
        self._dev(sgi_ft_folio=self._folio(nxt))
        dev = self._dev()
        with patch.object(type(self.Project), '_sgi_dev_ft_sync_range', lambda *args, **kwargs: False):
            dev.action_sgi_dev_assign_folio()
        self.assertEqual(dev.sgi_ft_folio, self._folio(nxt + 1))

    def test_04_sync_never_goes_back(self):
        nxt = self.Project._sgi_dev_ft_sync_range(self.company)
        top = self.Project._sgi_dev_ft_max_number(self.year, self.company)
        self.assertGreater(nxt, top)
        ahead = nxt + 50
        sequence = self.env['ir.sequence'].search([('code', '=', 'sgi.dev.ft')], limit=1)
        date_range = sequence.date_range_ids.filtered(lambda r: r.date_from.year == self.year)
        date_range.number_next_actual = ahead
        self.assertEqual(self.Project._sgi_dev_ft_sync_range(self.company), ahead)

    def test_05_folio_is_unique_per_company(self):
        first = self._dev(sgi_ft_folio='FT-998-2019')
        with self.assertRaises(ValidationError):
            self._dev(sgi_ft_folio='FT-998-2019')
        with self.assertRaises(ValidationError, msg="Sin distinguir mayúsculas ni espacios."):
            self._dev(sgi_ft_folio=' ft-998-2019 ')
        first.action_archive()
        with self.assertRaises(ValidationError, msg="El archivado sigue ocupando el folio."):
            self._dev(sgi_ft_folio='FT-998-2019')
        other_company = self.env['res.company'].create({'name': 'Otra compañía folio'})
        other = self._dev(sgi_ft_folio='FT-998-2019', company_id=other_company.id)
        self.assertEqual(other.sgi_ft_folio, 'FT-998-2019', "En otra compañía el folio es otro.")

    def test_06_dev_project_gets_the_company(self):
        dev = self.Project.create({'name': 'x', 'sgi_is_ft': True, 'sgi_dev_product_name': 'Sin compañía'})
        self.assertEqual(dev.company_id, self.env.company)

    # ------------------------------------------------------------------
    # Permiso del Usuario SGI
    # ------------------------------------------------------------------
    def _sgi_user(self):
        if self.company.id != 1:
            self.skipTest("La regla es de la compañía 1 y esta base tiene otra principal.")
        user = new_test_user(self.env, login='sgi_desarrollo_etapas', company_id=self.company.id,
                             groups='base.group_user,quimibond_sgi.group_sgi_user')
        self.assertFalse(user.has_group('project.group_project_manager'))
        return user

    def test_07_sgi_user_moves_and_saves_dev_projects(self):
        user = self._sgi_user()
        dev = self._dev()
        dev.with_user(user).write({'stage_id': self._stage('analisis').id})
        self.assertEqual(dev.sgi_dev_stage_key, 'analisis')
        dev.with_user(user).write({'sgi_dev_product_name': 'Jersey corregido'})
        self.assertIn('Jersey corregido', dev.name)
        created = self.Project.with_user(user).create({'name': 'x', 'sgi_is_ft': True, 'partner_id': self.partner.id,
                                                       'sgi_dev_product_name': 'Nuevo de Desarrollo'})
        self.assertEqual(created.company_id, self.company)
        with self.assertRaises(AccessError, msg="Sin borrar."):
            created.with_user(user).unlink()

    def test_08_sgi_user_keeps_the_customer_approval_lock(self):
        user = self._sgi_user()
        dev = self._dev()
        with self.assertRaises(UserError) as caught:
            dev.with_user(user).write({'stage_id': self._stage('muestra').id})
        self.assertNotIsInstance(caught.exception, AccessError, "Lo detiene la aprobación del cliente.")

    def test_09_sgi_user_cannot_write_other_projects(self):
        user = self._sgi_user()
        plain = self.Project.create({'name': 'Proyecto de otra área', 'company_id': self.company.id})
        with self.assertRaises(AccessError):
            plain.with_user(user).write({'name': 'Cambiado por Desarrollo'})
        with self.assertRaises(AccessError):
            self.Project.with_user(user).create({'name': 'Proyecto común de Desarrollo',
                                                 'company_id': self.company.id})
        manager = new_test_user(self.env, login='sgi_jefe_proyectos', company_id=self.company.id,
                                groups='base.group_user,project.group_project_manager')
        plain.with_user(manager).write({'name': 'Cambiado por el administrador'})
        self.assertEqual(plain.name, 'Cambiado por el administrador', "Proyecto / Administrador sigue igual.")

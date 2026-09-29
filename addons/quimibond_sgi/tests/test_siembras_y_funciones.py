# -*- coding: utf-8 -*-
"""57.5.0 (entrega 2, e2-siembras-y-funciones; A-006, D-12, E-008, A-007).

- La actualización del módulo solo corre ``seed_parameters``.
- ``recompute_pending_measures`` corre en el cron diario de indicadores y
  desde el botón del Administrador SGI (protegido en el servidor).
- El cron de «Mi procedimiento por publicar» es ``noupdate`` en la base."""
import re
from unittest.mock import patch

from odoo.exceptions import AccessError
from odoo.tests import TransactionCase, new_test_user, tagged
from odoo.tools.misc import file_open


@tagged('post_install', '-at_install')
class TestSiembrasYFunciones(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.manager = new_test_user(
            cls.env, login='e2_sf_manager', email='e2.sf.manager@example.com',
            groups='base.group_user,quimibond_sgi.group_sgi_manager')
        cls.admin = new_test_user(
            cls.env, login='e2_sf_admin', email='e2.sf.admin@example.com',
            groups='base.group_user,quimibond_sgi.group_sgi_admin')

    def test_01_update_only_seeds_parameters(self):
        with file_open('quimibond_sgi/data/sgi_parameters.xml') as handle:
            source = handle.read()
        functions = re.findall(r'<function\s[^>]*name="([^"]+)"', source)
        self.assertEqual(functions, ['seed_parameters'])

    def test_02_button_only_for_sgi_admin(self):
        Indicator = self.env['sgi.indicator']
        with self.assertRaises(AccessError):
            Indicator.with_user(self.manager).action_sgi_recompute_pending_measures()
        action = Indicator.with_user(self.admin).action_sgi_recompute_pending_measures()
        self.assertEqual(action['tag'], 'display_notification')

    def test_03_button_is_admin_only_in_the_view(self):
        arch = self.env.ref('quimibond_sgi.sgi_indicator_view_list').arch_db
        self.assertIn('action_sgi_recompute_pending_measures', arch)
        self.assertIn('quimibond_sgi.group_sgi_admin', arch)

    def test_04_daily_indicator_cron_recomputes(self):
        Config = type(self.env['sgi.config'])
        with patch.object(Config, 'recompute_pending_measures',
                          autospec=True, return_value={}) as mocked:
            self.env['sgi.cron'].cron_indicators(scheduled=True)
        self.assertTrue(mocked.called, "El cron diario debe recalcular las pendientes.")

    def test_05_stale_cron_is_noupdate(self):
        data = self.env['ir.model.data'].sudo().search([
            ('module', '=', 'quimibond_sgi'), ('name', '=', 'sgi_cron_my_procedure_stale')])
        self.assertTrue(data)
        self.assertTrue(data.noupdate)

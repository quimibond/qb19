# -*- coding: utf-8 -*-
"""57.7.0 (D-11): los modelos de Studio vacíos se borran solo si TODOS los de
la lista siguen en 0 filas; con uno solo con registros no se borra nada."""
from unittest.mock import patch

from odoo.exceptions import UserError
from odoo.tests import TransactionCase, new_test_user, tagged


@tagged('post_install', '-at_install')
class TestStudioCleanup(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        IrModel = cls.env['ir.model'].sudo()
        cls.empty = IrModel.create({'name': 'D11 vacío', 'model': 'x_sgi_d11_empty',
                                    'state': 'manual'})
        cls.full = IrModel.create({'name': 'D11 con datos', 'model': 'x_sgi_d11_full',
                                   'state': 'manual'})
        cls.env['x_sgi_d11_full'].sudo().create({'x_name': 'uno'})
        cls.Config = type(cls.env['sgi.config'])

    def _run(self, names, **kwargs):
        with patch.object(self.Config, '_SGI_STUDIO_MODELS', names):
            return self.env['sgi.config'].sgi_drop_empty_studio_models(**kwargs)

    def test_01_one_with_rows_aborts_everything(self):
        report = self._run(('x_sgi_d11_empty', 'x_sgi_d11_full'), dry_run=False)
        self.assertTrue(report['aborted'])
        self.assertEqual(report['counts'], {'x_sgi_d11_empty': 0, 'x_sgi_d11_full': 1})
        self.assertFalse(report['dropped'])
        self.assertTrue(self.empty.exists(), "Con un modelo con datos no se borra ninguno.")

    def test_02_dry_run_only_reports(self):
        report = self._run(('x_sgi_d11_empty', 'x_sgi_missing'))
        self.assertEqual(report['dropped'], ['x_sgi_d11_empty'])
        self.assertEqual(report['missing'], ['x_sgi_missing'])
        self.assertTrue(self.empty.exists())

    def test_03_drops_empty_and_logs(self):
        report = self._run(('x_sgi_d11_empty',), dry_run=False)
        self.assertEqual(report['dropped'], ['x_sgi_d11_empty'])
        self.assertFalse(self.empty.exists())
        self.assertTrue(self.env['ir.logging'].sudo().search_count([
            ('name', '=', 'quimibond_sgi.studio_cleanup'),
            ('message', 'ilike', 'x_sgi_d11_empty')]))

    def test_04_recount_before_drop(self):
        """Si el conteo justo antes de borrar ya no es 0, se revierte todo."""
        counts = iter([0, 5])
        with patch.object(self.Config, '_sgi_studio_row_count',
                          autospec=True, side_effect=lambda *a: next(counts)):
            with self.assertRaises(UserError):
                self._run(('x_sgi_d11_empty',), dry_run=False)

    def test_05_only_sgi_admin(self):
        user = new_test_user(self.env, login='d11_manager', email='d11@example.com',
                             groups='base.group_user,quimibond_sgi.group_sgi_manager')
        with self.assertRaises(UserError):
            self.env['sgi.config'].with_user(user).sgi_drop_empty_studio_models()

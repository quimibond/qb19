# -*- coding: utf-8 -*-
"""57.7.0 (D-11): los modelos de Studio vacíos se borran solo si TODOS los de
la lista siguen en 0 filas; con uno solo con registros no se borra nada.

57.12.0 (D-11 ampliada): sus acompañantes (``<modelo>_*``) se borran con el
padre; los que tienen filas esperadas (las 3 etapas de Studio) se respaldan
antes en CSV; uno con más filas, no esperado o al que apunta un campo de
fuera de la familia detiene todo."""
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

    def test_05_only_system(self):
        user = new_test_user(self.env, login='d11_manager', email='d11@example.com',
                             groups='base.group_user,quimibond_sgi.group_sgi_manager')
        with self.assertRaises(UserError):
            self.env['sgi.config'].with_user(user).sgi_drop_empty_studio_models()

    # ---- D-11 ampliada: acompañantes -----------------------------------------
    def _family(self, parent, stage_rows=3):
        IrModel = self.env['ir.model'].sudo()
        model = IrModel.create({'name': parent, 'model': parent, 'state': 'manual'})
        stage = IrModel.create({'name': parent + ' Stages', 'model': parent + '_stage',
                                'state': 'manual'})
        tag = IrModel.create({'name': parent + ' Tags', 'model': parent + '_tag', 'state': 'manual'})
        self.env['ir.model.fields'].sudo().create({
            'model_id': model.id, 'name': 'x_studio_stage_id', 'ttype': 'many2one',
            'relation': stage.model, 'state': 'manual'})
        for label in ("Nuevo", "En progreso", "Listo")[:stage_rows]:
            self.env[stage.model].sudo().create({'x_name': label})
        return model, stage, tag

    def _run_family(self, parent, companions, **kwargs):
        with patch.object(self.Config, '_SGI_STUDIO_COMPANIONS', companions):
            return self._run((parent,), **kwargs)

    def test_06_companions_dropped_with_csv_backup(self):
        model, stage, tag = self._family('x_sgi_d11_fam')
        expected = {stage.model: 3, tag.model: 0}
        report = self._run_family('x_sgi_d11_fam', expected)
        self.assertFalse(report['aborted'])
        self.assertEqual(report['backups'], [stage.model], "La prueba solo dice qué respaldaría.")
        self.assertEqual(sorted(report['dropped']), sorted([model.model, stage.model, tag.model]))
        self.assertTrue(stage.exists(), "La prueba no borra.")
        report = self._run_family('x_sgi_d11_fam', expected, dry_run=False)
        self.assertEqual(sorted(report['dropped']), sorted(['x_sgi_d11_fam', 'x_sgi_d11_fam_stage',
                                                            'x_sgi_d11_fam_tag']))
        self.assertFalse(model.exists() or stage.exists() or tag.exists())
        backup = self.env['ir.attachment'].sudo().browse(report['backups'])
        self.assertEqual(len(backup), 1)
        self.assertEqual(backup.res_model, 'res.company', "El respaldo sobrevive al modelo.")
        content = backup.raw.decode('utf-8')
        for label in ("Nuevo", "En progreso", "Listo"):
            self.assertIn(label, content)

    def test_07_companion_with_more_rows_aborts(self):
        model, stage, tag = self._family('x_sgi_d11_more')
        report = self._run_family('x_sgi_d11_more', {stage.model: 2, tag.model: 0}, dry_run=False)
        self.assertTrue(report['aborted'])
        self.assertFalse(report['dropped'])
        self.assertTrue(model.exists() and stage.exists() and tag.exists())

    def test_08_companion_pointed_from_outside_aborts(self):
        model, stage, tag = self._family('x_sgi_d11_ref')
        self.env['ir.model.fields'].sudo().create({
            'model_id': self.full.id, 'name': 'x_d11_stage_ref', 'ttype': 'many2one',
            'relation': stage.model, 'state': 'manual'})
        report = self._run_family('x_sgi_d11_ref', {stage.model: 3, tag.model: 0}, dry_run=False)
        self.assertTrue(report['aborted'])
        self.assertIn('x_sgi_d11_full.x_d11_stage_ref', " ".join(report['companion_problems']))
        self.assertTrue(model.exists() and stage.exists())

    def test_09_unexpected_companion_aborts(self):
        model, stage, tag = self._family('x_sgi_d11_unk')
        report = self._run_family('x_sgi_d11_unk', {stage.model: 3}, dry_run=False)
        self.assertTrue(report['aborted'])
        self.assertIn(tag.model, " ".join(report['companion_problems']))
        self.assertTrue(model.exists() and tag.exists())

    def test_10_production_companions_listed(self):
        """Los 5 acompañantes de producción (2026-09-29) con sus filas esperadas."""
        self.assertEqual(self.Config._SGI_STUDIO_COMPANIONS, {
            'x_actividades_obligato_stage': 3, 'x_calendario_de_obliga_stage': 3,
            'x_no_conformidades_stage': 3, 'x_no_conformidades_tag': 0,
            'x_no_conformidades_line_0ff2d': 0})

# -*- coding: utf-8 -*-
"""57.165.0 — Las rutas que se le dicen al usuario se abren.

Lo mismo que el MIID (57.161.0) en el Diagnóstico y en Mis pendientes: en vez
de una ruta escrita («SGI → Desempeño → Indicadores…»), un botón que abre la
pantalla; los pasos de la NC, como campos con su estado."""
from odoo.tests import TransactionCase, tagged

from ..models.sgi_menu_paths import sgi_menu_from_text, sgi_menu_path, sgi_open_menu_action


@tagged('post_install', '-at_install')
class TestRutasAbren(TransactionCase):

    def test_01_ruta_escrita_a_menu(self):
        indicators = self.env.ref('quimibond_sgi.menu_sgi_indicators')
        self.assertEqual(sgi_menu_from_text(self.env, sgi_menu_path('indicadores')), indicators)
        self.assertEqual(sgi_menu_from_text(self.env, sgi_menu_path('indicadores', 'columna Responsable')),
                         indicators, "El texto después de la ruta no estorba.")
        self.assertEqual(sgi_menu_from_text(self.env, "SGI → Planeación → Política integral (vigente)"),
                         self.env.ref('quimibond_sgi.menu_sgi_policy'))
        self.assertFalse(sgi_menu_from_text(self.env, "Revise el proceso con su dueño"))
        self.assertFalse(sgi_menu_from_text(self.env, False))
        action = sgi_open_menu_action(self.env, indicators)
        self.assertIn(action['type'], ('ir.actions.act_window', 'ir.actions.client'))

    def test_02_diagnostico_abre_donde_se_arregla(self):
        diag = self.env['sgi.diagnostic'].create({})
        with_fix = diag.line_ids.filtered('fix')
        self.assertTrue(with_fix, "El Diagnóstico trae hallazgos con «Dónde se arregla».")
        resolved = with_fix.filtered('fix_menu_id')
        self.assertTrue(resolved, "Las rutas del árbol del SGI se resuelven a su menú.")
        for line in resolved:
            self.assertTrue(line.fix.startswith(" → ".join(
                p.strip() for p in line.fix_menu_id.with_context(lang='es_MX').complete_name.split('/'))))
        self.assertIn(resolved[:1].action_open_fix()['type'], ('ir.actions.act_window', 'ir.actions.client'))

    def test_03_mis_pendientes_ir_a_hacerlo(self):
        process = self.env['sgi.process'].create({'code': 'ZRA1', 'name': 'Proceso rutas'})
        menu = self.env.ref('quimibond_sgi.menu_sgi_nc')
        activity = self.env['sgi.process.activity'].create({
            'process_id': process.id, 'name': 'Actividad rutas', 'odoo_menu_id': menu.id})
        row = self.env['sgi.my.pending'].sudo().create({
            'user_id': self.env.user.id, 'kind': 'actividad', 'name': 'Hacer rutas',
            'res_model': 'sgi.process.activity', 'res_id': activity.id})
        self.assertEqual(row.where_menu_id, menu)
        self.assertIn(row.action_open_where()['type'], ('ir.actions.act_window', 'ir.actions.client'))
        detail = self.env['sgi.my.pending']._sgi_activity_detail(activity) or ''
        self.assertNotIn('Dónde se hace', detail, "La ruta escrita ya no va en la ficha: es el botón.")

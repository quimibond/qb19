# -*- coding: utf-8 -*-
"""57.165.0 — Las rutas que se le dicen al usuario se abren.

Lo mismo que el MIID (57.161.0) en el Diagnóstico y en Mis pendientes: en vez
de una ruta escrita («SGI → Desempeño → Indicadores…»), un botón que abre la
pantalla; los pasos de la NC, como campos con su estado.

57.166.0: también los errores que dicen dónde se arregla (botón del
``RedirectWarning``) y los avisos del SGI (la ruta de la nota es liga)."""
from odoo.exceptions import RedirectWarning, UserError
from odoo.tests import TransactionCase, new_test_user, tagged

from ..models.sgi_menu_paths import (
    sgi_menu_from_text, sgi_menu_links, sgi_menu_path, sgi_open_menu_action, sgi_route_error)


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

    def test_04_error_con_boton(self):
        mast = new_test_user(self.env, login='zra_mast', groups='base.group_user,quimibond_sgi.group_sgi_manager')
        env = self.env(user=mast)
        exc = sgi_route_error(env, "Configúrelo en %s." % sgi_menu_path('fuentes_nc'), 'fuentes_nc')
        self.assertIsInstance(exc, RedirectWarning, "El administrador ve el menú: error con botón.")
        self.assertIsInstance(exc, UserError, "Sigue siendo UserError para los except y las pruebas.")
        self.assertEqual(type(exc).__module__ + '.' + type(exc).__name__, 'odoo.exceptions.RedirectWarning',
                         "El cliente web elige el diálogo con botón por este nombre.")
        action = self.env.ref('quimibond_sgi.menu_sgi_config_alert_sources').action
        self.assertEqual(exc.args[1], action.id)
        self.assertIn("Abrir", exc.args[2])
        # El riesgo ambiental a mano: el error trae el botón a la matriz.
        with self.assertRaises(RedirectWarning):
            env['sgi.risk'].create({'name': 'Ambiental a mano ZRA', 'instrument': 'ambiental'})

    def test_05_sin_acceso_sin_boton(self):
        plain = new_test_user(self.env, login='zra_sin_sgi', groups='base.group_user')
        exc = sgi_route_error(self.env(user=plain), "Configúrelo en Ajustes.", 'ajustes')
        self.assertIsInstance(exc, UserError)
        self.assertNotIsInstance(exc, RedirectWarning, "Sin acceso al menú no hay botón que lo niegue.")

    def test_06_aviso_con_liga(self):
        path = sgi_menu_path('indicadores')
        html = str(sgi_menu_links(self.env, "Revíselo en %s, columna <b>Responsable</b>." % path))
        action = self.env.ref('quimibond_sgi.menu_sgi_indicators').action
        self.assertIn('<a href="/odoo/action-%d">%s</a>' % (action.id, path), html)
        self.assertIn('<b>Responsable</b>', html, "La nota ya era HTML: se respeta.")
        self.assertEqual(str(sgi_menu_links(self.env, "Sin ruta")), "Sin ruta")
        # La ruta más larga gana: «… → Indicadores → Mediciones» no se parte.
        html = str(sgi_menu_links(self.env, sgi_menu_path('mediciones')))
        self.assertEqual(html.count('<a '), 1)
        process = self.env['sgi.process'].create({'code': 'ZRA2', 'name': 'Proceso avisos'})
        activity = self.env['sgi.cron']._sgi_schedule(
            process, "Aviso con ruta ZRA", "Abra %s." % path, self.env.user.id)
        self.assertIn('/odoo/action-%d' % action.id, str(activity.note))

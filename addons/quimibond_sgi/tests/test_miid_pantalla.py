# -*- coding: utf-8 -*-
"""57.161.0 — MIID y P-G01 por capítulos, con renglones y menús.

Jose 2026-10-20: «no se entiende ni se puede leer bien y no usa las mejores
prácticas de Odoo; no quiero texto plano». La pantalla es una pestaña por
capítulo: el texto sin la letra del PDF y, donde hay registros de Odoo, una
lista nativa. Las viñetas y tablas escritas son renglones (sgi.miid.item) y
las rutas escritas son menús que se abren. Los renglones entran a la huella.

Usa la empresa propia de test_miid (ZM Empresa MIID)."""
import importlib.util
import os

from odoo.exceptions import AccessError
from odoo.tests import tagged
from odoo.tools.misc import file_open

from .test_miid import _Case

_MODULE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@tagged('post_install', '-at_install')
class TestMiidPantalla(_Case):

    def _item(self, section, **vals):
        return self.env['sgi.miid.item'].with_user(self.mast).create(dict({
            'section_id': section.id, 'label': 'Concepto ZM'}, **vals))

    def test_01_pestana_sale_del_capitulo(self):
        Section = self.env['sgi.miid.section']
        self.assertEqual(self.s_proc.page, 'sistema', "4.4 del MIID va en «El sistema».")
        self.assertEqual(self.s_ctrl.page, 'cambios', "8 del MIID es el control de cambios.")
        pg = Section.create({'company_id': self.company.id, 'kind': 'pg01', 'clause': '6',
                             'name': 'Marco ZM', 'body': '<p>ZM</p>'})
        self.assertEqual(pg.page, 'marco', "El capítulo 6 del P-G01 es su marco normativo.")
        extra = Section.create({'company_id': self.company.id, 'clause': '12.1', 'name': 'Extra ZM'})
        self.assertEqual(extra.page, 'otros', "Un capítulo que la pantalla no conoce va en «Otros».")
        ident = Section.create({'company_id': self.company.id, 'name': 'Sin numeral ZM'})
        self.assertEqual(ident.page, 'general')

    def test_02_lista_nativa_y_texto_partido(self):
        self.s_proc.write({'body': '<p>Antes ZM</p><p>[[datos]]</p><p>Después ZM</p>'})
        screen = self.miid.with_user(self.user)
        self.assertIn(self.p1, screen.process_ids, "Los procesos son registros que se abren.")
        self.assertIn('Antes ZM', screen.scr_sistema_procesos_html)
        self.assertIn('Procesos ZM', screen.scr_sistema_procesos_html, "El título va antes de la lista.")
        self.assertNotIn('Después ZM', screen.scr_sistema_procesos_html)
        self.assertIn('Después ZM', screen.scr_sistema_html, "Lo de después sigue tras la lista.")
        self.assertNotIn('ZM1</td>', screen.scr_sistema_procesos_html,
                         "La tabla de procesos no se repite: es la lista nativa.")
        # Un bloque sin lista nativa (controles) se pinta como tabla en su pestaña.
        self._control('CO-ZM1-01', sgi_title='Control ZM de prueba')
        screen.invalidate_recordset()
        self.assertIn('Control ZM de prueba', screen.scr_cambios_html)
        self.assertNotIn('font-size:9px', screen.scr_sistema_procesos_html + screen.scr_cambios_html,
                         "En pantalla, la letra de Odoo; la del PDF solo en el PDF.")

    def test_03_renglones_en_tabla_y_en_lista(self):
        menu = self.env.ref('quimibond_sgi.menu_sgi_process_map')
        self.s_ctrl.write({'item_style': 'table', 'item_label_title': 'Tema ZM',
                           'item_clause_title': 'Cláusula ZM', 'item_text_title': 'Regla ZM'})
        self._item(self.s_ctrl, label='Riesgos ZM', clause='6.1', text='Regla de riesgos ZM', menu_id=menu.id)
        html = self._html()
        for text in ('Tema ZM', 'Cláusula ZM', 'Regla ZM', 'Riesgos ZM', 'Regla de riesgos ZM', 'En Odoo'):
            self.assertIn(text, html)
        self.assertIn('Mapa de procesos', html, "El PDF imprime la ruta del menú.")
        self.assertIn('font-size:10px', html, "57.161.0: las tablas del PDF van a 10 px (antes 9).")
        screen = self.miid.with_user(self.user)
        self.assertIn('/odoo/action-', screen.scr_cambios_html, "En pantalla la ruta es un botón.")
        self.s_ctrl.write({'item_style': 'list'})
        screen.invalidate_recordset()
        self.assertIn('<li', screen.scr_cambios_html)
        self.assertNotIn('Cláusula ZM', screen.scr_cambios_html, "La lista no lleva títulos de columna.")

    def test_04_renglones_en_la_huella_y_en_las_diferencias(self):
        before = self.miid._sgi_snapshot()
        item = self._item(self.s_proc, text='Uno ZM')
        after = self.miid._sgi_snapshot()
        self.assertNotEqual(self.Miid._sgi_hash(before), self.Miid._sgi_hash(after))
        self.assertTrue(any('Renglones nuevos' in d for d in self.Miid._sgi_diff(before, after)))
        item.with_user(self.mast).write({'text': 'Dos ZM'})
        changed = self.miid._sgi_snapshot()
        self.assertTrue(any('Cambiaron los renglones' in d for d in self.Miid._sgi_diff(after, changed)))
        menu = self.env.ref('quimibond_sgi.menu_sgi_nc')
        self.s_proc.with_user(self.mast).write({'menu_id': menu.id})
        with_menu = self.miid._sgi_snapshot()
        self.assertNotEqual(self.Miid._sgi_hash(changed), self.Miid._sgi_hash(with_menu),
                            "La ruta del menú se imprime: entra a la huella.")
        self.assertIn("Sección editada: 4.4 Procesos ZM", self.Miid._sgi_diff(changed, with_menu))

    def test_05_solo_el_jefe_mast_edita_y_cuenta_como_editado(self):
        with self.assertRaises(AccessError):
            self.env['sgi.miid.item'].with_user(self.user).create({
                'section_id': self.s_proc.id, 'label': 'No ZM'})
        self._item(self.s_proc)
        self.assertTrue(self.env['mail.message'].search_count([
            ('model', '=', 'sgi.miid.section'), ('res_id', '=', self.s_proc.id),
            ('body', 'ilike', 'Texto de la sección editado')]),
            "Editar renglones es editar la sección: la siembra ya no la pisa.")

    def test_07_no_pisa_los_campos_del_chatter(self):
        # 57.161.1: un campo «activity_ids» propio tumbó el registro.
        self.assertEqual(self.Miid._fields['activity_ids'].comodel_name, 'mail.activity')
        self.assertIn(self.env.ref('quimibond_sgi.sgi_miid_section_pg01_12_rutinas').page, ('rutinas',))

    def test_06_abrir_en_odoo(self):
        self.s_proc.with_user(self.mast).write({'menu_id': self.env.ref('quimibond_sgi.menu_sgi_process_map').id})
        action = self.s_proc.with_user(self.user).action_open_menu()
        self.assertEqual(action['type'], 'ir.actions.act_window')
        revisions = self.miid.with_user(self.user).action_open_revisions()
        self.assertIn(('sgi_code', '=', 'MIID'), revisions['domain'])


@tagged('post_install', '-at_install')
class TestMiidSiembraRenglones(_Case):
    """La siembra trae el formato y los renglones; la migración los pone en
    una base existente sin pisar lo editado y sin duplicar."""

    def test_10_siembra_de_renglones(self):
        from lxml import etree
        with file_open('quimibond_sgi/data/sgi_miid_items.xml', 'rb') as fh:
            tree = etree.parse(fh)
        items = tree.xpath('//record[@model="sgi.miid.item"]')
        self.assertGreaterEqual(len(items), 80)
        Data = self.env['ir.model.data'].sudo()
        for record in items:
            self.assertTrue(Data.search([('module', '=', 'quimibond_sgi'), ('name', '=', record.get('id'))]).noupdate,
                            record.get('id'))
        reglas = self.env.ref('quimibond_sgi.sgi_miid_section_52_reglas')
        self.assertEqual(len(reglas.item_ids), 12, "Una regla por renglón, con su cláusula y su pantalla.")
        self.assertTrue(all(reglas.item_ids.mapped('clause')))
        self.assertTrue(all(reglas.item_ids.mapped('menu_id')))
        with file_open('quimibond_sgi/data/sgi_miid_sections.xml', 'rb') as fh:
            seed = fh.read().decode()
        for written in ('SGI → Sistema', 'SGI → Planeación', 'SGI → Mejora', 'SGI → Inicio',
                        'SGI → Administración', '<table', '<ul'):
            self.assertNotIn(written, seed, "Las rutas son menús y las tablas, renglones: %s" % written)

    def test_11_migracion_idempotente(self):
        path = os.path.join(_MODULE_DIR, 'migrations', '19.0.57.161.0', 'post-migrate.py')
        spec = importlib.util.spec_from_file_location('sgi_mig_57_161_0', path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        reglas = self.env.ref('quimibond_sgi.sgi_miid_section_52_reglas')
        politica = self.env.ref('quimibond_sgi.sgi_miid_section_17_politica')
        # Como estaba en 57.160.x: viñetas en el texto, sin menú ni formato.
        self.env.cr.execute("UPDATE sgi_miid_section SET body = %s, menu_id = NULL, item_style = 'table' "
                            "WHERE id = %s", ('<ul><li>Regla vieja ZM</li></ul>', reglas.id))
        self.env.cr.execute("UPDATE sgi_miid_section SET menu_id = NULL WHERE id = %s", [politica.id])
        (reglas | politica).invalidate_recordset()
        self.env['mail.message'].sudo().search([
            ('model', '=', 'sgi.miid.section'), ('res_id', 'in', (reglas | politica).ids)]).unlink()
        module.migrate(self.env.cr, '19.0.57.160.1')
        module.migrate(self.env.cr, '19.0.57.160.1')
        self.assertNotIn('Regla vieja ZM', reglas.body)
        self.assertIn('candados y avisos', reglas.body)
        self.assertEqual(politica.menu_id, self.env.ref('quimibond_sgi.menu_sgi_policy'))
        self.assertEqual(politica.item_style, 'list')
        self.assertEqual(self.env['mail.message'].sudo().search_count([
            ('model', '=', 'sgi.miid.section'), ('res_id', '=', reglas.id),
            ('body', 'ilike', '57.161.0: texto actualizado')]), 1, "La segunda corrida no hace nada.")
        self.assertFalse(self.env['mail.message'].sudo().search_count([
            ('model', '=', 'sgi.miid.section'), ('res_id', 'in', (reglas | politica).ids),
            ('body', 'ilike', 'Texto de la sección editado')]), "La migración no es una edición a mano.")

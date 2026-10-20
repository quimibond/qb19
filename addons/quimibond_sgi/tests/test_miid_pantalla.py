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

from odoo.exceptions import AccessError, ValidationError
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
        self.assertGreaterEqual(len(items), 50)
        Data = self.env['ir.model.data'].sudo()
        for record in items:
            self.assertTrue(Data.search([('module', '=', 'quimibond_sgi'), ('name', '=', record.get('id'))]).noupdate,
                            record.get('id'))
        # 57.162.0: las reglas son registros de sgi.system.rule, no renglones.
        reglas = self.env.ref('quimibond_sgi.sgi_miid_section_52_reglas')
        self.assertEqual(reglas.live_block, 'reglas')
        rules = self.env['sgi.system.rule'].search([])
        self.assertGreaterEqual(len(rules), 15)
        self.assertTrue(all(rules.mapped('clause_ids')) and all(rules.mapped('menu_id')),
                        "Cada regla con sus cláusulas y su pantalla.")
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
        # Como estaba en 57.160.x: viñetas en el texto, sin menú.
        self.env.cr.execute("UPDATE sgi_miid_section SET body = %s, menu_id = NULL WHERE id = %s",
                            ('<ul><li>Regla vieja ZM</li></ul>', reglas.id))
        self.env.cr.execute("UPDATE sgi_miid_section SET menu_id = NULL WHERE id = %s", [politica.id])
        (reglas | politica).invalidate_recordset()
        self.env['mail.message'].sudo().search([
            ('model', '=', 'sgi.miid.section'), ('res_id', 'in', (reglas | politica).ids)]).unlink()
        module.migrate(self.env.cr, '19.0.57.160.1')
        module.migrate(self.env.cr, '19.0.57.160.1')
        self.assertNotIn('Regla vieja ZM', reglas.body)
        self.assertIn('candados y avisos', reglas.body)
        self.assertEqual(politica.menu_id, self.env.ref('quimibond_sgi.menu_sgi_policy'))
        self.assertEqual(self.env['mail.message'].sudo().search_count([
            ('model', '=', 'sgi.miid.section'), ('res_id', '=', reglas.id),
            ('body', 'ilike', '57.161.0: texto actualizado')]), 1, "La segunda corrida no hace nada.")
        self.assertFalse(self.env['mail.message'].sudo().search_count([
            ('model', '=', 'sgi.miid.section'), ('res_id', 'in', (reglas | politica).ids),
            ('body', 'ilike', 'Texto de la sección editado')]), "La migración no es una edición a mano.")

    def _mig_162(self):
        path = os.path.join(_MODULE_DIR, 'migrations', '19.0.57.162.0', 'post-migrate.py')
        spec = importlib.util.spec_from_file_location('sgi_mig_57_162_0', path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_12_migracion_57_162_renglones_y_bloques(self):
        module = self._mig_162()
        Data = self.env['ir.model.data'].sudo()
        terminos = self.env.ref('quimibond_sgi.sgi_miid_section_09_terminos')
        reglas = self.env.ref('quimibond_sgi.sgi_miid_section_52_reglas')
        roles = self.env.ref('quimibond_sgi.sgi_miid_section_18_roles')
        first = self.env.ref('quimibond_sgi.sgi_miid_item_09_terminos_01')
        # Como quedó en 57.161.0: renglón largo, un renglón que la siembra ya
        # no trae y la sección de reglas sin su bloque.
        first.sudo().write({'text': 'Texto largo ZM'})
        old = self.env['sgi.miid.item'].sudo().create({'section_id': reglas.id, 'label': 'Viejo ZM'})
        Data.create({'module': 'quimibond_sgi', 'name': 'sgi_miid_item_52_reglas_99', 'model': 'sgi.miid.item',
                     'res_id': old.id, 'noupdate': True})
        self.env.cr.execute("UPDATE sgi_miid_section SET live_block = NULL WHERE id = %s", [reglas.id])
        reglas.invalidate_recordset()
        # Una sección editada a mano no se toca.
        roles.sudo().message_post(body="Texto de la sección editado.")
        roles_item = roles.item_ids[:1]
        roles_item.sudo().write({'text': 'Rol ZM del Jefe MAST'})
        manual = self.env['sgi.miid.item'].sudo().create({'section_id': terminos.id, 'label': 'Sin siembra ZM'})
        module.migrate(self.env.cr, '19.0.57.161.1')
        module.migrate(self.env.cr, '19.0.57.161.1')
        self.assertNotEqual(first.text, 'Texto largo ZM', "El renglón sembrado vuelve al de la siembra.")
        self.assertFalse(old.exists(), "Lo que la siembra ya no trae se quita.")
        self.assertTrue(manual.exists(), "Un renglón sin xmlid no lo toca la siembra.")
        self.assertEqual(roles_item.text, 'Rol ZM del Jefe MAST')
        self.assertEqual(reglas.live_block, 'reglas', "La sección lee las reglas del sistema.")

    def test_13_migracion_57_162_llena_solo_lo_vacio(self):
        module = self._mig_162()
        iso9001 = self.env.ref('quimibond_sgi.sgi_norm_9001')
        iso14001 = self.env.ref('quimibond_sgi.sgi_norm_14001')
        clause = self.env.ref('quimibond_sgi.c_9001_84')
        dtype = self.env.ref('quimibond_sgi.sgi_doc_type_instructivo')
        iso9001.write({'scope': False})
        iso14001.write({'scope': 'Alcance ZM de MAST'})
        clause.write({'exclusion': False, 'exclusion_detail': False, 'exclusion_reason': False})
        dtype.write({'approver_text': 'Aprobador ZM', 'author_text': False})
        module.migrate(self.env.cr, '19.0.57.161.1')
        self.assertIn('entretelas', iso9001.scope)
        self.assertEqual(iso14001.scope, 'Alcance ZM de MAST', "Lo capturado no se pisa.")
        self.assertTrue(clause.exclusion)
        self.assertEqual(clause.exclusion_detail, '8.4.1 b)')
        self.assertTrue(dtype.author_text)
        self.assertEqual(dtype.approver_text, 'Aprobador ZM')


@tagged('post_install', '-at_install')
class TestMiidCampos(_Case):
    """57.162.0: alcance, no aplicabilidades, reglas y roles por tipo de
    documento son datos de su registro; el manual los lee y entran a la
    huella."""

    def _section(self, block, clause, kind='miid'):
        return self.env['sgi.miid.section'].create({
            'company_id': self.company.id, 'kind': kind, 'clause': clause, 'name': 'Bloque ZM %s' % block,
            'body': '<p>[[datos]]</p>', 'live_block': block})

    def test_20_alcance_y_no_aplica(self):
        self._section('alcance', '1.2')
        self._section('exclusiones', '1.3')
        norm = self.env['sgi.norm'].create({'code': 'ZM-NORMA', 'name': 'Norma ZM', 'scope': 'Alcance ZM'})
        clause = self.env['sgi.norm.clause'].create({'norm_id': norm.id, 'code': '7.7', 'name': 'Requisito ZM'})
        before = self.miid._sgi_snapshot()
        with self.assertRaises(ValidationError):
            clause.write({'exclusion': True})
        clause.write({'exclusion': True, 'exclusion_detail': '7.7.1 a)', 'exclusion_reason': 'Razón ZM'})
        html = self._html()
        for text in ('Alcance ZM', '7.7.1 a)', 'Razón ZM'):
            self.assertIn(text, html)
        after = self.miid._sgi_snapshot()
        self.assertTrue(any('ya no aplica' in d for d in self.Miid._sgi_diff(before, after)))

    def test_21_reglas_del_sistema(self):
        self._section('reglas', '5.2')
        self.env['sgi.system.rule'].search([]).write({'active': False})
        clause = self.env['sgi.norm.clause'].create({
            'norm_id': self.env.ref('quimibond_sgi.sgi_norm_9001').id, 'code': '9.9', 'name': 'Requisito ZM'})
        rule = self.env['sgi.system.rule'].create({
            'name': 'Regla ZM', 'text': 'Lo hace el sistema ZM', 'clause_ids': [(6, 0, clause.ids)],
            'menu_id': self.env.ref('quimibond_sgi.menu_sgi_nc').id})
        self.assertEqual(rule.clause_label, '9.9')
        screen = self.miid.with_user(self.user)
        self.assertEqual(screen.rule_ids, rule, "En pantalla, la lista nativa de reglas.")
        self.assertIn('Bloque ZM reglas', screen.scr_requisitos_reglas_html)
        html = self._html()
        self.assertIn('Lo hace el sistema ZM', html)
        self.assertIn('No conformidades', html, "El PDF imprime la ruta del menú.")
        snap = self.miid._sgi_snapshot()
        rule.write({'text': 'Otra regla ZM'})
        self.assertTrue(any('Cambió la regla del sistema' in d
                            for d in self.Miid._sgi_diff(snap, self.miid._sgi_snapshot())))

    def test_22_roles_por_tipo_en_el_pg01(self):
        pg01 = self.Miid._sgi_get(self.company, kind='pg01')
        self._section('elaboracion', '4.2', kind='pg01')
        self._section('aprobacion', '4.3', kind='pg01')
        dtype = self.env['sgi.document.type'].create({
            'code': 'zm_tipo', 'name': 'Tipo ZM', 'company_id': self.company.id,
            'author_text': 'Elabora ZM', 'reviewer_text': 'Revisa ZM', 'approver_text': 'Aprueba ZM'})
        html = pg01.with_user(self.user).scr_desarrollo_html
        for text in ('Tipo ZM', 'Elabora ZM', 'Revisa ZM', 'Aprueba ZM'):
            self.assertIn(text, html)
        snap = pg01._sgi_snapshot()
        dtype.write({'approver_text': 'Otro ZM'})
        self.assertNotEqual(self.Miid._sgi_hash(snap), self.Miid._sgi_hash(pg01._sgi_snapshot()),
                            "Quién firma entra a la huella del P-G01.")


@tagged('post_install', '-at_install')
class TestMiidPendientesParaAprobar(_Case):
    """57.164.0: lo que impide aprobar se ve como listas que se resuelven en
    la misma pantalla (confirmar la sección, ver qué le falta al proceso o
    publicarlo)."""

    def test_30_listas_de_pendientes(self):
        from odoo.tests import new_test_user
        self.s_proc.write({'to_confirm': True, 'to_confirm_note': 'Nota ZM por confirmar'})
        screen = self.miid.with_user(self.user)
        self.assertEqual(screen.to_confirm_section_ids, self.s_proc)
        self.assertEqual(screen.pending_process_ids, self.p1, "El proceso en borrador impide aprobar.")
        self.assertEqual(screen.approval_todo_count, 2)
        director = new_test_user(self.env, login='zm_miid_dir_todo',
                                 groups='base.group_user,quimibond_sgi.group_sgi_director')
        director.write({'company_ids': [(4, self.company.id)]})
        self.s_proc.with_user(director).action_sgi_confirm()
        self.assertFalse(self.s_proc.to_confirm)
        self.assertTrue(self.s_proc.message_ids.filtered(lambda m: 'quitado por' in (m.body or '')),
                        "Confirmar desde la pantalla deja el rastro de siempre.")
        self._publish()
        screen.invalidate_recordset()
        self.assertFalse(screen.pending_process_ids)
        self.assertEqual(screen.approval_todo_count, 0, "Sin pendientes, la lista se vacía.")
        action = self.s_proc.with_user(self.mast).action_open_form()
        self.assertEqual((action['res_model'], action['res_id']), ('sgi.miid.section', self.s_proc.id))

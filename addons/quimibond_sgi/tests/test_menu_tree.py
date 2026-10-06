# -*- coding: utf-8 -*-
"""J-023 (entrega 4): el árbol de menús del SGI en la base es el de
tools/sgi_menu_tree.txt (entradas, orden, nombres y grupos), y las rutas que
se le dicen al usuario (I-009) existen. 57.113.0: carpetas por capítulo
(tests/test_menus_capitulos.py prueba los movimientos y lo archivado)."""
from odoo.tests import TransactionCase, tagged
from odoo.tools.misc import file_open

from ..models.sgi_menu_paths import SGI_MENU_PATHS

TREE_FILE = 'quimibond_sgi/tools/sgi_menu_tree.txt'


def _expected_tree():
    rows = []
    with file_open(TREE_FILE) as handle:
        for raw in handle:
            line = raw.strip()
            if not line or line.startswith('#'):
                continue
            path, xmlid, groups = [part.strip() for part in line.split('|')]
            rows.append((path, xmlid, groups))
    return rows


@tagged('post_install', '-at_install')
class TestMenuTree(TransactionCase):

    def _menu_lines(self, lang='en_US'):
        """Árbol real bajo el raíz del SGI, en el orden en que Odoo lo pinta
        (secuencia, luego id)."""
        Menu = self.env['ir.ui.menu'].sudo().with_context(lang=lang, active_test=True)
        root = self.env.ref('quimibond_sgi.menu_sgi_root').with_context(lang=lang)
        xmlids = {}
        for data in self.env['ir.model.data'].sudo().search([('model', '=', 'ir.ui.menu')]):
            xmlids[data.res_id] = data.name if data.module == 'quimibond_sgi' \
                else '%s.%s' % (data.module, data.name)
        group_xmlids = {}

        def group_label(groups):
            names = []
            for group in groups:
                if group.id not in group_xmlids:
                    group_xmlids[group.id] = group.get_external_id().get(group.id) or str(group.id)
                names.append(group_xmlids[group.id])
            return ','.join(sorted(names)) or '-'

        lines = []

        def walk(menu, path):
            path = path + [menu.name]
            lines.append(('/'.join(path), xmlids.get(menu.id, str(menu.id)), group_label(menu.group_ids)))
            for child in Menu.search([('parent_id', '=', menu.id)], order='sequence, id'):
                walk(child, path)

        walk(root, [])
        return lines

    def _satellites(self):
        """Módulos instalados que dependen del SGI (quimibond_sgi_mapa,
        _studio, _knowledge…): cuelgan sus propios menús del árbol."""
        deps = self.env['ir.module.module.dependency'].sudo().search([('name', '=', 'quimibond_sgi')])
        return set(deps.module_id.filtered(lambda m: m.state == 'installed').mapped('name'))

    def test_01_tree_matches_file(self):
        expected = _expected_tree()
        # El archivo describe el árbol del núcleo. Con los satélites instalados
        # (base nueva con todos los módulos del repo; p. ej. «Cargar mapa de
        # procesos» de quimibond_sgi_mapa) sus menús se quitan de la
        # comparación; los del núcleo siguen en el mismo orden.
        satellites = self._satellites()
        actual = [row for row in self._menu_lines()
                  if '.' not in row[1] or row[1].split('.', 1)[0] not in satellites]
        missing = [row for row in expected if row not in actual]
        extra = [row for row in actual if row not in expected]
        self.assertFalse(
            missing or extra,
            "El árbol del SGI no coincide con tools/sgi_menu_tree.txt.\nFaltan o difieren:\n%s\nSobran:\n%s"
            % ("\n".join(" | ".join(r) for r in missing), "\n".join(" | ".join(r) for r in extra)))
        self.assertEqual(actual, expected, "Mismas entradas pero en otro orden (secuencias).")

    def test_02_same_names_in_every_language(self):
        """El usuario ve los nombres del código aunque su idioma sea es_MX
        (ir.ui.menu._sgi_sync_menu_names)."""
        langs = self.env['res.lang'].sudo().search([('active', '=', True)]).mapped('code')
        base = [row[0] for row in self._menu_lines('en_US')]
        for lang in langs:
            self.assertEqual([row[0] for row in self._menu_lines(lang)], base,
                             "Los menús del SGI en %s no dicen lo mismo que el código." % lang)

    def test_03_menu_paths_told_to_users_exist(self):
        """I-009: cada ruta de models/sgi_menu_paths.py es la ruta real de su
        menú (los nombres de los menús del módulo)."""
        own = set(self.env['ir.model.data'].sudo().search([
            ('module', '=', 'quimibond_sgi'), ('model', '=', 'ir.ui.menu')]).mapped('res_id'))
        for key, (xmlid, names) in SGI_MENU_PATHS.items():
            menu = self.env.ref(xmlid).sudo().with_context(lang='en_US')
            chain = []
            node = menu
            while node:
                chain.insert(0, node)
                node = node.parent_id
            self.assertEqual(len(chain), len(names), "%s: la ruta tiene otro largo." % key)
            for node, name in zip(chain, names):
                if node.id in own:
                    self.assertEqual(node.name, name, "%s: «%s» ya no se llama así." % (key, name))

    def test_04_every_module_menu_lives_in_sgi_menus_xml(self):
        """A-025: el árbol se lee en un solo archivo."""
        from lxml import etree
        with file_open('quimibond_sgi/views/sgi_menus.xml', 'rb') as handle:
            declared = {el.get('id') for el in etree.parse(handle).getroot().iter('menuitem')}
        own = self.env['ir.model.data'].sudo().search([
            ('module', '=', 'quimibond_sgi'), ('model', '=', 'ir.ui.menu')]).mapped('name')
        self.assertFalse(set(own) - declared,
                         "Menús del módulo declarados fuera de views/sgi_menus.xml.")

    def test_05_direction_open_to_every_sgi_user(self):
        """Decisión 10 de la tanda 2: Planeación (antes Dirección, mismo
        xmlid) la ven todos (sin grupos propios); Tablero, Revisión y
        Satisfacción (en Desempeño desde 57.113.0) y Partes interesadas, solo
        Auditor, MAST y Dirección."""
        self.assertFalse(self.env.ref('quimibond_sgi.menu_sgi_direction').group_ids)
        restricted = (self.env.ref('quimibond_sgi.group_sgi_auditor')
                      | self.env.ref('quimibond_sgi.group_sgi_manager')
                      | self.env.ref('quimibond_sgi.group_sgi_director'))
        for xmlid in ('menu_sgi_dashboard_health', 'menu_sgi_mgmt_review',
                      'menu_sgi_interested_parties', 'menu_sgi_satisfaction'):
            self.assertEqual(self.env.ref('quimibond_sgi.' + xmlid).group_ids, restricted, xmlid)

    def test_06_dropbox_by_group(self):
        """57.0.0 (decisión de Jose): en «Del Dropbox a Odoo» el buscador y
        «Formatos y documentos anteriores» los ve todo Usuario SGI; rutinas,
        procedimientos anteriores y avance solo Auditor, Jefe MAST, Dirección y
        Dueño de proceso."""
        for xmlid in ('menu_sgi_dropbox', 'menu_sgi_dropbox_search', 'menu_sgi_migration'):
            self.assertFalse(self.env.ref('quimibond_sgi.' + xmlid).group_ids, xmlid)
        restricted = (self.env.ref('quimibond_sgi.group_sgi_auditor')
                      | self.env.ref('quimibond_sgi.group_sgi_manager')
                      | self.env.ref('quimibond_sgi.group_sgi_director')
                      | self.env.ref('quimibond_sgi.group_sgi_process_owner'))
        for xmlid in ('menu_sgi_dropbox_procedures', 'menu_sgi_dropbox_routines',
                      'menu_sgi_dropbox_progress'):
            self.assertEqual(self.env.ref('quimibond_sgi.' + xmlid).group_ids, restricted, xmlid)

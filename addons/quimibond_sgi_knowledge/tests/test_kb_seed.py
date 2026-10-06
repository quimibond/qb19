# -*- coding: utf-8 -*-
"""1.2.0: siembra del espacio «SGI» en Conocimiento (decisiones 1 y 2, N1)."""
import ast
from unittest.mock import patch

from odoo.tests import tagged
from odoo.tools import file_open

from odoo.addons.quimibond_sgi_knowledge.models.sgi_knowledge_article import (
    SGI_KB_MANUALS, SGI_KB_OLD_ROOT_NAME, SGI_KB_VERSION, sgi_kb_hash)

from .common_kb import KbCase


@tagged('post_install', '-at_install')
class TestKbSeed(KbCase):

    def _find(self, key):
        return self.Article.with_context(active_test=False).search([('sgi_seed_key', '=', key)])

    def test_01_espacio(self):
        self.Article._sgi_kb_seed()
        self.Article._sgi_kb_seed()
        root = self._find('raiz')
        self.assertEqual(len(root), 1, "Una sola raíz aunque se siembre dos veces.")
        self.assertFalse(root.parent_id)
        self.assertEqual(root.category, 'workspace')
        self.assertEqual(root.internal_permission, 'read')
        writers = root.article_member_ids.filtered(lambda m: m.permission == 'write')
        self.assertTrue(writers, "Al menos un miembro con escritura.")
        self.assertIn(self.mast.partner_id, writers.partner_id, "El Jefe MAST escribe en la raíz.")
        for key, name in (('carpeta:ayuda', "Cómo usar el sistema"), ('carpeta:reglamentos', "Reglamentos")):
            folder = self._find(key)
            self.assertEqual(len(folder), 1, key)
            self.assertEqual(folder.name, name)
            self.assertEqual(folder.parent_id, root)
        article = self._find('proceso:ZK2')
        self.assertEqual(len(article), 1)
        self.assertEqual(article.parent_id, root)
        self.assertEqual(article.sgi_process_id, self.process)
        self.assertEqual(self.process.sgi_article_id, article)
        self.assertIn(self.owner.partner_id, article.article_member_ids.filtered(
            lambda m: m.permission == 'write').partner_id, "El dueño del proceso escribe en su proceso.")

    def test_02_espacio_viejo(self):
        old = self.Article.create({'name': 'SGI', 'internal_permission': 'write'})
        before = len(old.message_ids)
        self.Article._sgi_kb_seed()
        self.assertEqual(old.name, SGI_KB_OLD_ROOT_NAME)
        self.assertTrue(old.active, "No se archiva.")
        self.assertEqual(old.internal_permission, 'write', "Su permiso no cambia.")
        self.assertEqual(len(old.message_ids), before + 1, "Un mensaje en su chatter.")
        self.Article._sgi_kb_seed()
        self.assertEqual(len(old.message_ids), before + 1, "La segunda siembra no agrega mensaje.")
        self.assertNotEqual(self._find('raiz'), old)

    def test_03_manuales(self):
        self.Article._sgi_kb_seed()
        folder = self._find('carpeta:ayuda')
        for key, _name, filename, _source in SGI_KB_MANUALS:
            article = self._find(key)
            self.assertEqual(len(article), 1, key)
            self.assertEqual(article.parent_id, folder, key)
            self.assertNotIn('sgi-kb:', str(article.body or ''), "%s: quedó una liga sin resolver." % key)
            self.assertEqual(article.sgi_seed_hash, sgi_kb_hash(article.body), key)
            self.assertTrue(self.Article._sgi_kb_manual_html(filename).strip(), filename)
        with file_open('quimibond_sgi_knowledge/__manifest__.py') as handle:
            manifest = ast.literal_eval(handle.read())
        self.assertTrue(manifest['version'].endswith('.' + SGI_KB_VERSION),
                        "SGI_KB_VERSION debe ser la versión del manifest.")

    def test_04_opcion_a(self):
        self.Article._sgi_kb_seed()
        untouched = self._find('manual:glosario')
        edited = self._find('manual:rh')
        edited.with_user(self.mast).write({'body': '<p>Texto escrito a mano en Conocimiento.</p>'})
        edited_before = len(edited.message_ids)

        def fake(model, filename):
            return '<p>Texto nuevo del sistema para %s.</p>' % filename

        with patch.object(type(self.Article), '_sgi_kb_manual_html', fake):
            self.Article._sgi_kb_seed()
            self.assertIn('Texto nuevo del sistema para glosario.html', str(untouched.body))
            self.assertEqual(untouched.sgi_seed_hash, sgi_kb_hash(untouched.body))
            self.assertIn('Texto escrito a mano', str(edited.body), "Lo editado no se pisa.")
            self.assertEqual(len(edited.message_ids), edited_before + 1, "Un aviso en su chatter.")
            self.Article._sgi_kb_seed()
            self.assertEqual(len(edited.message_ids), edited_before + 1, "Una vez por versión.")

    def test_05_archivado_no_revive(self):
        self.Article._sgi_kb_seed()
        manual = self._find('manual:auditor')
        manual.write({'active': False})
        self.Article._sgi_kb_seed()
        manual = self._find('manual:auditor')
        self.assertEqual(len(manual), 1, "No se duplica.")
        self.assertFalse(manual.active, "Sigue archivado.")

# -*- coding: utf-8 -*-
"""1.2.0: «Ayuda» por rol, menús del satélite y botones en las vistas del núcleo."""
from odoo.tests import tagged, new_test_user

from .common_kb import KbCase


@tagged('post_install', '-at_install')
class TestKbLigas(KbCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Article._sgi_kb_seed()

    def _help_key(self, user):
        action = self.Article.with_user(user)._sgi_kb_help_action()
        article_id = int(action['url'].rstrip('/').rsplit('/', 1)[1])
        return self.Article.browse(article_id).sgi_seed_key

    def test_01_ayuda_por_rol(self):
        def user(login, groups):
            return new_test_user(self.env, login=login, groups='base.group_user,' + groups)

        cases = (
            (self.mast, 'manual:mast'),
            (user('zk_help_dir', 'quimibond_sgi.group_sgi_director'), 'manual:direccion'),
            (user('zk_help_aud', 'quimibond_sgi.group_sgi_auditor'), 'manual:auditor'),
            (user('zk_help_rh', 'quimibond_sgi.group_sgi_user,hr.group_hr_user'), 'manual:rh'),
            (self.owner, 'manual:jefe-de-area'),
            (self.plain, 'manual:operador-o-supervisor'),
        )
        for who, key in cases:
            self.assertEqual(self._help_key(who), key, who.login)

    def test_02_menus(self):
        Menu = self.env['ir.ui.menu']
        help_menu = self.env.ref('quimibond_sgi_knowledge.menu_sgi_kb_help')
        articles_menu = self.env.ref('quimibond_sgi_knowledge.menu_sgi_kb_articles')
        import_menu = self.env.ref('quimibond_sgi_knowledge.menu_sgi_kb_import')
        self.assertEqual(help_menu.parent_id, self.env.ref('quimibond_sgi.menu_sgi_panel'))
        self.assertEqual(articles_menu.parent_id, self.env.ref('quimibond_sgi.menu_sgi_processes'))
        self.assertEqual(import_menu.parent_id, self.env.ref('quimibond_sgi.menu_sgi_transition'))
        plain = Menu.with_user(self.plain)._visible_menu_ids()
        owner = Menu.with_user(self.owner)._visible_menu_ids()
        mast = Menu.with_user(self.mast)._visible_menu_ids()
        self.assertIn(help_menu.id, plain)
        self.assertNotIn(articles_menu.id, plain)
        self.assertNotIn(import_menu.id, plain)
        self.assertIn(articles_menu.id, owner)
        self.assertNotIn(import_menu.id, owner)
        self.assertIn(import_menu.id, mast)

    def test_03_vistas(self):
        Role = self.env['sgi.activity.role']
        for xmlid, kind in (('quimibond_sgi.sgi_activity_role_view_kanban_mp', 'kanban'),
                            ('quimibond_sgi.sgi_activity_role_view_list_mp_embedded', 'list'),
                            ('quimibond_sgi.sgi_activity_role_view_list_my_procedure', 'list')):
            arch = Role.get_views([(self.env.ref(xmlid).id, kind)])['views'][kind]['arch']
            self.assertIn('action_mp_article', arch, xmlid)
            self.assertIn('action_mp_instruction', arch, "«Instructivo» sigue abriendo el PDF.")
        doc_view = self.env.ref('quimibond_sgi.sgi_document_view_form')
        arch = self.Doc.with_user(self.mast).get_views([(doc_view.id, 'form')])['views']['form']['arch']
        self.assertIn('sgi_article_id', arch)
        self.assertIn('action_sgi_kb_publish', arch)
        process_view = self.env.ref('quimibond_sgi.sgi_process_view_form')
        arch = self.env['sgi.process'].get_views([(process_view.id, 'form')])['views']['form']['arch']
        self.assertIn('action_sgi_open_article', arch)

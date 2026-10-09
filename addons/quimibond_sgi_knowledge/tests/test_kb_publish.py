# -*- coding: utf-8 -*-
"""1.2.0: «Publicar» de DOC-5 para instructivos, controles operacionales,
protocolos y reglamentos (decisión 5, N4, N5)."""
from unittest.mock import patch

from odoo.exceptions import UserError
from odoo.tests import tagged

from .common_kb import KbCase


def _fake_text(self, attachment):
    return "Texto importado de prueba.", "texto de prueba"


@tagged('post_install', '-at_install')
class TestKbPublish(KbCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Article._sgi_kb_seed()

    def _import(self, docs, user=None):
        with patch.object(type(self.env['sgi.knowledge.import']), '_sgi_pdf_text', _fake_text):
            return super()._import(docs, user=user)

    def _publish(self, article, **vals):
        wizard = self.env['sgi.instruction.publish'].with_user(self.mast).create(dict({'article_id': article.id}, **vals))
        wizard.action_publish()
        return wizard

    def _current(self, code):
        return self.Doc.search([('sgi_code', '=', code), ('sgi_state', '=', 'vigente')])

    def test_01_co(self):
        folder = self.Doc.create({'name': 'ZK carpeta Conocimiento', 'type': 'folder'})
        parent = self._doc('P-V98', 'procedimiento', 'Procedimiento padre ZK', sgi_state='borrador')
        co = self._doc('CO-ZK2-10', 'control_operacional', 'Control de ruido',
                       sgi_previous_code='P-V99', sgi_parent_document_id=parent.id, folder_id=folder.id)
        self._import(co)
        article = self._article('CO-ZK2-10')
        self._publish(article)
        new = self._current('CO-ZK2-10')
        self.assertEqual(len(new), 1)
        self.assertEqual(new.sgi_revision, 1)
        self.assertEqual(co.sgi_state, 'obsoleto')
        self.assertEqual(new.sgi_previous_code, 'P-V99')
        self.assertEqual(new.sgi_parent_document_id, parent)
        self.assertEqual(new.folder_id, folder)
        self.assertEqual(new.sgi_doc_type, 'control_operacional')
        self.assertEqual(new.sgi_title, co.sgi_title, "El título no cambia.")
        self.assertEqual(new.sgi_article_id, article)
        self.assertEqual(new.sgi_content_hash, article._sgi_article_hash())
        self.assertTrue(article.is_locked)
        self.assertFalse(article.is_desynchronized)
        article.invalidate_recordset()
        article.with_user(self.plain).check_access('read')
        self.assertEqual(article.sgi_publish_state, 'publicado')

    def test_02_puestos(self):
        self._activity('Actividad del proceso ZK')
        co = self._doc('CO-ZK2-11', 'control_operacional', 'Control de emisiones')
        self._import(co)
        wizard = self._publish(self._article('CO-ZK2-11'))
        self.assertEqual(wizard.job_ids, self.job, "Sin actividad ni puestos: los del proceso.")
        new = self._current('CO-ZK2-11')
        self.assertIn(self.worker, new.sgi_ack_ids.employee_id, "Acuses para el puesto.")

    def test_03_reapunta_todas(self):
        it = self._doc('IT-ZK2-12', 'instructivo', 'Limpiar rodillos')
        first = self._activity('Limpiar rodillos A', doc=it)
        second = self._activity('Limpiar rodillos B', doc=it)
        self._import(it)
        article = self._article('IT-ZK2-12')
        self.assertEqual(first.instruction_article_id, article)
        self.assertEqual(second.instruction_article_id, article)
        self._publish(article)
        new = self._current('IT-ZK2-12')
        self.assertEqual(first.instruction_id, new)
        self.assertEqual(second.instruction_id, new, "Las dos actividades pasan a la revisión nueva.")

    def test_04_solo_mast(self):
        co = self._doc('CO-ZK2-13', 'control_operacional', 'Control de aguas')
        self._import(co)
        article = self._article('CO-ZK2-13')
        with self.assertRaises(UserError):
            article.with_user(self.owner).action_sgi_publish()
        article.with_user(self.owner).action_sgi_request_publish()
        article.with_user(self.owner).action_sgi_request_publish()
        activities = self.env['mail.activity'].search([
            ('res_model', '=', 'knowledge.article'), ('res_id', '=', article.id),
            ('sgi_cron_key', '=', 'kb_publicar')])
        self.assertEqual(len(activities), 1, "Una sola actividad para el Jefe MAST.")
        self.assertEqual(activities.user_id, self.mast)
        with self.assertRaises(UserError):
            article.with_user(self.plain).action_sgi_request_publish()

    def test_05_stale(self):
        it = self._doc('IT-ZK2-14', 'instructivo', 'Calibrar báscula')
        activity = self._activity('Calibrar báscula ZK', doc=it)
        self._import(it)
        article = self._article('IT-ZK2-14')
        self._publish(article)
        self.assertFalse(activity.instruction_article_stale)
        article.write({'is_locked': False})
        article.write({'body': '<p>Texto corregido después de publicar.</p>'})
        article.invalidate_recordset()
        self.assertEqual(article.sgi_publish_state, 'cambio')
        self.assertTrue(activity.instruction_article_stale)
        self._publish(article)
        self.assertEqual(self._current('IT-ZK2-14').sgi_revision, 2)
        with self.assertRaises(UserError):
            # Sin cambios contra la revisión vigente no se publica.
            self._publish(article)

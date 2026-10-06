# -*- coding: utf-8 -*-
"""1.2.0: importador por lotes (decisión 4, N3, N4, N6)."""
from unittest.mock import patch

from odoo.exceptions import AccessError
from odoo.tests import tagged

from .common_kb import KbCase


def _fake_text(self, attachment):
    return "Paso 1: apagar la máquina.\nPaso 2: enhebrar.\n\nPaso 3: probar.", "texto de prueba"


@tagged('post_install', '-at_install')
class TestKbImport(KbCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Article._sgi_kb_seed()
        cls.it = cls._doc('IT-ZK2-01', 'instructivo', 'Enhebrar la urdidora')
        cls.co = cls._doc('CO-ZK2-01', 'control_operacional', 'Manejo de residuos')
        cls.excluded = cls._doc('IT-P-I01-99', 'instructivo', 'Con credenciales')
        cls.activity = cls._activity('Enhebrar urdidora ZK', doc=cls.it)

    def _run(self, docs):
        with patch.object(type(self.env['sgi.knowledge.import']), '_sgi_pdf_text', _fake_text):
            return self._import(docs)

    def test_01_importa(self):
        wizard = self._run(self.it | self.co | self.excluded)
        article = self._article('IT-ZK2-01')
        self.assertEqual(len(article), 1)
        self.assertEqual(len(self._article('CO-ZK2-01')), 1)
        self.assertFalse(self._article('IT-P-I01-99'), "La familia P-I01 nunca se importa (N6).")
        self.assertIn('IT-P-I01-99 — excluido por L-001', wizard.summary)
        self.assertEqual(article.parent_id, self.process.sgi_article_id)
        self.assertEqual(article.sgi_kind, 'documento')
        self.assertEqual(article.sgi_process_id, self.process)
        self.assertEqual(article.sgi_doc_type, 'instructivo')
        self.assertIn('Paso 3: probar.', str(article.body))
        attachments = self.env['ir.attachment'].search(
            [('res_model', '=', 'knowledge.article'), ('res_id', '=', article.id)])
        self.assertTrue(attachments, "El PDF vigente va adjunto.")
        self.assertEqual(self.it.sgi_article_id, article)
        self.assertEqual(self.activity.instruction_article_id, article)
        self.assertEqual(self.activity.instruction_id, self.it, "Nunca escribe instruction_id.")
        self.assertIn('actividad ligada', article.sgi_import_note)
        self.assertEqual(article.sgi_publish_state, 'pdf')

    def test_02_idempotente(self):
        self._run(self.it | self.co)
        count = self.Article.search_count([('sgi_kind', '=', 'documento'), ('sgi_process_id', '=', self.process.id)])
        wizard = self._run(self.it | self.co)
        self.assertEqual(count, self.Article.search_count(
            [('sgi_kind', '=', 'documento'), ('sgi_process_id', '=', self.process.id)]), "0 nuevos.")
        self.assertIn('ya importado: 2', wizard.summary)

    def test_03_no_toca_el_documento(self):
        fields_ = ('sgi_state', 'sgi_revision', 'datas', 'name', 'sgi_content_hash', 'sgi_code',
                   'sgi_previous_code', 'sgi_job_ids', 'access_internal')
        before = {name: self.it[name] for name in fields_}
        acks = len(self.it.sgi_ack_ids)
        self._run(self.it)
        self.it.invalidate_recordset()
        for name in fields_:
            self.assertEqual(self.it[name], before[name], name)
        self.assertEqual(len(self.it.sgi_ack_ids), acks, "Sin acuses nuevos.")
        self.assertFalse(self.activity.instruction_article_stale,
                         "Un borrador importado no prende «cambió desde la publicación» (N3).")

    def test_04_borrador_restringido(self):
        self._run(self.it)
        article = self._article('IT-ZK2-01')
        with self.assertRaises(AccessError):
            article.with_user(self.plain).check_access('read')
        article.with_user(self.owner).check_access('write')
        article.with_user(self.mast).check_access('write')
        self.assertTrue(article.is_desynchronized)
        self.assertEqual(article.internal_permission, 'none')

    def test_05_restringido_no_se_copia(self):
        restricted = self._doc('IT-ZK2-02', 'instructivo', 'Restringido')
        restricted.sudo().write({'access_internal': 'none'})
        wizard = self._run(restricted)
        self.assertFalse(self._article('IT-ZK2-02'))
        self.assertIn('restringido', wizard.summary)
        self.assertFalse(restricted.sgi_article_id)

    def test_06_por_lotes(self):
        docs = self.it | self.co
        with patch.object(type(self.env['sgi.knowledge.import']), '_sgi_pdf_text', _fake_text):
            wizard = self.env['sgi.knowledge.import'].with_user(self.mast).create({
                'preset': 'manual', 'document_ids': [(6, 0, docs.ids)], 'batch_size': 1})
            wizard.action_import()
            self.assertIn('pendiente para el siguiente lote: 1', wizard.summary)
            wizard.action_import()
        self.assertTrue(self._article('IT-ZK2-01') and self._article('CO-ZK2-01'))

    def test_07_solo_mast(self):
        with self.assertRaises(AccessError):
            self.env['sgi.knowledge.import'].with_user(self.owner).create({'preset': 'co_c4'})

    def test_08_ligar_no_marca_el_procedimiento(self):
        """Ligar el artículo a la actividad no es un cambio al cuerpo del
        procedimiento (G14) ni a la huella de Mi procedimiento."""
        article = self.Article.create({'name': 'ZK artículo suelto', 'body': '<p>x</p>'})
        Process = type(self.env['sgi.process'])
        with patch.object(Process, '_sgi_flag_procedure_dirty', autospec=True) as flagged:
            self.activity.write({'instruction_article_id': article.id})
        flagged.assert_not_called()

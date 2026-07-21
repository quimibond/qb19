# -*- coding: utf-8 -*-
from odoo.tests import TransactionCase, tagged
from odoo.exceptions import UserError


@tagged('post_install', '-at_install')
class TestViewFile(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Doc = cls.env['documents.document']

    def test_01_binary_returns_act_url_with_attachment(self):
        """Un documento con adjunto binario abre su contenido inline (act_url al
        /web/content del adjunto, sin download=true)."""
        attachment = self.env['ir.attachment'].create({
            'name': 'proc.pdf',
            'raw': b'%PDF-1.4 contenido de prueba',
            'mimetype': 'application/pdf',
        })
        doc = self.Doc.create({
            'name': 'Procedimiento de ventas',
            'type': 'binary',
            'attachment_id': attachment.id,
        })
        action = doc.action_sgi_view_file()
        self.assertEqual(action['type'], 'ir.actions.act_url')
        self.assertEqual(action['target'], 'new')
        self.assertIn('/web/content/%d' % attachment.id, action['url'])
        self.assertNotIn('download=true', action['url'])

    def test_02_url_document_returns_act_url_to_link(self):
        """Un documento de tipo enlace abre su URL."""
        doc = self.Doc.create({
            'name': 'Enlace externo',
            'type': 'url',
            'url': 'https://www.quimibond.com/procedimiento',
        })
        action = doc.action_sgi_view_file()
        self.assertEqual(action['type'], 'ir.actions.act_url')
        self.assertEqual(action['url'], 'https://www.quimibond.com/procedimiento')

    def test_03_no_file_raises_user_error(self):
        """Sin adjunto ni enlace, avisa amablemente en vez de romper."""
        doc = self.Doc.create({
            'name': 'Documento sin archivo',
            'type': 'binary',
        })
        with self.assertRaises(UserError):
            doc.action_sgi_view_file()

    def test_04_open_in_documents_returns_native_action(self):
        """El smart button regresa la acción nativa de Documentos apuntando al
        registro."""
        doc = self.Doc.create({'name': 'Doc para explorador', 'type': 'binary'})
        action = doc.action_sgi_open_in_documents()
        self.assertEqual(action['type'], 'ir.actions.act_window')
        self.assertEqual(action['res_model'], 'documents.document')
        self.assertEqual(action['res_id'], doc.id)

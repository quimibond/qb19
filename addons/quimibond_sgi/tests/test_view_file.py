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

    def test_05_formulario_odoo_opens_menu_action(self):
        """El «documento» tipo Formulario de Odoo no es un archivo: abre la
        vista real de su menú, no exige clave PNTQ y «Ver archivo» redirige."""
        act = self.env['ir.actions.act_window'].create({
            'name': 'Prueba formulario', 'res_model': 'res.partner'})
        root = self.env['ir.ui.menu'].create({'name': 'AppFormulario'})
        menu = self.env['ir.ui.menu'].create({
            'name': 'MenuFormulario', 'parent_id': root.id,
            'action': 'ir.actions.act_window,%d' % act.id})
        doc = self.Doc.create({
            'name': 'Alta de clientes (vista de Odoo)', 'type': 'binary',
            'sgi_is_controlled': True,  # sin clave: el tipo lo exime del regex
            'sgi_doc_type': 'formulario_odoo',
            'sgi_odoo_menu_id': menu.id,
        })
        action = doc.action_sgi_open_odoo_form()
        self.assertEqual(action['res_model'], 'res.partner')
        # Ver archivo también lleva a la vista (no hay archivo que abrir).
        action2 = doc.action_sgi_view_file()
        self.assertEqual(action2['res_model'], 'res.partner')
        # Sin menú ni worksheet ligado, avisa en vez de romper.
        bare = self.Doc.create({
            'name': 'Formulario sin liga', 'type': 'binary',
            'sgi_doc_type': 'formulario_odoo'})
        with self.assertRaises(UserError):
            bare.action_sgi_open_odoo_form()

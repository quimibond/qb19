# -*- coding: utf-8 -*-
"""Cambio documental firmado en Sign (56.17.0): al enviar se crea la firma
(elaboró → revisó → aprobó) con la plantilla armada por el SGI, cada firma
aprueba su renglón, la última aplica el cambio y el botón Aprobar queda
bloqueado."""
import base64
import io
from unittest.mock import patch

from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged, new_test_user
from odoo.tools.pdf import PdfFileReader, PdfFileWriter

from .common_documents import sgi_hide_real_documents


def _blank_pdf(pages=1):
    writer = PdfFileWriter()
    for _n in range(pages):
        writer.add_blank_page(612, 792)
    out = io.BytesIO()
    writer.write(out)
    return out.getvalue()


@tagged('post_install', '-at_install')
class TestDocChangeSign(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        env = cls.env
        groups = 'base.group_user,quimibond_sgi.group_sgi_user'
        cls.owner = new_test_user(env, login='zs_dc_owner', email='owner@example.com', groups=groups)
        cls.reviewer = new_test_user(env, login='zs_dc_rev', email='rev@example.com', groups=groups)
        cls.mast = new_test_user(env, login='zs_dc_mast', email='mast@example.com',
                                 groups='base.group_user,quimibond_sgi.group_sgi_manager')
        reviewer_emp = env['hr.employee'].create({'name': 'ZS Dueño', 'user_id': cls.reviewer.id})
        cls.process = env['sgi.process'].create({'code': 'ZDC', 'name': 'ZS Proceso firma',
                                                 'owner_id': reviewer_emp.id})
        cls.category = env['approval.category'].create({
            'name': 'ZS Cambio documental firmado',
            'sgi_is_doc_change': True,
            'sgi_sign_required': True,
            'approval_minimum': 1,
            'approver_sequence': True,
            'approver_ids': [(0, 0, {'user_id': cls.mast.id, 'required': True})],
        })
        # Clave con la nomenclatura del tipo (PR-{proceso}, 56.32.0 / D-02):
        # «P-Z01» no la cumple y la base nueva la rechaza.
        cls.doc = env['documents.document'].create({
            'name': 'ZS Procedimiento firmado', 'type': 'binary',
            'sgi_is_controlled': True, 'sgi_doc_type': 'procedimiento',
            'sgi_code': 'PR-ZDC', 'sgi_revision': 0, 'sgi_state': 'vigente',
            'sgi_process_id': cls.process.id,
        })

    def _request(self, owner=None, **vals):
        base = {
            'name': 'ZS Cambio', 'category_id': self.category.id,
            'request_owner_id': (owner or self.owner).id,
            'sgi_change_kind': 'modificacion', 'sgi_document_id': self.doc.id,
            'sgi_new_revision': 1,
            'sgi_reason': 'Cambió el método', 'sgi_changes': 'Paso 4 nuevo',
        }
        base.update(vals)
        return self.env['approval.request'].create(base)

    def _confirm(self, req):
        with patch.object(type(req), '_sgi_render_doc_change_pdf', lambda self: _blank_pdf(2)):
            req.action_confirm()
        return req.sgi_sign_request_id

    def _sign(self, sign, user):
        sign.request_item_ids.filtered(lambda i: i.partner_id == user.partner_id).write({'state': 'completed'})

    def test_01_enviar_crea_la_firma_en_orden(self):
        req = self._request()
        sign = self._confirm(req)
        self.assertTrue(sign, "Enviar la solicitud la manda a firmar.")
        self.assertEqual(sign.reference_doc, req)
        items = sign.request_item_ids.sorted('mail_sent_order')
        self.assertEqual(items.partner_id, (self.owner | self.reviewer | self.mast).partner_id)
        self.assertEqual(items.mapped('mail_sent_order'), [1, 2, 3])
        boxes = sign.template_id.sign_item_ids
        self.assertEqual(len(boxes), 3, "Una caja por papel: elaboró, revisó, aprobó.")
        self.assertEqual(set(boxes.mapped('page')), {2}, "La hoja de firmas es la última página.")
        approvers = req.approver_ids.sorted('sequence')
        self.assertEqual(approvers.user_id, self.reviewer | self.mast)
        self.assertTrue(all(approvers.mapped('required')))
        self.assertEqual(req.request_status, 'pending')
        with self.assertRaises(UserError):
            req.with_user(self.mast).action_approve()

    def test_02_cada_firma_aprueba_y_la_ultima_aplica(self):
        req = self._request()
        sign = self._confirm(req)
        self._sign(sign, self.owner)
        self.assertEqual(set(req.approver_ids.mapped('status')) & {'approved'}, set(),
                         "Firmar como elaboró no aprueba a nadie.")
        self._sign(sign, self.reviewer)
        reviewer_line = req.approver_ids.filtered(lambda a: a.user_id == self.reviewer)
        self.assertEqual(reviewer_line.status, 'approved')
        self.assertEqual(req.request_status, 'pending')
        self._sign(sign, self.mast)
        self.assertEqual(req.request_status, 'approved')
        self.assertTrue(req.sgi_applied)
        self.assertEqual(self.doc.sgi_revision, 1, "La última firma aplica el cambio.")
        sign.write({'state': 'signed'})
        self.assertTrue(req.sgi_sign_archived)
        self.assertEqual(req.sgi_sign_progress, "3 de 3")

    def test_03_sin_dueno_no_se_envia(self):
        self.process.owner_id = False
        req = self._request()
        with self.assertRaises(UserError) as err:
            self._confirm(req)
        self.assertIn('dueño', str(err.exception))
        req2 = self._request(sgi_reason='')
        with self.assertRaises(UserError):
            self._confirm(req2)

    def test_04_quien_pide_y_dueno_son_la_misma_persona(self):
        req = self._request(owner=self.reviewer)
        sign = self._confirm(req)
        self.assertEqual(len(sign.request_item_ids), 2, "Firma una vez aunque tenga dos papeles.")
        self.assertEqual(len(sign.template_id.sign_item_ids), 3)
        self._sign(sign, self.reviewer)
        self._sign(sign, self.mast)
        self.assertEqual(req.request_status, 'approved')

    def test_05_se_publica_el_archivo_que_se_firmo(self):
        req = self._request()
        new_version = self.env['ir.attachment'].create({
            'name': 'PR-ZDC rev 01.pdf', 'datas': base64.b64encode(_blank_pdf()),
            'mimetype': 'application/pdf', 'res_model': 'approval.request', 'res_id': req.id})
        sign = self._confirm(req)
        self.assertEqual(req.sgi_change_attachment_id, new_version)
        signed_pdf = base64.b64decode(sign.template_id.document_ids.attachment_id.datas)
        self.assertEqual(len(PdfFileReader(io.BytesIO(signed_pdf)).pages), 3,
                         "Solicitud, versión nueva y al final la hoja de firmas.")
        self.assertEqual(set(sign.template_id.sign_item_ids.mapped('page')), {3})
        self.env['ir.attachment'].create({
            'name': 'firmado.pdf', 'datas': base64.b64encode(_blank_pdf()),
            'mimetype': 'application/pdf', 'res_model': 'approval.request', 'res_id': req.id})
        self.assertEqual(req._sgi_change_attachment(), new_version,
                         "El PDF firmado que llega después no sustituye a la revisión nueva.")

    def test_06_categoria_sin_firma_sigue_con_aprobaciones(self):
        self.category.sgi_sign_required = False
        req = self._request()
        req.action_confirm()
        self.assertFalse(req.sgi_sign_request_id)

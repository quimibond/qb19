# -*- coding: utf-8 -*-
"""Plantillas de Sign armadas por el SGI (56.18.0): acuses de lectura y
responsivas de EPP se mandan a firmar sin plantilla hecha a mano."""
import base64
import io
from unittest.mock import patch

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


def _pages(template):
    data = base64.b64decode(template.document_ids.attachment_id.datas)
    return len(PdfFileReader(io.BytesIO(data)).pages)


@tagged('post_install', '-at_install')
class TestSignBuilder(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        env = cls.env
        cls.mast = new_test_user(env, login='zs_sb_mast', email='mast.sb@example.com',
                                 groups='base.group_user,quimibond_sgi.group_sgi_manager')
        cls.user = new_test_user(env, login='zs_sb_emp', email='emp.sb@example.com',
                                 groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.employee = env['hr.employee'].create({'name': 'ZS Operador firma', 'user_id': cls.user.id})
        cls.doc = env['documents.document'].create({
            'name': 'ZS Instructivo.pdf', 'type': 'binary',
            'datas': base64.b64encode(_blank_pdf(2)), 'mimetype': 'application/pdf',
            'sgi_is_controlled': True, 'sgi_doc_type': 'instructivo',
            'sgi_code': 'IT-Z01', 'sgi_revision': 0, 'sgi_state': 'vigente',
        })
        cls.Builder = type(env['sgi.sign.builder'])

    def setUp(self):
        super().setUp()
        patcher = patch.object(self.Builder, '_sgi_render_pdf', lambda self, ref, records: _blank_pdf())
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_01_acuse_sin_plantilla_hecha_a_mano(self):
        ack = self.env['sgi.document.ack'].create({'document_id': self.doc.id, 'employee_id': self.employee.id})
        self.doc.with_user(self.mast).action_sgi_send_sign_requests()
        self.assertTrue(ack.sign_request_id, "Se mandó a firmar sin plantilla hecha a mano.")
        template = self.doc.sgi_sign_template_id
        self.assertTrue(self.doc.sgi_sign_template_auto)
        self.assertEqual(_pages(template), 3, "El instructivo (2 hojas) más la hoja «Leí y entendí».")
        self.assertEqual(template.sign_item_ids.page, 3)
        self.assertEqual(template.sign_item_ids.responsible_id,
                         self.env.ref('quimibond_sgi.sgi_sign_role_empleado'))
        self.assertEqual(ack.sign_request_id.request_item_ids.partner_id, self.user.partner_id)
        # Firmado en Sign → el acuse queda leído.
        ack.sign_request_id.write({'state': 'signed'})
        self.env['sgi.document.ack']._sgi_sync_from_sign()
        self.assertEqual(ack.state, 'leido')

    def test_02_revision_nueva_arma_otra_plantilla(self):
        first = self.doc._sgi_ack_sign_template()
        self.assertEqual(self.doc._sgi_ack_sign_template(), first, "Misma revisión, misma plantilla.")
        self.doc.sgi_revision = 1
        self.assertNotEqual(self.doc._sgi_ack_sign_template(), first)
        self.assertEqual(self.doc.sgi_sign_template_rev, 1)

    def test_03_plantilla_hecha_a_mano_se_respeta(self):
        manual = self.env['sgi.sign.builder']._sgi_template(
            "ZS manual", _blank_pdf(), 1, [(0, self.env.ref('quimibond_sgi.sgi_sign_role_empleado'))])
        self.doc.sgi_sign_template_id = manual
        self.assertFalse(self.doc.sgi_sign_template_auto)
        self.doc.sgi_revision = 5
        self.assertEqual(self.doc._sgi_ack_sign_template(), manual)

    def test_04_responsiva_de_epp(self):
        self.env['ir.config_parameter'].sudo().set_param('quimibond_sgi.epp_sign_template_id', '')
        delivery = self.env['sgi.epp.delivery'].create({
            'employee_id': self.employee.id,
            'line_ids': [(0, 0, {'quantity': 1, 'uom': 'par', 'name': 'Guantes'})],
        })
        delivery.action_send_sign_request()
        request = delivery.sign_request_id
        self.assertTrue(request)
        self.assertEqual(request.reference_doc, delivery)
        self.assertEqual(_pages(request.template_id), 2, "Responsiva más la hoja «Recibí el EPP».")
        self.assertEqual(request.template_id.sign_item_ids.page, 2)
        request.write({'state': 'signed'})
        self.env['sgi.epp.delivery']._sgi_sync_from_sign()
        self.assertEqual(delivery.state, 'firmada')

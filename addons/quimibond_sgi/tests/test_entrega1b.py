# -*- coding: utf-8 -*-
"""Entrega 1b de la auditoría 2026-09: Mis pendientes solo con lo de la
empresa del SGI (D-03) y sin documentos de procesos archivados."""
from datetime import timedelta

from odoo import fields
from odoo.tests import TransactionCase, new_test_user, tagged

from .common_documents import sgi_hide_real_documents


@tagged('post_install', '-at_install')
class TestEntrega1b(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        cls.sgi_company = cls.env['sgi.config']._sgi_company()
        cls.other = cls.env['res.company'].create({'name': 'ZS Otra empresa 1b'})
        cls.user = new_test_user(cls.env, login='zs_1b_user', groups='base.group_user',
                                 company_id=cls.sgi_company.id,
                                 company_ids=[(6, 0, (cls.sgi_company | cls.other).ids)])
        cls.Pending = cls.env['sgi.my.pending']

    def _request(self, company):
        category = self.env['approval.category'].with_company(company).create({
            'name': 'ZS Categoría %s' % company.name, 'approval_minimum': 1,
            'company_id': company.id,
            'approver_ids': [(0, 0, {'user_id': self.user.id, 'required': True})]})
        request = self.env['approval.request'].with_company(company).create({
            'name': 'ZS solicitud %s' % company.name, 'category_id': category.id,
            'request_owner_id': self.env.user.id})
        request.action_confirm()
        return request

    def test_01_sgi_company_is_declared(self):
        self.assertEqual(self.sgi_company, self.env.ref('base.main_company'))
        self.env['ir.config_parameter'].sudo().set_param('quimibond_sgi.sgi_company_id', self.other.id)
        self.assertEqual(self.env['sgi.config']._sgi_company(), self.other)

    def test_02_pending_only_from_the_sgi_company(self):
        mine = self._request(self.sgi_company)
        foreign = self._request(self.other)
        requests = self.Pending._sgi_pending_records(self.user)['solicitud'].request_id
        self.assertIn(mine, requests)
        self.assertNotIn(foreign, requests, "Una solicitud de otra empresa no es pendiente del SGI.")

    def test_03_no_documents_of_archived_processes(self):
        process = self.env['sgi.process'].create({'code': 'X1B', 'name': 'Proceso 1b'})
        doc = self.env['documents.document'].create({
            'name': 'ZS doc 1b', 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'formato', 'sgi_code': 'F-P-A94-01', 'sgi_state': 'vigente',
            'sgi_process_id': process.id, 'sgi_owner_id': self.user.id,
            'sgi_next_review_date': fields.Date.today() + timedelta(days=5)})
        self.assertIn(doc, self.Pending._sgi_pending_records(self.user)['documento'])
        process.active = False
        self.assertNotIn(doc, self.Pending._sgi_pending_records(self.user)['documento'])

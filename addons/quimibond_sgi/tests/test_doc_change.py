# -*- coding: utf-8 -*-
from datetime import date, timedelta

from odoo.tests import TransactionCase, tagged
from odoo.exceptions import ValidationError


@tagged('post_install', '-at_install')
class TestDocChange(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.category = cls.env['approval.category'].create({
            'name': 'Test Cambio Documental',
            'sgi_is_doc_change': True,
            'approval_minimum': 1,
        })
        cls.doc = cls.env['documents.document'].create({
            'name': 'Procedimiento de prueba',
            'type': 'binary',
            'sgi_is_controlled': True,
            'sgi_doc_type': 'procedimiento',
            'sgi_code': 'P-A01',
            'sgi_revision': '00',
            'sgi_state': 'vigente',
        })

    def _new_request(self, **vals):
        base = {
            'name': 'Solicitud de cambio',
            'category_id': self.category.id,
            'request_owner_id': self.env.user.id,
            'sgi_change_kind': 'modificacion',
            'sgi_document_id': self.doc.id,
        }
        base.update(vals)
        return self.env['approval.request'].create(base)

    def test_01_pilot_over_90_days(self):
        with self.assertRaises(ValidationError):
            self._new_request(
                sgi_pilot=True,
                sgi_pilot_start=date.today(),
                sgi_pilot_end=date.today() + timedelta(days=100),
            )

    def test_02_modification_requires_document(self):
        with self.assertRaises(ValidationError):
            self.env['approval.request'].create({
                'name': 'Sin documento',
                'category_id': self.category.id,
                'request_owner_id': self.env.user.id,
                'sgi_change_kind': 'modificacion',
            })

    def test_03_approval_updates_document(self):
        req = self._new_request(sgi_new_revision='01')
        req.approver_ids = [(0, 0, {'user_id': self.env.user.id, 'required': True})]
        req.action_confirm()
        req.action_approve()
        self.assertEqual(req.request_status, 'approved')
        self.assertTrue(req.sgi_applied)
        self.assertEqual(self.doc.sgi_revision, '01')
        self.assertEqual(self.doc.sgi_state, 'vigente')


@tagged('post_install', '-at_install')
class TestComplaintToNc(TransactionCase):

    def test_01_generate_nc_links_both(self):
        team = self.env.ref('quimibond_sgi.sgi_helpdesk_team_complaints')
        partner = self.env['res.partner'].create({'name': 'Cliente Reclamación'})
        ticket = self.env['helpdesk.ticket'].create({
            'name': 'Rollo con defecto',
            'team_id': team.id,
            'partner_id': partner.id,
            'sgi_qty_affected': 120.0,
        })
        ticket.action_sgi_generate_nc()
        self.assertTrue(ticket.sgi_alert_id)
        alert = ticket.sgi_alert_id
        self.assertEqual(alert.sgi_origin_type, 'reclamacion')
        self.assertEqual(alert.sgi_complaint_ticket_id, ticket)
        self.assertEqual(alert.partner_id, partner)

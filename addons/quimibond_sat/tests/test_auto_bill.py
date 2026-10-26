# -*- coding: utf-8 -*-
"""Factura de proveedor en borrador desde el XML del SAT.

El decodificador de CFDI lo pone l10n_mx_edi (Enterprise), que el CI no
tiene: aquí se simula con un decodificador que llena la factura como lo haría
el de la localización. Sin decodificador no se crea nada."""
from datetime import timedelta
from unittest.mock import patch

from odoo import Command, fields
from odoo.exceptions import UserError
from odoo.tests import tagged

from .common import SatCommon, syntage_invoice

UUID_A = 'ffffffff-0000-4000-8000-000000000001'
UUID_B = 'ffffffff-0000-4000-8000-000000000002'
XML = b'<?xml version="1.0"?><cfdi:Comprobante xmlns:cfdi="http://www.sat.gob.mx/cfd/4" Total="3287.86"/>'


@tagged('post_install', '-at_install', 'quimibond_sat')
class TestSatAutoBill(SatCommon):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.journal = cls.company_data['default_journal_purchase']
        cls.company.sat_auto_bill_journal_id = cls.journal

    def _upsert(self, obj):
        return self.env['sat.cfdi']._upsert_from_syntage(obj, self.company, event_type='pull')

    def _days_ago(self, days):
        return fields.Datetime.to_string(fields.Datetime.now() - timedelta(days=days))

    def _fake_decoder(self):
        test = self

        def decode(move, file_data, new):
            move.write({
                'partner_id': test.proveedor.id,
                'invoice_date': fields.Date.today(),
                'invoice_line_ids': [Command.create({
                    'name': 'Del CFDI', 'quantity': 1.0, 'price_unit': 3287.86,
                    'account_id': test.company_data['default_account_expense'].id,
                    'tax_ids': [Command.clear()],
                })],
            })

        def get_decoder(self_move, file_data, new=False):
            if file_data.get('name', '').endswith('.xml'):
                return {'decoder': decode, 'priority': 20}

        return patch.object(type(self.env['account.move']), '_get_edi_decoder', get_decoder)

    def _patch_xml(self):
        return patch.object(type(self.env['sat.cfdi']), '_fetch_xml', lambda rec: XML)

    def test_cron_creates_draft_bill_and_links(self):
        cfdi = self._upsert(syntage_invoice(UUID_A, issuedAt=self._days_ago(5)))
        self.assertEqual(cfdi.match_status, 'solo_sat')
        with self._fake_decoder(), self._patch_xml():
            created = self.env['sat.cfdi']._cron_auto_bills()
        self.assertEqual(created, 1)
        move = cfdi.move_id
        self.assertTrue(move)
        self.assertEqual(move.state, 'draft')            # nunca se publica sola
        self.assertEqual(move.move_type, 'in_invoice')
        self.assertEqual(move.journal_id, self.journal)
        self.assertEqual(cfdi.match_status, 'matched')
        self.assertEqual(cfdi.issue, 'borrador')
        self.assertEqual(cfdi.auto_bill_state, 'created')
        self.assertIn('%s.xml' % UUID_A.upper(), move.attachment_ids.mapped('name'))

    def test_window_and_existing_candidate(self):
        # Muy reciente (menos de 3 días): se deja para la captura normal
        recent = self._upsert(syntage_invoice(UUID_A, issuedAt=self._days_ago(1)))
        # Ya hay una factura del mismo proveedor y monto: no se duplica
        self._invoice(self.proveedor, 3287.86, day=fields.Date.to_string(fields.Date.today() - timedelta(days=6)))
        dup = self._upsert(syntage_invoice(UUID_B, id='b', issuedAt=self._days_ago(5)))
        with self._fake_decoder(), self._patch_xml():
            self.assertEqual(self.env['sat.cfdi']._cron_auto_bills(), 0)
        self.assertFalse(recent.move_id)
        self.assertFalse(recent.auto_bill_state)
        self.assertFalse(dup.auto_bill_state)
        self.assertTrue(dup.auto_bill_note)   # dice por qué no

    def test_off_without_journal(self):
        self.company.sat_auto_bill_journal_id = False
        cfdi = self._upsert(syntage_invoice(UUID_A, issuedAt=self._days_ago(5)))
        with self._fake_decoder(), self._patch_xml():
            self.assertEqual(self.env['sat.cfdi']._cron_auto_bills(), 0)
        self.assertFalse(cfdi.move_id)

    def test_without_decoder_creates_nothing(self):
        cfdi = self._upsert(syntage_invoice(UUID_A, issuedAt=self._days_ago(5)))
        moves_before = self.env['account.move'].search_count([])
        with patch.object(type(self.env['account.move']), '_get_edi_decoder', lambda *a, **k: None), \
                self._patch_xml():
            self.env['sat.cfdi']._cron_auto_bills()
            with self.assertRaisesRegex(UserError, 'l10n_mx_edi'):
                cfdi.action_create_bill()
        self.assertEqual(self.env['account.move'].search_count([]), moves_before)
        self.assertEqual(cfdi.auto_bill_state, 'error')
        self.assertFalse(cfdi.move_id)

    def test_button_on_cfdi(self):
        cfdi = self._upsert(syntage_invoice(UUID_A, type='E', issuedAt=self._days_ago(5)))
        with self._fake_decoder(), self._patch_xml():
            action = cfdi.action_create_bill()
        move = self.env['account.move'].browse(action['res_id'])
        self.assertEqual(move.move_type, 'in_refund')
        self.assertEqual(cfdi.move_id, move)

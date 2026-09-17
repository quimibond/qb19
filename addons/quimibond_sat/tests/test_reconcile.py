# -*- coding: utf-8 -*-
"""Asistente de conciliación: candidatas por RFC + monto (publicadas y en
borrador), liga con constancia en el chatter, XML adjunto cuando Syntage lo
entrega y XML cruzado. Sin l10n_mx_edi (CI) el XML solo queda adjunto."""
from unittest.mock import patch

from odoo.exceptions import UserError
from odoo.tests import tagged

from .common import SatCommon, syntage_invoice

U1 = 'bbbbbbbb-0000-4000-8000-000000000001'
U2 = 'bbbbbbbb-0000-4000-8000-000000000002'
XML = b'<?xml version="1.0"?><cfdi:Comprobante xmlns:cfdi="http://www.sat.gob.mx/cfd/4"/>'


@tagged('post_install', '-at_install', 'quimibond_sat')
class TestSatReconcile(SatCommon):

    def _upsert(self, obj):
        return self.env['sat.cfdi']._upsert_from_syntage(obj, self.company, event_type='pull')

    def _wizard(self, record):
        action = self.env['sat.reconcile.wizard'].open_for(record)
        return self.env['sat.reconcile.wizard'].browse(action['res_id'])

    def _xml_attachments(self, move):
        return self.env['ir.attachment'].search([
            ('res_model', '=', 'account.move'), ('res_id', '=', move.id), ('name', '=', U1.upper() + '.xml')])

    def test_candidates_from_cfdi(self):
        posted = self._invoice(self.proveedor, 3287.86, day='2026-04-10')
        draft = self._invoice(self.proveedor, 3287.86, day='2026-04-28', post=False)
        far = self._invoice(self.proveedor, 3287.86, day='2025-01-10')
        self._invoice(self.proveedor, 100.0, day='2026-04-28')            # otro monto
        self._invoice(self.cliente, 3287.86, move_type='out_invoice')     # otro RFC / sentido
        cfdi = self._upsert(syntage_invoice(U1))
        wiz = self._wizard(cfdi)
        # Más cercana en fecha primero; la de 2025 queda fuera de ±180 días
        self.assertEqual(wiz.line_ids.mapped('move_id').ids, [draft.id, posted.id])
        self.assertTrue(all(wiz.line_ids.mapped('exact')))
        self.assertEqual(wiz.line_ids[0].delta_days, -1)
        wiz.days = 0
        wiz.action_refresh()
        self.assertIn(far, wiz.line_ids.mapped('move_id'))
        self.assertIn(cfdi.name, wiz.summary)

    def test_reconcile_links_and_attaches_xml(self):
        bill = self._invoice(self.proveedor, 3287.86, day='2026-04-10')
        cfdi = self._upsert(syntage_invoice(U1))
        wiz = self._wizard(cfdi)
        line = wiz.line_ids.filtered(lambda l: l.move_id == bill)
        with patch.object(type(cfdi), '_fetch_xml', return_value=XML):
            res = line.action_pick()
        self.assertEqual(res['type'], 'ir.actions.act_window_close')
        self.assertEqual(cfdi.move_id, bill)
        self.assertEqual(cfdi.match_method, 'conciliado')
        self.assertEqual(cfdi.match_status, 'matched')
        self.assertEqual(cfdi.issue, 'ok')
        self.assertEqual(len(self._xml_attachments(bill)), 1)
        self.assertEqual(bill.sat_uuid, U1.upper())
        body = ' '.join(bill.message_ids.mapped('body'))
        self.assertIn(U1.upper(), body)
        self.assertIn('conciliado', body.lower())
        # El cruce automático por UUID respeta la decisión
        cfdi._match_move()
        self.assertEqual(cfdi.move_id, bill)
        # Conciliar dos veces no duplica el adjunto
        with patch.object(type(cfdi), '_fetch_xml', return_value=XML):
            cfdi._reconcile_with(bill)
        self.assertEqual(len(self._xml_attachments(bill)), 1)

    def test_reconcile_without_xml_is_soft(self):
        bill = self._invoice(self.proveedor, 3287.86, day='2026-04-10')
        cfdi = self._upsert(syntage_invoice(U1))
        with patch.object(type(cfdi), '_fetch_xml', side_effect=UserError('Syntage respondió 404')):
            body = cfdi._reconcile_with(bill)
        self.assertEqual(cfdi.move_id, bill)
        self.assertIn('XML no adjuntado', body)
        self.assertFalse(self._xml_attachments(bill))

    def test_reconcile_from_move(self):
        bill = self._invoice(self.proveedor, 3287.86, day='2026-04-10')
        cfdi = self._upsert(syntage_invoice(U1))
        self._upsert(syntage_invoice(U2, id='x', total=99.0, subtotal=99.0))
        wiz = self._wizard(bill)
        self.assertEqual(wiz.line_ids.mapped('cfdi_id'), cfdi)
        self.assertTrue(wiz.line_ids.exact)
        with patch.object(type(cfdi), '_fetch_xml', return_value=XML):
            wiz.line_ids.action_pick()
        self.assertEqual(bill.sat_cfdi_ids, cfdi)
        self.assertEqual(bill.sat_estado, 'vigente')
        self.assertEqual(bill.sat_uuid, U1.upper())

    def test_reconcile_steals_crossed_xml(self):
        bill = self._invoice(self.proveedor, 18585.72, day='2026-04-17')
        wrong = self._upsert(syntage_invoice(U2, id='w', total=3691.23, subtotal=3182.09))
        wrong.write({'move_id': bill.id})
        self.assertEqual(wrong.issue, 'monto')
        right = self._upsert(syntage_invoice(U1, total=18585.72, subtotal=16022.17))
        right._reconcile_with(bill, attach_xml=False)
        self.assertEqual(right.move_id, bill)
        self.assertFalse(wrong.move_id)
        self.assertEqual(wrong.match_status, 'solo_sat')
        self.assertIn('cruzado', wrong.note)

    def test_reconcile_wrong_company_or_type(self):
        bill = self._invoice(self.proveedor, 3287.86)
        cfdi = self._upsert(syntage_invoice(U1))
        entry = self.env['account.move'].create({'move_type': 'entry', 'company_id': self.company.id})
        with self.assertRaises(UserError):
            cfdi._reconcile_with(entry)
        self.assertFalse(cfdi.move_id)
        cfdi._reconcile_with(bill, attach_xml=False)
        self.assertEqual(cfdi.move_id, bill)

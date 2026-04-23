# -*- coding: utf-8 -*-
"""
Test: _push_invoices correctly captures cfdi_uuid from l10n_mx_edi in Odoo 19.

Bug profile (2026-04-24): 77 out_invoice rows in Supabase odoo_invoices (2025+) have
NULL cfdi_uuid despite being posted + timbrado in Odoo. Root cause: either (a) addon
reads the field before l10n_mx_edi populates it, or (b) field path changed in Odoo 19.

Audit finding (Step 3.1):
- The addon already avoids `move.l10n_mx_edi_cfdi_uuid` (stale after Odoo 17→19
  migration, see SP10.4 / SP10.6). Instead, `_build_cfdi_map(env, ids)` reads
  `l10n_mx_edi.document` records scored by (sat_state='valid', posted, move.id).
- Timing is the remaining gap: if `_push_invoices` runs between `action_post()`
  and `l10n_mx_edi` timbrado completion, the document row is not yet present
  and the UUID lands as NULL in Supabase.

Fix expectation: `_read_cfdi_uuid(move)` returns the UUID from either the
l10n_mx_edi.document table (preferred, via _build_cfdi_map) or the stored
computed field as a fallback. When the invoice is posted AND >5 minutes old
AND no UUID is resolved, a WARNING is logged under
`odoo.addons.quimibond_intelligence` so operators see stalled timbrado.
`_serialize_invoice(move)` is a thin wrapper that returns a dict containing
at minimum `{'odoo_invoice_id', 'cfdi_uuid'}`, using `_read_cfdi_uuid`.
"""
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install', 'quimibond')
class TestCfdiUuidPush(TransactionCase):

    def setUp(self):
        super().setUp()
        self.Move = self.env['account.move']
        self.Sync = self.env['quimibond.sync']
        self.partner = self.env['res.partner'].create({
            'name': 'Test Partner UUID',
            'vat': 'AAA010101AAA',
        })

    def _make_posted_invoice(self, uuid=None):
        """Create a posted out_invoice. If uuid provided, stub the cfdi_uuid
        via an l10n_mx_edi.document row so `_read_cfdi_uuid` can find it the
        same way production does (via _build_cfdi_map)."""
        move = self.Move.create({
            'move_type': 'out_invoice',
            'partner_id': self.partner.id,
            'invoice_date': '2026-04-01',
            'invoice_line_ids': [(0, 0, {
                'name': 'test',
                'quantity': 1,
                'price_unit': 100.0,
            })],
        })
        move.action_post()
        if uuid:
            # Seed the l10n_mx_edi.document table (same source the production
            # bulk _build_cfdi_map queries). We lowercase per SP10.6.
            Document = self.env['l10n_mx_edi.document'].sudo()
            Document.create({
                'move_id': move.id,
                'attachment_uuid': uuid.lower(),
                'sat_state': 'valid',
                'state': 'invoice_sent',
            })
        return move

    def test_push_reads_uuid_from_odoo19_field(self):
        """When the cfdi_uuid is available via l10n_mx_edi, _serialize_invoice
        must include it in the payload (Odoo 19 source of truth)."""
        uuid = 'DEADBEEF-CAFE-BABE-0000-000000000001'
        move = self._make_posted_invoice(uuid=uuid)
        payload = self.Sync.sudo()._serialize_invoice(move)
        self.assertEqual(
            (payload.get('cfdi_uuid') or '').lower(),
            uuid.lower(),
            msg='Expected cfdi_uuid to match l10n_mx_edi document UUID'
        )

    def test_push_handles_null_uuid_gracefully(self):
        """When UUID is still NULL (pre-timbrado), payload.cfdi_uuid must be
        None, not raise."""
        move = self._make_posted_invoice(uuid=None)
        payload = self.Sync.sudo()._serialize_invoice(move)
        self.assertIsNone(
            payload.get('cfdi_uuid'),
            msg='Pre-timbrado invoice should serialize with NULL UUID, not raise'
        )

    def test_push_logs_warning_for_old_untimbrada(self):
        """Posted >5 min ago without UUID → should log WARNING (ops signal)."""
        move = self._make_posted_invoice(uuid=None)
        # Backdate create_date to simulate >5 min-old invoice (bypasses ORM).
        self.env.cr.execute(
            "UPDATE account_move SET create_date = create_date - interval '10 minutes' "
            "WHERE id = %s",
            (move.id,),
        )
        move.invalidate_recordset(['create_date'])
        with self.assertLogs('odoo.addons.quimibond_intelligence',
                             level='WARNING') as cm:
            self.Sync.sudo()._serialize_invoice(move)
        self.assertTrue(
            any('cfdi_uuid' in msg for msg in cm.output),
            msg='Expected WARNING log mentioning cfdi_uuid for stale posted-no-UUID move'
        )

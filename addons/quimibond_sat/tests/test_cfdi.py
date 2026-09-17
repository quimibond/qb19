# -*- coding: utf-8 -*-
from odoo.tests import tagged

from .common import RFC_CLIENTE, RFC_QUIMIBOND, SatCommon, syntage_invoice

UUID_A = 'ccd25ca0-d823-48a6-938a-eb3af58c8c18'
UUID_B = '9adae280-4046-4615-b843-ccc05b30faf1'
UUID_C = '11111111-2222-4333-8444-555555555555'


@tagged('post_install', '-at_install', 'quimibond_sat')
class TestSatCfdi(SatCommon):

    def _upsert(self, obj, event_type='pull'):
        return self.env['sat.cfdi']._upsert_from_syntage(obj, self.company, event_type=event_type)

    def test_upsert_matches_received_invoice_by_document_uuid(self):
        move = self._invoice(self.proveedor, 3287.86, uuid=UUID_A)
        cfdi = self._upsert(syntage_invoice(UUID_A))
        self.assertEqual(cfdi.uuid, UUID_A)
        self.assertEqual(cfdi.direction, 'received')
        self.assertEqual(cfdi.tipo, 'I')
        self.assertEqual(cfdi.estado_sat, 'vigente')
        self.assertEqual(cfdi.counterparty_rfc, 'PSA0602073Q1')
        self.assertEqual(cfdi.partner_id, self.proveedor)
        self.assertEqual(cfdi.move_id, move)
        self.assertEqual(cfdi.match_method, 'uuid_document')
        self.assertEqual(cfdi.match_status, 'matched')
        self.assertEqual(cfdi.issue, 'ok')
        self.assertAlmostEqual(cfdi.amount_diff, 0.0, places=2)
        self.assertEqual(cfdi.name, 'I A 1234')
        self.assertEqual(cfdi.fecha_emision.strftime('%Y-%m-%d %H:%M:%S'), '2026-04-29 22:42:37')

    def test_upsert_is_idempotent_and_updates(self):
        self._upsert(syntage_invoice(UUID_A))
        again = self._upsert(syntage_invoice(UUID_A, total=9999.0, uuid=UUID_A.upper()))
        self.assertEqual(self.env['sat.cfdi'].search_count([('uuid', '=', UUID_A)]), 1)
        self.assertAlmostEqual(again.total, 9999.0)

    def test_solo_sat_then_matched_by_cron(self):
        cfdi = self._upsert(syntage_invoice(UUID_A))
        self.assertFalse(cfdi.move_id)
        self.assertEqual(cfdi.match_status, 'solo_sat')
        self.assertEqual(cfdi.issue, 'solo_sat')
        move = self._invoice(self.proveedor, 3287.86, uuid=UUID_A)
        self.env['sat.cfdi']._cron_match_unmatched()
        self.assertEqual(cfdi.move_id, move)
        self.assertEqual(cfdi.issue, 'ok')

    def test_amount_discrepancy_and_cancellations(self):
        move = self._invoice(self.proveedor, 3000.00, uuid=UUID_A)
        cfdi = self._upsert(syntage_invoice(UUID_A))
        self.assertEqual(cfdi.issue, 'monto')
        self.assertAlmostEqual(cfdi.amount_diff, 287.86, places=2)
        # Cancelado en el SAT, sigue publicado en Odoo
        self._upsert(syntage_invoice(UUID_A, status='CANCELADO', canceledAt='2026-05-01 10:00:00'),
                     event_type='invoice.updated')
        self.assertEqual(cfdi.estado_sat, 'cancelado')
        self.assertEqual(cfdi.fecha_cancelacion.strftime('%Y-%m-%d'), '2026-05-01')
        self.assertEqual(cfdi.issue, 'cancelado_sat')
        # invoice.deleted también cancela
        self._upsert(syntage_invoice(UUID_A), event_type='invoice.deleted')
        self.assertEqual(cfdi.estado_sat, 'cancelado')
        # Vigente en el SAT, cancelada en Odoo
        self._upsert(syntage_invoice(UUID_A))
        move.button_draft()
        move.button_cancel()
        self.assertEqual(cfdi.issue, 'cancelado_odoo')

    def test_issued_direction_and_customer_partner(self):
        move = self._invoice(self.cliente, 5000.0, move_type='out_invoice', uuid=UUID_B)
        obj = syntage_invoice(UUID_B, isIssuer=True, isReceiver=False, total=5000.0, subtotal=5000.0,
                              issuer={'rfc': RFC_QUIMIBOND, 'name': 'QUIMIBOND'},
                              receiver={'rfc': RFC_CLIENTE, 'name': 'EUROTECNICA TEXTIL'})
        cfdi = self._upsert(obj)
        self.assertEqual(cfdi.direction, 'issued')
        self.assertEqual(cfdi.counterparty_rfc, RFC_CLIENTE)
        self.assertEqual(cfdi.partner_id, self.cliente)
        self.assertEqual(cfdi.move_id, move)
        # Sin banderas isIssuer/isReceiver se resuelve por el RFC de la compañía
        obj2 = dict(obj, uuid=UUID_C, id='x')
        obj2.pop('isIssuer')
        obj2.pop('isReceiver')
        self.assertEqual(self._upsert(obj2).direction, 'issued')

    def test_duplicate_capture_prefers_posted_latest(self):
        older = self._invoice(self.proveedor, 3287.86, uuid=UUID_A)
        newer = self._invoice(self.proveedor, 3287.86, uuid=UUID_A)
        cfdi = self._upsert(syntage_invoice(UUID_A))
        self.assertEqual(cfdi.move_id, newer)
        newer.button_draft()
        newer.button_cancel()
        cfdi.action_rematch()
        self.assertEqual(cfdi.move_id, older)

    def test_manual_link_ignore_and_unlink(self):
        move = self._invoice(self.proveedor, 3287.86)
        cfdi = self._upsert(syntage_invoice(UUID_A))
        self.assertFalse(cfdi.move_id)
        cfdi.write({'move_id': move.id})
        self.assertEqual(cfdi.match_method, 'manual')
        self.assertEqual(cfdi.match_status, 'matched')
        cfdi.action_unlink_move()
        self.assertEqual(cfdi.match_status, 'solo_sat')
        cfdi.action_ignore()
        self.assertEqual(cfdi.match_status, 'ignorado')
        self.assertEqual(cfdi.issue, 'ignorado')
        self.assertTrue(cfdi.ignore_reason)
        cfdi.action_rematch()
        self.assertEqual(cfdi.match_status, 'solo_sat')

    def test_compare_buckets(self):
        Compare = self.env['sat.compare.line']
        matched_move = self._invoice(self.proveedor, 3287.86, uuid=UUID_A)
        self._upsert(syntage_invoice(UUID_A))
        self._upsert(syntage_invoice(UUID_B, id='b'))                       # solo SAT
        only_odoo = self._invoice(self.proveedor, 100.0, uuid=UUID_C)        # solo Odoo con UUID
        no_uuid = self._invoice(self.cliente, 200.0, move_type='out_invoice')  # solo Odoo sin UUID
        draft = self._invoice(self.cliente, 300.0, move_type='out_invoice', post=False)  # no cuenta
        self.env.flush_all()

        by_move = {l.move_id.id: l for l in Compare.search([('move_id', '!=', False)])}
        self.assertEqual(by_move[matched_move.id].bucket, 'ambos')
        self.assertEqual(by_move[matched_move.id].issue, 'ok')
        self.assertEqual(by_move[only_odoo.id].bucket, 'solo_odoo')
        self.assertEqual(by_move[only_odoo.id].uuid, UUID_C)
        self.assertEqual(by_move[no_uuid.id].bucket, 'solo_odoo_sin_uuid')
        self.assertNotIn(draft.id, by_move)
        solo_sat = Compare.search([('uuid', '=', UUID_B)])
        self.assertEqual(len(solo_sat), 1)
        self.assertEqual(solo_sat.bucket, 'solo_sat')
        self.assertEqual(solo_sat.issue, 'solo_sat')
        self.assertFalse(solo_sat.move_id)
        self.assertAlmostEqual(solo_sat.total_sat, 3287.86, places=2)

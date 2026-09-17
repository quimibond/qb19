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

    # ── ingesta (sin folio fiscal en Odoo) ─────────────────────────────

    def test_upsert_maps_payload(self):
        cfdi = self._upsert(syntage_invoice(UUID_A))
        self.assertEqual(cfdi.uuid, UUID_A)
        self.assertEqual(cfdi.direction, 'received')
        self.assertEqual(cfdi.tipo, 'I')
        self.assertEqual(cfdi.estado_sat, 'vigente')
        self.assertEqual(cfdi.counterparty_rfc, 'PSA0602073Q1')
        self.assertEqual(cfdi.counterparty_name, 'PAPELERA SANDOVAL')
        self.assertEqual(cfdi.partner_id, self.proveedor)
        self.assertEqual(cfdi.name, 'I A 1234')
        self.assertAlmostEqual(cfdi.total, 3287.86, places=2)
        self.assertAlmostEqual(cfdi.impuestos_trasladados, 453.5, places=2)
        self.assertEqual(cfdi.metodo_pago, 'PUE')
        self.assertEqual(cfdi.fecha_emision.strftime('%Y-%m-%d %H:%M:%S'), '2026-04-29 22:42:37')
        self.assertEqual(cfdi.match_status, 'solo_sat')
        self.assertEqual(cfdi.issue, 'solo_sat')
        self.assertEqual(cfdi.last_event_type, 'pull')

    def test_upsert_is_idempotent_and_updates(self):
        self._upsert(syntage_invoice(UUID_A))
        again = self._upsert(syntage_invoice(UUID_A, total=9999.0, uuid=UUID_A.upper()),
                             event_type='invoice.updated')
        self.assertEqual(self.env['sat.cfdi'].search_count([('uuid', '=', UUID_A)]), 1)
        self.assertAlmostEqual(again.total, 9999.0)
        self.assertEqual(again.last_event_type, 'invoice.updated')

    def test_issued_direction_and_customer_partner(self):
        obj = syntage_invoice(UUID_B, isIssuer=True, isReceiver=False, total=5000.0, subtotal=5000.0,
                              issuer={'rfc': RFC_QUIMIBOND, 'name': 'QUIMIBOND'},
                              receiver={'rfc': RFC_CLIENTE, 'name': 'EUROTECNICA TEXTIL'})
        cfdi = self._upsert(obj)
        self.assertEqual(cfdi.direction, 'issued')
        self.assertEqual(cfdi.counterparty_rfc, RFC_CLIENTE)
        self.assertEqual(cfdi.partner_id, self.cliente)
        # Sin banderas isIssuer/isReceiver se resuelve por el RFC de la compañía
        obj2 = dict(obj, uuid=UUID_C, id='x')
        obj2.pop('isIssuer')
        obj2.pop('isReceiver')
        self.assertEqual(self._upsert(obj2).direction, 'issued')
        # RFC genérico: sin partner
        obj3 = syntage_invoice('22222222-2222-4333-8444-555555555555', id='y',
                               issuer={'rfc': 'XAXX010101000', 'name': 'PUBLICO EN GENERAL'})
        self.assertFalse(self._upsert(obj3).partner_id)

    def test_cancellation_events(self):
        cfdi = self._upsert(syntage_invoice(UUID_A))
        self._upsert(syntage_invoice(UUID_A, status='CANCELADO', canceledAt='2026-05-01 10:00:00'),
                     event_type='invoice.updated')
        self.assertEqual(cfdi.estado_sat, 'cancelado')
        self.assertEqual(cfdi.fecha_cancelacion.strftime('%Y-%m-%d'), '2026-05-01')
        self._upsert(syntage_invoice(UUID_A))
        self.assertEqual(cfdi.estado_sat, 'vigente')
        self._upsert(syntage_invoice(UUID_A), event_type='invoice.deleted')
        self.assertEqual(cfdi.estado_sat, 'cancelado')
        self.assertTrue(cfdi.fecha_cancelacion)

    # ── hallazgos con ligado manual (no requiere folio fiscal) ─────────

    def test_manual_link_issues_ignore_and_unlink(self):
        move = self._invoice(self.proveedor, 3000.00)
        cfdi = self._upsert(syntage_invoice(UUID_A))
        cfdi.write({'move_id': move.id})
        self.assertEqual(cfdi.match_method, 'manual')
        self.assertEqual(cfdi.match_status, 'matched')
        self.assertEqual(cfdi.issue, 'monto')
        self.assertAlmostEqual(cfdi.amount_diff, 287.86, places=2)
        # Una actualización desde Syntage respeta el ligado manual
        self._upsert(syntage_invoice(UUID_A, total=3000.00, subtotal=3000.00), event_type='invoice.updated')
        self.assertEqual(cfdi.move_id, move)
        self.assertEqual(cfdi.issue, 'ok')
        # Cancelado en el SAT, sigue publicado en Odoo
        self._upsert(syntage_invoice(UUID_A, total=3000.00, status='CANCELADO', canceledAt='2026-05-01 10:00:00'),
                     event_type='invoice.updated')
        self.assertEqual(cfdi.issue, 'cancelado_sat')
        # Vigente en el SAT, cancelada en Odoo
        self._upsert(syntage_invoice(UUID_A, total=3000.00))
        move.button_draft()
        move.button_cancel()
        self.assertEqual(cfdi.issue, 'cancelado_odoo')
        # Desligar e ignorar
        cfdi.action_unlink_move()
        self.assertEqual(cfdi.match_status, 'solo_sat')
        self.assertEqual(cfdi.issue, 'solo_sat')
        cfdi.action_ignore()
        self.assertEqual(cfdi.match_status, 'ignorado')
        self.assertEqual(cfdi.issue, 'ignorado')
        self.assertTrue(cfdi.ignore_reason)
        cfdi.action_rematch()
        self.assertEqual(cfdi.match_status, 'solo_sat')

    def test_pick_move_prefers_posted_latest(self):
        older = self._invoice(self.proveedor, 100.0)
        newer = self._invoice(self.proveedor, 100.0)
        draft = self._invoice(self.proveedor, 100.0, post=False)
        Cfdi = self.env['sat.cfdi']
        self.assertEqual(Cfdi._pick_move(older | newer | draft), newer)
        newer.button_draft()
        newer.button_cancel()
        self.assertEqual(Cfdi._pick_move(older | newer | draft), older)
        self.assertEqual(Cfdi._pick_move(draft | newer), draft)
        self.assertFalse(Cfdi._pick_move(self.env['account.move']))

    # ── cruce por folio fiscal (requiere l10n_mx_edi) ──────────────────

    def test_match_by_document_uuid(self):
        self._require_mx_edi()
        move = self._invoice(self.proveedor, 3287.86, uuid=UUID_A)
        cfdi = self._upsert(syntage_invoice(UUID_A))
        self.assertEqual(cfdi.move_id, move)
        self.assertEqual(cfdi.match_method, 'uuid_document')
        self.assertEqual(cfdi.match_status, 'matched')
        self.assertEqual(cfdi.issue, 'ok')
        self.assertAlmostEqual(cfdi.amount_diff, 0.0, places=2)

    def test_solo_sat_then_matched_by_cron(self):
        self._require_mx_edi()
        cfdi = self._upsert(syntage_invoice(UUID_A))
        self.assertFalse(cfdi.move_id)
        move = self._invoice(self.proveedor, 3287.86, uuid=UUID_A)
        self.env['sat.cfdi']._cron_match_unmatched()
        self.assertEqual(cfdi.move_id, move)
        self.assertEqual(cfdi.issue, 'ok')

    def test_duplicate_capture_prefers_posted_latest(self):
        self._require_mx_edi()
        older = self._invoice(self.proveedor, 3287.86, uuid=UUID_A)
        newer = self._invoice(self.proveedor, 3287.86, uuid=UUID_A)
        cfdi = self._upsert(syntage_invoice(UUID_A))
        self.assertEqual(cfdi.move_id, newer)
        newer.button_draft()
        newer.button_cancel()
        cfdi.action_rematch()
        self.assertEqual(cfdi.move_id, older)

    # ── comparación ────────────────────────────────────────────────────

    def test_compare_buckets(self):
        Compare = self.env['sat.compare.line']
        linked = self._invoice(self.proveedor, 3287.86)
        self._upsert(syntage_invoice(UUID_A)).write({'move_id': linked.id})   # ambos (manual)
        self._upsert(syntage_invoice(UUID_B, id='b'))                        # solo SAT
        no_uuid = self._invoice(self.cliente, 200.0, move_type='out_invoice')  # solo Odoo sin UUID
        draft = self._invoice(self.cliente, 300.0, move_type='out_invoice', post=False)  # no cuenta
        self._upsert(syntage_invoice(UUID_C, id='c', type='P'))              # los P no entran
        self.env.flush_all()

        by_move = {line.move_id.id: line for line in Compare.search([('move_id', '!=', False)])}
        self.assertEqual(by_move[linked.id].bucket, 'ambos')
        self.assertEqual(by_move[linked.id].issue, 'ok')
        self.assertAlmostEqual(by_move[linked.id].total_odoo, 3287.86, places=2)
        self.assertEqual(by_move[no_uuid.id].bucket, 'solo_odoo_sin_uuid')
        self.assertEqual(by_move[no_uuid.id].direction, 'issued')
        self.assertEqual(by_move[no_uuid.id].counterparty_rfc, RFC_CLIENTE)
        self.assertNotIn(draft.id, by_move)
        solo_sat = Compare.search([('uuid', '=', UUID_B)])
        self.assertEqual(len(solo_sat), 1)
        self.assertEqual(solo_sat.bucket, 'solo_sat')
        self.assertEqual(solo_sat.issue, 'solo_sat')
        self.assertFalse(solo_sat.move_id)
        self.assertAlmostEqual(solo_sat.total_sat, 3287.86, places=2)
        self.assertFalse(Compare.search([('uuid', '=', UUID_C)]))

    def test_compare_solo_odoo_with_uuid(self):
        self._require_mx_edi()
        Compare = self.env['sat.compare.line']
        only_odoo = self._invoice(self.proveedor, 100.0, uuid=UUID_C)
        self.env.flush_all()
        line = Compare.search([('move_id', '=', only_odoo.id)])
        self.assertEqual(len(line), 1)
        self.assertEqual(line.bucket, 'solo_odoo')
        self.assertEqual(line.uuid, UUID_C)

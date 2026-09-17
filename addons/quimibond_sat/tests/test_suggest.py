# -*- coding: utf-8 -*-
"""Segunda pasada: sugerencias por RFC + monto + fecha, XML cruzado y las
columnas de conciliación al centavo de la comparación."""
from odoo.tests import tagged

from .common import SatCommon, syntage_invoice

UUID_OK = 'aaaaaaaa-0000-4000-8000-000000000001'
UUID_WRONG = 'aaaaaaaa-0000-4000-8000-000000000002'
UUID_OTHER = 'aaaaaaaa-0000-4000-8000-000000000003'


@tagged('post_install', '-at_install', 'quimibond_sat')
class TestSatSuggest(SatCommon):

    def _upsert(self, obj):
        return self.env['sat.cfdi']._upsert_from_syntage(obj, self.company, event_type='pull')

    def test_suggestion_for_bill_without_xml(self):
        # 19 días entre factura y CFDI: se sugiere (ventana ±45) pero el cron
        # no la liga solo (aceptación automática solo a ±10 días).
        bill = self._invoice(self.proveedor, 3287.86, day='2026-04-10')
        cfdi = self._upsert(syntage_invoice(UUID_OK))
        self.assertEqual(cfdi.match_status, 'solo_sat')
        self.assertEqual(self.env['sat.cfdi']._cron_suggest_matches(), 1)
        self.assertFalse(cfdi.move_id)
        self.assertEqual(cfdi.suggested_move_id, bill)
        self.assertEqual(cfdi.suggestion_reason, 'sin_uuid')
        # Otro RFC o monto distinto no se sugiere
        other = self._upsert(syntage_invoice(UUID_OTHER, id='o', total=99.0, subtotal=99.0))
        other.action_suggest()
        self.assertFalse(other.suggested_move_id)
        # Aceptar liga por 'sugerido' y el cruce por UUID ya no lo toca
        cfdi.action_accept_suggestion()
        self.assertEqual(cfdi.move_id, bill)
        self.assertEqual(cfdi.match_method, 'sugerido')
        self.assertEqual(cfdi.issue, 'ok')
        cfdi._match_move()
        self.assertEqual(cfdi.move_id, bill)
        # Rechazar una sugerencia la borra y no vuelve a proponerse
        other2 = self._upsert(syntage_invoice('aaaaaaaa-0000-4000-8000-000000000004', id='p', total=3287.86))
        other2.action_suggest()
        self.assertFalse(other2.suggested_move_id)  # la factura ya está tomada
        bill2 = self._invoice(self.proveedor, 3287.86, day='2026-04-20')
        other2.action_suggest()
        self.assertEqual(other2.suggested_move_id, bill2)
        other2.action_reject_suggestion()
        self.assertFalse(other2.suggested_move_id)
        other2.action_suggest()
        self.assertFalse(other2.suggested_move_id)

    def test_crossed_xml(self):
        # La factura de $18,585.72 trae ligado (a mano, como haría un XML
        # equivocado) el CFDI de $3,691.23 del mismo proveedor.
        bill = self._invoice(self.proveedor, 18585.72, day='2026-04-17')
        wrong = self._upsert(syntage_invoice(UUID_WRONG, id='w', total=3691.23, subtotal=3182.09))
        wrong.write({'move_id': bill.id})
        self.assertEqual(wrong.issue, 'monto')
        right = self._upsert(syntage_invoice(UUID_OK, total=18585.72, subtotal=16022.17,
                                             issuedAt='2026-04-28 10:00:00'))
        right.action_suggest()
        self.assertEqual(right.suggested_move_id, bill)
        self.assertEqual(right.suggestion_reason, 'uuid_cruzado')
        right.action_accept_suggestion()
        self.assertEqual(right.move_id, bill)
        self.assertEqual(right.issue, 'ok')
        self.assertFalse(wrong.move_id)
        self.assertEqual(wrong.match_status, 'solo_sat')
        self.assertIn('XML cruzado', wrong.note)

    def test_compare_cent_columns(self):
        Compare = self.env['sat.compare.line']
        bill = self._invoice(self.proveedor, 1000.0, day='2026-04-10')
        cfdi = self._upsert(syntage_invoice(UUID_OK, total=1000.0, subtotal=862.07, issuedAt='2026-04-10 12:00:00'))
        cfdi.write({'move_id': bill.id})
        refund = self._upsert(syntage_invoice(UUID_OTHER, id='r', type='E', total=200.0, subtotal=172.41,
                                              issuedAt='2026-04-12 12:00:00'))
        cancelled = self._upsert(syntage_invoice(UUID_WRONG, id='c', total=500.0, status='CANCELADO',
                                                 canceledAt='2026-04-13 00:00:00', issuedAt='2026-04-13 12:00:00'))
        self.env.flush_all()
        rows = {r.uuid: r for r in Compare.search([('uuid', 'in', [UUID_OK, UUID_OTHER, UUID_WRONG])])}
        self.assertEqual(rows[UUID_OK].mes.strftime('%Y-%m-%d'), '2026-04-01')
        self.assertAlmostEqual(rows[UUID_OK].sat_vigente, 1000.0, places=2)
        self.assertAlmostEqual(rows[UUID_OK].odoo_posted, 1000.0, places=2)
        self.assertAlmostEqual(rows[UUID_OK].delta, 0.0, places=2)
        self.assertEqual(refund.tipo, 'E')
        self.assertAlmostEqual(rows[UUID_OTHER].sat_vigente, -200.0, places=2)   # egreso resta
        self.assertAlmostEqual(rows[UUID_OTHER].delta, -200.0, places=2)
        self.assertAlmostEqual(rows[UUID_WRONG].sat_vigente, 0.0, places=2)      # cancelado no cuenta
        self.assertAlmostEqual(rows[UUID_WRONG].delta, 0.0, places=2)
        self.assertFalse(cancelled.move_id)

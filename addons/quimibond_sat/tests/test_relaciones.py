# -*- coding: utf-8 -*-
"""CFDI relacionados: sustitución (04) y compensados por nota de crédito (01/03)."""
from odoo.tests import tagged

from .common import SatCommon, syntage_invoice

UUID_I = '77777777-0000-4000-8000-000000000001'
UUID_E = '77777777-0000-4000-8000-000000000002'
UUID_OLD = '77777777-0000-4000-8000-000000000003'
UUID_NEW = '77777777-0000-4000-8000-000000000004'


def relation(kind, uuid):
    return [{'@type': 'InvoiceRelation', 'type': kind, 'relatedInvoiceUuid': uuid}]


@tagged('post_install', '-at_install', 'quimibond_sat')
class TestSatRelaciones(SatCommon):

    def _upsert(self, obj):
        return self.env['sat.cfdi']._upsert_from_syntage(obj, self.company, event_type='pull')

    def test_relations_are_stored(self):
        e = self._upsert(syntage_invoice(UUID_E, type='E', relations=relation(3, UUID_I.upper())))
        self.assertEqual(e.relacion_tipo, '03')
        self.assertEqual(e.relacionados, UUID_I)

    def test_invoice_cancelled_by_credit_note(self):
        inv = self._upsert(syntage_invoice(UUID_I, total=243716.0))
        self.assertEqual(inv.match_status, 'solo_sat')
        e = self._upsert(syntage_invoice(UUID_E, id='e', type='E', total=243716.0,
                                         relations=relation(3, UUID_I)))
        self.assertEqual(e.match_status, 'ignorado')
        self.assertEqual(inv.match_status, 'ignorado')
        self.assertIn('Compensado', inv.ignore_reason)

    def test_partial_credit_is_not_compensated(self):
        inv = self._upsert(syntage_invoice(UUID_I, total=1000.0))
        e = self._upsert(syntage_invoice(UUID_E, id='e', type='E', total=300.0, relations=relation(1, UUID_I)))
        self.assertEqual(inv.match_status, 'solo_sat')
        self.assertEqual(e.match_status, 'solo_sat')

    def test_credit_with_bill_in_odoo_is_not_compensated(self):
        bill = self._invoice(self.proveedor, 3287.86)
        inv = self._upsert(syntage_invoice(UUID_I))
        inv.write({'move_id': bill.id})
        e = self._upsert(syntage_invoice(UUID_E, id='e', type='E', relations=relation(1, UUID_I)))
        self.assertEqual(e.match_status, 'solo_sat')   # falta la nota de crédito en Odoo

    def test_substitution_takes_the_bill(self):
        bill = self._invoice(self.proveedor, 3287.86)
        old = self._upsert(syntage_invoice(UUID_OLD))
        old.write({'move_id': bill.id})
        self._upsert(syntage_invoice(UUID_OLD, status='CANCELADO', canceledAt='2026-05-01 10:00:00'))
        self.assertEqual(old.issue, 'cancelado_sat')
        new = self._upsert(syntage_invoice(UUID_NEW, id='n', relations=relation(4, UUID_OLD)))
        self.assertEqual(new.move_id, bill)
        self.assertEqual(new.match_method, 'sustitucion')
        self.assertEqual(new.issue, 'ok')
        self.assertFalse(old.move_id)
        self.assertEqual(old.issue, 'cancelado_sin_factura')

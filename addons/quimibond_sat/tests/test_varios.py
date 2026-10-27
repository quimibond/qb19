# -*- coding: utf-8 -*-
"""Una factura con varios CFDI (el proveedor timbró dos, Odoo los capturó
juntos) y un CFDI en varias facturas (mismo UUID)."""
from unittest.mock import patch

from odoo.tests import tagged

from .common import SatCommon, syntage_invoice

UUID_A = '88888888-0000-4000-8000-000000000001'
UUID_B = '88888888-0000-4000-8000-000000000002'
UUID_C = '88888888-0000-4000-8000-000000000003'


@tagged('post_install', '-at_install', 'quimibond_sat')
class TestSatVarios(SatCommon):

    def _upsert(self, obj):
        return self.env['sat.cfdi']._upsert_from_syntage(obj, self.company, event_type='pull')

    def _rows(self, uuids):
        self.env.flush_all()
        self.env.invalidate_all()
        return self.env['sat.compare.line'].search([('uuid', 'in', uuids)])

    def test_bill_with_two_cfdis(self):
        bill = self._invoice(self.proveedor, 3000.0)
        a = self._upsert(syntage_invoice(UUID_A, total=1000.0, folio='1'))
        b = self._upsert(syntage_invoice(UUID_B, id='b', total=2000.0, folio='2'))
        a._reconcile_with(bill, attach_xml=False)
        self.assertEqual(a.issue, 'monto')          # solo falta el otro CFDI
        # El segundo encuentra la factura por lo que le falta
        self.assertIn(bill, b._reconcile_candidates())
        b._reconcile_with(bill, attach_xml=False)
        self.assertEqual(a.move_id, bill)            # no se desliga: no estaba cruzado
        self.assertEqual(a.issue, 'ok')
        self.assertEqual(b.issue, 'ok')
        rows = self._rows([UUID_A, UUID_B])
        self.assertEqual(set(rows.mapped('issue')), {'ok'})
        self.assertAlmostEqual(sum(rows.mapped('delta')), 0.0, places=2)
        self.assertAlmostEqual(sum(rows.mapped('total_odoo')), 3000.0, places=2)

    def test_crossed_still_unlinks(self):
        bill = self._invoice(self.proveedor, 3000.0)
        wrong = self._upsert(syntage_invoice(UUID_A, total=1234.0))
        right = self._upsert(syntage_invoice(UUID_B, id='b', total=3000.0))
        wrong._reconcile_with(bill, attach_xml=False)
        right._reconcile_with(bill, attach_xml=False)
        self.assertFalse(wrong.move_id)
        self.assertIn('XML cruzado', wrong.note)
        self.assertEqual(right.issue, 'ok')

    def test_currency_purchase_matches_in_pesos(self):
        """Compra de dólares: el banco timbra en pesos, la factura en USD.
        Al tipo de Odoo (1 USD = 20 MXN) cuadran en pesos: no es hallazgo."""
        bill = self._invoice(self.proveedor, 105000.0, currency=self.usd)
        cfdi = self._upsert(syntage_invoice(UUID_A, total=2100000.0))
        cfdi.write({'move_id': bill.id})
        self.assertEqual(cfdi.issue, 'moneda_ok')
        self.assertEqual(self._rows([UUID_A]).issue, 'moneda_ok')

    def test_cfdi_split_in_two_bills(self):
        self._require_mx_edi()
        big = self._invoice(self.proveedor, 124634.60, uuid=UUID_C)
        small = self._invoice(self.proveedor, 1944.16, uuid=UUID_C)
        cfdi = self._upsert(syntage_invoice(UUID_C, total=126578.76))
        self.assertIn(cfdi.move_id, big | small)
        self.assertEqual(cfdi.issue, 'ok')
        row = self._rows([UUID_C])
        self.assertEqual(row.issue, 'ok')
        self.assertAlmostEqual(row.total_odoo, 126578.76, places=2)

    def test_factura_cancelada_y_recapturada(self):
        """El CFDI quedó ligado por UUID a una factura que se canceló y se
        volvió a capturar con el mismo UUID: el cruce lo pasa a la publicada."""
        vieja = self._invoice(self.proveedor, 8584.0)
        cfdi = self._upsert(syntage_invoice(UUID_A, total=8584.0))
        cfdi.write({'move_id': vieja.id, 'match_method': 'uuid_document', 'match_status': 'matched'})
        vieja.button_cancel()
        nueva = self._invoice(self.proveedor, 8584.0)
        Cfdi = type(self.env['sat.cfdi'])
        with patch.object(Cfdi, '_candidate_moves', lambda rec: (vieja | nueva, 'uuid_document')):
            self.env['sat.cfdi']._cron_match_unmatched(full=True)
        self.assertEqual(cfdi.move_id, nueva)
        self.assertEqual(cfdi.match_status, 'matched')

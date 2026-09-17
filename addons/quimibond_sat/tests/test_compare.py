# -*- coding: utf-8 -*-
"""Comparación al centavo: tolerancia de un centavo, moneda distinta y
columnas en MXN (tipo de cambio del CFDI)."""
from odoo.tests import tagged

from .common import SatCommon, syntage_invoice

UUID_A = 'bbbbbbbb-0000-4000-8000-000000000001'
UUID_B = 'bbbbbbbb-0000-4000-8000-000000000002'
UUID_C = 'bbbbbbbb-0000-4000-8000-000000000003'


@tagged('post_install', '-at_install', 'quimibond_sat')
class TestSatCompare(SatCommon):

    def _upsert(self, obj):
        return self.env['sat.cfdi']._upsert_from_syntage(obj, self.company, event_type='pull')

    def _row(self, uuid):
        self.env.flush_all()
        return self.env['sat.compare.line'].search([('uuid', '=', uuid)], limit=1)

    def test_one_cent_tolerated_two_cents_not(self):
        bill_a = self._invoice(self.proveedor, 1000.00, day='2026-04-10')
        bill_b = self._invoice(self.proveedor, 2000.00, day='2026-04-11')
        a = self._upsert(syntage_invoice(UUID_A, total=1000.01, issuedAt='2026-04-10 12:00:00'))
        b = self._upsert(syntage_invoice(UUID_B, id='b', total=2000.02, issuedAt='2026-04-11 12:00:00'))
        a.write({'move_id': bill_a.id})
        b.write({'move_id': bill_b.id})
        self.assertEqual(a.issue, 'ok')
        self.assertAlmostEqual(a.amount_diff, 0.01, places=2)
        self.assertEqual(b.issue, 'monto')
        self.assertEqual(self._row(UUID_A).issue, 'ok')
        row_b = self._row(UUID_B)
        self.assertEqual(row_b.issue, 'monto')
        self.assertAlmostEqual(row_b.amount_diff, 0.02, places=2)
        self.assertAlmostEqual(row_b.delta, 0.02, places=2)

    def test_currency_mismatch(self):
        """Compra de dólares: el banco timbra en MXN y la factura en Odoo
        quedó en USD. Los totales no son comparables: 'moneda'."""
        bill = self._invoice(self.proveedor, 105000.00, day='2026-09-14', currency=self.usd)
        cfdi = self._upsert(syntage_invoice(UUID_A, total=1796025.00, issuedAt='2026-09-14 12:00:00'))
        cfdi.write({'move_id': bill.id})
        self.assertEqual(cfdi.issue, 'moneda')
        row = self._row(UUID_A)
        self.assertEqual(row.issue, 'moneda')
        self.assertEqual(row.moneda, 'MXN')
        self.assertEqual(row.moneda_odoo, 'USD')
        self.assertAlmostEqual(row.total_sat_mxn, 1796025.00, places=2)
        # Sin moneda común, el lado Odoo va al tipo de cambio de Odoo (1 USD = 20 MXN)
        self.assertAlmostEqual(row.total_odoo_mxn, 2100000.00, places=2)

    def test_usd_rows_in_mxn_with_cfdi_rate(self):
        """Factura en USD con CFDI en USD: cuadra al centavo en dólares y las
        columnas MXN usan el tipo de cambio del CFDI en ambos lados."""
        inv = self._invoice(self.cliente, 100.00, move_type='out_invoice', day='2026-09-10', currency=self.usd)
        cfdi = self._upsert(syntage_invoice(
            UUID_A, total=100.00, currency='USD', exchangeRate=17.5, isIssuer=True, isReceiver=False,
            issuer={'rfc': 'PNT920218IW5', 'name': 'QUIMIBOND'}, receiver={'rfc': 'ETE790612A92', 'name': 'ETE'},
            issuedAt='2026-09-10 12:00:00'))
        cfdi.write({'move_id': inv.id})
        self.assertEqual(cfdi.direction, 'issued')
        self.assertEqual(cfdi.issue, 'ok')
        row = self._row(UUID_A)
        self.assertEqual(row.issue, 'ok')
        self.assertEqual(row.moneda, 'USD')
        self.assertAlmostEqual(row.total_sat, 100.00, places=2)
        self.assertAlmostEqual(row.total_sat_mxn, 1750.00, places=2)
        self.assertAlmostEqual(row.total_odoo_mxn, 1750.00, places=2)
        self.assertAlmostEqual(row.sat_vigente, 1750.00, places=2)
        self.assertAlmostEqual(row.odoo_posted, 1750.00, places=2)
        self.assertAlmostEqual(row.delta, 0.0, places=2)
        # Solo en Odoo, en USD: MXN según Odoo (1 USD = 20 MXN)
        alone = self._invoice(self.cliente, 50.00, move_type='out_invoice', day='2026-09-11', currency=self.usd)
        self.env.flush_all()
        row = self.env['sat.compare.line'].search([('move_id', '=', alone.id)], limit=1)
        self.assertEqual(row.moneda, 'USD')
        self.assertIn(row.issue, ('solo_odoo', 'solo_odoo_sin_uuid'))
        self.assertAlmostEqual(row.total_odoo, 50.00, places=2)
        self.assertAlmostEqual(row.total_odoo_mxn, 1000.00, places=2)
        self.assertAlmostEqual(row.delta, -1000.00, places=2)

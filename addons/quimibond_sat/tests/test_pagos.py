# -*- coding: utf-8 -*-
"""Complementos de pago: ingesta (webhook, API, importación), liga a la
factura del SAT y comparación de pagos SAT vs Odoo. También el estado SAT en
la factura de Odoo."""
from unittest.mock import patch

from odoo import fields
from odoo.tests import tagged

from .common import RFC_QUIMIBOND, SatCommon, syntage_invoice

UUID_A = 'dddddddd-0000-4000-8000-000000000001'
UUID_B = 'dddddddd-0000-4000-8000-000000000002'
UUID_C = 'dddddddd-0000-4000-8000-000000000003'
UUID_X = 'dddddddd-0000-4000-8000-00000000000f'


def syntage_payment(pid, invoice_uuid, amount, date='2026-04-30 12:00:00', **overrides):
    obj = {
        'id': pid, '@id': '/invoices/payments/%s' % pid, '@type': 'InvoicePayment',
        'date': date, 'amount': amount, 'currency': 'MXN', 'exchangeRate': None,
        'installment': 1, 'invoiceUuid': invoice_uuid, 'previousBalance': abs(amount),
        'outstandingBalance': 0, 'paymentMethod': '03', 'canceledAt': None,
        'batchPayment': {'id': 'bp-%s' % pid, 'date': date, 'paymentMethod': '03',
                         'operationNumber': 'OP-%s' % pid},
    }
    obj.update(overrides)
    return obj


@tagged('post_install', '-at_install', 'quimibond_sat')
class TestSatPagos(SatCommon):

    def _upsert(self, obj):
        return self.env['sat.cfdi']._upsert_from_syntage(obj, self.company, event_type='pull')

    def _pago(self, obj, event_type='pull'):
        return self.env['sat.cfdi.pago']._upsert_from_syntage(obj, self.company, event_type=event_type)

    def _pay(self, move, amount=None):
        wizard = self.env['account.payment.register'].with_context(
            active_model='account.move', active_ids=move.ids).create({
                'payment_date': move.invoice_date or fields.Date.today(),
                **({'amount': amount} if amount is not None else {}),
            })
        wizard.action_create_payments()
        move.invalidate_recordset()

    def _row(self, cfdi):
        self.env.flush_all()
        self.env.invalidate_all()
        return self.env['sat.pago.compare'].browse(cfdi.id)

    def test_upsert_links_invoice_and_cancels(self):
        cfdi = self._upsert(syntage_invoice(UUID_A, total=1000.0, paymentType='PPD'))
        pago = self._pago(syntage_payment('p1', UUID_A.upper(), 600.0))
        self.assertEqual(pago.invoice_cfdi_id, cfdi)
        self.assertEqual(pago.kind, 'pago')            # factura recibida
        self.assertAlmostEqual(pago.monto, 600.0, places=2)
        self.assertEqual(pago.num_operacion, 'OP-p1')
        self.assertEqual(pago.estado_sat, 'vigente')
        # Idempotente y actualiza
        again = self._pago(syntage_payment('p1', UUID_A, 650.0), event_type='invoice_payment.updated')
        self.assertEqual(again, pago)
        self.assertAlmostEqual(pago.monto, 650.0, places=2)
        self.assertEqual(self.env['sat.cfdi.pago'].search_count([('syntage_id', '=', 'p1')]), 1)
        self._pago(syntage_payment('p1', UUID_A, 650.0), event_type='invoice_payment.deleted')
        self.assertEqual(pago.estado_sat, 'cancelado')
        # Pago que llega antes que su factura: sin liga; al llegar la factura se liga solo
        orphan = self._pago(syntage_payment('p2', UUID_B, -300.0))
        self.assertFalse(orphan.invoice_cfdi_id)
        self.assertEqual(orphan.kind, 'pago')         # importe negativo = pagamos
        inv = self._upsert(syntage_invoice(UUID_B, id='b', total=300.0, isIssuer=True, isReceiver=False,
                                           issuer={'rfc': RFC_QUIMIBOND, 'name': 'Q'},
                                           receiver={'rfc': 'ETE790612A92', 'name': 'ETE'}))
        orphan.invalidate_recordset()
        self.assertEqual(orphan.invoice_cfdi_id, inv)
        self.assertEqual(orphan.kind, 'cobro')

    def test_compare_issues(self):
        Compare = self.env['sat.pago.compare']
        # PPD sin pago en ningún lado: cuadra
        bill = self._invoice(self.proveedor, 1000.0, day='2026-04-10')
        cfdi = self._upsert(syntage_invoice(UUID_A, total=1000.0, paymentType='PPD', issuedAt='2026-04-10 12:00:00'))
        cfdi.write({'move_id': bill.id})
        row = self._row(cfdi)
        self.assertEqual(row.issue, 'ok')
        self.assertAlmostEqual(row.saldo_sat, 1000.0, places=2)
        self.assertAlmostEqual(row.saldo_odoo, 1000.0, places=2)
        # Odoo pagó, el SAT no tiene complemento
        self._pay(bill)
        row = self._row(cfdi)
        self.assertAlmostEqual(row.pagado_odoo, 1000.0, places=2)
        self.assertEqual(row.issue, 'sin_complemento')
        self.assertAlmostEqual(row.delta, -1000.0, places=2)
        # Llega el complemento: cuadra
        self._pago(syntage_payment('p1', UUID_A, 1000.0))
        row = self._row(cfdi)
        self.assertEqual(row.n_pagos, 1)
        self.assertAlmostEqual(row.pagado_sat, 1000.0, places=2)
        self.assertEqual(row.issue, 'ok')
        # Complemento en el SAT, nada pagado en Odoo
        bill2 = self._invoice(self.proveedor, 500.0, day='2026-04-11')
        cfdi2 = self._upsert(syntage_invoice(UUID_B, id='b', total=500.0, paymentType='PPD',
                                             issuedAt='2026-04-11 12:00:00'))
        cfdi2.write({'move_id': bill2.id})
        self._pago(syntage_payment('p2', UUID_B, 500.0))
        row = self._row(cfdi2)
        self.assertEqual(row.issue, 'complemento_sin_pago')
        self.assertAlmostEqual(row.delta, 500.0, places=2)
        # PUE pagada en Odoo sin complemento: no lo requiere
        bill3 = self._invoice(self.proveedor, 200.0, day='2026-04-12')
        cfdi3 = self._upsert(syntage_invoice(UUID_C, id='c', total=200.0, paymentType='PUE',
                                             issuedAt='2026-04-12 12:00:00'))
        cfdi3.write({'move_id': bill3.id})
        self._pay(bill3)
        self.assertEqual(self._row(cfdi3).issue, 'pue')
        # Sin factura en Odoo
        cfdi4 = self._upsert(syntage_invoice(UUID_X, id='x', total=50.0, paymentType='PPD'))
        self.assertEqual(self._row(cfdi4).issue, 'sin_factura')
        self.assertEqual(Compare.search_count([('issue', 'in', ('sin_complemento', 'complemento_sin_pago'))]), 1)

    def test_import_batch_accepts_strings(self):
        self._upsert(syntage_invoice(UUID_A, total=1000.0, paymentType='PPD'))
        rows = [
            {'id': 'i1', 'date': '2026-04-30 12:00:00', 'amount': '400.00', 'currency': 'MXN',
             'exchangeRate': '', 'installment': '1', 'invoiceUuid': UUID_A, 'previousBalance': '1000',
             'outstandingBalance': '600', 'paymentMethod': '03', 'canceledAt': ''},
            {'id': 'i2', 'date': '2026-05-30 12:00:00', 'amount': '600.00', 'installment': '2',
             'invoiceUuid': UUID_A, 'canceledAt': '2026-06-01 00:00:00'},
            {'date': '2026-05-30 12:00:00', 'amount': '1'},   # sin id
        ]
        result = self.env['sat.cfdi.pago'].action_import_payments(rows, company_id=self.company.id)
        self.assertEqual(result['upserted'], 2)
        self.assertEqual(result['errored'], 1)
        pagos = self.env['sat.cfdi.pago'].search([('syntage_id', 'in', ['i1', 'i2'])], order='syntage_id')
        self.assertAlmostEqual(pagos[0].monto, 400.0, places=2)
        self.assertEqual(pagos[0].parcialidad, 1)
        self.assertAlmostEqual(pagos[0].saldo_insoluto, 600.0, places=2)
        self.assertEqual(pagos[1].estado_sat, 'cancelado')

    def test_webhook_payment_event(self):
        cfdi = self._upsert(syntage_invoice(UUID_A, total=1000.0, paymentType='PPD'))
        event = {
            'id': 'evt_pago_1', 'type': 'invoice_payment.created',
            'taxpayer': {'id': RFC_QUIMIBOND, 'name': 'QUIMIBOND'},
            'data': {'object': syntage_payment('wp1', UUID_A, 1000.0)},
        }
        rec, new = self.env['sat.webhook.event']._record(event)
        self.assertTrue(new)
        rec.process()
        self.assertEqual(rec.state, 'processed')
        self.assertEqual(rec.cfdi_id, cfdi)
        pago = self.env['sat.cfdi.pago'].search([('syntage_id', '=', 'wp1')])
        self.assertEqual(pago.last_event_type, 'invoice_payment.created')

    def test_pull_payments_filters_unknown_invoices(self):
        self.env['ir.config_parameter'].sudo().set_param('quimibond_sat.api_key', 'k')
        cfdi = self._upsert(syntage_invoice(UUID_A, total=1000.0, paymentType='PPD'))
        calls = []

        def _request(client, method, path, params=None, json=None, headers=None, timeout=60):
            calls.append(path)
            return {'hydra:member': [
                syntage_payment('a1', UUID_A, 1000.0),
                syntage_payment('z1', UUID_X, 99.0),      # factura que no está en Odoo
                {'id': 'bad', 'amount': 'x', 'invoiceUuid': UUID_A},   # id y uuid ok, importe raro → 0
            ], 'hydra:view': {}}

        Client = type(self.env['sat.syntage.client'])
        with patch.object(Client, '_request', _request):
            result = self.env['sat.cfdi'].action_pull_period('2026-04-01', '2026-04-30', mode='payments')
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['items_fetched'], 3)
        self.assertEqual(result['items_upserted'], 2)
        self.assertIn('/invoices/payments?', calls[0])
        self.assertIn('date%5Bafter%5D=2026-03-31', calls[0])
        self.assertIn('date%5Bbefore%5D=2026-05-02', calls[0])
        self.assertEqual(self.env['sat.cfdi.pago'].search([('syntage_id', '=', 'a1')]).invoice_cfdi_id, cfdi)
        self.assertFalse(self.env['sat.cfdi.pago'].search([('syntage_id', '=', 'z1')]))
        log = self.env['sat.sync.log'].browse(result['log_id'])
        self.assertEqual(log.mode, 'payments')
        self.assertIn('1 de facturas que no están en Odoo', log.summary)

    def test_move_sat_status_and_alert(self):
        bill = self._invoice(self.proveedor, 1000.0, day='2026-04-10')
        self.assertEqual(bill.sat_estado, 'sin_cfdi')
        self.assertFalse(bill.sat_alerta)
        cfdi = self._upsert(syntage_invoice(UUID_A, total=1000.0))
        cfdi.write({'move_id': bill.id})
        bill.invalidate_recordset()
        self.assertEqual(bill.sat_estado, 'vigente')
        self.assertEqual(bill.sat_cfdi_count, 1)
        self.assertFalse(bill.sat_alerta)
        # Cancelado en el SAT, publicada en Odoo
        self._upsert(syntage_invoice(UUID_A, total=1000.0, status='CANCELADO', canceledAt='2026-05-01 10:00:00'))
        bill.invalidate_recordset()
        self.assertEqual(bill.sat_estado, 'cancelado')
        self.assertIn('CANCELADA en el SAT', bill.sat_alerta)
        # Vigente en el SAT, cancelada en Odoo
        self._upsert(syntage_invoice(UUID_A, total=1000.0))
        bill.button_draft()
        bill.button_cancel()
        bill.invalidate_recordset()
        self.assertIn('VIGENTE en el SAT', bill.sat_alerta)
        # Monto distinto
        bill2 = self._invoice(self.proveedor, 900.0, day='2026-04-11')
        cfdi2 = self._upsert(syntage_invoice(UUID_B, id='b', total=1000.0))
        cfdi2.write({'move_id': bill2.id})
        bill2.invalidate_recordset()
        self.assertIn('no coincide', bill2.sat_alerta)
        action = bill2.action_open_sat_cfdi()
        self.assertEqual(action['domain'], [('move_id', '=', bill2.id)])

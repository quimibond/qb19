# -*- coding: utf-8 -*-
"""Complementos de pago contra pagos de Odoo (1.30.0): cada renglón sabe de qué
CFDI de pago viene y con qué pago de Odoo se cruza; las copias del mismo
documento relacionado y los REP cancelados en el SAT no cuentan."""
from unittest.mock import patch

from odoo import fields
from odoo.tests import tagged

from .common import SatCommon, syntage_invoice
from .test_pagos import syntage_payment

UUID_F = 'abababab-0000-4000-8000-000000000001'
UUID_G = 'abababab-0000-4000-8000-000000000002'
UUID_P1 = 'abababab-0000-4000-8000-0000000000a1'
UUID_P2 = 'abababab-0000-4000-8000-0000000000a2'


def syntage_rep(uuid, sid, **overrides):
    """Un CFDI de pago (tipo P) como lo manda Syntage: total 0."""
    return syntage_invoice(uuid, id=sid, type='P', total=0, subtotal=0, paymentType=None,
                           transferredTaxes={'total': 0}, **overrides)


def pago_de_rep(pid, invoice_uuid, amount, rep_sid, **overrides):
    obj = syntage_payment(pid, invoice_uuid, amount, **overrides)
    obj['batchPayment'] = dict(obj['batchPayment'], amount=amount,
                               invoice={'id': rep_sid, '@id': '/invoices/%s' % rep_sid})
    return obj


@tagged('post_install', '-at_install', 'quimibond_sat')
class TestSatRep(SatCommon):

    def _upsert(self, obj):
        return self.env['sat.cfdi']._upsert_from_syntage(obj, self.company, event_type='pull')

    def _pago(self, obj):
        return self.env['sat.cfdi.pago']._upsert_from_syntage(obj, self.company, event_type='pull')

    def _pay(self, move, day='2026-04-30'):
        wizard = self.env['account.payment.register'].with_context(
            active_model='account.move', active_ids=move.ids).create({'payment_date': day})
        wizard.action_create_payments()
        move.invalidate_recordset()
        return move._get_reconciled_payments() | move.matched_payment_ids

    def _row(self, cfdi):
        self.env.flush_all()
        self.env.invalidate_all()
        return self.env['sat.pago.compare'].browse(cfdi.id)

    def _factura_pagada(self, uuid, sid, amount=1000.0):
        bill = self._invoice(self.proveedor, amount, day='2026-04-10')
        cfdi = self._upsert(syntage_invoice(uuid, id=sid, total=amount, paymentType='PPD',
                                            issuedAt='2026-04-10 12:00:00'))
        cfdi.write({'move_id': bill.id})
        return bill, cfdi, self._pay(bill)

    def test_rep_y_pago_de_odoo(self):
        bill, cfdi, pay = self._factura_pagada(UUID_F, 'f')
        # El renglón llega antes que su CFDI de pago: se liga cuando éste entra
        pago = self._pago(pago_de_rep('r1', UUID_F, 1000.0, 'rep-1'))
        self.assertEqual(pago.payment_cfdi_syntage_id, 'rep-1')
        self.assertFalse(pago.payment_cfdi_id)
        self.assertAlmostEqual(pago.batch_monto, 1000.0, places=2)
        rep = self._upsert(syntage_rep(UUID_P1, 'rep-1'))
        self.assertEqual(pago.payment_cfdi_id, rep)
        self.assertEqual(pago.payment_uuid, UUID_P1)
        # Sin folio fiscal en el pago de Odoo: el pago conciliado con la factura
        self.assertTrue(pay)
        self.assertEqual(pago.account_payment_id, pay[:1])
        self.assertEqual(pago.payment_match, 'factura')
        self.assertTrue(pago.cuenta)
        self.assertEqual(self._row(cfdi).issue, 'ok')

    def test_copia_del_mismo_documento_no_cuenta(self):
        bill, cfdi, pay = self._factura_pagada(UUID_F, 'f')
        self._upsert(syntage_rep(UUID_P1, 'rep-1'))
        bueno = self._pago(pago_de_rep('r1', UUID_F, 1000.0, 'rep-1'))
        # Mismo pago, mismo documento, otro id de Syntage y sin su CFDI de pago
        copia = self._pago(pago_de_rep('r1-bis', UUID_F, -1000.0, 'rep-otro'))
        self.assertEqual(copia.duplicado_de_id, bueno)
        self.assertFalse(copia.cuenta)
        self.assertFalse(bueno.duplicado_de_id)
        row = self._row(cfdi)
        self.assertEqual(row.n_pagos, 1)
        self.assertEqual(row.issue, 'ok')
        # Dos REP distintos y vigentes por el mismo pago no son copia: son dos
        # CFDI en el SAT y sobran complementos.
        bill2, cfdi2, pay2 = self._factura_pagada(UUID_G, 'g', 500.0)
        self._upsert(syntage_rep(UUID_P1.replace('a1', 'b1'), 'rep-b1'))
        self._upsert(syntage_rep(UUID_P2, 'rep-b2'))
        a = self._pago(pago_de_rep('g1', UUID_G, 500.0, 'rep-b1'))
        b = self._pago(pago_de_rep('g2', UUID_G, 500.0, 'rep-b2'))
        self.assertFalse(a.duplicado_de_id or b.duplicado_de_id)
        self.assertEqual(self._row(cfdi2).issue, 'complemento_duplicado')

    def test_rep_con_pago_cancelado_y_consulta_al_sat(self):
        bill, cfdi, pay = self._factura_pagada(UUID_F, 'f')
        rep = self._upsert(syntage_rep(UUID_P1, 'rep-1'))
        pago = self._pago(pago_de_rep('r1', UUID_F, 1000.0, 'rep-1'))
        self.assertEqual(pago.account_payment_id, pay[:1])
        # Se cancela el pago en Odoo; el REP sigue vigente en el SAT
        pay.action_draft()
        pay.action_cancel()
        self.env['sat.cfdi.pago']._cron_link_payments()
        self.assertEqual(pago.account_payment_id, pay[:1])
        self.assertEqual(pago.account_payment_state, 'canceled')
        self.assertEqual(self._row(cfdi).issue, 'rep_pago_cancelado')
        # El REP de un pago cancelado va primero a la consulta del SAT
        self.assertIn(rep, self.env['sat.cfdi']._verify_candidates(1))
        Cfdi = type(self.env['sat.cfdi'])
        with patch.object(Cfdi, '_sat_query', lambda self, e, r, t, u: 'cancelado'):
            self.env['sat.cfdi']._cron_verify_sat_status(limit=5)
        self.assertEqual(rep.estado_sat, 'cancelado')
        self.assertFalse(pago.cuenta)
        row = self._row(cfdi)
        self.assertEqual(row.n_pagos, 0)
        self.assertNotEqual(row.issue, 'rep_pago_cancelado')

    def test_folio_fiscal_del_pago(self):
        if 'l10n_mx_edi_cfdi_uuid' not in self.env['account.payment']._fields:
            self.skipTest('l10n_mx_edi no está instalado: los pagos no tienen folio fiscal')
        bill, cfdi, pay = self._factura_pagada(UUID_F, 'f')
        pay[:1].write({'l10n_mx_edi_cfdi_uuid': UUID_P1.upper()})
        self._upsert(syntage_rep(UUID_P1, 'rep-1'))
        pago = self._pago(pago_de_rep('r1', UUID_F, 1000.0, 'rep-1'))
        self.assertEqual(pago.account_payment_id, pay[:1])
        self.assertEqual(pago.payment_match, 'uuid')

    def test_iva_sin_copias(self):
        ppd = self._upsert(syntage_invoice(UUID_F, id='f', total=1160.0, subtotal=1000.0, paymentType='PPD',
                                           transferredTaxes={'total': 160.0, 'valueAddedTax': 160.0},
                                           issuedAt='2026-04-12 12:00:00'))
        self.assertTrue(ppd)
        self._upsert(syntage_rep(UUID_P1, 'rep-1'))
        self._pago(pago_de_rep('i1', UUID_F, 1160.0, 'rep-1', date='2026-05-15 12:00:00'))
        self._pago(pago_de_rep('i1-bis', UUID_F, 1160.0, 'rep-otro', date='2026-05-15 12:00:00'))
        amounts = self.env['sat.iva.month']._sat_amounts(self.company, fields.Date.to_date('2026-05-01'),
                                                         fields.Date.to_date('2026-06-01'))
        iva, n = amounts[('received', fields.Date.to_date('2026-05-01'))]
        self.assertAlmostEqual(iva, 160.0, places=2)
        self.assertEqual(n, 1)

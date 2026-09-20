# -*- coding: utf-8 -*-
"""Política de CFDI por contacto, aceptación automática y alerta diaria."""
from datetime import timedelta

from odoo import fields
from odoo.tests import tagged

from .common import RFC_PROVEEDOR, SatCommon, syntage_invoice

UUID_A = 'cccccccc-0000-4000-8000-000000000001'
UUID_B = 'cccccccc-0000-4000-8000-000000000002'
UUID_C = 'cccccccc-0000-4000-8000-000000000003'


@tagged('post_install', '-at_install', 'quimibond_sat')
class TestSatPolicy(SatCommon):

    def _upsert(self, obj):
        return self.env['sat.cfdi']._upsert_from_syntage(obj, self.company, event_type='pull')

    def _rows(self, domain):
        self.env.flush_all()
        self.env.invalidate_all()
        return self.env['sat.compare.line'].search(domain)

    def test_poliza_policy_ignores_cfdi_and_bills(self):
        self.proveedor.sat_cfdi_policy = 'poliza'
        cfdi = self._upsert(syntage_invoice(UUID_A, total=1000.0, issuedAt='2026-04-10 12:00:00'))
        self.assertEqual(cfdi.match_status, 'ignorado')
        self.assertIn('póliza', cfdi.ignore_reason)
        row = self._rows([('uuid', '=', UUID_A)])
        self.assertEqual(row.bucket, 'ignorado')
        self.assertAlmostEqual(row.sat_vigente, 0.0, places=2)   # fuera de conciliación
        self.assertAlmostEqual(row.delta, 0.0, places=2)
        # Factura del banco sin XML: cubeta "por póliza", no "sin UUID"
        bill = self._invoice(self.proveedor, 500.0, day='2026-04-11')
        row = self._rows([('move_id', '=', bill.id)])
        self.assertEqual(row.bucket, 'poliza')
        self.assertEqual(row.issue, 'poliza')
        self.assertAlmostEqual(row.odoo_posted, 0.0, places=2)
        self.assertAlmostEqual(row.delta, 0.0, places=2)

    def test_foreign_and_sin_cfdi_bills_are_out_of_scope(self):
        foreign = self.env['res.partner'].create({
            'name': 'ICOMATEX SA', 'is_company': True, 'country_id': self.env.ref('base.es').id})
        bill = self._invoice(foreign, 12000.0, day='2026-03-31')
        row = self._rows([('move_id', '=', bill.id)])
        self.assertEqual(row.bucket, 'sin_cfdi')
        self.assertAlmostEqual(row.delta, 0.0, places=2)
        nomina = self.env['res.partner'].create({'name': 'NOMINA', 'sat_cfdi_policy': 'sin_cfdi'})
        bill = self._invoice(nomina, 30000.0, day='2026-03-15')
        row = self._rows([('move_id', '=', bill.id)])
        self.assertEqual(row.bucket, 'sin_cfdi')
        # Un proveedor mexicano normal sin XML sigue siendo "sin UUID en Odoo"
        bill = self._invoice(self.proveedor, 700.0, day='2026-03-16')
        row = self._rows([('move_id', '=', bill.id)])
        self.assertIn(row.bucket, ('solo_odoo_sin_uuid', 'solo_odoo'))

    def test_auto_accept_exact_match_only(self):
        exact = self._invoice(self.proveedor, 3287.86, day='2026-04-27')
        near = self._invoice(self.proveedor, 5000.00, day='2026-05-10')
        a = self._upsert(syntage_invoice(UUID_A, total=3287.86, issuedAt='2026-04-29 12:00:00'))
        b = self._upsert(syntage_invoice(UUID_B, id='b', total=5004.00, subtotal=4313.79,
                                         issuedAt='2026-05-11 12:00:00'))
        self.assertEqual(a.match_status, 'solo_sat')
        self.env['sat.cfdi']._cron_suggest_matches()
        a.invalidate_recordset()
        b.invalidate_recordset()
        # Total exacto, fecha a 2 días, sin rival: se liga solo
        self.assertEqual(a.move_id, exact)
        self.assertEqual(a.match_method, 'sugerido')
        self.assertIn('automáticamente', a.note)
        # $4 de diferencia (dentro del ±0.5% de la sugerencia): se sugiere, no se liga
        self.assertFalse(b.move_id)
        self.assertEqual(b.suggested_move_id, near)
        self.assertEqual(b.suggestion_reason, 'sin_uuid')

    def test_auto_accept_skips_when_two_cfdi_want_same_bill(self):
        bill = self._invoice(self.proveedor, 19592.40, day='2026-01-07')
        a = self._upsert(syntage_invoice(UUID_A, total=19592.40, issuedAt='2026-01-07 12:00:00'))
        b = self._upsert(syntage_invoice(UUID_B, id='b', total=19592.40, issuedAt='2026-01-07 12:05:00'))
        self.env['sat.cfdi']._cron_suggest_matches()
        (a + b).invalidate_recordset()
        self.assertFalse(a.move_id)
        self.assertFalse(b.move_id)
        self.assertEqual((a + b).mapped('suggested_move_id'), bill)

    def test_daily_alert_mail(self):
        ICP = self.env['ir.config_parameter'].sudo()
        ICP.set_param('quimibond_sat.alert_email', '')
        self.assertFalse(self.env['sat.cfdi']._cron_daily_alert())
        ICP.set_param('quimibond_sat.alert_email', 'ceo@example.com, conta@example.com')
        # Fechas relativas a hoy: el hallazgo debe caer en la ventana de
        # "nuevos en los últimos 7 días" cualquier día que corra la prueba.
        issued = fields.Date.today() - timedelta(days=3)
        bill = self._invoice(self.proveedor, 1000.0, day=issued.isoformat())
        cfdi = self._upsert(syntage_invoice(UUID_C, total=1000.0, status='CANCELADO',
                                            canceledAt='%s 00:00:00' % (issued + timedelta(days=1)).isoformat(),
                                            issuedAt='%s 12:00:00' % issued.isoformat()))
        cfdi.write({'move_id': bill.id})
        self.assertEqual(cfdi.issue, 'cancelado_sat')
        self.env.flush_all()
        mail = self.env['sat.cfdi']._cron_daily_alert()
        self.assertTrue(mail)
        self.assertIn('hallazgos abiertos', mail.subject)
        self.assertEqual(mail.email_to, 'ceo@example.com, conta@example.com')
        self.assertIn('PAPELERA SANDOVAL', mail.body_html)
        self.assertIn('Cancelado en el SAT', mail.body_html)
        self.assertEqual(RFC_PROVEEDOR, self.proveedor.vat)

# -*- coding: utf-8 -*-
"""Conciliación sin ruido (1.19.0): antes de los datos del SAT, cancelados sin
factura, ligado a borrador, redondeo, pagos a terceros y RFC por póliza."""
from odoo import Command, fields
from odoo.tests import tagged

from .common import SatCommon, syntage_invoice

UUID_A = 'dddddddd-0000-4000-8000-000000000001'
UUID_B = 'dddddddd-0000-4000-8000-000000000002'


@tagged('post_install', '-at_install', 'quimibond_sat')
class TestSatRuido(SatCommon):

    def _upsert(self, obj):
        return self.env['sat.cfdi']._upsert_from_syntage(obj, self.company, event_type='pull')

    def _row(self, domain):
        self.env.flush_all()
        self.env.invalidate_all()
        return self.env['sat.compare.line'].search(domain, limit=1)

    def test_antes_de_los_datos_del_sat(self):
        old = self._invoice(self.proveedor, 500.0, day='2024-11-15')
        new = self._invoice(self.proveedor, 600.0, day='2026-04-10')
        self.assertEqual(self._row([('move_id', '=', old.id)]).bucket, 'solo_odoo_sin_uuid')
        self.company.sat_data_since = '2025-01-01'
        row = self._row([('move_id', '=', old.id)])
        self.assertEqual(row.bucket, 'antes_sat')
        self.assertEqual(row.issue, 'antes_sat')
        self.assertAlmostEqual(row.delta, 0.0, places=2)   # fuera de conciliación
        self.assertEqual(self._row([('move_id', '=', new.id)]).bucket, 'solo_odoo_sin_uuid')

    def test_cancelado_sin_factura_no_es_pendiente(self):
        cfdi = self._upsert(syntage_invoice(UUID_A, status='CANCELADO', canceledAt='2026-04-30 10:00:00'))
        self.assertEqual(cfdi.estado_sat, 'cancelado')
        self.assertEqual(cfdi.issue, 'cancelado_sin_factura')
        row = self._row([('uuid', '=', UUID_A)])
        self.assertEqual(row.bucket, 'cancelado_sin_factura')
        self.assertEqual(row.issue, 'cancelado_sin_factura')
        self.assertAlmostEqual(row.delta, 0.0, places=2)

    def test_ligado_a_borrador(self):
        draft = self._invoice(self.proveedor, 3287.86, post=False)
        cfdi = self._upsert(syntage_invoice(UUID_A))
        cfdi.write({'move_id': draft.id})
        self.assertEqual(cfdi.issue, 'borrador')
        row = self._row([('uuid', '=', UUID_A)])
        self.assertEqual(row.issue, 'borrador')
        self.assertEqual(row.state_odoo, 'draft')
        self.assertAlmostEqual(row.total_odoo, 3287.86, places=2)   # antes salía 0 y "monto distinto"
        draft.action_post()
        cfdi.invalidate_recordset()
        self.assertEqual(cfdi.issue, 'ok')
        self.assertEqual(self._row([('uuid', '=', UUID_A)]).issue, 'ok')

    def test_redondeo_menos_de_un_peso(self):
        bill_a = self._invoice(self.proveedor, 215066.00, day='2026-04-10')
        bill_b = self._invoice(self.proveedor, 1000.00, day='2026-04-11')
        a = self._upsert(syntage_invoice(UUID_A, total=215065.46, issuedAt='2026-04-10 12:00:00'))
        b = self._upsert(syntage_invoice(UUID_B, id='b', total=1001.00, issuedAt='2026-04-11 12:00:00'))
        a.write({'move_id': bill_a.id})
        b.write({'move_id': bill_b.id})
        self.assertEqual(a.issue, 'redondeo')
        self.assertEqual(b.issue, 'monto')
        self.assertEqual(self._row([('uuid', '=', UUID_A)]).issue, 'redondeo')
        self.assertEqual(self._row([('uuid', '=', UUID_B)]).issue, 'monto')

    def test_pagos_a_terceros_explican_la_diferencia(self):
        """Agente aduanal: su CFDI trae los honorarios; la factura de Odoo
        además los impuestos de pedimento que pagó por cuenta de Quimibond."""
        account = self.company_data['default_account_expense']
        impuestos = self.env['product.product'].create({'name': 'IMPUESTOS ADUANALES'})
        bill = self.env['account.move'].create({
            'move_type': 'in_invoice', 'partner_id': self.proveedor.id,
            'currency_id': self.company.currency_id.id,
            'invoice_date': fields.Date.from_string('2026-04-29'),
            'invoice_line_ids': [
                Command.create({'name': 'Trámite', 'quantity': 1.0, 'price_unit': 1944.0,
                                'account_id': account.id, 'tax_ids': [Command.clear()]}),
                Command.create({'product_id': impuestos.id, 'name': 'Impuestos', 'quantity': 1.0,
                                'price_unit': 846.0, 'account_id': account.id, 'tax_ids': [Command.clear()]}),
            ],
        })
        bill.action_post()
        cfdi = self._upsert(syntage_invoice(UUID_A, total=1944.0))
        cfdi.write({'move_id': bill.id})
        self.assertEqual(cfdi.issue, 'monto')
        self.assertEqual(self._row([('uuid', '=', UUID_A)]).issue, 'monto')
        impuestos.product_tmpl_id.sat_pago_terceros = True
        self.env.flush_all()
        cfdi.invalidate_recordset()
        self.assertEqual(cfdi.issue, 'terceros')
        row = self._row([('uuid', '=', UUID_A)])
        self.assertEqual(row.issue, 'terceros')
        self.assertAlmostEqual(row.pago_terceros, 846.0, places=2)

    def test_rfc_por_poliza_sin_contacto(self):
        cfdi = self._upsert(syntage_invoice(
            UUID_A, total=5580.0, issuer={'rfc': 'SAT970701NN3', 'name': 'SERVICIO DE ADMINISTRACION TRIBUTARIA'}))
        self.assertFalse(cfdi.partner_id)
        self.assertEqual(cfdi.match_status, 'ignorado')
        self.assertEqual(self._row([('uuid', '=', UUID_A)]).bucket, 'ignorado')
        # El parámetro reemplaza la lista por default
        self.env['ir.config_parameter'].sudo().set_param('quimibond_sat.rfc_poliza', 'IMS421231I45')
        other = self._upsert(syntage_invoice(
            UUID_B, id='b', issuer={'rfc': 'SAT970701NN3', 'name': 'SERVICIO DE ADMINISTRACION TRIBUTARIA'}))
        self.assertEqual(other.match_status, 'solo_sat')

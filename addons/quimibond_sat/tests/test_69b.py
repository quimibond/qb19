# -*- coding: utf-8 -*-
"""Lista 69-B del SAT: el estado del proveedor sale de su CFDI más reciente,
las facturas de un definitivo no se publican sin documentar las operaciones,
aviso en la factura, columna en la comparación y sección en la alerta."""
from odoo.exceptions import UserError
from odoo.tests import tagged

from .common import RFC_PROVEEDOR, SatCommon, syntage_invoice

UUID_A = 'eeeeeeee-0000-4000-8000-000000000001'
UUID_B = 'eeeeeeee-0000-4000-8000-000000000002'
UUID_C = 'eeeeeeee-0000-4000-8000-000000000003'


def efos(uuid, status, **overrides):
    return syntage_invoice(uuid, issuer={'rfc': RFC_PROVEEDOR, 'name': 'PAPELERA SANDOVAL',
                                         'blacklistStatus': status}, **overrides)


@tagged('post_install', '-at_install', 'quimibond_sat')
class TestSat69b(SatCommon):

    def _upsert(self, obj):
        return self.env['sat.cfdi']._upsert_from_syntage(obj, self.company, event_type='pull')

    def test_status_follows_latest_cfdi(self):
        self._upsert(efos(UUID_A, 'definitive', issuedAt='2026-03-10 12:00:00'))
        self.assertEqual(self.proveedor.sat_69b_status, 'definitive')
        # Un CFDI más nuevo sin estado: ya no está en la lista
        self._upsert(efos(UUID_B, None, id='b', issuedAt='2026-04-10 12:00:00'))
        self.assertFalse(self.proveedor.sat_69b_status)
        # Uno viejo que llega después (descarga histórica) no regresa el estado
        self._upsert(efos(UUID_C, 'presumed', id='c', issuedAt='2026-02-10 12:00:00'))
        self.assertFalse(self.proveedor.sat_69b_status)

    def test_definitive_blocks_posting_until_documented(self):
        self._upsert(efos(UUID_A, 'definitive'))
        bill = self._invoice(self.proveedor, 100.0, post=False)
        self.assertIn('69-B', bill.sat_alerta)
        with self.assertRaisesRegex(UserError, '69-B'):
            bill.action_post()
        self.assertEqual(bill.state, 'draft')
        self.proveedor.sat_69b_autorizado = True
        bill.action_post()
        self.assertEqual(bill.state, 'posted')
        self.assertIn('DEFINITIVA', bill.sat_alerta)   # el aviso se mantiene
        # Las facturas de cliente no se tocan
        inv = self._invoice(self.cliente, 100.0, move_type='out_invoice')
        self.assertEqual(inv.state, 'posted')

    def test_presumed_warns_but_posts(self):
        self._upsert(efos(UUID_A, 'presumed'))
        bill = self._invoice(self.proveedor, 100.0)
        self.assertEqual(bill.state, 'posted')
        self.assertIn('PRESUNTO', bill.sat_alerta)

    def test_compare_column_and_alert(self):
        self.proveedor.sat_69b_autorizado = True   # ya publicada antes de la lista
        bill = self._invoice(self.proveedor, 3287.86)
        cfdi = self._upsert(efos(UUID_A, 'definitive'))
        cfdi.write({'move_id': bill.id})
        self.env.flush_all()
        self.env.invalidate_all()
        row = self.env['sat.compare.line'].search([('uuid', '=', UUID_A)])
        self.assertEqual(row.issue, 'ok')
        self.assertEqual(row.lista_69b, 'definitive')
        self.env['ir.config_parameter'].sudo().set_param('quimibond_sat.alert_email', 'conta@example.com')
        mail = self.env['sat.cfdi']._cron_daily_alert()
        self.assertTrue(mail)
        self.assertIn('69-B', mail.subject)
        self.assertIn('PAPELERA SANDOVAL', mail.body_html)

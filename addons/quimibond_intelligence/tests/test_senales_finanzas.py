# -*- coding: utf-8 -*-
from odoo.tests import tagged

from .common import SenalesCommon


@tagged('post_install', '-at_install', 'quimibond_intelligence')
class TestSenalesFinanzas(SenalesCommon):

    def _factura(self, partner, tipo, monto, vence_hace, publicar=True):
        inv = self.env['account.move'].create({
            'move_type': tipo, 'partner_id': partner.id, 'invoice_date': self.hace(vence_hace + 30),
            'invoice_date_due': self.hace(vence_hace),
            'invoice_line_ids': [(0, 0, {'name': 'x', 'quantity': 1, 'price_unit': monto})],
        })
        if publicar:
            inv.action_post()
        return inv

    def test_cartera_vencida_agrupa_por_cliente_y_manda_rfc(self):
        contacto = self.env['res.partner'].create({'name': 'Contacto', 'parent_id': self.cliente.id})
        a = self._factura(self.cliente, 'out_invoice', 1000, 40)
        b = self._factura(contacto, 'out_invoice', 500, 10)
        self._factura(self.cliente, 'out_invoice', 700, -5)          # no vencida
        self._factura(self.cliente, 'out_invoice', 900, 20, publicar=False)  # borrador
        filas = self.filas('cartera_vencida')
        f = self.fila_de(filas, 'cartera_vencida:partner:%d' % self.cliente.id)
        self.assertEqual({d['id'] for d in f['documentos']}, {a.id, b.id})
        self.assertAlmostEqual(f['valor'], 1500.0)
        self.assertEqual(f['payload']['rfc'], 'ABC010101XYZ')
        self.assertEqual(f['payload']['dias_max'], 40)
        self.assertEqual(f['vence'], str(self.hace(40)))
        self.assertIn('2 facturas', f['valor_texto'])
        self.assertEqual(len(filas), 1)

    def test_cxp_vencida_es_positiva_y_por_proveedor(self):
        self._factura(self.proveedor, 'in_invoice', 300, 3)
        f = self.fila_de(self.filas('cxp_vencida'), 'cxp_vencida:partner:%d' % self.proveedor.id)
        self.assertAlmostEqual(f['valor'], 300.0)

    def test_factura_proveedor_borrador_respeta_umbral(self):
        inv = self._factura(self.proveedor, 'in_invoice', 100, 0, publicar=False)
        self.env.cr.execute('UPDATE account_move SET create_date = %s WHERE id = %s', (self.hace(5), inv.id))
        inv.invalidate_recordset(['create_date'])
        self.assertTrue(self.fila_de(self.filas('factura_proveedor_borrador', {'umbrales': {'dias': 3}}), 'factura_proveedor_borrador:account.move:%d' % inv.id))
        self.assertEqual([f for f in self.filas('factura_proveedor_borrador', {'umbrales': {'dias': 10}}) if f['clave'].endswith(':%d' % inv.id)], [])

    def test_senales_de_modelos_no_instalados_devuelven_none(self):
        # En Community no hay hr_payroll, cash flow ni SGI: None = sin lote (situacion_salud lo reporta), nunca [] (que resolvería todo).
        for s, modelo in (('nomina_borrador', 'hr.payslip'), ('cash_bajo_piso', 'cash.flow.forecast.engine'), ('indicador_financiero_rojo', 'sgi.indicator.measure')):
            if modelo not in self.env:
                self.assertIsNone(self.filas(s), s)
        if 'sat.compare.line' in self.env:  # quimibond_sat sí está en el CI
            self.assertIsInstance(self.filas('sat_discrepancia'), list)

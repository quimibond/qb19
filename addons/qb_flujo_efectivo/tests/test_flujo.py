# -*- coding: utf-8 -*-
"""Pruebas del flujo de efectivo a 13 semanas (corren en Odoo.sh: depende de
``account_reports``, que es Enterprise)."""
from datetime import date

from odoo import Command, fields
from odoo.addons.account.tests.common import AccountTestInvoicingCommon
from odoo.tests import freeze_time, tagged

HOY = date(2026, 11, 24)  # martes


@tagged('post_install', '-at_install', 'qb_flujo_efectivo')
@freeze_time('2026-11-24')
class TestFlujo13Semanas(AccountTestInvoicingCommon):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.config = cls.env['qb.flujo.config'].create({
            'company_id': cls.env.company.id, 'cartera_desde': date(2026, 1, 1), 'dias_incobrable': 90,
            'incluir_pedidos': False,
        })
        cls.bank = cls.company_data['default_journal_bank']
        cls.env['qb.flujo.caja'].create({'config_id': cls.config.id, 'journal_id': cls.bank.id,
                                         'fuente': 'contable'})
        cls.cliente = cls.env['res.partner'].create({'name': 'Cliente flujo'})
        cls.proveedor = cls.env['res.partner'].create({'name': 'Proveedor flujo'})
        cls.revisar = cls.env['res.partner'].create({'name': 'Proveedor dudoso'})
        cls.config.partner_revisar_ids = [Command.link(cls.revisar.id)]

    def _factura(self, partner, importe, fecha, vence, move_type='out_invoice'):
        cuenta = (self.company_data['default_account_revenue'] if move_type.startswith('out_')
                  else self.company_data['default_account_expense'])
        move = self.env['account.move'].create({
            'move_type': move_type, 'partner_id': partner.id, 'invoice_date': fecha, 'date': fecha,
            'invoice_date_due': vence,
            'invoice_line_ids': [Command.create({'name': 'x', 'quantity': 1, 'price_unit': importe,
                                                 'tax_ids': [Command.clear()], 'account_id': cuenta.id})],
        })
        move.action_post()
        return move

    def _calcular(self):
        return self.env['qb.flujo.motor'].calcular(self.config, HOY)

    def test_cobranza_por_vencimiento_y_reglas_de_incobrable(self):
        self._factura(self.cliente, 1000, date(2026, 11, 1), date(2026, 12, 10))      # semana 3
        self._factura(self.cliente, 200, date(2026, 10, 1), date(2026, 11, 1))        # vencida 23 días
        self._factura(self.cliente, 300, date(2026, 6, 1), date(2026, 7, 1))          # > 90 días
        self._factura(self.cliente, 400, date(2025, 12, 1), date(2026, 12, 1))        # anterior al corte
        res = self._calcular()
        self.assertAlmostEqual(res['filas']['cobro']['semanas'][2], 1000)
        self.assertAlmostEqual(res['filas']['cobro_vencido']['semanas'][0], 200)
        self.assertAlmostEqual(res['info']['incobrable']['total'], 700)

    def test_pagos_vencidos_revisar_y_despues_de_13_semanas(self):
        self._factura(self.proveedor, 500, date(2026, 10, 1), date(2026, 10, 31), 'in_invoice')
        self._factura(self.proveedor, 800, date(2026, 11, 20), date(2026, 12, 1), 'in_invoice')
        self._factura(self.proveedor, 900, date(2026, 11, 20), date(2027, 6, 1), 'in_invoice')
        self._factura(self.revisar, 50, date(2026, 11, 1), date(2026, 11, 2), 'in_invoice')
        res = self._calcular()
        self.assertAlmostEqual(res['filas']['pago_vencido']['semanas'][0], -500)
        self.assertAlmostEqual(res['filas']['pago']['semanas'][1], -800)
        self.assertAlmostEqual(res['info']['pago_despues']['total'], -900)
        self.assertAlmostEqual(res['info']['revisar']['total'], -50)

    def test_compromiso_semanal_y_su_proveedor_no_se_duplica(self):
        self.env['qb.flujo.compromiso'].create({
            'config_id': self.config.id, 'name': 'Nómina', 'categoria': 'nomina', 'importe': 100,
            'periodicidad': 'semanal', 'dia_semana': '4', 'partner_id': self.proveedor.id,
        })
        self._factura(self.proveedor, 800, date(2026, 11, 20), date(2026, 12, 1), 'in_invoice')
        res = self._calcular()
        fila = [f for k, f in res['filas'].items() if k.startswith('comp_')][0]
        self.assertEqual(fila['semanas'], [-100.0] * 13)
        self.assertFalse(res['filas']['pago']['detalle'])

    def test_estimar_con_lo_pagado(self):
        comp = self.env['qb.flujo.compromiso'].create({
            'config_id': self.config.id, 'name': 'IMSS', 'categoria': 'imss', 'importe': 1,
            'periodicidad': 'mensual', 'dia_mes': 17, 'partner_id': self.proveedor.id, 'texto_memo': 'IMSS',
            'periodos_historial': 3,
        })
        for fecha, importe, memo in [(date(2026, 10, 17), 300, 'IMSS SEP'), (date(2026, 11, 17), 500, 'IMSS OCT'),
                                     (date(2026, 11, 18), 999, 'OTRA COSA')]:
            pago = self.env['account.payment'].create({
                'payment_type': 'outbound', 'partner_type': 'supplier', 'partner_id': self.proveedor.id,
                'amount': importe, 'date': fecha, 'journal_id': self.bank.id, 'memo': memo})
            pago.action_post()
        comp.action_estimar()
        self.assertAlmostEqual(comp.importe_estimado, 400)
        self.assertEqual(comp.importe, 400)

    def test_reporte_trae_13_semanas_y_total(self):
        self._factura(self.cliente, 1000, date(2026, 11, 1), date(2026, 12, 10))
        report = self.env.ref('qb_flujo_efectivo.qb_flujo_report')
        options = report.get_options({'date': {'date_to': fields.Date.to_string(HOY), 'filter': 'custom',
                                               'mode': 'single'}})
        self.assertEqual(len(options['columns']), 14)
        lines = report._get_lines(options)
        nombres = [line['name'] for line in lines]
        self.assertIn('Saldo inicial de caja', nombres)
        self.assertIn('Saldo final de caja', nombres)
        final = [line for line in lines if line['name'] == 'Saldo final de caja'][0]
        self.assertEqual(len(final['columns']), 14)

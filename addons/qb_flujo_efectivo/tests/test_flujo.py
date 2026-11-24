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
            'incluir_pedidos': False, 'fuente_ventas': 'ninguna', 'semanas_reparto_clientes': 1,
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

    # 1.1.0 ---------------------------------------------------------------
    def test_fecha_esperada_gana_al_vencimiento_y_al_incobrable(self):
        normal = self._factura(self.cliente, 1000, date(2026, 11, 1), date(2026, 11, 26))
        normal.qb_fecha_flujo = date(2026, 12, 16)                                     # semana 4
        vieja = self._factura(self.cliente, 300, date(2026, 6, 1), date(2026, 7, 1))   # > 90 días
        vieja.qb_fecha_flujo = date(2026, 12, 2)                                       # semana 2
        res = self._calcular()
        self.assertAlmostEqual(res['filas']['cobro']['semanas'][3], 1000)
        self.assertAlmostEqual(res['filas']['cobro_vencido']['semanas'][1], 300)
        self.assertFalse(res['info'].get('incobrable', {}).get('total'))

    def test_vencido_se_reparte_en_semanas(self):
        self.config.semanas_reparto_clientes = 4
        self._factura(self.cliente, 400, date(2026, 10, 1), date(2026, 11, 1))
        res = self._calcular()
        self.assertEqual([round(v, 2) for v in res['filas']['cobro_vencido']['semanas'][:5]],
                         [100.0, 100.0, 100.0, 100.0, 0.0])

    def test_compromiso_con_facturas_resta_lo_facturado_del_mes(self):
        self.env['qb.flujo.compromiso'].create({
            'config_id': self.config.id, 'name': 'Renta', 'categoria': 'financiero', 'importe': 1000,
            'periodicidad': 'mensual', 'dia_mes': 15, 'partner_id': self.proveedor.id,
            'excluir_facturas': False,
        })
        self._factura(self.proveedor, 600, date(2026, 12, 1), date(2026, 12, 5), 'in_invoice')
        res = self._calcular()
        self.assertAlmostEqual(res['filas']['pago']['semanas'][1], -600)
        fila = [f for k, f in res['filas'].items() if k.startswith('comp_')][0]
        self.assertAlmostEqual(fila['semanas'][3], -400)   # 15-dic: 1000 - 600 ya facturado
        self.assertAlmostEqual(fila['semanas'][7], -1000)  # 15-ene

    def test_compromiso_anual(self):
        self.env['qb.flujo.compromiso'].create({
            'config_id': self.config.id, 'name': 'Aguinaldo', 'categoria': 'nomina', 'importe': 5000,
            'periodicidad': 'anual', 'mes': 12, 'dia_mes': 18,
        })
        res = self._calcular()
        fila = [f for k, f in res['filas'].items() if k.startswith('comp_')][0]
        self.assertAlmostEqual(sum(fila['semanas']), -5000)
        self.assertAlmostEqual(fila['semanas'][3], -5000)

    def test_ventas_estimadas_con_el_promedio(self):
        self.config.fuente_ventas = 'historial'
        self.config.meses_ventas = 3
        self.config.proporcion_compras = 50
        for mes in (8, 9, 10):
            self._factura(self.cliente, 3000, date(2026, mes, 5), date(2026, mes, 5))
        res = self._calcular()
        ventas = sum(res['filas']['venta_estimada']['semanas'])
        compras = sum(res['filas']['compra_estimada']['semanas'])
        # noviembre: 3000 en los 7 días que quedan; diciembre y enero completos; febrero hasta el 21
        self.assertAlmostEqual(res['filas']['venta_estimada']['semanas'][0], 3000 * 6 / 7, delta=1)
        self.assertGreater(ventas, 9000)
        # las compras se pagan a 30 días (sin historial): parte cae después de la semana 13
        self.assertLess(compras, 0)
        self.assertLessEqual(-compras, ventas / 2 + 1)

    def test_foto_y_real(self):
        foto = self.env['qb.flujo.foto'].tomar(self.config, HOY)
        self.assertEqual(len(foto.linea_ids), 13)
        linea = foto.linea_ids.filtered(lambda line: line.semana == 1)
        linea.llenar_real()
        self.assertTrue(linea.real_listo)
        self.assertAlmostEqual(linea.desviacion_neto, linea.neto_real - linea.neto_proy)

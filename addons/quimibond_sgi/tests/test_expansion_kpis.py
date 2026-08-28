# -*- coding: utf-8 -*-
"""KPIs del Plan de Expansión Comercial (EX-*).

Datos propios (no demo). Periodo 2041 para aislar de la facturación de demo
y del periodo 2040 que usan otras suites.
"""
from datetime import date

from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestExpansionKpis(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Indicator = cls.env['sgi.indicator']
        cls.company = cls.env.company
        cls.uom = cls.env.ref('uom.product_uom_unit')
        cls.product = cls.env['product.product'].create({
            'name': 'No tejido EX', 'type': 'consu', 'uom_id': cls.uom.id})
        cls.income = cls.env['account.account'].search(
            [('account_type', '=', 'income')], limit=1)
        cls.expense = cls.env['account.account'].search(
            [('account_type', '=', 'expense')], limit=1)
        cls.customer_a = cls.env['res.partner'].create({'name': 'Cliente EX-A'})
        cls.customer_b = cls.env['res.partner'].create({'name': 'Cliente EX-B'})
        cls.supplier = cls.env['res.partner'].create({'name': 'Proveedor EX'})
        cls.period = date(2041, 6, 1)
        cls.period_end = date(2041, 6, 30)

    def _indicator(self, mode):
        return self.Indicator.create({
            'code': 'TEX-%s' % mode[:6].upper(),
            'name': 'KPI %s' % mode, 'calc_mode': mode})

    def _invoice(self, move_type, partner, amount, when, account=None):
        move = self.env['account.move'].create({
            'move_type': move_type,
            'partner_id': partner.id, 'invoice_date': when,
            'invoice_line_ids': [(0, 0, {
                'product_id': self.product.id, 'quantity': 1.0,
                'product_uom_id': self.uom.id, 'price_unit': amount,
                'account_id': (account or self.income).id,
                'tax_ids': [(6, 0, [])]})]})
        move.action_post()
        return move

    def test_01_compras_vs_ventas(self):
        self._invoice('out_invoice', self.customer_a, 1000.0, self.period)
        self._invoice('out_refund', self.customer_a, 100.0, self.period)
        self._invoice('in_invoice', self.supplier, 720.0, self.period,
                      account=self.expense)
        ind = self._indicator('compras_vs_ventas')
        value = ind._calc_compras_vs_ventas(self.period, self.period_end)
        # 720 de compras sobre 900 netos de venta = 80%.
        self.assertEqual(value, 80.0)

    def test_02_notas_credito(self):
        self._invoice('out_invoice', self.customer_a, 1000.0, self.period)
        self._invoice('out_refund', self.customer_a, 15.0, self.period)
        ind = self._indicator('notas_credito')
        value = ind._calc_notas_credito(self.period, self.period_end)
        self.assertEqual(value, 1.5, "15 de NC sobre 1000 brutos = 1.5%.")

    def test_03_clientes_nuevos(self):
        # A facturó antes del periodo (cliente viejo); B estrena en el periodo.
        self._invoice('out_invoice', self.customer_a, 500.0, date(2041, 1, 10))
        self._invoice('out_invoice', self.customer_a, 500.0, self.period)
        self._invoice('out_invoice', self.customer_b, 300.0, self.period)
        ind = self._indicator('clientes_nuevos')
        value = ind._calc_clientes_nuevos(self.period, self.period_end)
        self.assertEqual(value, 1.0, "Solo B es cliente nuevo del periodo.")

    def test_04_concentracion_top3(self):
        # Ventana rodante de 12 meses: 4 clientes, el top 3 concentra 900/1000.
        partners = [self.customer_a, self.customer_b,
                    self.env['res.partner'].create({'name': 'Cliente EX-C'}),
                    self.env['res.partner'].create({'name': 'Cliente EX-D'})]
        for partner, amount in zip(partners, (400.0, 300.0, 200.0, 100.0)):
            self._invoice('out_invoice', partner, amount, date(2041, 2, 10))
        ind = self._indicator('concentracion_top3')
        value = ind._calc_concentracion_top3(self.period, self.period_end)
        self.assertEqual(value, 90.0)

    def test_05_sin_datos_queda_pendiente(self):
        # Sin facturación del periodo, los modos devuelven None (medición
        # pendiente), nunca un 0 que pinte el semáforo sin haber medido.
        for mode in ('compras_vs_ventas', 'notas_credito', 'clientes_nuevos',
                     'concentracion_top3', 'facturacion_usd', 'dso_cartera',
                     'margen_ventas'):
            ind = self.Indicator.create({
                'code': 'TEX0-%s' % mode[:5].upper(),
                'name': 'KPI vacío %s' % mode, 'calc_mode': mode})
            self.assertIsNone(
                ind._sgi_compute_value(date(2043, 1, 1), date(2043, 1, 31)),
                "Modo %s debe quedar pendiente sin datos." % mode)

    def test_06_dso_cartera(self):
        # 300 pendientes de cobro con 900 netos facturados en 90 días → 30 días.
        self._invoice('out_invoice', self.customer_a, 600.0, self.period)
        self._invoice('out_invoice', self.customer_b, 300.0, self.period)
        moves = self.env['account.move'].search([
            ('move_type', '=', 'out_invoice'),
            ('partner_id', '=', self.customer_a.id),
            ('invoice_date', '=', self.period)])
        self.env['account.payment.register'].with_context(
            active_model='account.move', active_ids=moves.ids).create(
            {'payment_date': self.period}).action_create_payments()
        ind = self._indicator('dso_cartera')
        value = ind._calc_dso_cartera(self.period, self.period_end)
        self.assertEqual(value, 30.0)

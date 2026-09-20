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

    def _invoice(self, move_type, partner, amount, when, account=None,
                 due=None, product=None):
        vals = {
            'move_type': move_type,
            'partner_id': partner.id, 'invoice_date': when,
            'invoice_line_ids': [(0, 0, {
                'product_id': (product or self.product).id, 'quantity': 1.0,
                'product_uom_id': self.uom.id, 'price_unit': amount,
                'account_id': (account or self.income).id,
                'tax_ids': [(6, 0, [])]})]}
        if due:
            vals['invoice_date_due'] = due
        move = self.env['account.move'].create(vals)
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
        # Solo los modos acotados al periodo/ventana: los de saldo abierto
        # (cartera, DPO) sí ven datos preexistentes de la base y se prueban
        # aparte contra su propia línea base.
        for mode in ('compras_vs_ventas', 'notas_credito', 'clientes_nuevos',
                     'concentracion_top3', 'facturacion_usd', 'dso_cartera',
                     'margen_ventas', 'retencion_clientes',
                     'clientes_reactivados', 'ventas_fuera_top10',
                     'concentracion_productos', 'pedidos_cancelados',
                     'entregas_completas'):
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

    def test_07_cartera_vencida(self):
        # La cartera abierta NO es acotada al periodo: el esperado se calcula
        # sumando la línea base preexistente de la BD (copia de producción).
        ind = self._indicator('cartera_vencida')
        base = ind._sgi_open_moves(
            ('out_invoice', 'out_refund'), self.period_end)
        total0 = sum(base.mapped('amount_residual_signed'))
        overdue0 = sum(base.filtered(
            lambda m: (m.invoice_date_due or m.invoice_date) < self.period_end
        ).mapped('amount_residual_signed'))
        # 600 vencida (venció el 5-jun) y 400 al corriente (vence en agosto).
        self._invoice('out_invoice', self.customer_a, 600.0, self.period,
                      due=date(2041, 6, 5))
        self._invoice('out_invoice', self.customer_b, 400.0, self.period,
                      due=date(2041, 8, 1))
        expected = round((overdue0 + 600.0) / (total0 + 1000.0) * 100.0, 2)
        value = ind._calc_cartera_vencida(self.period, self.period_end)
        self.assertEqual(value, expected)

    def test_08_retencion_clientes(self):
        # Ventana previa (jul-2039..jun-2040): facturan A y B.
        # Ventana actual (jul-2040..jun-2041): solo repite A → 50%.
        self._invoice('out_invoice', self.customer_a, 100.0, date(2040, 5, 10))
        self._invoice('out_invoice', self.customer_b, 100.0, date(2040, 5, 11))
        self._invoice('out_invoice', self.customer_a, 100.0, self.period)
        ind = self._indicator('retencion_clientes')
        value = ind._calc_retencion_clientes(self.period, self.period_end)
        self.assertEqual(value, 50.0)

    def test_09_clientes_reactivados(self):
        # A: última factura hace ~8.5 meses → reactivado.
        # B: facturó el mes pasado → activo normal, no cuenta.
        # C: sin historia → cliente nuevo, no reactivado.
        customer_c = self.env['res.partner'].create({'name': 'Cliente EX-R'})
        self._invoice('out_invoice', self.customer_a, 100.0, date(2040, 9, 15))
        self._invoice('out_invoice', self.customer_b, 100.0, date(2041, 5, 20))
        for partner in (self.customer_a, self.customer_b, customer_c):
            self._invoice('out_invoice', partner, 100.0, self.period)
        ind = self._indicator('clientes_reactivados')
        value = ind._calc_clientes_reactivados(self.period, self.period_end)
        self.assertEqual(value, 1.0)

    def test_10_ventas_fuera_top10(self):
        # 10 clientes de 100 y uno de 50: fuera del top 10 = 50/1050 = 4.76%.
        for i in range(10):
            partner = self.env['res.partner'].create(
                {'name': 'Cliente EX-T%s' % i})
            self._invoice('out_invoice', partner, 100.0, self.period)
        self._invoice('out_invoice', self.customer_a, 50.0, self.period)
        ind = self._indicator('ventas_fuera_top10')
        value = ind._calc_ventas_fuera_top10(self.period, self.period_end)
        self.assertEqual(value, 4.76)

    def test_11_concentracion_productos(self):
        # 6 productos: 5 de 100 y uno de 100 → top 5 = 500/600 = 83.33%.
        for i in range(6):
            product = self.env['product.product'].create({
                'name': 'No tejido EX-P%s' % i, 'type': 'consu',
                'uom_id': self.uom.id})
            self._invoice('out_invoice', self.customer_a, 100.0, self.period,
                          product=product)
        ind = self._indicator('concentracion_productos')
        value = ind._calc_concentracion_productos(self.period, self.period_end)
        self.assertEqual(value, 83.33)

    def test_12_pedidos_cancelados(self):
        # 3 confirmados + 1 cancelado del periodo = 25%.
        for state in ('sale', 'sale', 'sale', 'cancel'):
            order = self.env['sale.order'].create({
                'partner_id': self.customer_a.id,
                'date_order': self.period})
            order.write({'state': state})
        ind = self._indicator('pedidos_cancelados')
        value = ind._calc_pedidos_cancelados(self.period, self.period_end)
        self.assertEqual(value, 25.0)

    def test_13_dpo_pagos(self):
        # El saldo a proveedores tampoco es acotado al periodo: esperado con
        # línea base. Compras de 90 días = solo la factura creada (2041).
        ind = self._indicator('dpo_pagos')
        payable0 = -sum(ind._sgi_open_moves(
            ('in_invoice', 'in_refund'), self.period_end
        ).mapped('amount_residual_signed'))
        self._invoice('in_invoice', self.supplier, 900.0, self.period,
                      account=self.expense)
        expected = round((payable0 + 900.0) / 900.0 * 90.0, 1)
        value = ind._calc_dpo_pagos(self.period, self.period_end)
        self.assertEqual(value, expected)

    def test_14_filtro_de_compania(self):
        # Los KPIs financieros miden UNA compañía (la principal, o la del
        # parámetro): sin el filtro, el motor sumaba todo el grupo.
        ind = self._indicator('notas_credito')
        self.assertEqual(ind._sgi_kpi_company(),
                         self.env.ref('base.main_company'))
        self._invoice('out_invoice', self.customer_a, 1000.0, self.period)
        self._invoice('out_refund', self.customer_a, 15.0, self.period)
        self.assertEqual(
            ind._calc_notas_credito(self.period, self.period_end), 1.5)
        # Apuntando el parámetro a otra compañía (sin facturas), el mismo
        # periodo queda sin datos: prueba que el filtro sí corta.
        other = self.env['res.company'].create({'name': 'Otra Cía KPI'})
        self.env['ir.config_parameter'].sudo().set_param(
            'quimibond_sgi.kpi_company_id', other.id)
        self.assertEqual(ind._sgi_kpi_company(), other)
        self.assertIsNone(
            ind._calc_notas_credito(self.period, self.period_end))

    def test_15_recompute_pending_measures(self):
        # Una medición que nació pendiente se llena sola cuando el modo ya
        # calcula (deuda B.16); si sigue sin datos, no se toca.
        self._invoice('out_invoice', self.customer_a, 1000.0, self.period)
        self._invoice('out_refund', self.customer_a, 15.0, self.period)
        ind = self._indicator('notas_credito')
        measure = self.env['sgi.indicator.measure'].create({
            'indicator_id': ind.id, 'period_date': self.period,
            'value': 0.0, 'state': 'pendiente'})
        empty = self.env['sgi.indicator.measure'].create({
            'indicator_id': ind.id, 'period_date': date(2043, 5, 1),
            'value': 0.0, 'state': 'pendiente'})
        self.env['sgi.config'].recompute_pending_measures()
        self.assertEqual(measure.state, 'capturado')
        self.assertEqual(measure.value, 1.5)
        self.assertEqual(empty.state, 'pendiente')

    def test_16_fix_kpi_seeds_idempotente(self):
        energia = self.env.ref('quimibond_sgi.sgi_ind_consumo_energia')
        embarques = self.env.ref('quimibond_sgi.sgi_ind_embarques_sin_error')
        energia.uom = 'kWh'
        embarques.write({'target_objective': 100, 'target_acceptable': 98})
        broken = self.env['sgi.indicator.measure'].create({
            'indicator_id': energia.id, 'period_date': date(2042, 3, 1),
            'value': 0.0, 'state': 'capturado',
            'note': "Configure el proveedor de energía en Ajustes para medir "
                    "este indicador automáticamente."})
        self.env['sgi.config'].fix_kpi_seeds()
        self.assertEqual(energia.uom, 'MXN')
        self.assertEqual(embarques.target_objective, 99)
        self.assertEqual(broken.state, 'pendiente')
        # Segunda corrida: no vuelve a tocar nada (una meta ajustada por MAST
        # a otro valor se respeta).
        embarques.target_objective = 97
        self.env['sgi.config'].fix_kpi_seeds()
        self.assertEqual(embarques.target_objective, 97)

# -*- coding: utf-8 -*-
"""19.0.3.11.0 — venta real, comparativo y faltantes (diseño v3, 2.7 y 2.8). Facturas propias en
2069 para aislar; la reconstrucción se pide con ese rango."""
from datetime import date

from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestSalesActual(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        Param = cls.env['ir.config_parameter'].sudo()
        Param.set_param('quimibond_sgi.budget_planning_rate', '0')
        Param.set_param('quimibond_sgi.price_min_plausible', '0')
        cls.income = cls.env['account.account'].search(
            [('account_type', '=', 'income'), ('company_ids', 'in', cls.env.company.id)], limit=1)
        Param.set_param('quimibond_sgi.sales_account_prefixes', cls.income.code)
        cls.team_ind = cls.env['crm.team'].create({'name': 'Industrial real'})
        cls.team_conf = cls.env['crm.team'].create({'name': 'Confección real'})
        Param.set_param('quimibond_ventas_presupuesto.team_industrial', cls.team_ind.id)
        Param.set_param('quimibond_ventas_presupuesto.team_confeccion', cls.team_conf.id)
        Param.set_param('quimibond_ventas_presupuesto.team_especiales', cls.team_ind.id)
        cls.uom_m = cls.env.ref('uom.product_uom_meter')
        cls.client = cls.env['res.partner'].create({'name': 'Cliente real', 'is_company': True})
        cls.product = cls.env['product.product'].create({
            'name': 'Tela real', 'default_code': 'WREAL160', 'type': 'consu', 'uom_id': cls.uom_m.id})
        cls.product.write({'qb_m_por_kg': 4.0, 'qb_conv_origen': 'codigo'})
        cls.saldo = cls.env['product.product'].create({
            'name': 'SALDO real', 'default_code': 'SALDO-R', 'type': 'consu', 'uom_id': cls.uom_m.id})
        cls.Actual = cls.env['qb.sales.actual']

    def _invoice(self, when, lines, team=None, move_type='out_invoice'):
        move = self.env['account.move'].create({
            'move_type': move_type, 'partner_id': self.client.id, 'invoice_date': when,
            'team_id': (team or self.team_ind).id if team is not False else False,
            'invoice_line_ids': [(0, 0, {
                'product_id': product.id, 'quantity': qty, 'price_unit': price,
                'account_id': self.income.id, 'tax_ids': [(6, 0, [])]}) for product, qty, price in lines]})
        move.action_post()
        return move

    def _rebuild(self):
        self.Actual.action_qb_rebuild(date(2069, 1, 1), date(2069, 12, 31))
        return self.Actual.search([('partner_id', '=', self.client.id)])

    def test_01_venta_real_con_mercado_metros_y_marcas(self):
        self._invoice(date(2069, 3, 10), [(self.product, 100.0, 2.0), (self.saldo, 10.0, 1.0)])
        self._invoice(date(2069, 3, 20), [(self.product, 10.0, 2.0)], move_type='out_refund')
        rows = self._rebuild()
        tela = rows.filtered(lambda r: r.product_id == self.product)
        self.assertEqual(len(tela), 2)
        self.assertEqual(set(tela.mapped('mercado')), {'industrial'})
        self.assertAlmostEqual(sum(tela.mapped('amount')), 180.0, places=2, msg="La nota de crédito resta")
        self.assertAlmostEqual(sum(tela.mapped('qty_venta')), 90.0, places=2)
        self.assertAlmostEqual(sum(tela.mapped('qty_kg')), 22.5, places=2, msg="90 m ÷ 4 m/kg")
        self.assertTrue(all(tela.mapped('conv_ok')))
        self.assertEqual(tela[0].month, date(2069, 3, 1))
        saldo = rows.filtered(lambda r: r.product_id == self.saldo)
        self.assertTrue(saldo.es_saldo)
        # Reconstruir no duplica.
        self.assertEqual(len(self._rebuild()), 3)

    def test_02_proyecto_vigente_va_a_proyectos(self):
        self.env['qb.sales.project'].create({'partner_id': self.client.id, 'product_id': self.product.id,
                                             'date_from': date(2069, 1, 1)})
        self._invoice(date(2069, 5, 10), [(self.product, 50.0, 2.0)])
        row = self._rebuild()
        self.assertEqual(row.mercado, 'proyectos_industrial')
        self.assertTrue(row.es_proyecto)

    def test_03_comparativo_presupuesto_contra_real_sin_saldos(self):
        budget = self.env['sgi.sales.budget'].create({'year': 2069})
        self.env['sgi.sales.budget.line'].create({
            'budget_id': budget.id, 'product_id': self.product.id, 'partner_id': self.client.id,
            'mercado': 'industrial', 'date': date(2069, 3, 1), 'uom_id': self.uom_m.id,
            'qty_budget': 120.0, 'price_input': 2.0, 'price_currency_id': self.env.company.currency_id.id})
        budget.state = 'aprobado'
        self._invoice(date(2069, 3, 10), [(self.product, 100.0, 2.0), (self.saldo, 10.0, 1.0)])
        self._rebuild()
        Compare = self.env['qb.sales.compare']
        Compare.invalidate_model()
        rows = Compare.search([('partner_id', '=', self.client.id), ('year', '=', 2069),
                               '|', ('fuente', '!=', 'presupuesto'), ('budget_state', '=', 'aprobado')])
        self.assertAlmostEqual(sum(rows.mapped('amount_budget')), 240.0, places=2)
        self.assertAlmostEqual(sum(rows.mapped('amount_real')), 200.0, places=2, msg="Sin el saldo")
        self.assertAlmostEqual(sum(rows.mapped('amount_diff')), -40.0, places=2)
        self.assertAlmostEqual(sum(rows.mapped('qty_m_budget')), 120.0, places=2)
        self.assertEqual(set(rows.mapped('articulo_id')), {self.product.qb_articulo_id or self.product})

    def test_04_faltantes(self):
        self._invoice(date(2069, 4, 10), [(self.product, 5.0, 2.0)], team=False)
        rows = self._rebuild()
        self.assertEqual(rows.mercado, 'otro', "Sin equipo de mercado: Sin mercado")
        # Los faltantes miran el año en curso; se prueba la regla con la fila de 2069.
        rows.date = date.today()
        Missing = self.env['qb.sales.missing']
        Missing.action_qb_rebuild()
        mine = Missing.search([('kind', '=', 'venta_sin_mercado'), ('partner_id', '=', self.client.id)])
        self.assertEqual(len(mine), 1)
        self.assertAlmostEqual(mine.amount, 10.0, places=2)

    def test_05_ve02_sin_saldos(self):
        """3.12.0: el VE-02 y el presupuesto miden la venta sin saldos ni desperdicio; los demás
        indicadores de venta siguen igual."""
        self.env['ir.config_parameter'].sudo().set_param('quimibond_sgi.kpi_company_id', self.env.company.id)
        self._invoice(date(2069, 6, 10), [(self.product, 100.0, 2.0), (self.saldo, 10.0, 3.0)])
        Indicator = self.env['sgi.indicator']
        start, end = date(2069, 6, 1), date(2069, 6, 30)
        self.assertAlmostEqual(Indicator._sgi_net_invoiced(start, end), 230.0, places=2,
                               msg="Sin el contexto, como siempre (VE-01 y los demás)")
        self.assertAlmostEqual(Indicator.with_context(qb_sin_saldos=True)._sgi_net_invoiced(start, end),
                               200.0, places=2)
        self.assertAlmostEqual(Indicator._qb_saldo_amount(start, end), 30.0, places=2)
        budget = self.env['sgi.sales.budget'].create({'year': 2070})
        self.assertAlmostEqual(budget._qb_company_net_invoiced(start, end), 200.0, places=2,
                               msg="La base del año anterior del presupuesto, sin saldos")
        # La venta real cuadra con el VE-02 por construcción.
        self.Actual.action_qb_rebuild(start, end)
        rows = self.Actual.search([('partner_id', '=', self.client.id), ('es_saldo', '=', False)])
        self.assertAlmostEqual(sum(rows.mapped('amount')), 200.0, places=2)

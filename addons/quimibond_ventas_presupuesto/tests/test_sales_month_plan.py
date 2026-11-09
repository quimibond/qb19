# -*- coding: utf-8 -*-
"""19.0.3.14.0 — presupuesto del mes (diseño v3, 2.5): foto del pronóstico con el precio del
presupuesto anual o de la lista, ajuste, envío congelado y Confección de los vendedores. Mes
2071-05 para aislar."""
from datetime import date

from odoo.exceptions import AccessError, UserError
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestSalesMonthPlan(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        Param = cls.env['ir.config_parameter'].sudo()
        Param.set_param('quimibond_sgi.budget_planning_rate', '0')
        Param.set_param('quimibond_sgi.price_min_plausible', '0')
        cls.team_ind = cls.env['crm.team'].create({'name': 'Industrial mes'})
        Param.set_param('quimibond_ventas_presupuesto.team_industrial', cls.team_ind.id)
        cls.uom_m = cls.env.ref('uom.product_uom_meter')
        cls.client = cls.env['res.partner'].create({'name': 'Cliente mes', 'is_company': True})
        cls.product = cls.env['product.product'].create({
            'name': 'Tela mes', 'default_code': 'WMES160', 'type': 'consu', 'uom_id': cls.uom_m.id})
        cls.product.write({'qb_m_por_kg': 5.0, 'qb_conv_origen': 'codigo'})
        cls.month = date(2071, 5, 1)
        Forecast = cls.env['qb.forecast.line']
        for week, qty in ((date(2071, 5, 4), 100.0), (date(2071, 5, 11), 150.0), (date(2071, 6, 1), 999.0)):
            Forecast.create({'partner_id': cls.client.id, 'product_id': cls.product.id, 'week': week,
                             'qty_origen': qty, 'mercado': 'industrial'})
        cls.budget = cls.env['sgi.sales.budget'].create({'year': 2071})
        cls.env['sgi.sales.budget.line'].create({
            'budget_id': cls.budget.id, 'product_id': cls.product.id, 'partner_id': cls.client.id,
            'mercado': 'industrial', 'date': cls.month, 'uom_id': cls.uom_m.id, 'qty_budget': 1.0,
            'price_input': 3.0, 'price_currency_id': cls.env.company.currency_id.id})
        cls.budget.state = 'aprobado'

    def _build(self):
        action = self.env['qb.sales.month.plan.build'].create({'month': date(2071, 5, 20), 'rate': 18.0}).action_build()
        return self.env['qb.sales.month.plan'].browse(action['res_id'])

    def test_01_foto_del_pronostico_con_precio_del_presupuesto(self):
        plan = self._build()
        self.assertEqual(plan.name, "Presupuesto de mayo 2071")
        self.assertEqual(plan.budget_id, self.budget)
        self.assertEqual(plan.rate, 18.0)
        line = plan.line_ids
        self.assertEqual(len(line), 1, "Las semanas del mes se suman; junio no entra")
        self.assertAlmostEqual(line.qty_forecast, 250.0, places=2)
        self.assertAlmostEqual(line.qty_kg, 50.0, places=2)
        self.assertEqual((line.price_input, line.price_origen), (3.0, 'presupuesto'))
        self.assertAlmostEqual(plan.amount_total, 750.0, places=2)
        self.assertAlmostEqual(plan.amount_linea, 750.0, places=2)
        # Ajuste antes de enviar: no toca el pronóstico.
        line.qty = 200.0
        self.assertAlmostEqual(plan.amount_total, 600.0, places=2)
        self.assertAlmostEqual(sum(self.env['qb.forecast.line'].search(
            [('partner_id', '=', self.client.id), ('month', '=', self.month)]).mapped('qty_venta')), 250.0)

    def test_02_proyecto_vigente_va_a_proyectos(self):
        self.env['qb.sales.project'].create({'partner_id': self.client.id, 'product_id': self.product.id,
                                             'date_from': date(2071, 1, 1)})
        plan = self._build()
        self.assertEqual(plan.line_ids.mercado, 'proyectos_industrial')
        self.assertAlmostEqual(plan.amount_proyectos, 750.0, places=2)
        self.assertAlmostEqual(plan.amount_linea, 0.0, places=2)

    def test_03_enviar_congela_y_volver_a_armar_conserva_lo_manual(self):
        plan = self._build()
        manual = self.env['qb.sales.month.plan.line'].create({
            'plan_id': plan.id, 'product_id': self.product.id, 'mercado': 'confeccion', 'qty': 10.0,
            'price_input': 2.0})
        plan.action_qb_build()
        self.assertIn(manual, plan.line_ids, "Lo capturado a mano se conserva al volver a armar")
        self.assertEqual(len(plan.line_ids), 2)
        action = plan.action_qb_send()
        self.assertEqual(plan.state, 'enviado')
        self.assertEqual(action['res_model'], 'mail.compose.message')
        salesman = self.env['res.users'].create({
            'name': 'Vendedor mes', 'login': 'vendedor_mes',
            'group_ids': [(6, 0, [self.env.ref('sales_team.group_sale_salesman').id,
                                  self.env.ref('quimibond_sgi.group_sgi_user').id])]})
        with self.assertRaises(UserError):
            manual.with_user(salesman).qty = 20.0

    def test_04_vendedor_solo_confeccion(self):
        plan = self._build()
        salesman = self.env['res.users'].create({
            'name': 'Vendedor conf', 'login': 'vendedor_conf',
            'group_ids': [(6, 0, [self.env.ref('sales_team.group_sale_salesman').id,
                                  self.env.ref('quimibond_sgi.group_sgi_user').id])]})
        Line = self.env['qb.sales.month.plan.line'].with_user(salesman)
        conf = Line.create({'plan_id': plan.id, 'product_id': self.product.id, 'mercado': 'confeccion',
                            'qty': 5.0})
        self.assertEqual(conf.mercado, 'confeccion')
        with self.assertRaises(AccessError):
            plan.line_ids.filtered(lambda l: l.mercado == 'industrial').with_user(salesman).qty = 1.0
        with self.assertRaises(UserError):
            plan.with_user(salesman).rate = 20.0

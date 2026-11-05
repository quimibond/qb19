# -*- coding: utf-8 -*-
"""19.0.3.5.0 — presupuesto anual (diseño v3, 2.6): un documento por año y revisión con
los cinco mercados, tipo de cambio por mes, precio capturable (sugerido de la lista) y
renglones de proyecto sin producto. Datos propios; años 2050–2059 para aislar."""
import base64
import io
from datetime import date

from odoo.exceptions import UserError, ValidationError
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestPresupuestoAnual(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Budget = cls.env['sgi.sales.budget']
        cls.Line = cls.env['sgi.sales.budget.line']
        cls.Param = cls.env['ir.config_parameter'].sudo()
        cls.Param.set_param('quimibond_sgi.budget_planning_rate', '0')
        cls.Param.set_param('quimibond_sgi.price_min_plausible', '0')
        cls.team_ind = cls.env['crm.team'].create({'name': 'Industrial anual'})
        cls.team_conf = cls.env['crm.team'].create({'name': 'Confección anual'})
        cls.Param.set_param('quimibond_ventas_presupuesto.team_industrial', cls.team_ind.id)
        cls.Param.set_param('quimibond_ventas_presupuesto.team_confeccion', cls.team_conf.id)
        cls.Param.set_param('quimibond_ventas_presupuesto.team_especiales', cls.team_ind.id)
        cls.uom_m = cls.env.ref('uom.product_uom_meter')
        cls.uom_kg = cls.env.ref('uom.product_uom_kgm')
        usd, mxn = cls.env.ref('base.USD'), cls.env.ref('base.MXN')
        (usd | mxn).active = True
        cls.own = cls.env.company.currency_id
        cls.foreign = mxn if cls.own == usd else usd
        cls.product = cls.env['product.product'].create({
            'name': 'Tela anual', 'default_code': 'WANUAL150', 'type': 'consu',
            'uom_id': cls.uom_m.id, 'list_price': 1.0})
        cls.product.write({'qb_m_por_kg': 5.0, 'qb_conv_origen': 'codigo'})
        cls.client = cls.env['res.partner'].create({
            'name': 'Cliente anual', 'is_company': True, 'team_id': cls.team_ind.id})
        cls.manager = cls.env['res.users'].create({
            'name': 'MAST anual', 'login': 'mast_anual',
            'group_ids': [(6, 0, [cls.env.ref('quimibond_sgi.group_sgi_manager').id])]})
        cls.sale_mgr = cls.env['res.users'].create({
            'name': 'Ventas anual', 'login': 'ventas_anual',
            'group_ids': [(6, 0, [cls.env.ref('sales_team.group_sale_manager').id])]})

    def _factor(self, rate):
        return rate if self.own == self.env.ref('base.MXN') else 1.0 / rate

    def _line(self, budget, **vals):
        base = {'budget_id': budget.id, 'product_id': self.product.id,
                'date': date(budget.year, 3, 1), 'uom_id': self.uom_m.id,
                'qty_budget': 1000.0, 'partner_id': self.client.id}
        base.update(vals)
        return self.Line.create(base)

    def test_01_cabecera_anual(self):
        budget = self.Budget.create({'year': 2050})
        self.assertEqual(budget.revision, 0)
        self.assertEqual(budget.name, "Presupuesto 2050 REV0")
        self.assertEqual(budget.objetivo_pct, 20.0)
        self.assertEqual(budget.minimo_pct, 15.0)
        with self.assertRaises(ValidationError):
            self.Budget.create({'year': 2050})  # dos revisiones abiertas del mismo año

    def test_02_mercado_del_cliente_y_equipo(self):
        budget = self.Budget.create({'year': 2051})
        line = self._line(budget)
        self.assertEqual(line.mercado, 'industrial', "El mercado sale del equipo del cliente.")
        self.assertEqual(line.team_id, self.team_ind)
        global_line = self._line(budget, partner_id=False, product_id=self.env['product.product'].create({
            'name': 'Entretela anual', 'type': 'consu', 'uom_id': self.uom_m.id}).id)
        self.assertEqual(global_line.mercado, 'confeccion', "Sin cliente: Confección.")
        self.assertEqual(global_line.team_id, self.team_conf)
        line.mercado = 'proyectos_confeccion'
        self.assertEqual(line.team_id, self.team_conf, "Proyectos usan el equipo de su mercado base.")

    def test_03_precio_capturado_y_tipo_de_cambio_del_mes(self):
        budget = self.Budget.create({'year': 2052})
        line = self._line(budget, price_input=2.0, price_currency_id=self.foreign.id)
        self.assertEqual(line.price_origen, 'captura')
        self.assertEqual(line.price_problem, 'sin_tc', "Dólares sin tipo de cambio: cuenta 0.")
        self.assertEqual(line.amount_budget, 0.0)
        self.assertEqual(budget.no_rate_count, 1)
        budget.rate_default = 18.5
        budget.action_qb_apply_rate_all()
        self.assertEqual(len(budget.rate_ids), 12)
        self.assertFalse(line.price_problem)
        self.assertAlmostEqual(line.amount_currency, 2000.0, places=2)
        self.assertAlmostEqual(line.amount_budget, 2000.0 * self._factor(18.5), places=2)
        # El tipo de cambio de marzo manda sobre los demás.
        budget.rate_ids.filtered(lambda r: r.month == 3).rate = 20.0
        self.assertAlmostEqual(line.amount_budget, 2000.0 * self._factor(20.0), places=2)
        self.assertEqual(line.rate, 20.0)

    def test_04_precio_por_kilo_con_rendimiento(self):
        budget = self.Budget.create({'year': 2053})
        line = self._line(budget, price_input=50.0, price_basis='kg')
        self.assertAlmostEqual(line.qty_m, 1000.0)
        self.assertAlmostEqual(line.qty_kg, 200.0, msg="1000 m ÷ 5 m/kg.")
        self.assertAlmostEqual(line.amount_budget, 200.0 * 50.0, places=2)
        sin_rend = self.env['product.product'].create({
            'name': 'Tela sin rendimiento', 'type': 'consu', 'uom_id': self.uom_m.id})
        line2 = self._line(budget, product_id=sin_rend.id, price_input=50.0, price_basis='kg')
        self.assertEqual(line2.price_problem, 'sin_conversion')
        self.assertEqual(line2.amount_budget, 0.0)

    def test_05_renglon_de_proyecto(self):
        budget = self.Budget.create({'year': 2054})
        line = self._line(budget, product_id=False, mercado='proyectos_industrial',
                          proyecto_descripcion='Tela nueva para asiento', proyecto_gramaje=200.0,
                          proyecto_ancho_cm=150.0, price_input=3.0, price_basis='m')
        self.assertEqual(line.price_origen, 'proyecto')
        self.assertAlmostEqual(line.qty_kg, 300.0, msg="1000 m × 1.5 m × 200 g/m².")
        self.assertEqual(line.conv_origen, 'proyecto')
        self.assertAlmostEqual(line.amount_budget, 3000.0, places=2)
        self.assertIn('Proyecto:', line.display_name)
        with self.assertRaises(ValidationError):
            self._line(budget, product_id=False, mercado='industrial',
                       proyecto_descripcion='Sin código', date=date(2054, 4, 1))
        with self.assertRaises(ValidationError):
            self._line(budget, product_id=False, mercado='proyectos_industrial',
                       date=date(2054, 5, 1))

    def test_06_precio_sugerido_de_la_lista(self):
        pl = self.env['product.pricelist'].create({'name': 'Lista anual', 'currency_id': self.foreign.id})
        self.env['product.pricelist.item'].create({
            'pricelist_id': pl.id, 'applied_on': '1_product',
            'product_tmpl_id': self.product.product_tmpl_id.id,
            'compute_price': 'fixed', 'fixed_price': 7.5})
        client = self.env['res.partner'].create({
            'name': 'Cliente con lista anual', 'is_company': True, 'team_id': self.team_ind.id,
            'property_product_pricelist': pl.id})
        budget = self.Budget.create({'year': 2055})
        line = self._line(budget, partner_id=client.id)
        self.assertAlmostEqual(line.price_input, 7.5)
        self.assertEqual(line.price_currency_id, self.foreign)
        self.assertEqual(line.price_origen, 'lista')
        line.price_input = 8.0
        self.assertEqual(line.price_origen, 'captura')

    def test_07_aprobar_reemplaza_y_congela(self):
        self.Param.set_param('quimibond_sgi.budget_planning_rate', '19')
        budget = self.Budget.create({'year': 2056})
        line = self._line(budget, price_input=1.0, price_currency_id=self.foreign.id)
        otro = self.env['product.product'].create({
            'name': 'Otra tela anual', 'type': 'consu', 'uom_id': self.uom_m.id})
        sin_cliente = self._line(budget, product_id=otro.id, partner_id=False, mercado='industrial',
                                 price_input=1.0, date=date(2056, 4, 1))
        budget.with_user(self.manager).action_send_to_review()
        with self.assertRaises(UserError):
            budget.with_user(self.manager).action_approve()  # Industrial sin cliente
        sin_cliente.with_user(self.manager).mercado = 'confeccion'
        budget.with_user(self.manager).action_send_to_review()
        budget.with_user(self.manager).action_approve()
        self.assertEqual(budget.state, 'aprobado')
        self.assertEqual(budget.rate_ids.filtered(lambda r: r.month == 3).rate, 19.0,
                         "Al aprobar, el tipo de cambio usado queda en el documento.")
        amount = line.amount_budget
        self.Param.set_param('quimibond_sgi.budget_planning_rate', '25')
        line.invalidate_recordset()
        self.assertAlmostEqual(line.amount_budget, amount, places=2)
        with self.assertRaises(UserError):
            budget.rate_ids.with_user(self.sale_mgr).write({'rate': 30.0})
        # Revisión nueva: la REV0 sigue aprobada hasta aprobar la REV1.
        action = budget.with_user(self.manager).action_revise()
        rev1 = self.Budget.browse(action['res_id'])
        self.assertEqual(rev1.revision_label, 'REV1')
        self.assertEqual(budget.state, 'aprobado')
        rev1.with_user(self.manager).action_send_to_review()
        rev1.with_user(self.manager).action_approve()
        self.assertEqual(budget.state, 'reemplazado')
        self.assertEqual(self.Budget.search_count([('year', '=', 2056), ('state', '=', 'aprobado')]), 1)

    def test_08_ve02_lee_solo_la_vigente(self):
        budget = self.Budget.create({'year': 2057})
        self._line(budget, price_input=10.0)
        budget.state = 'aprobado'
        ind = self.env['sgi.indicator']
        self.assertAlmostEqual(ind._sgi_year_budget_amount(date(2057, 1, 1), date(2057, 12, 31)),
                               10000.0, places=2)
        rev1 = budget.with_user(self.manager).action_revise()
        rev1 = self.Budget.browse(rev1['res_id'])
        rev1.line_ids.price_input = 20.0
        rev1.state = 'revisado'
        rev1.with_user(self.manager).action_approve()
        self.assertAlmostEqual(ind._sgi_year_budget_amount(date(2057, 1, 1), date(2057, 12, 31)),
                               20000.0, places=2, msg="El VE-02 lee la revisión aprobada, no la reemplazada.")

    def test_09_objetivo_y_texto_por_mercado(self):
        budget = self.Budget.create({'year': 2058})
        self._line(budget, price_input=10.0)
        budget.base_anio_anterior = 8000.0
        self.assertAlmostEqual(budget.objetivo_amount, 9600.0)
        self.assertAlmostEqual(budget.minimo_amount, 9200.0)
        self.assertAlmostEqual(budget.growth_pct, 25.0)
        self.assertIn('Industrial', budget.amount_by_mercado_text)

    def test_10_importa_mercado_y_precio(self):
        import openpyxl
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = 'Presupuesto'
        ws.append(['PRODUCTO', 'CLIENTE', 'MERCADO', 'UNIDAD', 'ENERO m', 'PRECIO', 'MONEDA'])
        ws.append(['WANUAL150', 'Cliente anual', 'Especiales', 'm', 100, 4.25, self.foreign.name])
        ws.append(['WANUAL150', '', 'Galaxia', 'm', 100, 1, ''])
        buf = io.BytesIO()
        wb.save(buf)
        budget = self.Budget.create({'year': 2059})
        wiz = self.env['sgi.sales.budget.import'].create({
            'budget_id': budget.id, 'file': base64.b64encode(buf.getvalue()),
            'sheet_name': 'Presupuesto'})
        wiz.action_import()
        self.assertEqual(len(budget.line_ids), 1)
        line = budget.line_ids
        self.assertEqual(line.mercado, 'especiales')
        self.assertAlmostEqual(line.price_input, 4.25)
        self.assertEqual(line.price_currency_id, self.foreign)
        self.assertEqual(line.price_origen, 'excel')
        self.assertIn('Galaxia', wiz.result)

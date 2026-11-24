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
        # 3.32.0: estas pruebas aprueban presupuestos de uno o dos renglones; el aviso de meses sin
        # renglones tiene su propia prueba (test_25).
        cls.env = cls.env(context=dict(cls.env.context, qb_approve_exception=True))
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
        # En Odoo 19 res.partner no tiene team_id: el equipo del cliente sale del de su vendedor.
        cls.seller_ind = cls.env['res.users'].create({
            'name': 'Vendedor anual', 'login': 'vendedor_anual',
            'group_ids': [(6, 0, [cls.env.ref('sales_team.group_sale_salesman').id])]})
        cls.env['crm.team.member'].create({'crm_team_id': cls.team_ind.id, 'user_id': cls.seller_ind.id})
        cls.client = cls.env['res.partner'].create({
            'name': 'Cliente anual', 'is_company': True, 'user_id': cls.seller_ind.id})
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
            'name': 'Cliente con lista anual', 'is_company': True, 'user_id': self.seller_ind.id,
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

    def test_11_ajustes_cambian_el_equipo_de_los_renglones(self):
        """3.5.1: al guardar otro equipo de mercado en Ajustes, los renglones lo toman (también
        en un aprobado: el equipo no entra al importe)."""
        budget = self.Budget.create({'year': 2060})
        line = self._line(budget, price_input=1.0)
        budget.state = 'aprobado'
        nuevo = self.env['crm.team'].create({'name': 'Industrial nuevo'})
        settings = self.env['res.config.settings'].create({'qb_team_industrial_id': nuevo.id})
        settings.execute()
        self.assertEqual(line.team_id, nuevo)
        self.assertAlmostEqual(line.amount_budget, 1000.0, places=2)

    def _invoice_anual(self, team, partner, product, when, amount, account=None):
        income = account or self.env['account.account'].search([('account_type', '=', 'income')], limit=1)
        move = self.env['account.move'].create({
            'move_type': 'out_invoice', 'partner_id': partner.id, 'invoice_date': when,
            'team_id': team.id,
            'invoice_line_ids': [(0, 0, {
                'product_id': product.id, 'quantity': 1, 'price_unit': amount,
                'account_id': income.id, 'tax_ids': [(6, 0, [])]})]})
        move.action_post()
        return move

    def test_12_conciliacion_por_partida(self):
        """3.5.3: cada peso de la venta del año cae en una sola partida y el residuo es 0."""
        income = self.env['account.account'].search([('account_type', '=', 'income')], limit=1)
        self.Param.set_param('quimibond_sgi.sales_account_prefixes', income.code)
        otro = self.env['product.product'].create({
            'name': 'Otra tela conciliación', 'type': 'consu', 'uom_id': self.uom_m.id})
        saldo = self.env['product.product'].create({
            'name': 'SALDO DE TELA prueba', 'default_code': 'SALDO PRUEBA', 'type': 'consu'})
        nuevo = self.env['res.partner'].create({'name': 'Cliente nuevo anual', 'is_company': True})
        budget = self.Budget.create({'year': 2061})
        self._line(budget, date=date(2061, 3, 1), price_input=1.0)
        self._invoice_anual(self.team_ind, self.client, self.product, date(2061, 3, 10), 100.0)   # capturada
        self._invoice_anual(self.team_ind, self.client, self.product, date(2061, 3, 20), 7.0)     # capturada
        self._invoice_anual(self.team_ind, self.client, otro, date(2061, 3, 12), 30.0)            # no presupuestado
        self._invoice_anual(self.team_ind, nuevo, otro, date(2061, 3, 12), 40.0)                  # cliente nuevo
        self._invoice_anual(self.team_ind, self.client, self.product, date(2061, 5, 5), 50.0)     # mes sin renglones
        self._invoice_anual(self.team_ind, self.client, saldo, date(2061, 3, 5), 5.0)             # saldo
        self._invoice_anual(self.team_conf, nuevo, otro, date(2061, 3, 5), 60.0)                  # mercado sin renglones
        budget.action_refresh_actuals()
        d = budget._sgi_reconcile_data()
        self.assertAlmostEqual(d['A'], 292.0, places=2)
        self.assertAlmostEqual(d['B'], 107.0, places=2)
        self.assertAlmostEqual(d['unbudgeted'], 30.0, places=2)
        self.assertAlmostEqual(d['new_client'], 40.0, places=2)
        self.assertAlmostEqual(d['month_without_budget'], 50.0, places=2)
        self.assertAlmostEqual(d['saldos'], 5.0, places=2)
        self.assertAlmostEqual(d['market_without_budget'], 60.0, places=2)
        self.assertAlmostEqual(d['residual'], 0.0, places=2)
        self.assertAlmostEqual(budget.amount_real_unbudgeted, 185.0, places=2)
        # Un segundo mes con renglón: la venta de mayo pasa a «otro mes» si el renglón es de junio.
        self._line(budget, date=date(2061, 6, 1), product_id=otro.id, partner_id=nuevo.id, price_input=1.0)
        self._invoice_anual(self.team_ind, nuevo, otro, date(2061, 5, 6), 9.0)
        self._line(budget, date=date(2061, 5, 1), product_id=self.env['product.product'].create({
            'name': 'Tela mayo', 'type': 'consu', 'uom_id': self.uom_m.id}).id, price_input=1.0)
        budget.action_refresh_actuals()
        d = budget._sgi_reconcile_data()
        # Mayo ya tiene renglones: la venta de mayo del producto presupuestado en marzo (50) y
        # las de «otro» al cliente nuevo en marzo (40) y mayo (9), presupuestado en junio, son
        # «otro mes». La de «otro» al cliente presupuestado (30) pasa a «a otro cliente».
        self.assertAlmostEqual(d['other_month'], 99.0, places=2)
        self.assertAlmostEqual(d['by_client_others'], 30.0, places=2)
        self.assertAlmostEqual(d['market_without_budget'], 60.0, places=2)
        self.assertAlmostEqual(d['residual'], 0.0, places=2)
        budget.action_reconcile_invoiced()

    def test_13_traer_renglones_de_otro_presupuesto(self):
        """3.5.3: la REV2 junta los renglones de Industrial (por cliente) y de Confección (sin
        cliente) del mismo producto; lo repetido no se copia."""
        industrial = self.Budget.create({'year': 2062})
        self._line(industrial, date=date(2062, 4, 1), price_input=2.0)
        industrial.state = 'aprobado'
        confeccion = self.Budget.with_context(tracking_disable=True).create({'year': 2062})
        self._line(confeccion, date=date(2062, 4, 1), partner_id=False, mercado='confeccion', price_input=3.0)
        confeccion.state = 'obsoleto'
        rev = self.Budget.browse(industrial.with_user(self.manager).action_revise()['res_id'])
        copied, skipped = rev.action_qb_copy_lines_from([confeccion.id, industrial.id])
        self.assertEqual(copied, 1, "El de Confección entra; el de Industrial ya está en la revisión.")
        self.assertEqual(len(skipped), 1)
        self.assertEqual(sorted(rev.line_ids.mapped('mercado')), ['confeccion', 'industrial'])
        self.assertAlmostEqual(rev.amount_budget_total, 2000.0 + 3000.0, places=2)
        wizard = self.env['qb.sales.budget.copy'].create({'budget_id': rev.id, 'source_ids': [(6, 0, confeccion.ids)]})
        wizard.action_copy()
        self.assertIn('ya existe', wizard.result)

    def test_14_cumplimiento_al_corte(self):
        """3.5.4: venta de los equipos de los mercados contra presupuesto, los dos al corte; un año
        cerrado corta el 31 de diciembre. Lo vendido fuera de los renglones sí cuenta."""
        income = self.env['account.account'].search([('account_type', '=', 'income')], limit=1)
        self.Param.set_param('quimibond_sgi.sales_account_prefixes', income.code)
        budget = self.Budget.create({'year': 2021})
        self._line(budget, date=date(2021, 3, 1), price_input=1.0)                      # 1000 Industrial
        otro = self.env['product.product'].create({'name': 'Conf 2021', 'type': 'consu', 'uom_id': self.uom_m.id})
        self._line(budget, date=date(2021, 3, 1), product_id=otro.id, partner_id=False,
                   mercado='confeccion', price_input=1.0)                                # 1000 Confección
        self._invoice_anual(self.team_ind, self.client, self.product, date(2021, 3, 10), 900.0)
        self._invoice_anual(self.team_ind, self.client, otro, date(2021, 6, 10), 300.0)  # fuera de renglones
        self._invoice_anual(self.team_conf, self.client, otro, date(2021, 3, 10), 500.0)
        self.assertEqual(budget.cutoff_date, date(2021, 12, 31))
        self.assertAlmostEqual(budget.amount_budget_to_date, 2000.0, places=2)
        self.assertAlmostEqual(budget.amount_sales_to_date, 1700.0, places=2)
        self.assertAlmostEqual(budget.fulfillment_pct, 85.0, places=1)
        self.assertIn('Industrial 120.0 %', budget.fulfillment_by_mercado_text)
        self.assertIn('Confección 50.0 %', budget.fulfillment_by_mercado_text)
        future = self.Budget.create({'year': 2099})
        self.assertFalse(future.cutoff_date)
        self.assertEqual(future.fulfillment_pct, 0.0)

    def test_15_obsoleto_sin_cumplimiento(self):
        budget = self.Budget.create({'year': 2022})
        self._line(budget, date=date(2022, 3, 1), price_input=1.0)
        self.assertTrue(budget.cutoff_date)
        budget.state = 'obsoleto'
        self.assertFalse(budget.cutoff_date)
        self.assertEqual(budget.fulfillment_pct, 0.0)
        self.assertFalse(budget.fulfillment_by_mercado_text)

    def test_16_desviacion_contra_precio_presupuestal(self):
        """3.5.6: la desviación compara lo facturado contra el precio presupuestal del renglón,
        en su base (aquí por kilo), no contra la lista."""
        income = self.env['account.account'].search([('account_type', '=', 'income')], limit=1)
        self.Param.set_param('quimibond_sgi.sales_account_prefixes', income.code)
        budget = self.Budget.create({'year': 2023})
        line = self._line(budget, date=date(2023, 3, 1), price_input=10.0, price_basis='kg')
        move = self.env['account.move'].create({
            'move_type': 'out_invoice', 'partner_id': self.client.id, 'invoice_date': date(2023, 3, 5),
            'team_id': self.team_ind.id,
            'invoice_line_ids': [(0, 0, {
                'product_id': self.product.id, 'quantity': 1000.0, 'product_uom_id': self.uom_m.id,
                'price_unit': 2.2, 'account_id': income.id, 'tax_ids': [(6, 0, [])]})]})
        move.action_post()
        budget.action_refresh_actuals()
        # 2,200 pesos por 1,000 m = 200 kg → 11 pesos/kg contra 10 presupuestados: +10 %.
        self.assertAlmostEqual(line.price_real_unit_currency, 11.0, places=2)
        self.assertAlmostEqual(line.price_gap_pct, 10.0, places=1)
        self.assertEqual(line.price_gap_alert, 'leve')
        self.assertFalse(line.price_gap_fx)
        # 3.6.2: la diferencia (2,200 − 2,000) es toda precio: 1 peso/kg × 200 kg.
        self.assertAlmostEqual(line.effect_volume, 0.0, places=2)
        self.assertAlmostEqual(line.effect_price, 200.0, places=2)
        self.assertAlmostEqual(line.effect_fx, 0.0, places=2)
        self.assertAlmostEqual(budget.amount_difference, 200.0, places=2)
        self.assertAlmostEqual(budget.effect_price, 200.0, places=2)
        self.assertAlmostEqual(budget.effect_volume + budget.effect_price + budget.fx_effect
                               + budget.effect_unbudgeted, budget.amount_difference, places=2)
        # 3.6.3: el renglón del mercado suma igual que el total.
        self.assertIn("= volumen 0 + precio 200 + TC 0 + sin presupuesto 0",
                      budget.effects_by_mercado_text)

    def test_17_pdf_y_excel_del_presupuesto(self):
        """3.6.0: resumen por mercado y detalle; el PDF se genera y el Excel se descarga."""
        budget = self.Budget.create({'year': 2024})
        self._line(budget, date=date(2024, 3, 1), price_input=2.0)
        otro = self.env['product.product'].create({'name': 'Conf 2024', 'type': 'consu', 'uom_id': self.uom_m.id})
        self._line(budget, date=date(2024, 4, 1), product_id=otro.id, partner_id=False,
                   mercado='confeccion', price_input=1.0)
        data = budget._qb_report_data()
        self.assertEqual([s['label'] for s in data['summary']], ["Industrial", "Confección"])
        self.assertAlmostEqual(data['total'], 3000.0, places=2)
        self.assertAlmostEqual(data['summary'][0]['cells'][3], 2000.0, places=2)
        html, _ttype = self.env['ir.actions.report']._render_qweb_html(
            'quimibond_ventas_presupuesto.action_report_sales_budget', budget.ids)
        self.assertIn('Resumen por mercado', html.decode())
        action = budget.action_qb_download_xlsx()
        self.assertIn('/web/content/', action['url'])

    def test_18_pdf_y_excel_del_pronostico(self):
        """3.6.0: matriz cliente × producto × semana del pronóstico vigente."""
        monday = date(2030, 1, 7)
        self.env['qb.forecast.line'].create({
            'partner_id': self.client.id, 'product_id': self.product.id, 'week': monday,
            'qty_origen': 500.0, 'uom_origen': 'm'})
        wizard = self.env['qb.forecast.print'].create({'date_from': monday, 'weeks': 4, 'unit': 'm',
                                                       'partner_ids': [(6, 0, self.client.ids)]})
        data = wizard._qb_matrix()
        self.assertEqual(len(data['mondays']), 4)
        self.assertEqual(len(data['groups']), 1)
        self.assertAlmostEqual(data['groups'][0]['rows'][0]['cells'][monday], 500.0, places=2)
        html, _ttype = self.env['ir.actions.report']._render_qweb_html(
            'quimibond_ventas_presupuesto.action_report_forecast', wizard.ids)
        self.assertIn('Pronóstico de ventas vigente', html.decode())
        self.assertIn('/web/content/', wizard.action_xlsx()['url'])
        self.assertEqual(wizard.action_print()['type'], 'ir.actions.report')

    def test_19_pasar_precios_a_la_moneda_de_la_lista(self):
        """3.6.1: precio en pesos de una lista en dólares → dólares con el tipo de cambio del
        documento; el importe no cambia."""
        pl = self.env['product.pricelist'].create({'name': 'Lista USD 19', 'currency_id': self.foreign.id})
        self.env['product.pricelist.item'].create({
            'pricelist_id': pl.id, 'applied_on': '1_product',
            'product_tmpl_id': self.product.product_tmpl_id.id,
            'compute_price': 'fixed', 'fixed_price': 2.0})
        client = self.env['res.partner'].create({
            'name': 'Cliente USD 19', 'is_company': True, 'user_id': self.seller_ind.id,
            'property_product_pricelist': pl.id})
        budget = self.Budget.create({'year': 2025})
        line = self._line(budget, partner_id=client.id, price_input=36.0,
                          price_currency_id=self.own.id, date=date(2025, 3, 1))
        amount = line.amount_budget
        budget.rate_default = 18.0
        budget.action_qb_prices_to_list_currency()
        self.assertEqual(line.price_currency_id, self.foreign)
        self.assertEqual(len(budget.rate_ids), 12)
        self.assertAlmostEqual(line.price_input, round(36.0 / 18.0, 4), places=4)
        if self.own == self.env.ref('base.MXN'):
            self.assertAlmostEqual(line.amount_budget, amount, places=2)

    def test_20_cumplimiento_a_tc_presupuestal(self):
        """3.6.1: lo facturado en dólares, a TC presupuestal; la diferencia es el efecto del TC."""
        if self.own != self.env.ref('base.MXN'):
            return  # la prueba es para una compañía en pesos (producción)
        income = self.env['account.account'].search([('account_type', '=', 'income')], limit=1)
        self.Param.set_param('quimibond_sgi.sales_account_prefixes', income.code)
        self.env['res.currency.rate'].create({
            'currency_id': self.foreign.id, 'name': date(2019, 3, 1), 'rate': 1.0 / 20.0,
            'company_id': self.env.company.id})
        budget = self.Budget.create({'year': 2019})
        self._line(budget, date=date(2019, 3, 1), price_input=1.0)
        budget.rate_default = 18.0
        budget.action_qb_apply_rate_all()
        move = self.env['account.move'].create({
            'move_type': 'out_invoice', 'partner_id': self.client.id, 'invoice_date': date(2019, 3, 5),
            'team_id': self.team_ind.id, 'currency_id': self.foreign.id,
            'invoice_line_ids': [(0, 0, {
                'product_id': self.product.id, 'quantity': 1.0, 'price_unit': 50.0,
                'account_id': income.id, 'tax_ids': [(6, 0, [])]})]})
        move.action_post()
        self.assertAlmostEqual(budget.amount_sales_to_date, 1000.0, places=2)     # 50 USD × 20
        self.assertAlmostEqual(budget.amount_sales_budget_rate, 900.0, places=2)  # 50 USD × 18
        self.assertAlmostEqual(budget.fx_effect, 100.0, places=2)
        self.assertAlmostEqual(budget.fulfillment_budget_rate_pct, 90.0, places=1)

    def test_21_poner_cliente_a_varios_renglones(self):
        """3.6.4: renglón por renglón choca con el anti-doble-conteo; todos juntos, no."""
        budget = self.Budget.create({'year': 2018})
        lines = (self._line(budget, mercado='especiales', partner_id=False, date=date(2018, 1, 1))
                 | self._line(budget, mercado='especiales', partner_id=False, date=date(2018, 3, 1)))
        self.assertEqual(budget.no_client_count, 2)
        with self.assertRaises(ValidationError):
            with self.env.cr.savepoint():
                lines[0].partner_id = self.client
        budget.action_qb_set_partner(lines.ids, self.client.id)
        self.assertEqual(lines.partner_id, self.client)
        self.assertEqual(budget.no_client_count, 0)
        wizard = self.env['qb.sales.budget.set.partner'].with_context(
            active_model='sgi.sales.budget', active_id=budget.id).create({})
        self.assertEqual(wizard.budget_id, budget)
        self.assertFalse(wizard.line_ids)  # ya no queda ninguno sin cliente

    def test_22_propuesta_del_anio_siguiente(self):
        """3.7.0: la propuesta sale de lo vendido: Industrial por cliente, Confección sin cliente; el
        precio, el real del periodo base si la lista no lo tiene. 3.33.0: sin crecimiento por mercado, y
        cada renglón dice de dónde sale."""
        income = self.env['account.account'].search([('account_type', '=', 'income')], limit=1)
        self.Param.set_param('quimibond_sgi.sales_account_prefixes', income.code)
        otro = self.env['res.partner'].create({'name': 'Maquila propuesta', 'is_company': True})
        saldo = self.env['product.product'].create({
            'name': 'SALDO propuesta', 'default_code': 'SALDO-P', 'type': 'consu', 'uom_id': self.uom_m.id})

        def invoice(partner, team, when, lines):
            move = self.env['account.move'].create({
                'move_type': 'out_invoice', 'partner_id': partner.id, 'invoice_date': when,
                'team_id': team.id,
                'invoice_line_ids': [(0, 0, {
                    'product_id': product.id, 'quantity': qty, 'price_unit': price,
                    'account_id': income.id, 'tax_ids': [(6, 0, [])]}) for product, qty, price in lines]})
            move.action_post()

        invoice(self.client, self.team_ind, date(2016, 3, 10), [(self.product, 100.0, 2.0)])
        invoice(otro, self.team_conf, date(2016, 5, 10), [(self.product, 50.0, 3.0), (saldo, 10.0, 1.0)])
        budget_id = self.Budget.action_qb_build_proposal(
            2017, base='anio', include_dev=False)
        budget = self.Budget.browse(budget_id)
        self.assertEqual(budget.year, 2017)
        self.assertEqual(budget.state, 'borrador')
        self.assertEqual(len(budget.line_ids), 2)  # el saldo no entra
        ind = budget.line_ids.filtered(lambda l: l.mercado == 'industrial')
        conf = budget.line_ids.filtered(lambda l: l.mercado == 'confeccion')
        self.assertEqual(ind.partner_id, self.client)
        self.assertEqual(ind.date, date(2017, 3, 1))
        self.assertAlmostEqual(ind.qty_budget, 100.0, places=2)
        self.assertAlmostEqual(ind.price_input, 2.0, places=4)
        self.assertEqual(ind.price_origen, 'historico')
        self.assertIn("marzo 2016", ind.support_qty)
        self.assertIn(self.client.name, ind.support_qty)
        self.assertIn("Promedio facturado en 03/2016", ind.support_price)
        self.assertTrue(ind.support_ok)
        self.assertEqual((budget.base_date_from, budget.base_date_to), (date(2016, 1, 1), date(2016, 12, 31)))
        self.assertEqual(ind.action_qb_view_support()['res_model'], 'account.move.line')
        # Un crecimiento general ya no se acepta: solo supuestos por cliente con motivo.
        with self.assertRaises(UserError):
            self.Budget.action_qb_build_proposal(2019, growth={'industrial': 10.0}, base='anio')
        self.assertFalse(conf.partner_id)
        self.assertEqual(conf.date, date(2017, 5, 1))
        self.assertAlmostEqual(conf.qty_budget, 50.0, places=2)
        self.assertAlmostEqual(conf.price_input, 3.0, places=4)
        self.assertEqual(budget.no_client_count, 0)
        # Con renglones, no se vuelve a armar encima.
        with self.assertRaises(UserError):
            self.Budget.action_qb_build_proposal(2017, base='anio')

    def test_23_tablero_y_meses_del_presupuesto_del_mes(self):
        """3.22.0: el tablero, las tablas por mercado, mes y cliente, y los meses y mercados sin renglones
        toman el presupuesto del mes (la REV2 de 2026 no trae Industrial en noviembre ni diciembre)."""
        income = self.env['account.account'].search([('account_type', '=', 'income')], limit=1)
        self.Param.set_param('quimibond_sgi.sales_account_prefixes', income.code)
        conf_product = self.env['product.product'].create({
            'name': 'Entretela tablero', 'type': 'consu', 'uom_id': self.uom_m.id})
        budget = self.Budget.create({'year': 2018})
        self._line(budget, date=date(2018, 3, 1), price_input=1.0)                       # 1000 Industrial
        self._line(budget, date=date(2018, 11, 1), product_id=conf_product.id, partner_id=False,
                   mercado='confeccion', qty_budget=500.0, price_input=1.0)              # 500 Confección
        plan = self.env['qb.sales.month.plan'].create({'month': date(2018, 11, 1)})
        Plan = self.env['qb.sales.month.plan.line']
        Plan.create({'plan_id': plan.id, 'partner_id': self.client.id, 'product_id': self.product.id,
                     'mercado': 'industrial', 'qty': 200.0, 'price_input': 2.0})         # 400: hueco
        Plan.create({'plan_id': plan.id, 'product_id': conf_product.id, 'mercado': 'confeccion',
                     'qty': 100.0, 'price_input': 1.0})                                  # el anual manda
        self.assertEqual(budget._qb_month_plan_fill(), {(date(2018, 11, 1), 'industrial'): 400.0})
        conf_client = self.env['res.partner'].create({'name': 'Maquila tablero', 'is_company': True})
        self._invoice_anual(self.team_ind, self.client, self.product, date(2018, 3, 10), 900.0)
        self._invoice_anual(self.team_ind, self.client, self.product, date(2018, 11, 10), 450.0)
        self._invoice_anual(self.team_conf, conf_client, conf_product, date(2018, 11, 12), 300.0)
        budget.invalidate_recordset()

        self.assertAlmostEqual(budget.amount_budget_total, 1500.0, places=2)
        self.assertAlmostEqual(budget.amount_month_plan_fill, 400.0, places=2)
        self.assertAlmostEqual(budget.amount_year_full, 1900.0, places=2)
        self.assertAlmostEqual(budget.amount_budget_to_date, 1900.0, places=2)
        self.assertAlmostEqual(budget.amount_sales_to_date, 1650.0, places=2)
        self.assertAlmostEqual(budget.effect_month_plan, -400.0, places=2)
        self.assertAlmostEqual(
            budget.effect_volume + budget.effect_price + budget.fx_effect + budget.effect_month_plan
            + budget.effect_unbudgeted, budget.amount_difference, places=2,
            msg="Las partes suman la diferencia")
        self.assertIn('Industrial 96.4 %', budget.fulfillment_by_mercado_text)  # 1350 / 1400
        self.assertAlmostEqual(budget.amount_close_estimate, 1650.0, places=2, msg="Año cerrado: lo vendido")
        self.assertAlmostEqual(budget.fulfillment_industrial_pct, 96.4, places=1)
        self.assertAlmostEqual(budget.fulfillment_confeccion_pct, 60.0, places=1)  # 300 / 500

        # El comparativo trae el presupuesto del mes ligado a este presupuesto y la venta del año.
        compare = budget.action_qb_compare_mercado()
        self.assertEqual(compare['res_model'], 'qb.sales.compare')
        self.assertEqual(compare['context']['pivot_row_groupby'], ['mercado'])
        self.env.flush_all()  # la vista SQL lee los renglones ya guardados
        rows = self.env['qb.sales.compare'].search(compare['domain'])
        self.assertAlmostEqual(sum(rows.mapped('amount_budget')), 1900.0, places=2)
        self.assertAlmostEqual(sum(rows.filtered(lambda r: r.fuente == 'presupuesto_mes').mapped('amount_budget')),
                               400.0, places=2)
        for action in (budget.action_qb_compare_mes(), budget.action_qb_compare_cliente()):
            self.assertEqual(action['domain'], compare['domain'])

        lines = budget.action_qb_open_lines()
        self.assertEqual(lines['domain'], [('budget_id', '=', budget.id)])
        self.assertEqual(lines['context']['group_by'], ['mercado', 'partner_id'])
        analysis = budget.with_context(qb_pivot_rows=['mercado']).action_open_analysis()
        self.assertEqual(analysis['context']['pivot_row_groupby'], ['mercado'])

    def test_24_cierre_estimado_con_presupuesto_del_mes(self):
        """3.22.0: un mes que falta se estima con el presupuesto del mes; si no hay, con el anual."""
        today = date.today()
        budget = self.Budget.create({'year': today.year + 1})
        self._line(budget, date=date(today.year + 1, 2, 1), price_input=1.0)            # 1000 anual
        self._line(budget, date=date(today.year + 1, 3, 1), price_input=1.0)            # 1000 anual
        plan = self.env['qb.sales.month.plan'].create({'month': date(today.year + 1, 3, 1)})
        self.env['qb.sales.month.plan.line'].create({
            'plan_id': plan.id, 'partner_id': self.client.id, 'product_id': self.product.id,
            'mercado': 'industrial', 'qty': 700.0, 'price_input': 1.0})
        budget.invalidate_recordset()
        self.assertFalse(budget._qb_month_plan_fill(), "Marzo ya trae Industrial en el anual")
        # Marzo: el presupuesto del mes trae Industrial y manda; febrero no tiene, va el anual.
        self.assertAlmostEqual(budget.amount_close_estimate, 1000.0 + 700.0, places=2)
        # Un mercado que el presupuesto del mes no trae usa el anual.
        conf = self.env['product.product'].create({'name': 'Entretela estimado', 'type': 'consu',
                                                   'uom_id': self.uom_m.id})
        self._line(budget, date=date(today.year + 1, 3, 1), product_id=conf.id, partner_id=False,
                   mercado='confeccion', qty_budget=300.0, price_input=1.0)
        budget.invalidate_recordset()
        self.assertAlmostEqual(budget.amount_close_estimate, 1000.0 + 700.0 + 300.0, places=2)

    def test_25_meses_sin_renglones_no_se_aprueban(self):
        """3.32.0: la REV2 de 2026 se aprobó sin Industrial ni Proyectos en noviembre y diciembre.
        Un mercado sin renglones en un mes bloquea la aprobación, salvo la excepción de Dirección."""
        env = self.env(context=dict(self.env.context, qb_approve_exception=False))
        budget = env['sgi.sales.budget'].create({'year': 2058})
        conf = self.env['product.product'].create({'name': 'Entretela meses', 'type': 'consu',
                                                   'uom_id': self.uom_m.id})
        esp = self.env['product.product'].create({'name': 'Especial meses', 'type': 'consu',
                                                  'uom_id': self.uom_m.id})
        for month in range(1, 13):
            if month < 11:  # Industrial sin noviembre ni diciembre
                self._line(budget, date=date(2058, month, 1), price_input=1.0)
            self._line(budget, date=date(2058, month, 1), product_id=conf.id, partner_id=False,
                       mercado='confeccion', price_input=1.0)
            self._line(budget, date=date(2058, month, 1), product_id=esp.id, mercado='especiales',
                       price_input=1.0)
        # Un proyecto industrial cuenta para Industrial.
        self._line(budget, date=date(2058, 11, 1), product_id=False, mercado='proyectos_industrial',
                   proyecto_descripcion='Proyecto meses', price_input=1.0)
        budget.invalidate_recordset()
        self.assertEqual(budget.empty_month_count, 1)
        self.assertEqual(budget.empty_months_text, "Industrial: diciembre")
        budget.with_user(self.manager).action_send_to_review()
        with self.assertRaises(UserError):
            budget.with_user(self.manager).action_approve()
        budget.with_user(self.manager).action_qb_approve_empty_months()
        self.assertEqual(budget.state, 'aprobado')
        self.assertIn("Industrial: diciembre", "".join(budget.message_ids.mapped('body')))

    def test_26_propuesta_con_pronostico_y_supuestos_por_cliente(self):
        """3.32.0: los meses que el pronóstico de un cliente cubre completos salen del pronóstico; los
        demás, de la venta base. Un supuesto por cliente cambia la venta base desde su mes (−100 % = deja
        de comprar); 3.33.0: queda guardado en el presupuesto y sin evidencia sus renglones no tienen
        soporte."""
        income = self.env['account.account'].search([('account_type', '=', 'income')], limit=1)
        self.Param.set_param('quimibond_sgi.sales_account_prefixes', income.code)
        otro = self.env['res.partner'].create({'name': 'Cliente que se va', 'is_company': True})

        por_pedido = self.env['product.product'].create({
            'name': 'Tela por pedido', 'type': 'consu', 'uom_id': self.uom_m.id})

        def invoice(partner, when, qty, product=None):
            move = self.env['account.move'].create({
                'move_type': 'out_invoice', 'partner_id': partner.id, 'invoice_date': when,
                'team_id': self.team_ind.id,
                'invoice_line_ids': [(0, 0, {
                    'product_id': (product or self.product).id, 'quantity': qty, 'price_unit': 2.0,
                    'account_id': income.id, 'tax_ids': [(6, 0, [])]})]})
            move.action_post()

        for month in (1, 2, 3):
            invoice(self.client, date(2017, month, 10), 100.0)
        invoice(self.client, date(2017, 1, 10), 40.0, por_pedido)
        invoice(otro, date(2017, 3, 10), 100.0)
        invoice(otro, date(2017, 7, 10), 100.0)
        Forecast = self.env['qb.forecast.line']
        Forecast.create({'partner_id': self.client.id, 'product_id': self.product.id,
                         'week': date(2018, 1, 8), 'qty_origen': 300.0})
        # El lunes 26 de febrero cubre hasta el domingo 4 de marzo: enero y febrero completos.
        Forecast.create({'partner_id': self.client.id, 'product_id': self.product.id,
                         'week': date(2018, 2, 26), 'qty_origen': 50.0})
        budget = self.Budget.browse(self.Budget.action_qb_build_proposal(
            2018, base='anio', include_dev=False,
            partner_growth=[{'partner_id': otro.id, 'growth_pct': -100.0, 'month_from': 7,
                             'note': 'Termina su programa'},
                            {'partner_id': self.client.id, 'growth_pct': 10.0, 'month_from': 1,
                             'note': 'Programa nuevo', 'evidence_url': 'https://correo/123'}]))
        pedido = budget.line_ids.filtered(lambda l: l.product_id == por_pedido)
        self.assertEqual((pedido.date, pedido.qty_origen, pedido.qty_budget), (date(2018, 1, 1), 'venta', 44.0),
                         "Un producto que no viene en el pronóstico sigue con la venta base (+10 % del supuesto)")
        mine = budget.line_ids.filtered(
            lambda l: l.partner_id == self.client and l.product_id == self.product).sorted('date')
        self.assertEqual(mine.mapped('date'), [date(2018, 1, 1), date(2018, 2, 1), date(2018, 3, 1)])
        self.assertEqual(mine.mapped('qty_origen'), ['pronostico', 'pronostico', 'venta'])
        self.assertEqual(mine.mapped('qty_budget'), [300.0, 50.0, 110.0])
        self.assertAlmostEqual(mine[0].price_input, 2.0, places=4,
                               msg="Sin lista, el precio real del periodo base del cliente")
        self.assertIn("semana", mine[0].support_qty)
        self.assertEqual(mine[0].action_qb_view_support()['res_model'], 'qb.forecast.line')
        self.assertTrue(mine[2].assumption_id, "El mes de venta base lleva el supuesto del cliente")
        self.assertTrue(mine[2].support_ok, "Supuesto con evidencia")
        theirs = budget.line_ids.filtered(lambda l: l.partner_id == otro)
        self.assertEqual(theirs.mapped('date'), [date(2018, 3, 1)], "Julio: deja de comprar")
        self.assertEqual(theirs.qty_budget, 100.0, "Su supuesto empieza en julio")
        self.assertEqual(len(budget.assumption_ids), 2, "Los supuestos quedan en el presupuesto")
        sin_evidencia = budget.assumption_ids.filtered(lambda a: a.partner_id == otro)
        self.assertFalse(sin_evidencia.evidence_ok)
        summary = "".join(budget.message_ids.mapped('body'))
        self.assertIn("Pronóstico vigente", summary)
        self.assertIn("Termina su programa", summary)
        # Sin el pronóstico, todo sale de la venta base.
        budget.line_ids.unlink()
        budget = self.Budget.browse(self.Budget.action_qb_build_proposal(
            2018, base='anio', include_dev=False, use_forecast=False))
        mine = budget.line_ids.filtered(lambda l: l.partner_id == self.client and l.product_id == self.product)
        self.assertEqual(set(mine.mapped('qty_origen')), {'venta'})
        self.assertEqual(mine.mapped('qty_budget'), [100.0, 100.0, 100.0])

    def test_27_soporte_por_renglon_bloquea_la_aprobacion(self):
        """3.33.0 (Jose, 23-nov: «todo tiene que tener soporte»; un renglón sin soporte bloquea, con
        excepción de Dirección). Cambiar cantidad o precio pide motivo; un renglón a mano pide soporte;
        con precios en dólares, el tipo de cambio pide su fuente."""
        env = self.env(context=dict(self.env.context, qb_approve_exception=False))
        budget = env['sgi.sales.budget'].create({'year': 2059})
        conf = self.env['product.product'].create({'name': 'Entretela soporte', 'type': 'consu',
                                                   'uom_id': self.uom_m.id})
        esp = self.env['product.product'].create({'name': 'Especial soporte', 'type': 'consu',
                                                  'uom_id': self.uom_m.id})
        lines = env['sgi.sales.budget.line']
        for month in range(1, 13):
            lines |= self._line(budget, date=date(2059, month, 1), price_input=1.0, price_origen='historico',
                                qty_origen='venta', qty_sugerida=1000.0, support_qty="Facturado")
            lines |= self._line(budget, date=date(2059, month, 1), product_id=conf.id, partner_id=False,
                                mercado='confeccion', price_input=1.0, support_note="Plan de Confección")
            lines |= self._line(budget, date=date(2059, month, 1), product_id=esp.id, mercado='especiales',
                                price_input=1.0, support_note="Plan de Especiales")
        budget.invalidate_recordset()
        self.assertEqual(budget.unsupported_count, 0, budget.line_ids.filtered(lambda l: not l.support_ok))
        self.assertFalse(budget.approval_exceptions_text)
        # A mano, sin soporte.
        manual = self._line(budget, date=date(2059, 1, 1), product_id=self.env['product.product'].create({
            'name': 'Tela a mano', 'type': 'consu', 'uom_id': self.uom_m.id}).id, price_input=1.0)
        self.assertFalse(manual.support_ok)
        # Cantidad cambiada sin motivo.
        ind = lines.filtered(lambda l: l.mercado == 'industrial')[:1]
        ind.qty_budget = 1200.0
        self.assertFalse(ind.support_ok)
        budget.invalidate_recordset()
        self.assertEqual(budget.unsupported_count, 2)
        self.assertIn("2 renglón(es) sin soporte", budget.approval_exceptions_text)
        self.assertEqual(budget.action_qb_view_unsupported()['domain'][-1], ('support_ok', '=', False))
        budget.with_user(self.manager).action_send_to_review()
        with self.assertRaises(UserError):
            budget.with_user(self.manager).action_approve()
        ind.ajuste_motivo = "El cliente confirmó 1,200 m en su release"
        manual.support_note = "Pedido abierto PO-77"
        self.assertTrue(ind.support_ok and manual.support_ok)
        # Dólares sin fuente del tipo de cambio.
        usd = self.env.ref('base.USD')
        usd.active = True
        ind.write({'price_currency_id': usd.id})
        budget.invalidate_recordset()
        self.assertIn("tipo de cambio sin fuente", budget.approval_exceptions_text)
        budget.rate_source = "FIX de Banxico del 30/11/2058"
        budget.invalidate_recordset()
        self.assertFalse(budget.approval_exceptions_text)
        ind.price_currency_id = budget.currency_id  # sin tipo de cambio del mes contaría 0
        budget.with_user(self.manager).action_approve()
        self.assertEqual(budget.state, 'aprobado')

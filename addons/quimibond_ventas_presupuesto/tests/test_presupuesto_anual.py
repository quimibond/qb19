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

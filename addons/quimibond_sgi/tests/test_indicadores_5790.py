# -*- coding: utf-8 -*-
"""57.90.0: ventas sin activo fijo ni anticipos, indicadores de foto y
``sgi_recalculate`` sin ``detail_ids``.

Periodo 2042 para no chocar con la facturación de otras suites.
"""
from datetime import date, datetime

from dateutil.relativedelta import relativedelta

from odoo import fields
from odoo.tests import TransactionCase, tagged

from ..models.sgi_indicator_detail import SNAPSHOT_NOTE


@tagged('post_install', '-at_install')
class TestIndicadores5790(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Indicator = cls.env['sgi.indicator']
        cls.company = cls.env.company
        cls.env['ir.config_parameter'].sudo().set_param(
            'quimibond_sgi.kpi_company_id', cls.company.id)

        def account(code, name, account_type):
            return cls.env['account.account'].create({
                'code': code, 'name': name, 'account_type': account_type,
                'company_ids': [(6, 0, cls.company.ids)]})
        cls.sales = account('ZV40101', 'Ventas prueba', 'income')
        cls.asset_sale = account('ZV70423', 'Utilidad venta activo fijo prueba', 'income_other')
        cls.advance = account('ZV20601', 'Anticipo de clientes prueba', 'liability_current')
        cls.env['ir.config_parameter'].sudo().set_param(
            'quimibond_sgi.sales_account_prefixes', 'ZV401')
        cls.customer = cls.env['res.partner'].create({'name': 'Cliente 5790'})
        cls.lessor = cls.env['res.partner'].create({'name': 'Arrendadora 5790'})
        cls.period = date(2042, 3, 1)
        cls.period_end = date(2042, 3, 31)

    def _invoice(self, partner, lines, when=None, move_type='out_invoice'):
        move = self.env['account.move'].create({
            'move_type': move_type, 'partner_id': partner.id,
            'invoice_date': when or self.period,
            'invoice_line_ids': [(0, 0, {
                'name': 'línea', 'quantity': 1.0, 'price_unit': amount,
                'account_id': acc.id, 'tax_ids': [(6, 0, [])]}) for acc, amount in lines]})
        move.action_post()
        return move

    def _indicator(self, mode, code, **vals):
        return self.Indicator.create(dict({'code': code, 'name': 'KPI %s' % mode,
                                           'calc_mode': mode}, **vals))

    # ---- 1. ventas del giro ------------------------------------------------
    def test_01_activo_fijo_y_anticipo_no_son_venta(self):
        mixed = self._invoice(self.customer, [(self.sales, 1000.0), (self.advance, 200.0)])
        asset = self._invoice(self.lessor, [(self.asset_sale, 11300.0)])
        ind = self._indicator('crecimiento_ventas', 'Z5790-VE')
        self.assertEqual(ind._sgi_net_invoiced(self.period, self.period_end), 1000.0,
                         "Solo la línea 401: ni el anticipo ni el activo fijo.")
        self._invoice(self.customer, [(self.sales, 100.0)], move_type='out_refund')
        self.assertEqual(ind._sgi_net_invoiced(self.period, self.period_end), 900.0,
                         "La nota de crédito resta.")
        # Un año antes: 450 de venta y una venta de activo fijo que no cuenta.
        self._invoice(self.customer, [(self.sales, 450.0)], self.period - relativedelta(years=1))
        self._invoice(self.lessor, [(self.asset_sale, 5000.0)], self.period - relativedelta(years=1))
        self.assertEqual(ind._calc_crecimiento_ventas(self.period, self.period_end), 100.0)
        # La evidencia por factura no trae la venta de activo fijo.
        evidence = ind._sgi_evidence_records(self.period, self.period_end)
        self.assertIn(mixed.id, evidence['ids'])
        self.assertNotIn(asset.id, evidence['ids'])

    def test_02_activo_fijo_no_hace_cliente(self):
        self._invoice(self.customer, [(self.sales, 600.0)])
        self._invoice(self.lessor, [(self.asset_sale, 11300.0)])
        nuevos = self._indicator('clientes_nuevos', 'Z5790-CN')
        self.assertEqual(nuevos._calc_clientes_nuevos(self.period, self.period_end), 1.0,
                         "La arrendadora no es cliente nuevo.")
        top3 = self._indicator('concentracion_top3', 'Z5790-T3')
        self.assertEqual(top3._calc_concentracion_top3(self.period, self.period_end), 100.0,
                         "El único cliente del giro concentra todo.")

    # ---- 2. indicadores de foto -------------------------------------------
    def test_03_foto_no_se_reconstruye(self):
        ind = self._indicator('cartera_vencida', 'Z5790-EX08')
        self.assertTrue(ind.snapshot, "Los modos de saldo pendiente son foto.")
        last = ind._sgi_default_period()
        past = last - relativedelta(months=3)
        result = ind.sgi_recalculate(period_date=past, save=True)[0]
        self.assertEqual(result['state'], 'sin_dato')
        self.assertEqual(result['note'], SNAPSHOT_NOTE)
        measure = self.env['sgi.indicator.measure'].browse(result['measure_id'])
        self.assertEqual(measure.state, 'sin_dato')
        current = ind.sgi_recalculate(period_date=last)[0]
        self.assertNotEqual(current['note'], SNAPSHOT_NOTE,
                            "El último periodo cerrado sí se mide.")
        # Un modo de periodo no es foto; una fórmula se marca a mano.
        self.assertFalse(self._indicator('crecimiento_ventas', 'Z5790-NF').snapshot)
        formula = self._indicator('configurable', 'Z5790-CF', snapshot=True)
        self.assertTrue(formula.snapshot)
        self.assertTrue(formula._sgi_snapshot_blocked(past))
        self.assertFalse(formula._sgi_snapshot_blocked(last))

    def test_04_limpieza_de_historia(self):
        ind = self._indicator('cartera_vencida_60', 'Z5790-EX09')
        Measure = self.env['sgi.indicator.measure']
        last = ind._sgi_default_period()
        old = Measure.create({'indicator_id': ind.id, 'period_date': last - relativedelta(months=2),
                              'value': 95.0, 'state': 'capturado'})
        validated = Measure.create({'indicator_id': ind.id,
                                    'period_date': last - relativedelta(months=1),
                                    'value': 90.0, 'state': 'validado'})
        current = Measure.create({'indicator_id': ind.id, 'period_date': last,
                                  'value': 30.0, 'state': 'capturado'})
        since = fields.Datetime.to_string(fields.Datetime.now() - relativedelta(hours=1))
        changed = ind._sgi_snapshot_clear_history(since)
        self.assertEqual(changed, old)
        self.assertEqual(old.state, 'sin_dato')
        self.assertIn('95.0', old.note, "El valor anterior queda en la nota.")
        self.assertEqual(validated.state, 'validado', "Una validada es evidencia.")
        self.assertEqual((current.state, current.value), ('capturado', 30.0))

    # ---- 3. respuesta corta --------------------------------------------------
    def test_05_recalcular_sin_detalle(self):
        self._invoice(self.customer, [(self.sales, 1000.0)])
        ind = self._indicator('crecimiento_ventas', 'Z5790-VD')
        full = ind.sgi_recalculate(period_date=self.period)[0]
        short = ind.sgi_recalculate(period_date=self.period, with_details=False)[0]
        self.assertIn('detail_ids', full)
        self.assertNotIn('detail_ids', short)
        self.assertEqual(short['detail_count'], full['detail_count'])
        self.assertGreaterEqual(short['detail_count'], 1)

    # ---- CO-01 ---------------------------------------------------------------
    def _receipt(self, promised, done):
        """OC del 2042-03-02 10:00 UTC con la fecha prometida dada, recibida en
        ``done`` (UTC)."""
        product = self.env['product.product'].create({'name': 'Insumo 5790', 'type': 'consu'})
        supplier = self.env['res.partner'].create({'name': 'Proveedor 5790'})
        ordered = datetime(2042, 3, 2, 10, 0, 0)
        po = self.env['purchase.order'].create({
            'partner_id': supplier.id, 'date_order': ordered,
            'order_line': [(0, 0, {'product_id': product.id, 'product_qty': 1.0,
                                   'price_unit': 10.0, 'date_planned': promised or ordered})]})
        po.button_confirm()
        picking = po.picking_ids
        picking.move_ids.quantity = 1.0
        picking.move_ids.picked = True
        picking._action_done()
        picking.date_done = done
        return po, picking

    def test_06_otd_compras_sin_promesa_no_cuenta(self):
        # Recibida el mismo día prometido pero más tarde (hora local): a tiempo.
        _po, same_day = self._receipt(datetime(2042, 3, 10, 15, 0), datetime(2042, 3, 10, 23, 0))
        _po, late = self._receipt(datetime(2042, 3, 10, 15, 0), datetime(2042, 3, 13, 18, 0))
        no_promise_po, no_promise = self._receipt(None, datetime(2042, 3, 12, 18, 0))
        self.assertEqual(no_promise_po.date_planned, no_promise_po.date_order)
        ind = self._indicator('otd_compras', 'Z5790-CO01')
        detail = ind._detail_otd_compras(self.period, self.period_end)
        self.assertEqual((detail['numerator'], detail['denominator']), (1, 2))
        self.assertEqual(detail['value'], 50.0)
        self.assertNotIn(no_promise.id, detail['ids'])
        self.assertIn(same_day.id, detail['ids'])
        self.assertIn(late.id, detail['ids'])
        self.assertIn('sin fecha prometida', ind._note_otd_compras(self.period, self.period_end))

    # ---- S6-04: solicitud de cambio por despliegue ---------------------------
    def test_07_despliegue_crea_su_solicitud(self):
        Param = self.env['ir.config_parameter'].sudo()
        category = self.env['approval.category'].create({
            'name': 'Cambio en Odoo prueba 5790', 'approval_minimum': 1})
        Param.set_param('quimibond_sgi.change_approval_category_id', category.id)
        Param.set_param('quimibond_sgi.change_request_owner_id', self.env.user.id)
        Param.set_param('quimibond_sgi.deploy_versions', '')
        Cron = self.env['sgi.cron']
        self.assertFalse(Cron._sgi_register_deploys(force=True),
                         "La primera corrida solo guarda la base.")
        self.assertIn('quimibond_sgi', Param.get_param('quimibond_sgi.deploy_versions'))
        self.assertFalse(Cron._sgi_register_deploys(force=True), "Sin cambios, nada.")
        Param.set_param('quimibond_sgi.deploy_versions', '{"quimibond_sgi": "19.0.0.0.0"}')
        Param.set_param('database.is_neutralized', 'True')
        self.assertFalse(Cron._sgi_register_deploys(), "Una copia neutralizada no crea nada.")
        created = Cron._sgi_register_deploys(force=True)
        installed = self.env['ir.module.module'].search(
            [('name', '=', 'quimibond_sgi')]).latest_version
        request = created.filtered(lambda r: r.name == 'Despliegue quimibond_sgi %s' % installed)
        self.assertEqual(len(request), 1)
        self.assertEqual(request.category_id, category)
        self.assertEqual(request.request_status, 'new', "Nadie la envía ni la aprueba sola.")
        self.assertIn(installed, request.reference)
        self.assertIn('19.0.0.0.0', str(request.reason))
        self.assertFalse(Cron._sgi_register_deploys(force=True), "Ya registrada: no se repite.")

    def test_08_base_incluye_modulos_por_actualizar(self):
        # 57.90.1: durante la actualización el módulo está «por actualizar»;
        # la base de versiones también debe contarlo.
        module = self.env['ir.module.module'].search([('name', '=', 'quimibond_sgi')])
        module.state = 'to upgrade'
        self.assertIn('quimibond_sgi', self.env['sgi.cron']._sgi_repo_modules())

    def test_09_changelog_cubre_el_salto(self):
        from ..models.sgi_deploy_change import _changelog_entry
        from odoo.modules.module import get_module_path
        path = get_module_path('quimibond_sgi')
        both = _changelog_entry(path, '19.0.57.90.1', '19.0.57.89.0')
        self.assertIn('## 19.0.57.90.1', both)
        self.assertIn('## 19.0.57.90.0', both)
        self.assertNotIn('## 19.0.57.89.0', both)
        self.assertNotIn('## 19.0.57.90.0', _changelog_entry(path, '19.0.57.90.1'))

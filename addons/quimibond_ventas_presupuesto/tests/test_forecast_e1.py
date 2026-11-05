# -*- coding: utf-8 -*-
"""E1 (diseño v3): artículo y unidades, pronóstico vigente con bitácora, carga del forecast de
Excel y Demanda por cliente. Datos inventados en 2040 (no chocan con producción)."""
import base64
import io
from datetime import date, datetime, timedelta

import openpyxl

from odoo.tests import TransactionCase, tagged

YARD = 0.9144


@tagged('post_install', '-at_install')
class TestForecastE1(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.uom_m = cls.env.ref('uom.product_uom_meter')
        cls.uom_kg = cls.env.ref('uom.product_uom_kgm')
        cls.customer = cls.env['res.partner'].create(
            {'name': 'ZZCLIENTE E1 PRUEBA', 'is_company': True, 'customer_rank': 1})
        # La misma tela en metros y en kilos (regla de Dirección: con «I» es kilos).
        cls.tela_m = cls.env['product.product'].create({
            'name': 'Tela E1 en metros', 'default_code': 'ZJ038Q22JNT160', 'type': 'consu',
            'uom_id': cls.uom_m.id, 'sale_ok': True})
        cls.tela_kg = cls.env['product.product'].create({
            'name': 'Tela E1 en kilos', 'default_code': 'IZJ038Q22JNT160', 'type': 'consu',
            'uom_id': cls.uom_kg.id, 'sale_ok': True})
        (cls.tela_m | cls.tela_kg).action_qb_actualizar_articulo()

    # --- Artículo y unidades ---------------------------------------------------
    def test_01_articulo_juntando_metros_y_kilos(self):
        self.assertEqual(self.tela_kg.qb_articulo_id, self.tela_m)
        self.assertEqual(self.tela_m.qb_articulo_id, self.tela_m)
        # Sin ficha técnica: el rendimiento sale del código (38 g/m², 160 cm).
        self.assertEqual(self.tela_m.qb_conv_origen, 'codigo')
        self.assertAlmostEqual(self.tela_m.qb_m_por_kg, 1000.0 / (38 * 1.6), places=3)
        self.assertAlmostEqual(self.tela_kg.qb_convertir(100.0, 'venta', 'm'), 100.0 * 1000.0 / (38 * 1.6),
                               places=1)
        self.assertAlmostEqual(self.tela_m.qb_convertir(1000.0, 'yd', 'venta'), 1000.0 * YARD, places=4)

    def test_02_sin_dato_no_se_inventa(self):
        raro = self.env['product.product'].create({
            'name': 'Sin código de tela', 'default_code': 'ZZRARO1', 'type': 'consu',
            'uom_id': self.uom_m.id, 'sale_ok': True})
        raro.action_qb_actualizar_articulo()
        self.assertEqual(raro.qb_conv_origen, 'sin_dato')
        self.assertIsNone(raro.qb_convertir(10.0, 'kg', 'venta'))
        line = self.env['qb.forecast.line'].create({
            'partner_id': self.customer.id, 'product_id': raro.id, 'week': date(2040, 3, 7),
            'qty_origen': 10.0, 'uom_origen': 'kg'})
        self.assertFalse(line.conv_ok)
        self.assertEqual(line.qty_venta, 0.0)

    # --- Pronóstico vigente y bitácora -------------------------------------
    def test_03_renglon_convierte_y_deja_bitacora(self):
        line = self.env['qb.forecast.line'].create({
            'partner_id': self.customer.id, 'product_id': self.tela_m.id, 'week': date(2040, 3, 7),
            'qty_origen': 1000.0, 'uom_origen': 'yd'})
        self.assertEqual(line.week, date(2040, 3, 5), "La semana se guarda en lunes")
        self.assertTrue(line.conv_ok)
        self.assertAlmostEqual(line.qty_venta, 914.4, places=2)
        self.assertAlmostEqual(line.qty_m, 914.4, places=2)
        self.assertAlmostEqual(line.qty_kg, 914.4 / self.tela_m.qb_m_por_kg, places=2)
        changes = self.env['qb.forecast.change'].search([('line_id', '=', line.id)])
        self.assertEqual(len(changes), 1)
        line.qty_origen = 500.0
        changes = self.env['qb.forecast.change'].search([('line_id', '=', line.id)], order='id')
        self.assertEqual(len(changes), 2)
        self.assertAlmostEqual(changes[-1].qty_delta, -457.2, places=1)
        line.unlink()
        gone = self.env['qb.forecast.change'].search(
            [('product_id', '=', self.tela_m.id), ('note', '=', 'Semana quitada')])
        self.assertEqual(len(gone), 1)

    def test_04_pedido_consume_el_pronostico(self):
        line = self.env['qb.forecast.line'].create({
            'partner_id': self.customer.id, 'product_id': self.tela_m.id, 'week': date(2040, 3, 5),
            'qty_origen': 1000.0})
        # El cliente pide la misma tela en kilos para esa semana: consume el pronóstico en metros.
        order = self.env['sale.order'].create({
            'partner_id': self.customer.id, 'commitment_date': datetime(2040, 3, 7, 12, 0),
            'order_line': [(0, 0, {'product_id': self.tela_kg.id, 'product_uom_qty': 10.0})]})
        order.action_confirm()
        line.action_qb_refresh()
        expected = 10.0 * self.tela_m.qb_m_por_kg
        self.assertAlmostEqual(line.qty_pedido, expected, places=1)
        self.assertAlmostEqual(line.qty_neta, 1000.0 - expected, places=1)
        self.assertEqual(line.zona, 'planeado')
        Demand = self.env['qb.demand.line']
        Demand.action_qb_rebuild()
        rows = Demand.search([('partner_id', '=', self.customer.id)])
        self.assertEqual(set(rows.mapped('tipo')), {'pedido', 'pronostico'})
        self.assertEqual(set(rows.mapped('articulo_id')), {self.tela_m})
        self.assertAlmostEqual(sum(rows.mapped('qty_m')), 1000.0, places=1,
                               msg="Pedido + pronóstico sin pedido = el pronóstico (sin doble conteo)")
        pedido = rows.filtered(lambda r: r.tipo == 'pedido')
        self.assertAlmostEqual(pedido.qty_kg_producir, 10.0, places=2)

    # --- Carga del forecast de Excel ---------------------------------------
    def _xlsx(self, qty_week2=1000.0, with_week10=True):
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = 'ZZCLIENTE E1'
        for week in range(1, 53):
            ws.cell(row=3, column=2 + week, value=float(week))
        ws['A4'] = 2040.0
        ws['B5'] = 'FECHA DE ENTREGA'
        ws['A6'] = 'PRODUCTO'
        ws['B6'] = 'PO'
        ws['A7'] = 'ZJ038Q22JNT160'
        ws['B7'] = 'PARTE-E1'
        ws.cell(row=7, column=4, value=qty_week2)          # semana 2
        if with_week10:
            ws.cell(row=7, column=12, value=2000.0)        # semana 10
        ws['A8'] = 'ZZNOEXISTE160'
        ws.cell(row=8, column=4, value=50.0)
        summary = wb.create_sheet('PRONOSTICO 2040')
        summary.append(['CLIENTE', 'UNIDADES', 'PRODUCTO / SEMANA'])
        summary.append(['ZZCLIENTE E1', 'yd', 'ZJ038Q22JNT160'])
        buffer = io.BytesIO()
        wb.save(buffer)
        return base64.b64encode(buffer.getvalue())

    def _import(self, data):
        wizard = self.env['qb.forecast.import'].create({
            'file': data, 'filename': 'forecast.xlsx', 'date_from': date(2040, 1, 1), 'years': '2040'})
        wizard.action_import()
        return wizard

    def test_05_carga_del_excel(self):
        wizard = self._import(self._xlsx())
        self.assertEqual(wizard.state, 'done')
        sheet = self.env['qb.forecast.sheet'].search([('name', '=', 'ZZCLIENTE E1')])
        self.assertEqual(sheet.partner_id, self.customer, "La hoja se liga sola por nombre")
        lines = self.env['qb.forecast.line'].search([('partner_id', '=', self.customer.id)], order='week')
        self.assertEqual(lines.mapped('week'), [date(2040, 1, 9), date(2040, 3, 5)])
        self.assertEqual(set(lines.mapped('origen')), {'excel'})
        self.assertEqual(lines[0].uom_origen, 'yd')
        self.assertAlmostEqual(lines[0].qty_venta, 1000.0 * YARD, places=2)
        self.assertEqual(lines[0].customer_part, 'PARTE-E1')
        self.assertIn('ZZNOEXISTE160', wizard.result)
        # Una semana que escribió un release no la toca el Excel.
        release_line = self.env['qb.forecast.line'].create({
            'partner_id': self.customer.id, 'product_id': self.tela_m.id, 'week': date(2040, 6, 4),
            'qty_origen': 77.0, 'origen': 'release'})
        # Segunda carga: la semana 2 cambia y la 10 ya no viene.
        self._import(self._xlsx(qty_week2=1500.0, with_week10=False))
        lines = self.env['qb.forecast.line'].search([('partner_id', '=', self.customer.id)], order='week')
        self.assertEqual(lines.mapped('week'), [date(2040, 1, 9), date(2040, 6, 4)])
        self.assertAlmostEqual(lines[0].qty_origen, 1500.0)
        self.assertTrue(release_line.exists())
        self.assertEqual(release_line.qty_origen, 77.0)

    def test_06_dos_hojas_del_mismo_cliente_son_dos_plantas(self):
        wb = openpyxl.Workbook()
        for index, title in enumerate(('ZZCLIENTE E1 NORTE', 'ZZCLIENTE E1 SUR')):
            ws = wb.active if index == 0 else wb.create_sheet()
            ws.title = title
            for week in range(1, 53):
                ws.cell(row=3, column=2 + week, value=float(week))
            ws['A4'] = 2040.0
            ws['B5'] = 'FECHA DE ENTREGA'
            ws['A6'] = 'ZJ038Q22JNT160'
            ws.cell(row=6, column=4, value=100.0 * (index + 1))
        buffer = io.BytesIO()
        wb.save(buffer)
        self._import(base64.b64encode(buffer.getvalue()))
        lines = self.env['qb.forecast.line'].search([('partner_id', '=', self.customer.id)])
        self.assertEqual(sorted(lines.mapped('plant')), ['ZZCLIENTE E1 NORTE', 'ZZCLIENTE E1 SUR'])
        self.assertEqual(sorted(lines.mapped('qty_origen')), [100.0, 200.0])

    # --- 3.0.0: cobertura sobre el pronóstico vigente y pronósticos viejos ----
    def test_07_cobertura_semana_firme_sin_pedido(self):
        from odoo import fields as odoo_fields
        today = odoo_fields.Date.context_today(self.env.user)
        week = today - timedelta(days=today.weekday()) + timedelta(weeks=1)
        self.env['qb.forecast.line'].create({
            'partner_id': self.customer.id, 'product_id': self.tela_m.id, 'week': week,
            'qty_origen': 700.0})
        Cron = self.env['sgi.cron']
        Cron.cron_forecast_coverage()
        Activity = self.env['mail.activity']
        domain = [('res_model', '=', 'res.partner'), ('res_id', '=', self.customer.id),
                  ('summary', 'ilike', 'Cobertura del pronóstico')]
        acts = Activity.search(domain)
        self.assertEqual(len(acts), 1, "Una actividad por cliente con semanas firmes sin pedido.")
        self.assertIn('ZJ038Q22JNT160', acts.note or '')
        Cron.cron_forecast_coverage()
        self.assertEqual(len(Activity.search(domain)), 1, "La segunda corrida no duplica.")

    def test_08_pronostico_viejo_no_se_crea(self):
        from odoo.exceptions import UserError
        Budget = self.env['sgi.sales.budget']
        with self.assertRaises(UserError):
            Budget.create({'year': 2040, 'kind': 'pronostico',
                           'partner_id': self.customer.id})
        old = Budget.with_context(qb_allow_old_forecast=True).create({
            'year': 2040, 'kind': 'pronostico', 'partner_id': self.customer.id})
        with self.assertRaises(UserError):
            old.action_send_to_review()
        self.assertTrue(Budget.create({'year': 2040}),
                        "El presupuesto se sigue creando normal.")

    def test_09_pedido_sin_entrega_abierta_no_es_demanda(self):
        """Un faltante cerrado sin backorder (entrega cancelada) ya no cuenta como pedido pendiente."""
        order = self.env['sale.order'].create({
            'partner_id': self.customer.id, 'commitment_date': datetime(2040, 4, 4, 12, 0),
            'order_line': [(0, 0, {'product_id': self.tela_m.id, 'product_uom_qty': 300.0})]})
        order.action_confirm()
        Demand = self.env['qb.demand.line']
        Demand.action_qb_rebuild()
        self.assertTrue(Demand.search([('order_id', '=', order.id)]),
                        "Con la entrega abierta el pedido es demanda.")
        order.action_lock()
        Demand.action_qb_rebuild()
        self.assertTrue(Demand.search([('order_id', '=', order.id)]),
                        "Bloqueado sigue siendo demanda: producción bloquea cada pedido al confirmarlo.")
        order.action_unlock()
        order.picking_ids.action_cancel()
        self.assertTrue(order.order_line.qty_delivered < order.order_line.product_uom_qty)
        Demand.action_qb_rebuild()
        self.assertFalse(Demand.search([('order_id', '=', order.id)]),
                         "Sin entrega abierta ya no es demanda, aunque falte cantidad en la línea.")

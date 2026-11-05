# -*- coding: utf-8 -*-
"""3.1.0 (diseño v3, sección 2.3): captura semanal en matriz y «Pegar semanas desde Excel».
Datos inventados; las semanas se calculan desde hoy porque lo anterior a la semana en curso no se toca."""
from datetime import timedelta

from odoo import fields
from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestForecastCaptura(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.uom_m = cls.env.ref('uom.product_uom_meter')
        cls.customer = cls.env['res.partner'].create(
            {'name': 'ZZCLIENTE CAPTURA', 'is_company': True, 'customer_rank': 1})
        cls.tela = cls.env['product.product'].create({
            'name': 'Tela captura', 'default_code': 'ZJ045Q22JNT160', 'type': 'consu',
            'uom_id': cls.uom_m.id, 'sale_ok': True})
        cls.tela.action_qb_actualizar_articulo()
        today = fields.Date.context_today(cls.env.user)
        cls.monday = today - timedelta(days=today.weekday()) + timedelta(weeks=1)
        cls.iso_year, cls.iso_week = cls.monday.isocalendar()[0], cls.monday.isocalendar()[1]
        cls.Line = cls.env['qb.forecast.line']

    def _lines(self):
        return self.Line.search([('partner_id', '=', self.customer.id)])

    # --- Matriz --------------------------------------------------------------
    def _cell(self, monday):
        # La columna de la matriz es un rango de fechas; aquí como lo manda web_grid (semana que empieza en
        # domingo, para probar que la línea cae en el lunes).
        sunday = monday - timedelta(days=1)
        return [('partner_id', '=', self.customer.id), ('plant', '=', False),
                ('product_id', '=', self.tela.id), ('uom_origen', '=', 'yd'),
                ('week', '>=', fields.Date.to_string(sunday)),
                ('week', '<', fields.Date.to_string(sunday + timedelta(days=7)))]

    def test_01_matriz_crea_suma_y_quita(self):
        domain = self._cell(self.monday)
        self.Line.grid_update_cell(domain, 'qty_origen', 1000.0)
        line = self._lines()
        self.assertEqual(len(line), 1)
        self.assertEqual(line.week, self.monday, "La semana se guarda en lunes.")
        self.assertEqual((line.qty_origen, line.uom_origen, line.origen), (1000.0, 'yd', 'captura'))
        self.assertAlmostEqual(line.qty_venta, 914.4, places=2)
        self.Line.grid_update_cell(domain, 'qty_origen', -400.0)
        self.assertEqual(line.qty_origen, 600.0)
        self.Line.grid_update_cell(domain, 'qty_origen', -600.0)
        self.assertFalse(self._lines(), "En cero la semana se quita.")
        changes = self.env['qb.forecast.change'].search([('partner_id', '=', self.customer.id)])
        self.assertEqual(len(changes), 3, "Alta, cambio y baja quedan en la bitácora.")

    def test_02_matriz_sin_cliente_pide_agrupar(self):
        domain = [leaf for leaf in self._cell(self.monday) if leaf[0] != 'partner_id']
        with self.assertRaises(UserError):
            self.Line.grid_update_cell(domain, 'qty_origen', 10.0)
        self.Line.with_context(default_partner_id=self.customer.id).grid_update_cell(domain, 'qty_origen', 10.0)
        self.assertEqual(self._lines().qty_origen, 10.0)

    # --- Pegar desde Excel ---------------------------------------------------
    def _paste(self, text, **kwargs):
        vals = dict(partner_id=self.customer.id, text=text, year=self.iso_year, first_week=self.iso_week)
        vals.update(kwargs)
        wizard = self.env['qb.forecast.paste'].create(vals)
        wizard.action_apply()
        return wizard

    def test_04_pegar_con_encabezado_de_semanas(self):
        w = self.iso_week
        if w > 50:
            self.skipTest("La prueba usa tres semanas seguidas del mismo año.")
        text = "\tsemana\t%d\t%d\t%d\n%s\tPARTE-9\t1,000\t\t2500\n" % (w, w + 1, w + 2, self.tela.default_code)
        wizard = self._paste(text, uom_origen='yd', plant='Silao')
        lines = self._lines()
        self.assertEqual({(l.week, l.qty_origen) for l in lines},
                         {(self.monday, 1000.0), (self.monday + timedelta(weeks=2), 2500.0)})
        self.assertEqual(set(lines.mapped('plant')), {'Silao'})
        self.assertEqual(set(lines.mapped('uom_origen')), {'yd'})
        self.assertEqual(set(lines.mapped('customer_part')), {'PARTE-9'})
        self.assertEqual(set(lines.mapped('origen')), {'excel'})
        self.assertIn('Semanas nuevas: 2', wizard.result)

    def test_05_pegar_otra_vez_cambia_y_la_semana_vacia_quita(self):
        code = self.tela.default_code
        self._paste("%s\t100\t200\n" % code)
        self._paste("%s\t150\t\n" % code)
        lines = self._lines()
        self.assertEqual({(l.week, l.qty_origen) for l in lines}, {(self.monday, 150.0)})
        self._paste("%s\t\t300\n" % code, replace_blanks=False)
        self.assertEqual({(l.week, l.qty_origen) for l in self._lines()},
                         {(self.monday, 150.0), (self.monday + timedelta(weeks=1), 300.0)},
                         "Sin «las vacías quitan», la semana vacía se respeta.")

    def test_06_no_pisa_release_ni_lo_pasado_y_reporta_codigos(self):
        release_line = self.Line.create({
            'company_id': self.env.company.id, 'partner_id': self.customer.id, 'product_id': self.tela.id,
            'week': self.monday, 'qty_origen': 70.0, 'origen': 'release'})
        wizard = self._paste("%s\t999\nZZNOEXISTE160\t5\n" % self.tela.default_code)
        self.assertEqual(release_line.qty_origen, 70.0, "Lo que escribió un release no se pisa.")
        self.assertIn('ZZNOEXISTE160', wizard.result)
        past = self.monday - timedelta(weeks=3)
        with self.assertRaises(UserError, msg="Lo pasado no se toca: no queda nada que pegar."):
            self._paste("%s\t40\n" % self.tela.default_code, year=past.isocalendar()[0],
                        first_week=past.isocalendar()[1])
        self.assertFalse(self._lines().filtered(lambda l: l.week == past))

    def test_07_pegar_por_parte_confirmada(self):
        part = self.env['qb.customer.part'].create({
            'partner_id': self.customer.id, 'customer_part': 'LX-777', 'product_id': self.tela.id})
        part.action_confirm()
        self._paste("LX-777\t250\n")
        line = self._lines()
        self.assertEqual((line.product_id, line.qty_origen, line.customer_part_id), (self.tela, 250.0, part))

    def test_08_bloque_sin_renglones(self):
        with self.assertRaises(UserError):
            self._paste("TOTAL\t5\n")

    # --- 3.2.0: matriz con el catálogo de semanas como columna ----------------
    def test_10_cada_renglon_lleva_su_semana_del_catalogo(self):
        line = self.Line.create({'partner_id': self.customer.id, 'product_id': self.tela.id,
                                 'week': self.monday + timedelta(days=2), 'qty_origen': 5.0})
        self.assertEqual(line.week_id.monday, self.monday)
        self.assertIn("-S%02d" % self.iso_week, line.week_id.name)
        line.week = self.monday + timedelta(weeks=1)
        self.assertEqual(line.week_id.monday, self.monday + timedelta(weeks=1))
        same = self.Line.create({'partner_id': self.customer.id, 'product_id': self.tela.id,
                                 'week': self.monday + timedelta(weeks=1), 'plant': 'Otra', 'qty_origen': 1.0})
        self.assertEqual(same.week_id, line.week_id, "Una sola semana del catálogo por lunes.")

    def test_11_la_matriz_muestra_las_proximas_13_semanas(self):
        far = self.monday + timedelta(weeks=30)
        far_week = self.env['qb.forecast.week']._qb_get([far])[far]
        weeks = self.Line._read_group_week_id(far_week, [])
        self.assertEqual(len(weeks), 14, "Las próximas 13 semanas, aunque estén vacías, más las que tienen datos.")
        self.assertEqual(weeks[-1], far_week, "En orden de fecha.")

    def test_12_matriz_por_semana_del_catalogo(self):
        week = self.env['qb.forecast.week']._qb_get([self.monday])[self.monday]
        domain = [('partner_id', '=', self.customer.id), ('plant', '=', False), ('product_id', '=', self.tela.id),
                  ('uom_origen', '=', False), ('week_id', '=', week.id)]
        self.Line.grid_update_cell(domain, 'qty_origen', 300.0)
        line = self._lines()
        self.assertEqual((line.week, line.week_id, line.qty_origen), (self.monday, week, 300.0))
        self.Line.grid_update_cell(domain, 'qty_origen', 50.0)
        self.assertEqual(line.qty_origen, 350.0)

    def test_13_vistas_de_matriz_cargan(self):
        grid = self.env.ref('quimibond_ventas_presupuesto.qb_forecast_line_view_grid')
        self.assertIn('grid', self.Line.get_views([(grid.id, 'grid')])['views'])
        budget_grid = self.env.ref('quimibond_ventas_presupuesto.sgi_sales_budget_line_grid_qty')
        self.assertIn('grid', self.env['sgi.sales.budget.line'].get_views([(budget_grid.id, 'grid')])['views'])

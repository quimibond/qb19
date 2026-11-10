# -*- coding: utf-8 -*-
"""19.0.3.8.0 — proyectos comerciales ligados a Desarrollo de producto (diseño v3, 2.6 y 2.11).
19.0.3.9.0 — origen «desarrollo» en la propuesta del año. Datos propios; años 2060–2069 para
aislar (en una copia de producción los desarrollos reales también entran a la propuesta: las
pruebas miran solo los suyos)."""
from datetime import date

from odoo.exceptions import ValidationError
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestSalesProject(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Budget = cls.env['sgi.sales.budget']
        cls.Line = cls.env['sgi.sales.budget.line']
        cls.Project = cls.env['qb.sales.project']
        cls.Dev = cls.env['project.project']
        Param = cls.env['ir.config_parameter'].sudo()
        Param.set_param('quimibond_sgi.budget_planning_rate', '0')
        Param.set_param('quimibond_sgi.price_min_plausible', '0')
        cls.team_ind = cls.env['crm.team'].create({'name': 'Industrial proyectos'})
        cls.team_conf = cls.env['crm.team'].create({'name': 'Confección proyectos'})
        Param.set_param('quimibond_ventas_presupuesto.team_industrial', cls.team_ind.id)
        Param.set_param('quimibond_ventas_presupuesto.team_confeccion', cls.team_conf.id)
        Param.set_param('quimibond_ventas_presupuesto.team_especiales', cls.team_ind.id)
        cls.uom_m = cls.env.ref('uom.product_uom_meter')
        cls.client = cls.env['res.partner'].create({
            'name': 'Cliente proyecto', 'is_company': True})
        cls.product = cls.env['product.product'].create({
            'name': 'Tela proyecto', 'default_code': 'WPROY160', 'type': 'consu',
            'uom_id': cls.uom_m.id})
        cls.stage = lambda key: cls.env.ref('quimibond_sgi.sgi_dev_stage_%s' % key)

    def _dev(self, **vals):
        base = {'name': 'Desarrollo prueba', 'sgi_is_ft': True, 'partner_id': self.client.id,
                'sgi_dev_product_name': 'Jersey proyecto', 'sgi_dev_team_id': self.team_ind.id,
                # Sin el análisis aprobado por Ventas, el SGI no deja pasar a Cotización.
                'sgi_dev_review_state': 'aprobado'}
        base.update(vals)
        return self.Dev.create(base)

    def _sales_project(self, dev):
        return self.Project.search([('dev_project_id', '=', dev.id)])

    def test_01_nace_en_cotizacion_con_cliente_y_articulo(self):
        dev = self._dev()
        dev.write({'stage_id': self.stage('cotizacion').id})
        self.assertFalse(self._sales_project(dev), "Sin artículo todavía no nace.")
        dev.write({'sgi_dev_product_id': self.product.id})
        project = self._sales_project(dev)
        self.assertEqual(len(project), 1)
        self.assertEqual((project.partner_id, project.product_id, project.mercado, project.origen,
                          project.state),
                         (self.client, self.product, 'proyectos_industrial', 'desarrollo', 'vigente'))
        self.assertEqual(dev.qb_sales_project_count, 1)
        action = dev.action_qb_view_sales_project()
        self.assertEqual(action['res_id'], project.id)

    def test_02_solicitud_no_crea(self):
        dev = self._dev(sgi_dev_product_id=self.product.id)
        self.assertFalse(self._sales_project(dev), "Antes de Cotización no hay proyecto comercial.")

    def test_03_producto_de_linea_usa_el_articulo_base(self):
        dev = self._dev(sgi_dev_analysis_result='linea', sgi_dev_base_product_id=self.product.id)
        dev.write({'stage_id': self.stage('cotizacion').id})
        self.assertEqual(self._sales_project(dev).product_id, self.product)

    def test_04_cerrado_sin_producto_queda_perdido_y_reabre(self):
        dev = self._dev(sgi_dev_product_id=self.product.id)
        dev.write({'stage_id': self.stage('cotizacion').id})
        project = self._sales_project(dev)
        dev.write({'stage_id': self.stage('cerrado_sin_producto').id})
        self.assertEqual(project.state, 'perdido')
        self.assertTrue(project.date_to)
        dev.write({'stage_id': self.stage('cotizacion').id})
        self.assertEqual(project.state, 'vigente', "Si el desarrollo vuelve, el proyecto se reabre.")
        self.assertFalse(project.date_to)

    def test_05_confeccion_va_a_proyectos_confeccion(self):
        dev = self._dev(sgi_dev_team_id=self.team_conf.id, sgi_dev_product_id=self.product.id)
        dev.write({'stage_id': self.stage('cotizacion').id})
        self.assertEqual(self._sales_project(dev).mercado, 'proyectos_confeccion')

    def test_06_renglon_sin_codigo_toma_el_producto_del_desarrollo(self):
        dev = self._dev()
        budget = self.Budget.create({'year': 2060})
        line = self.Line.create({'budget_id': budget.id, 'dev_project_id': dev.id,
                                 'date': date(2060, 4, 1), 'uom_id': self.uom_m.id,
                                 'qty_budget': 500.0})
        self.assertFalse(line.product_id)
        self.assertEqual(line.partner_id, self.client, "El cliente sale del desarrollo.")
        self.assertEqual(line.mercado, 'proyectos_industrial')
        self.assertEqual(line.proyecto_descripcion, 'Jersey proyecto')
        project = line.qb_project_id
        self.assertTrue(project, "El renglón de Proyectos crea su proyecto comercial.")
        self.assertEqual((project.dev_project_id, project.origen), (dev, 'presupuesto'))
        # El desarrollo genera su producto (el generador escribe sgi_dev_product_id).
        dev.write({'sgi_dev_product_id': self.product.id})
        self.assertEqual(project.product_id, self.product)
        self.assertEqual(line.product_id, self.product, "El renglón sin código toma el producto.")

    def test_07_aprobado_no_cambia_de_producto(self):
        dev = self._dev()
        budget = self.Budget.create({'year': 2061})
        line = self.Line.create({'budget_id': budget.id, 'dev_project_id': dev.id,
                                 'date': date(2061, 4, 1), 'uom_id': self.uom_m.id,
                                 'qty_budget': 500.0, 'price_input': 10.0})
        budget.sudo().write({'state': 'aprobado'})
        dev.write({'sgi_dev_product_id': self.product.id})
        self.assertFalse(line.product_id, "Un presupuesto aprobado es evidencia: no cambia.")
        self.assertEqual(line.qb_project_id.product_id, self.product)

    def test_08_renglon_de_proyectos_sin_desarrollo(self):
        budget = self.Budget.create({'year': 2062})
        line = self.Line.create({'budget_id': budget.id, 'product_id': self.product.id,
                                 'partner_id': self.client.id, 'mercado': 'proyectos_industrial',
                                 'date': date(2062, 1, 1), 'uom_id': self.uom_m.id,
                                 'qty_budget': 100.0})
        other = self.Line.create({'budget_id': budget.id, 'product_id': self.product.id,
                                  'partner_id': self.client.id, 'mercado': 'proyectos_industrial',
                                  'date': date(2062, 2, 1), 'uom_id': self.uom_m.id,
                                  'qty_budget': 100.0})
        self.assertTrue(line.qb_project_id)
        self.assertEqual(line.qb_project_id, other.qb_project_id, "Un proyecto por cliente y producto.")
        self.assertEqual(line.qb_project_id.date_from, date(2062, 1, 1))
        # Después llega el desarrollo del mismo cliente y artículo: se liga, no se duplica.
        dev = self._dev(sgi_dev_product_id=self.product.id)
        dev.write({'stage_id': self.stage('cotizacion').id})
        self.assertEqual(self._sales_project(dev), line.qb_project_id)
        self.assertEqual(line.dev_project_id, dev)
        # Un renglón de línea (fuera de Proyectos) no crea proyecto.
        plain = self.Line.create({'budget_id': budget.id, 'product_id': self.env['product.product'].create({
            'name': 'Línea', 'type': 'consu', 'uom_id': self.uom_m.id}).id,
            'partner_id': self.client.id, 'date': date(2062, 1, 1), 'uom_id': self.uom_m.id,
            'qty_budget': 1.0})
        self.assertFalse(plain.qb_project_id)

    def test_09_pasa_a_linea_al_aprobar_el_anio_siguiente(self):
        project = self.Project.create({'partner_id': self.client.id, 'product_id': self.product.id,
                                       'date_from': date(2063, 3, 1)})
        budget = self.Budget.create({'year': 2064})
        self.Line.create({'budget_id': budget.id, 'product_id': self.product.id,
                          'partner_id': self.client.id, 'mercado': 'industrial',
                          'date': date(2064, 1, 1), 'uom_id': self.uom_m.id, 'qty_budget': 10.0,
                          'price_input': 1.0})
        budget.sudo()._qb_graduate_projects()
        self.assertEqual(project.state, 'graduado')
        self.assertEqual(project.date_to, date(2063, 12, 31),
                         "3.16.3: pasa a línea al cierre del año anterior, no el día que se aprueba")

    def test_10_un_vigente_por_cliente_y_producto(self):
        self.Project.create({'partner_id': self.client.id, 'product_id': self.product.id})
        with self.assertRaises(ValidationError):
            self.Project.create({'partner_id': self.client.id, 'product_id': self.product.id})
        with self.assertRaises(ValidationError):
            self.Project.create({'partner_id': self.client.id})  # sin producto ni descripción

    def test_11_propuesta_trae_el_desarrollo_desde_la_fecha_requerida(self):
        """3.9.0: un desarrollo en Cotización con consumo anual da un renglón de Proyectos por
        mes desde su fecha requerida, con el precio objetivo y su origen."""
        dev = self._dev(sgi_dev_product_id=self.product.id, sgi_dev_consumption_annual=12000.0,
                        sgi_dev_consumption_uom='m', sgi_dev_required_date=date(2065, 10, 15),
                        sgi_dev_target_price=40.0, sgi_dev_volume_uom='m')
        dev.write({'stage_id': self.stage('cotizacion').id})
        sin_consumo = self._dev(name='Sin consumo', sgi_dev_product_name='Sin consumo')
        sin_consumo.write({'stage_id': self.stage('cotizacion').id})
        budget = self.Budget.browse(self.Budget.action_qb_build_proposal(2065, base='anio', dev_pct=50.0))
        lines = budget.line_ids.filtered(lambda l: l.dev_project_id == dev)
        self.assertEqual(sorted(lines.mapped('date')), [date(2065, 10, 1), date(2065, 11, 1), date(2065, 12, 1)])
        self.assertEqual(set(lines.mapped('qty_budget')), {500.0}, "12,000 m al año × 50 % / 12 meses")
        self.assertEqual(set(lines.mapped('qty_origen')), {'desarrollo'})
        self.assertEqual(set(lines.mapped('qty_sugerida')), {500.0})
        self.assertEqual(set(lines.mapped('mercado')), {'proyectos_industrial'})
        self.assertEqual(lines.partner_id, self.client)
        self.assertEqual(set(lines.mapped('price_input')), {40.0})
        self.assertEqual(set(lines.mapped('price_origen')), {'desarrollo'})
        self.assertEqual(lines.qb_project_id, self._sales_project(dev), "Ligados a su proyecto comercial.")
        self.assertFalse(budget.line_ids.filtered(lambda l: l.dev_project_id == sin_consumo))

    def test_12_propuesta_desarrollo_sin_codigo_en_kilos(self):
        """Sin producto todavía: renglón de proyecto con la descripción del desarrollo, en kilos."""
        dev = self._dev(sgi_dev_volume=300.0, sgi_dev_volume_uom='kg')
        dev.write({'stage_id': self.stage('cotizacion').id})
        budget = self.Budget.browse(self.Budget.action_qb_build_proposal(2066, base='anio'))
        lines = budget.line_ids.filtered(lambda l: l.dev_project_id == dev)
        self.assertEqual(len(lines), 12, "Sin fecha requerida: los 12 meses.")
        self.assertFalse(lines.product_id)
        self.assertEqual(lines.uom_id, self.env.ref('uom.product_uom_kgm'))
        self.assertEqual(set(lines.mapped('qty_budget')), {300.0})
        self.assertTrue(all(lines.mapped('proyecto_descripcion')))
        # Sin desarrollos: nada de él.
        other = self.Budget.browse(self.Budget.action_qb_build_proposal(2067, base='anio', include_dev=False))
        self.assertFalse(other.line_ids.filtered(lambda l: l.dev_project_id == dev))

    def test_13_pronostico_y_demanda_marcan_el_articulo_en_desarrollo(self):
        """3.10.0: el pronóstico y la Demanda por cliente dicen el estado de desarrollo del
        artículo (del producto o, si no lo tiene, de su artículo)."""
        tela = self.env['product.product'].create({
            'name': 'Tela en desarrollo', 'default_code': 'WDESA160', 'type': 'consu',
            'uom_id': self.uom_m.id, 'sgi_dev_state': 'desarrollo'})
        tela_kg = self.env['product.product'].create({
            'name': 'Tela en desarrollo kg', 'default_code': 'IWDESA160', 'type': 'consu',
            'uom_id': self.env.ref('uom.product_uom_kgm').id, 'qb_articulo_id': tela.id})
        Forecast = self.env['qb.forecast.line']
        line = Forecast.create({'partner_id': self.client.id, 'product_id': tela.id,
                                'week': date(2068, 3, 5), 'qty_origen': 100.0})
        line_kg = Forecast.create({'partner_id': self.client.id, 'product_id': tela_kg.id,
                                   'week': date(2068, 3, 5), 'qty_origen': 10.0})
        self.assertEqual(line.dev_state, 'desarrollo')
        self.assertEqual(line_kg.dev_state, 'desarrollo', "Sin estado propio: el de su artículo")
        self.assertIn(line, Forecast.search([('dev_state', 'in', ('desarrollo', 'archivado'))]))
        Demand = self.env['qb.demand.line']
        Demand.action_qb_rebuild()
        row = Demand.search([('forecast_line_id', '=', line.id)])
        self.assertEqual(row.dev_state, 'desarrollo')
        tela.product_tmpl_id.sgi_dev_state = 'pilotaje'
        self.assertEqual(line.dev_state, 'pilotaje', "Cambia con el artículo")
        self.assertEqual(line_kg.dev_state, 'pilotaje')
        tela.product_tmpl_id.sgi_dev_state = 'liberado'
        self.assertEqual(line.dev_state, 'liberado')
        plain = Forecast.create({'partner_id': self.client.id, 'product_id': self.product.id,
                                 'week': date(2068, 3, 5), 'qty_origen': 1.0})
        self.assertFalse(plain.dev_state, "Producto de línea: sin marca")

    def test_14_desarrollo_repetido_no_traba_la_etapa(self):
        """3.16.3: si otro proyecto vigente ya es de ese cliente y artículo, el segundo desarrollo
        que genera el mismo producto no queda trabado: su proyecto se queda sin producto y avisa."""
        first = self._dev(sgi_dev_product_id=self.product.id)
        first.write({'stage_id': self.stage('cotizacion').id})
        second = self._dev(name='Segundo desarrollo', sgi_dev_product_name='Segundo')
        budget = self.Budget.create({'year': 2069})
        line = self.Line.create({'budget_id': budget.id, 'dev_project_id': second.id,
                                 'date': date(2069, 4, 1), 'uom_id': self.uom_m.id, 'qty_budget': 5.0})
        project = line.qb_project_id
        second.write({'stage_id': self.stage('cotizacion').id, 'sgi_dev_product_id': self.product.id})
        self.assertFalse(project.product_id)
        self.assertTrue(project.message_ids.filtered(lambda m: 'ya es el proyecto vigente' in (m.body or '')))

    def test_15_presupuestado_no_suma_la_revision(self):
        """3.16.3: «Presupuestado» toma un documento por año (el aprobado), no también su revisión."""
        approved = self.Budget.create({'year': 2070})
        vals = {'product_id': self.product.id, 'partner_id': self.client.id,
                'mercado': 'proyectos_industrial', 'date': date(2070, 1, 1), 'uom_id': self.uom_m.id,
                'qty_budget': 10.0, 'price_input': 2.0}
        line = self.Line.create(dict(vals, budget_id=approved.id))
        approved.sudo().write({'state': 'aprobado'})
        draft = self.Budget.create({'year': 2070})
        self.Line.create(dict(vals, budget_id=draft.id))
        project = line.qb_project_id
        self.assertEqual(project.budget_line_count, 2)
        self.assertAlmostEqual(project.amount_budget, line.amount_budget, places=2)

    def test_16_copia_conserva_el_mercado(self):
        """3.16.3: un renglón con desarrollo que viene con mercado (copia, nueva revisión) lo conserva."""
        dev = self._dev(sgi_dev_product_id=self.product.id)
        budget = self.Budget.create({'year': 2071})
        line = self.Line.create({'budget_id': budget.id, 'dev_project_id': dev.id, 'mercado': 'industrial',
                                 'date': date(2071, 4, 1), 'uom_id': self.uom_m.id, 'qty_budget': 5.0})
        self.assertEqual(line.mercado, 'industrial')
        self.assertEqual(self.Line.create({'budget_id': budget.id, 'dev_project_id': dev.id,
                                           'date': date(2071, 5, 1), 'uom_id': self.uom_m.id,
                                           'qty_budget': 5.0}).mercado, 'proyectos_industrial',
                         "Sin mercado, el de proyectos del desarrollo")

    def test_17_propuesta_con_dos_desarrollos_iguales(self):
        """3.16.3: dos desarrollos del mismo cliente y artículo no tumban la propuesta: entra uno."""
        for name in ('Uno', 'Dos'):
            dev = self._dev(name=name, sgi_dev_product_id=self.product.id, sgi_dev_consumption_annual=1200.0,
                            sgi_dev_consumption_uom='m', sgi_dev_volume_uom='m', sgi_dev_target_price=10.0)
            dev.write({'stage_id': self.stage('cotizacion').id})
        budget = self.Budget.browse(self.Budget.action_qb_build_proposal(2072, base='anio'))
        lines = budget.line_ids.filtered(lambda l: l.product_id == self.product)
        self.assertEqual(len(lines), 12, "Un renglón por mes, de uno solo de los dos desarrollos")

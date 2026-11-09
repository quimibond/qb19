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
            'name': 'Cliente proyecto', 'is_company': True, 'team_id': cls.team_ind.id})
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

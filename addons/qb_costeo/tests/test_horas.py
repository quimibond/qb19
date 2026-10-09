# -*- coding: utf-8 -*-
from datetime import date, datetime

from odoo.tests import tagged

from .common import CosteoCase


@tagged('post_install', '-at_install')
class TestHoras(CosteoCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        kg = cls.env.ref('uom.product_uom_kgm')
        m = cls.env.ref('uom.product_uom_meter')
        cls.kg, cls.m = kg, m
        # Centro de tejido con una máquina de galga 18 y una de galga 24.
        cal = cls.wc_tin.resource_calendar_id
        tag18 = cls.env['mrp.workcenter.tag'].create({'name': 'GALGA 18'})
        tag24 = cls.env['mrp.workcenter.tag'].create({'name': 'GALGA 24'})
        cls.wc_g18 = cls.env['mrp.workcenter'].create({
            'name': 'CIRCULAR G18', 'resource_calendar_id': cal.id,
            'tag_ids': [(6, 0, [tag18.id])]})
        cls.wc_g24 = cls.env['mrp.workcenter'].create({
            'name': 'CIRCULAR G24', 'resource_calendar_id': cal.id,
            'tag_ids': [(6, 0, [tag24.id])]})
        cls.c_tej = cls.env['qb.centro'].create({
            'code': 'TEJ', 'name': 'Tejido', 'nature': 'directo',
            'driver': 'workorder', 'velocidad_min': 2, 'velocidad_max': 25,
            'workcenter_ids': [(6, 0, [cls.wc_g18.id, cls.wc_g24.id])]})

        def prod(code, uom):
            return cls.env['product.product'].create({
                'name': code, 'default_code': code, 'type': 'consu',
                'is_storable': True, 'uom_id': uom.id})
        cls.crudo = prod('WJ080Q21HNT165', kg)        # galga 18, con órdenes
        cls.crudo_h = prod('WJ080Q21HBL155', kg)      # hermano: otro color/ancho
        cls.crudo_g = prod('WJ090Q25HNT170', kg)      # galga 18, sin órdenes ni hermano
        cls.crudo_x = prod('WJ090Q50HNT170', kg)      # galga 24: solo promedio
        cls.tenido = prod('WJ080Q21INT165', kg)
        cls.terminado = prod('WJ080Q21JNT165', m)
        Bom = cls.env['mrp.bom']
        cls.bom_i = Bom.create({
            'product_tmpl_id': cls.tenido.product_tmpl_id.id, 'product_qty': 1,
            'product_uom_id': kg.id,
            'bom_line_ids': [(0, 0, {'product_id': cls.crudo.id,
                                     'product_qty': 1, 'product_uom_id': kg.id})],
            'operation_ids': [(0, 0, {'name': 'Teñir', 'workcenter_id': cls.wc_tin.id,
                                      'time_cycle_manual': 30})]})  # 0.5 h/kg
        cls.bom_j = Bom.create({
            'product_tmpl_id': cls.terminado.product_tmpl_id.id, 'product_qty': 100,
            'product_uom_id': m.id,
            'bom_line_ids': [(0, 0, {'product_id': cls.tenido.id,
                                     'product_qty': 13.76, 'product_uom_id': kg.id})],
            'operation_ids': [(0, 0, {'name': 'Rama', 'workcenter_id': cls.wc_aca.id,
                                      'time_cycle_manual': 4})]})  # 4 min / 100 m
        cls.H = cls.env['qb.producto.horas']

    def _mo_tejido(self, product, kg, horas, wc=None, fecha=None):
        wc = wc or self.wc_g18
        mo = self.env['mrp.production'].create({
            'product_id': product.id, 'product_qty': kg,
            'product_uom_id': product.uom_id.id})
        mo.action_confirm()
        mo.qty_producing = kg
        mo.button_mark_done()
        fin = fecha or datetime(2026, 9, 10, 12)
        mo.write({'date_finished': fin})
        wo = self.env['mrp.workorder'].create({
            'name': 'WO', 'production_id': mo.id, 'workcenter_id': wc.id,
            'product_uom_id': product.uom_id.id})
        wo.write({'state': 'done', 'duration': horas * 60, 'date_finished': fin})
        return mo

    def _fila(self, product, centro):
        return self.H.search([('product_id', '=', product.id),
                              ('centro_id', '=', centro.id)])

    def test_medido_con_banda_y_herencia(self):
        self._mo_tejido(self.crudo, 100, 10)     # 10 kg/h
        self._mo_tejido(self.crudo, 200, 20)     # 10 kg/h
        self._mo_tejido(self.crudo, 100, 1)      # 100 kg/h: fuera de banda
        self.H.recalcular(self.crudo | self.tenido | self.terminado)
        f = self._fila(self.crudo, self.c_tej)
        self.assertEqual(f.fuente, 'medido')
        self.assertAlmostEqual(f.horas_propias, 0.1, 6)
        self.assertEqual(f.n_ordenes, 2)
        self.assertEqual(f.calidad, 'alta')
        # El teñido hereda el tejido del crudo y tiene su estándar de tintorería.
        ft = self._fila(self.tenido, self.c_tej)
        self.assertEqual(ft.fuente, 'ninguna')
        self.assertAlmostEqual(ft.horas_heredadas, 0.1, 6)
        fti = self._fila(self.tenido, self.c_tin)
        self.assertEqual(fti.fuente, 'estandar')
        self.assertAlmostEqual(fti.horas_propias, 0.5, 6)
        # El terminado: 13.76 kg por 100 m → 0.1376 kg/m.
        fj = self._fila(self.terminado, self.c_tej)
        self.assertAlmostEqual(fj.horas_unidad, 0.1 * 0.1376, 6)
        fja = self._fila(self.terminado, self.c_aca)
        self.assertEqual(fja.fuente, 'estandar')
        self.assertAlmostEqual(fja.horas_propias, 4 / 60.0 / 100, 6)
        fjt = self._fila(self.terminado, self.c_tin)
        self.assertAlmostEqual(fjt.horas_heredadas, 0.5 * 0.1376, 6)

    def test_hermano_galga_y_estimado(self):
        self._mo_tejido(self.crudo, 100, 10)                      # g18: 10 kg/h
        self._mo_tejido(self.crudo_x, 100, 5, wc=self.wc_g24)     # g24: 20 kg/h
        otro = self.env['product.product'].create({
            'name': 'WK200R46HNT160', 'default_code': 'WK200R46HNT160',
            'type': 'consu', 'is_storable': True, 'uom_id': self.kg.id})
        self.H.recalcular(self.crudo_h | self.crudo_g | otro)
        # Hermano: misma raíz WJ080Q21H.
        fh = self._fila(self.crudo_h, self.c_tej)
        self.assertEqual(fh.fuente, 'hermano')
        self.assertAlmostEqual(fh.horas_propias, 0.1, 6)
        self.assertEqual(fh.calidad, 'media')
        # Galga 18 sin hermano: promedio de las máquinas galga 18 (10 kg/h).
        fg = self._fila(self.crudo_g, self.c_tej)
        self.assertEqual(fg.fuente, 'galga')
        self.assertAlmostEqual(fg.horas_propias, 0.1, 6)
        # Galga 24 (otro, R46): máquina galga 24 a 20 kg/h.
        fo = self._fila(otro, self.c_tej)
        self.assertEqual(fo.fuente, 'galga')
        self.assertAlmostEqual(fo.horas_propias, 0.05, 6)

    def test_estimado_sin_galga(self):
        self._mo_tejido(self.crudo, 100, 10)
        sin = self.env['product.product'].create({
            'name': 'CRUDO ESPECIAL', 'default_code': 'CRUDOESP',
            'type': 'consu', 'is_storable': True, 'uom_id': self.kg.id})
        # Sin nomenclatura ni receta no se sabe su centro: no hay fila.
        self.H.recalcular(sin)
        self.assertFalse(self._fila(sin, self.c_tej))
        # Con una operación en tejido (sin tiempo) sí aplica → estimado.
        self.env['mrp.bom'].create({
            'product_tmpl_id': sin.product_tmpl_id.id, 'product_qty': 1,
            'product_uom_id': self.kg.id,
            'operation_ids': [(0, 0, {'name': 'Tejer', 'workcenter_id': self.wc_g18.id,
                                      'time_cycle_manual': 0})]})
        self.H.recalcular(sin)
        f = self._fila(sin, self.c_tej)
        self.assertEqual(f.fuente, 'estimado')
        self.assertAlmostEqual(f.horas_propias, 0.1, 6)
        self.assertEqual(f.calidad, 'baja')

    def test_manual_con_vigencia(self):
        self._mo_tejido(self.crudo, 100, 10)
        self.H.recalcular(self.crudo)
        f = self._fila(self.crudo, self.c_tej)
        f.write({'horas_manual': 0.08, 'manual_motivo': 'máquina nueva',
                 'manual_vigente_hasta': date(2099, 1, 1)})
        self.H.recalcular(self.crudo)
        f = self._fila(self.crudo, self.c_tej)
        self.assertEqual(f.fuente, 'manual')
        self.assertAlmostEqual(f.horas_propias, 0.08, 6)
        # Vencido: vuelve a lo medido, el dato manual se conserva.
        f.manual_vigente_hasta = date(2020, 1, 1)
        self.H.recalcular(self.crudo)
        f = self._fila(self.crudo, self.c_tej)
        self.assertEqual(f.fuente, 'medido')
        self.assertAlmostEqual(f.horas_manual, 0.08, 6)

    def test_receta_vigente_es_la_de_mas_cantidad(self):
        bom2 = self.env['mrp.bom'].create({
            'product_tmpl_id': self.tenido.product_tmpl_id.id, 'product_qty': 1,
            'product_uom_id': self.kg.id,
            'operation_ids': [(0, 0, {'name': 'Teñir largo', 'workcenter_id': self.wc_tin.id,
                                      'time_cycle_manual': 120})]})  # 2 h/kg
        for _ in range(3):
            mo = self.env['mrp.production'].create({
                'product_id': self.tenido.id, 'product_qty': 100,
                'product_uom_id': self.kg.id, 'bom_id': bom2.id})
            mo.action_confirm()
            mo.qty_producing = 100
            mo.button_mark_done()
        self.assertEqual(self.H._bom_vigente(self.tenido), bom2)
        self.H.recalcular(self.tenido)
        f = self._fila(self.tenido, self.c_tin)
        self.assertAlmostEqual(f.horas_propias, 2.0, 6)

    def test_driver_del_centro_y_diagnostico(self):
        """Sin órdenes ni receta en el centro, las horas salen de su
        configuración: velocidad de rama (m), horas por carga (kg), horas
        fijas. Calidad baja, fuente «driver»."""
        self.c_aca.velocidad_m_h = 1500
        self.c_tin.write({'kg_por_carga': 400, 'horas_por_carga': 8})
        tela = self.env['product.product'].create({
            'name': 'WJ090Q21JBL160', 'default_code': 'WJ090Q21JBL160',
            'type': 'consu', 'is_storable': True, 'uom_id': self.m.id})
        tenido = self.env['product.product'].create({
            'name': 'WJ090Q21IBL160', 'default_code': 'WJ090Q21IBL160',
            'type': 'consu', 'is_storable': True, 'uom_id': self.kg.id})
        self.H.recalcular(tela | tenido)
        fa = self._fila(tela, self.c_aca)
        self.assertEqual(fa.fuente, 'driver')
        self.assertAlmostEqual(fa.horas_propias, 1 / 1500.0, 6)
        self.assertEqual(fa.calidad, 'baja')
        self.assertIn('Velocidad', fa.detalle)
        ft = self._fila(tenido, self.c_tin)
        self.assertEqual(ft.fuente, 'driver')
        self.assertAlmostEqual(ft.horas_propias, 8 / 400.0, 6)
        # Con velocidad en 0 el centro no inventa horas.
        self.c_aca.velocidad_m_h = 0
        self.H.recalcular(tela)
        self.assertFalse(self._fila(tela, self.c_aca))
        # Diagnóstico por MCP: qué ve _medido.
        self._mo_tejido(self.crudo, 100, 10)
        d = self.H.diagnostico_medido(self.crudo.id, self.c_tej.id)
        self.assertEqual(d['producto']['ordenes_trabajo'], 1)
        self.assertAlmostEqual(d['producto']['qty_terminada'], 100, 2)
        self.assertAlmostEqual(d['producto']['TEJ'][0], 0.1, 6)
        self.assertEqual(d['centros'][0]['productos_medidos'], 1)

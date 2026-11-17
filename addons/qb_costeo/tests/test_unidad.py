# -*- coding: utf-8 -*-
"""1.12.0: absorción por unidad producida (kg o m del departamento)."""
from datetime import date

from odoo.exceptions import ValidationError
from odoo.tests import tagged

from .common import CosteoCase


@tagged('post_install', '-at_install')
class TestAbsorcionUnidad(CosteoCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        wh = cls.env['stock.warehouse'].search(
            [('company_id', '=', cls.company.id)], limit=1)
        cls.tipo_aca = cls.env['stock.picking.type'].create({
            'name': 'Acabado prueba', 'code': 'mrp_operation',
            'sequence_code': 'QBACA', 'warehouse_id': wh.id})
        metro = cls.env.ref('uom.product_uom_meter')
        cls.tela = cls.env['product.product'].create({
            'name': 'Tela acabada', 'type': 'consu', 'is_storable': True,
            'uom_id': metro.id})
        cls.bom_tela = cls.env['mrp.bom'].create({
            'product_tmpl_id': cls.tela.product_tmpl_id.id,
            'product_qty': 1, 'product_uom_id': metro.id,
            'picking_type_id': cls.tipo_aca.id})
        cls.c_aca.write({'absorcion': 'unidad',
                         'unidad_tipo_ids': [(6, 0, [cls.tipo_aca.id])]})
        cls.env['qb.parametro'].set_float('absorcion_unidad_meses', 1)

    def producir(self, qty, fecha):
        mo = self.env['mrp.production'].create({
            'product_id': self.tela.id, 'product_qty': qty,
            'product_uom_id': self.tela.uom_id.id, 'bom_id': self.bom_tela.id,
            'picking_type_id': self.tipo_aca.id})
        mo.action_confirm()
        mo.qty_producing = qty
        mo.button_mark_done()
        mo.write({'date_finished': fecha})
        self.env.flush_all()
        return mo

    def _tarifa(self, centro):
        return self.periodo.tarifa_ids.filtered(
            lambda t: t.centro_id == centro)

    def test_tarifa_por_unidad(self):
        """El gasto de Acabado entre los metros acabados del mes; el gasto
        indirecto ya no se reparte por horas (unidades distintas) sino por
        nómina directa (sin RH: partes iguales)."""
        self.gasto(self.acc_renta, 100000)      # 60k TIN / 40k ACA
        self.gasto(self.acc_mant, 30000)        # indirecto
        self.producir(1000, '2026-09-12 10:00:00')
        self.periodo.action_calcular()
        aca = self._tarifa(self.c_aca)
        self.assertEqual(aca.horas_reales_fuente, 'unidad')
        self.assertEqual(aca.horas_fuente, 'promedio')
        self.assertAlmostEqual(aca.horas_reales, 1000.0, 2)
        self.assertAlmostEqual(aca.horas_normales, 1000.0, 2)
        self.assertAlmostEqual(aca.pool_fijo, 40000 + 15000, 0)
        self.assertAlmostEqual(aca.tarifa_fija, 55.0, 4)   # $ por metro
        self.assertAlmostEqual(aca.ociosidad, 0.0, 2)

    def test_ociosidad_contra_promedio(self):
        """La capacidad normal es el promedio mensual de producción: un mes
        flojo deja sin absorber la parte que le faltó."""
        self.env['qb.parametro'].set_float('absorcion_unidad_meses', 2)
        self.gasto(self.acc_renta, 100000)      # 40k ACA
        self.producir(3000, '2026-08-12 10:00:00')
        self.producir(1000, '2026-09-12 10:00:00')
        self.periodo.action_calcular()
        aca = self._tarifa(self.c_aca)
        self.assertAlmostEqual(aca.horas_normales, 2000.0, 2)
        self.assertAlmostEqual(aca.horas_reales, 1000.0, 2)
        self.assertAlmostEqual(aca.tarifa_fija, aca.pool_fijo / 2000.0, 4)
        self.assertAlmostEqual(aca.ociosidad, aca.pool_fijo / 2.0, 0)
        self.assertFalse(aca.capacidad_superada)

    def test_unidades_heredadas_por_receta(self):
        """La tela hecha en el centro carga 1 m; lo que se fabrica con ella
        hereda sus metros por receta."""
        self.producir(1000, '2026-09-12 10:00:00')
        final = self.env['product.product'].create({
            'name': 'Entretela con recubrimiento', 'type': 'consu',
            'is_storable': True, 'uom_id': self.tela.uom_id.id})
        self.env['mrp.bom'].create({
            'product_tmpl_id': final.product_tmpl_id.id, 'product_qty': 1,
            'product_uom_id': final.uom_id.id,
            'bom_line_ids': [(0, 0, {
                'product_id': self.tela.id, 'product_qty': 2,
                'product_uom_id': self.tela.uom_id.id})]})
        Horas = self.env['qb.producto.horas']
        Horas.recalcular(self.tela | final)
        h_tela = Horas.search([('product_id', '=', self.tela.id),
                               ('centro_id', '=', self.c_aca.id)])
        h_final = Horas.search([('product_id', '=', final.id),
                                ('centro_id', '=', self.c_aca.id)])
        self.assertEqual(h_tela.fuente, 'unidad')
        self.assertAlmostEqual(h_tela.horas_propias, 1.0, 6)
        self.assertEqual(h_tela.calidad, 'alta')
        self.assertAlmostEqual(h_final.horas_propias, 0.0, 6)
        self.assertAlmostEqual(h_final.horas_unidad, 2.0, 6)

    def test_no_publica_tarifa_por_unidad(self):
        """Una tarifa en $ por metro no se escribe como costo por hora."""
        self.c_aca.publica_tarifa = True
        self.gasto(self.acc_renta, 100000)
        self.producir(1000, '2026-09-12 10:00:00')
        self.periodo.action_calcular()
        self.periodo._publicar_tarifas()
        self.assertEqual(self.wc_aca.costs_hour, 5)

    def test_unidad_exige_tipos(self):
        with self.assertRaises(ValidationError):
            self.c_tin.write({'absorcion': 'unidad'})

    def test_produccion_del_mes(self):
        self.producir(700, '2026-09-05 10:00:00')
        self.producir(300, '2026-10-02 10:00:00')   # fuera de septiembre
        self.assertAlmostEqual(self.c_aca.produccion_unidades(
            date(2026, 9, 1), date(2026, 10, 1)), 700.0, 2)

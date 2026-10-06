# -*- coding: utf-8 -*-
from datetime import date

from odoo.tests import tagged

from .common import CosteoCase


@tagged('post_install', '-at_install')
class TestCosto(CosteoCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        kg = cls.env.ref('uom.product_uom_kgm')
        m = cls.env.ref('uom.product_uom_meter')
        cls.hilo = cls.env['product.product'].create({
            'name': 'HILO', 'default_code': 'HILO', 'type': 'consu',
            'is_storable': True, 'uom_id': kg.id, 'standard_price': 40.0})
        cls.crudo = cls.env['product.product'].create({
            'name': 'crudo', 'default_code': 'WJ080Q21HNT165', 'type': 'consu',
            'is_storable': True, 'uom_id': kg.id})
        cls.tela = cls.env['product.product'].create({
            'name': 'tela', 'default_code': 'WJ080Q21JNT165', 'type': 'consu',
            'is_storable': True, 'uom_id': m.id, 'list_price': 15.0})
        Bom = cls.env['mrp.bom']
        Bom.create({'product_tmpl_id': cls.crudo.product_tmpl_id.id, 'product_qty': 1,
                    'product_uom_id': kg.id,
                    'bom_line_ids': [(0, 0, {'product_id': cls.hilo.id, 'product_qty': 1.0,
                                             'product_uom_id': kg.id})],
                    'operation_ids': [(0, 0, {'name': 'Tejer', 'workcenter_id': cls.wc_tin.id,
                                              'time_cycle_manual': 6})]})   # 0.1 h/kg
        Bom.create({'product_tmpl_id': cls.tela.product_tmpl_id.id, 'product_qty': 100,
                    'product_uom_id': m.id,
                    'bom_line_ids': [(0, 0, {'product_id': cls.crudo.id, 'product_qty': 10,
                                             'product_uom_id': kg.id})],
                    'operation_ids': [(0, 0, {'name': 'Rama', 'workcenter_id': cls.wc_aca.id,
                                              'time_cycle_manual': 3})]})   # 0.0005 h/m

    def _factura(self, product, qty, precio, fecha=date(2026, 9, 12)):
        partner = self.env['res.partner'].create({'name': 'Cliente'})
        inv = self.env['account.move'].create({
            'move_type': 'out_invoice', 'partner_id': partner.id,
            'invoice_date': fecha,
            'invoice_line_ids': [(0, 0, {
                'product_id': product.id, 'quantity': qty, 'price_unit': precio,
                'account_id': self.acc_ventas.id, 'tax_ids': [(5, 0, 0)]})]})
        inv.action_post()
        return inv

    def test_costo_por_producto(self):
        # Tarifas: TIN (el tejido de prueba corre en wc_tin) y ACA.
        self.gasto(self.acc_renta, 100000)      # 60k TIN / 40k ACA
        self.gasto(self.acc_luz, 9000)          # TIN variable
        self.gasto(self.acc_admin, 50000)
        self.venta(500000)
        self.workorder_done(self.wc_tin, 90)
        self.workorder_done(self.wc_aca, 44)
        self._factura(self.tela, 1000, 15.0)
        self.periodo.action_calcular()
        self.periodo.action_calcular_costos()
        f = self.periodo.costo_ids.filtered(lambda r: r.product_id == self.tela)
        self.assertTrue(f)
        tin = self.periodo.tarifa_ids.filtered(lambda t: t.centro_id == self.c_tin)
        aca = self.periodo.tarifa_ids.filtered(lambda t: t.centro_id == self.c_aca)
        # MP: 10 kg crudo / 100 m × 1 kg hilo × $40 = $4.00/m
        self.assertAlmostEqual(f.mp_unit, 4.0, 4)
        # Horas: tejido 0.1 h/kg × 0.1 kg/m = 0.01 h/m en TIN; rama 3 min/100 m.
        fab = 0.01 * tin.tarifa + 0.0005 * aca.tarifa
        self.assertAlmostEqual(f.fabricacion_unit, fab, 3)
        self.assertAlmostEqual(f.energia_unit, 0.01 * tin.tarifa_variable
                               + 0.0005 * aca.tarifa_variable, 3)
        self.assertEqual(len(f.linea_ids), 2)
        self.assertAlmostEqual(f.costo_produccion, 4.0 + fab, 3)
        # Ventas: 1000 m a $15 (más la venta genérica sin producto).
        self.assertAlmostEqual(f.qty_vendida, 1000, 2)
        self.assertAlmostEqual(f.precio_prom, 15.0, 4)
        self.assertAlmostEqual(f.op_pct, self.periodo.op_pct, 6)
        self.assertAlmostEqual(f.op_unit, self.periodo.op_pct * 15.0, 4)
        self.assertAlmostEqual(f.costo_total, f.costo_vendible + f.op_unit, 4)
        self.assertAlmostEqual(f.piso_lleno, f.costo_vendible / (1 - f.op_pct), 4)
        self.assertAlmostEqual(f.margen_neto_pct, 100 * (15 - f.costo_total) / 15, 2)
        self.assertAlmostEqual(f.margen_neto_total, 15000 - f.costo_total * 1000, 1)
        self.assertEqual(f.calidad, 'alta')         # horas estándar, sin validaciones
        self.assertIn(f.semaforo, ('verde', 'ambar', 'rojo'))
        # Resumen del período y compuerta.
        self.assertAlmostEqual(self.periodo.costos_ventas, 15000, 1)
        self.assertEqual(self.periodo.costos_calidad_pct, 100.0)
        self.assertNotIn('costo por producto', self.periodo.bloqueos or '')

    def test_sin_costos_no_cierra(self):
        self.gasto(self.acc_renta, 1000)
        self.workorder_done(self.wc_tin, 1)
        self.workorder_done(self.wc_aca, 1)
        self.periodo.action_calcular()
        self.assertIn('costo por producto', self.periodo.bloqueos)

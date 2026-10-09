# -*- coding: utf-8 -*-
from datetime import date

from odoo.exceptions import UserError
from odoo.tests import tagged

from .common import CosteoCase


@tagged('post_install', '-at_install')
class TestPeriodo(CosteoCase):

    def _tarifa(self, centro):
        return self.periodo.tarifa_ids.filtered(
            lambda t: t.centro_id == centro)

    def test_tarifa_por_centro_con_reparto(self):
        """Renta 60/40, luz a tintorería, nómina sin centro por partes
        iguales (sin RH), mantenimiento indirecto por horas normales."""
        self.gasto(self.acc_renta, 100000)      # 60k TIN / 40k ACA
        self.gasto(self.acc_luz, 9000)          # TIN variable
        self.gasto(self.acc_nomina, 20000)      # sin RH → partes iguales
        self.gasto(self.acc_mant, 30000)        # indirecto → por horas
        self.gasto(self.acc_admin, 50000)
        self.venta(500000)
        self.workorder_done(self.wc_tin, 90)
        self.workorder_done(self.wc_aca, 44)

        self.periodo.action_calcular()
        tin = self._tarifa(self.c_tin)
        aca = self._tarifa(self.c_aca)

        # Septiembre 2026 tiene 22 días hábiles × 8 h = 176 h; la rama al 50 %.
        self.assertAlmostEqual(tin.horas_normales, 176.0, 1)
        self.assertAlmostEqual(aca.horas_normales, 88.0, 1)
        self.assertEqual(tin.horas_fuente, 'calendario')
        self.assertEqual(tin.horas_reales, 90.0)
        self.assertEqual(tin.horas_reales_fuente, 'workorder')

        # Mantenimiento 30k por horas normales: 176/264 → TIN 20k, ACA 10k.
        self.assertAlmostEqual(tin.pool_fijo, 60000 + 10000 + 20000, 0)
        self.assertAlmostEqual(aca.pool_fijo, 40000 + 10000 + 10000, 0)
        self.assertAlmostEqual(tin.pool_variable, 9000, 0)
        self.assertAlmostEqual(aca.pool_variable, 0, 0)

        self.assertAlmostEqual(tin.tarifa_fija, 90000 / 176.0, 2)
        self.assertAlmostEqual(tin.tarifa_variable, 9000 / 90.0, 2)
        self.assertAlmostEqual(tin.tarifa, 90000 / 176.0 + 100.0, 2)
        # Ociosidad: pool fijo − tarifa fija × horas reales.
        self.assertAlmostEqual(tin.ociosidad, 90000 - 90000 / 176.0 * 90, 0)
        self.assertAlmostEqual(aca.ociosidad, 60000 - 60000 / 88.0 * 44, 0)
        self.assertAlmostEqual(self.periodo.op_pct, 0.1, 4)
        self.assertAlmostEqual(self.periodo.ociosidad_total,
                               tin.ociosidad + aca.ociosidad, 0)

    def test_capacidad_capturada_y_superada(self):
        self.c_tin.write({'capacidad_h_mes': 50,
                          'capacidad_motivo': 'una sola máquina'})
        self.gasto(self.acc_renta, 10000)
        self.workorder_done(self.wc_tin, 60)
        self.periodo.action_calcular()
        tin = self._tarifa(self.c_tin)
        self.assertEqual(tin.horas_fuente, 'capturada')
        self.assertTrue(tin.capacidad_superada)
        self.assertEqual(tin.horas_normales, 60.0)  # IAS 2: la real
        self.assertEqual(tin.ociosidad, 0.0)

    def test_horas_estandar_sin_workorders(self):
        """Sin órdenes de trabajo, las horas reales salen del estándar de la
        receta × lo producido."""
        product = self.env['product.product'].create({
            'name': 'Tela std', 'type': 'consu', 'is_storable': True,
            'uom_id': self.env.ref('uom.product_uom_meter').id})
        bom = self.env['mrp.bom'].create({
            'product_tmpl_id': product.product_tmpl_id.id,
            'product_qty': 100, 'product_uom_id': product.uom_id.id,
            'operation_ids': [(0, 0, {
                'name': 'Rama', 'workcenter_id': self.wc_aca.id,
                'time_cycle_manual': 30})],   # 30 min por 100 m
        })
        mo = self.env['mrp.production'].create({
            'product_id': product.id, 'product_qty': 1000,
            'product_uom_id': product.uom_id.id, 'bom_id': bom.id})
        mo.action_confirm()
        mo.qty_producing = 1000
        mo.button_mark_done()
        mo.write({'date_finished': '2026-09-12 10:00:00'})
        horas, fuente = self.c_aca.horas_reales(date(2026, 9, 1),
                                                 date(2026, 10, 1))
        self.assertEqual(fuente, 'operacion')
        self.assertAlmostEqual(horas, 5.0, 2)   # 1000 m × 0.5 h / 100 m

    def test_compuerta_de_cierre(self):
        self.gasto(self.acc_renta, 10000)
        self.gasto(self.acc_sin, 5000)           # sin clasificar
        with self.assertRaises(UserError):
            self.periodo.action_cerrar()         # no calculado
        self.periodo.action_calcular()
        self.assertIn('sin clasificar', self.periodo.bloqueos)
        self.assertIn('no tiene horas reales', self.periodo.bloqueos)
        with self.assertRaises(UserError):
            self.periodo.action_cerrar()
        # Se corrige: clasificar la cuenta y registrar horas.
        self.env['qb.cuenta.clase'].create({
            'account_id': self.acc_sin.id, 'bucket': 'no_costeo'})
        self.workorder_done(self.wc_tin, 10)
        self.workorder_done(self.wc_aca, 10)
        self.periodo.action_calcular()
        self.assertIn('costo por producto', self.periodo.bloqueos)
        self.periodo.action_calcular_costos()
        self.assertFalse(self.periodo.bloqueos)
        self.periodo.action_cerrar()
        self.assertEqual(self.periodo.state, 'cerrado')
        # Publicación: solo el centro marcado, redondeada a centavos.
        tin = self._tarifa(self.c_tin)
        self.assertEqual(self.wc_tin.costs_hour, round(tin.tarifa, 2))
        self.assertEqual(self.wc_aca.costs_hour, 5)
        self.assertTrue(tin.publicada)
        # Cerrado: no se recalcula; reabrir pide motivo.
        with self.assertRaises(UserError):
            self.periodo.action_calcular()
        with self.assertRaises(UserError):
            self.periodo.action_reabrir()
        self.periodo.motivo_reapertura = 'prueba'
        self.periodo.action_reabrir()
        self.assertEqual(self.periodo.state, 'borrador')
        self.assertEqual(self.periodo.reaperturas, 1)

    def test_cierre_bloqueado_por_validacion_en_top(self):
        product = self.env['product.product'].create({
            'name': 'Tela top', 'type': 'consu', 'is_storable': True})
        comp = self.env['product.product'].create({
            'name': 'Hilo cero', 'type': 'consu', 'standard_price': 0,
            'uom_id': self.env.ref('uom.product_uom_kgm').id})
        self.env['mrp.bom'].create({
            'product_tmpl_id': product.product_tmpl_id.id,
            'product_qty': 1, 'product_uom_id': product.uom_id.id,
            'bom_line_ids': [(0, 0, {'product_id': comp.id, 'product_qty': 1,
                                     'product_uom_id': comp.uom_id.id})]})
        self.env['qb.producto.validacion'].revisar(product)
        # Venta del producto en el mes → entra al top.
        self.asiento([(self.acc_ventas, 0, 1000), (self.acc_contra, 1000, 0)])
        move = self.env['account.move'].search(
            [('journal_id', '=', self.journal.id)], order='id desc', limit=1)
        move.button_draft()
        move.line_ids.filtered(
            lambda l: l.account_id == self.acc_ventas).product_id = product
        move.action_post()
        self.gasto(self.acc_renta, 1000)
        self.workorder_done(self.wc_tin, 1)
        self.workorder_done(self.wc_aca, 1)
        self.periodo.action_calcular()
        self.assertIn('Validaciones abiertas', self.periodo.bloqueos)
        self.assertIn('Tela top', self.periodo.bloqueos)

    def test_refs_excluidas_y_suavizado(self):
        self.gasto(self.acc_renta, 12000, ref='CIERRE ANUAL 2025')
        self.gasto(self.acc_renta, 6000, fecha=date(2026, 8, 10))
        self.gasto(self.acc_renta, 3000)
        self.workorder_done(self.wc_tin, 1)
        self.workorder_done(self.wc_aca, 1)
        self.periodo.suavizado_fijo_meses = 2
        self.periodo.action_calcular()
        # (6000 + 3000) / 2 meses = 4500; el cierre anual no entra.
        total = sum(self.periodo.tarifa_ids.mapped('pool_fijo'))
        self.assertAlmostEqual(total, 4500, 0)

    def test_para_cotizar_es_el_ultimo_cerrado(self):
        Periodo = self.env['qb.periodo']
        self.assertFalse(Periodo.para_cotizar())
        self.gasto(self.acc_renta, 1000)
        self.workorder_done(self.wc_tin, 1)
        self.workorder_done(self.wc_aca, 1)
        self.periodo.action_calcular()
        self.periodo.action_calcular_costos()
        self.periodo.action_cerrar()
        Periodo.create({'period': date(2026, 10, 1)})
        self.assertEqual(Periodo.para_cotizar(), self.periodo)

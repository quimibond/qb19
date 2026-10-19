# -*- coding: utf-8 -*-
"""1.6.0: rentabilidad por cliente y por producto con el costo de este módulo."""
from dateutil.relativedelta import relativedelta

from odoo import fields
from odoo.tests import tagged

from .common import CosteoCase


@tagged('post_install', '-at_install')
class TestRentabilidad(CosteoCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        m = cls.env.ref('uom.product_uom_meter')
        cls.hoy = fields.Date.today()
        cls.mes = cls.hoy.replace(day=1)
        Periodo = cls.env['qb.periodo']
        # Un período del mes de las facturas y otro viejo (el «último costeado» de otro producto).
        cls.p_mes = Periodo.search([('period', '=', cls.mes), ('company_id', '=', cls.company.id)]) or \
            Periodo.create({'period': cls.mes})
        viejo = cls.mes - relativedelta(months=3)
        cls.p_viejo = Periodo.search([('period', '=', viejo), ('company_id', '=', cls.company.id)]) or \
            Periodo.create({'period': viejo})

        def prod(code):
            return cls.env['product.product'].create({
                'name': code, 'default_code': code, 'type': 'consu', 'uom_id': m.id})
        cls.tela = prod('WJ060Q21JNT165')
        cls.vieja = prod('WJ070Q21JNT165')
        cls.sin_costo = prod('WJ080Q21JNT165')
        Costo = cls.env['qb.costo.unitario']
        Costo.create({'periodo_id': cls.p_mes.id, 'product_id': cls.tela.id, 'company_id': cls.company.id,
                      'costo_variable': 20, 'costo_vendible': 30, 'op_pct': 0.10, 'calidad': 'alta'})
        Costo.create({'periodo_id': cls.p_viejo.id, 'product_id': cls.vieja.id, 'company_id': cls.company.id,
                      'costo_variable': 10, 'costo_vendible': 15, 'op_pct': 0.10, 'calidad': 'baja'})
        cls.bueno = cls.env['res.partner'].create({'name': 'Cliente que paga'})
        cls.malo = cls.env['res.partner'].create({'name': 'Cliente barato'})
        cls.mixto = cls.env['res.partner'].create({'name': 'Cliente sin costo'})

    def _factura(self, partner, product, qty, precio):
        inv = self.env['account.move'].create({
            'move_type': 'out_invoice', 'partner_id': partner.id, 'invoice_date': self.hoy,
            'invoice_line_ids': [(0, 0, {
                'product_id': product.id, 'quantity': qty, 'price_unit': precio,
                'account_id': self.acc_ventas.id, 'tax_ids': [(5, 0, 0)]})]})
        inv.action_post()
        return inv

    def test_margenes_semaforo_y_cobertura(self):
        self._factura(self.bueno, self.tela, 100, 50)      # 5,000; costo de su mes
        self._factura(self.malo, self.tela, 100, 25)       # 2,500
        self._factura(self.malo, self.vieja, 100, 20)      # 2,000; costo del período viejo
        self._factura(self.mixto, self.sin_costo, 100, 40)  # 4,000 sin costo
        self._factura(self.mixto, self.tela, 10, 50)        # 500 con costo
        Cliente = self.env['qb.rentabilidad.cliente']
        bueno = Cliente.search([('partner_id', '=', self.bueno.id)])
        self.assertAlmostEqual(bueno.revenue_12m, 5000, 2)
        self.assertAlmostEqual(bueno.contrib_12m, 3000, 2)        # 5,000 − 100 × 20
        self.assertAlmostEqual(bueno.margen_bruto_12m, 2000, 2)   # 5,000 − 100 × 30
        self.assertAlmostEqual(bueno.margen_neto_12m, 1500, 2)    # 5,000 × 0.9 − 3,000
        self.assertAlmostEqual(bueno.margen_neto_pct, 30, 2)
        self.assertEqual(bueno.semaforo, 'verde')
        self.assertAlmostEqual(bueno.mismo_mes_pct, 100, 2)
        self.assertAlmostEqual(bueno.calidad_alta_pct, 100, 2)
        malo = Cliente.search([('partner_id', '=', self.malo.id)])
        # tela: 2,500 × 0.9 − 3,000 = −750; vieja: 2,000 × 0.9 − 1,500 = 300.
        self.assertAlmostEqual(malo.margen_neto_12m, -450, 2)
        self.assertEqual(malo.semaforo, 'rojo')
        self.assertAlmostEqual(malo.mismo_mes_pct, 2500 / 4500 * 100, 2)
        self.assertIn('Pierde', malo.veredicto)
        mixto = Cliente.search([('partner_id', '=', self.mixto.id)])
        self.assertAlmostEqual(mixto.cobertura_pct, 500 / 4500 * 100, 2)
        self.assertEqual(mixto.semaforo, 'sin_costo', 'Con 11 % de la venta costeada no hay veredicto')
        Producto = self.env['qb.rentabilidad.producto']
        tela = Producto.search([('product_id', '=', self.tela.id)])
        self.assertAlmostEqual(tela.qty_12m, 210, 2)
        self.assertAlmostEqual(tela.precio_prom, 8000 / 210, 4)
        self.assertEqual(tela.n_clientes, 3)
        # 8,000 × 0.9 − 210 × 30 = 900.
        self.assertAlmostEqual(tela.margen_neto_12m, 900, 2)
        self.assertEqual(Producto.search([('product_id', '=', self.sin_costo.id)]).semaforo, 'sin_costo')
        self.assertIn(self.malo, Cliente.search([('semaforo', '=', 'rojo')]).partner_id)

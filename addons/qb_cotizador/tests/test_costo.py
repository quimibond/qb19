# -*- coding: utf-8 -*-
from datetime import date

from odoo.exceptions import UserError
from odoo.tests import tagged

from .common import CotizadorCase


@tagged('post_install', '-at_install')
class TestCosto(CotizadorCase):

    def test_01_hermano_y_manual(self):
        cot = self._cot(product_id=False, spec_descripcion='Jersey 150 nuevo', spec_gramaje=150,
                        spec_ancho=1.7, spec_galga='28', costo_fuente='hermano',
                        hermano_product_id=self.hermana.id)
        cot.action_calcular()
        self.assertEqual(cot.costo_id.product_id, self.hermana)
        self.assertAlmostEqual(cot.piso_ocioso, 8.0, places=4)
        self.assertIn('Hermano', cot.supuestos)
        manual = self._cot(product_id=False, spec_descripcion='Sin receta', costo_fuente='manual',
                           mp_unit=5.0, energia_unit=0.5, fabricacion_unit=2.0, rendimiento=1.0)
        manual.action_calcular()
        self.assertAlmostEqual(manual.costo_variable, 5.5)
        self.assertAlmostEqual(manual.costo_produccion, 7.0)
        self.assertEqual(manual.op_pct, 0.10, 'Sin operación capturada toma la del período')
        self.assertEqual(manual.calidad, 'baja')

    def test_02_sin_costo_o_sin_periodo(self):
        sin = self.env['product.product'].create({'name': 'Sin costo', 'type': 'consu'})
        cot = self._cot(product_id=sin.id)
        with self.assertRaises(UserError):
            cot.action_calcular()
        self.periodo.write({'state': 'borrador'})
        with self.assertRaises(UserError, msg='Sin período cerrado no se cotiza'):
            self._cot().action_calcular()

    def test_03_divisa(self):
        # En el CI la compañía está en USD; en producción en MXN. La divisa
        # de la prueba es la que NO sea la de la compañía.
        usd, eur = self.env.ref('base.USD'), self.env.ref('base.EUR')
        divisa = eur if self.company.currency_id == usd else usd
        divisa.write({'active': True})
        self.env['res.currency.rate'].create({'currency_id': divisa.id, 'name': date(2026, 1, 1),
                                              'company_rate': 1 / 18.0, 'company_id': self.company.id})
        cot = self._cot(currency_id=divisa.id, precio_objetivo=1.0)
        cot.action_calcular()
        self.assertTrue(cot.es_divisa)
        self.assertAlmostEqual(cot.fx_rate, 18.0, places=2)
        self.assertAlmostEqual(cot.precio_mxn, 18.0, places=2)
        self.assertEqual(cot.semaforo, 'verde')
        self.assertAlmostEqual(cot.piso_lleno_divisa, cot.piso_lleno / 18.0, places=4)

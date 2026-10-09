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
        # 1.3.0: sin período cerrado se cotiza con el último que tenga costos, marcado provisional.
        self.periodo.write({'state': 'borrador'})
        prov = self._cot()
        prov.action_calcular()
        self.assertEqual(prov.periodo_id, self.periodo)
        self.assertEqual(prov.calidad, 'baja')
        self.assertIn('sin cerrar', prov.calidad_detalle)
        self.assertGreater(prov.piso_lleno, 0)
        self.env['qb.costo.unitario'].sudo().search([('periodo_id', '=', self.periodo.id)]).unlink()
        with self.assertRaises(UserError, msg='Sin ningún período con costos no se cotiza'):
            self._cot().action_calcular()

    def test_02b_sin_costo_no_hay_semaforo(self):
        # 1.3.0: antes de calcular el costo el piso es 0 y el semáforo salía verde («cubre todo»).
        cot = self._cot(precio_objetivo=14.0)
        self.assertTrue(cot.sin_costo)
        self.assertFalse(cot.semaforo)
        self.assertEqual(cot.margen_neto_pct, 0.0)
        with self.assertRaises(UserError, msg='Sin costo no se pide aprobación'):
            cot.action_enviar_aprobacion()
        cot.action_calcular()
        self.assertFalse(cot.sin_costo)
        self.assertEqual(cot.semaforo, 'verde')

    def test_03_divisa(self):
        # En el CI la compañía está en USD; en producción en MXN. La divisa
        # de la prueba es la que NO sea la de la compañía.
        usd, eur = self.env.ref('base.USD'), self.env.ref('base.EUR')
        divisa = eur if self.company.currency_id == usd else usd
        divisa.write({'active': True})
        self.env['res.currency.rate'].create({'currency_id': divisa.id, 'name': date(2026, 1, 1),
                                              'company_rate': 1 / 18.0, 'company_id': self.company.id})
        cot = self._cot(currency_id=divisa.id, precio_objetivo=1.0)
        # 1.3.0: nace con el TC de Odoo, no con 1.0.
        self.assertTrue(cot.es_divisa)
        self.assertAlmostEqual(cot.fx_rate, 18.0, places=2)
        self.assertEqual(cot.fx_fuente, 'odoo')
        self.assertEqual(cot.fx_fecha, date(2026, 1, 1))
        self.assertFalse(cot.moneda_alerta)
        cot.action_calcular()
        self.assertAlmostEqual(cot.fx_rate, 18.0, places=2)
        self.assertAlmostEqual(cot.precio_mxn, 18.0, places=2)
        self.assertEqual(cot.semaforo, 'verde')
        self.assertAlmostEqual(cot.piso_lleno_divisa, cot.piso_lleno / 18.0, places=4)
        # TC capturado a mano: el cálculo lo respeta; «TC de hoy» vuelve al de Odoo.
        cot.write({'fx_rate': 17.5, 'fx_fuente': 'manual'})
        cot.action_calcular()
        self.assertAlmostEqual(cot.fx_rate, 17.5, places=4)
        self.assertAlmostEqual(cot.precio_mxn, 17.5, places=2)
        self.assertIn('Capturado a mano', cot.supuestos)
        cot.action_tc_hoy()
        self.assertAlmostEqual(cot.fx_rate, 18.0, places=2)
        self.assertEqual(cot.fx_fuente, 'odoo')
        # Divisa con TC 1.0 (capturado así): alerta y no se manda a aprobar.
        mal = self._cot(currency_id=divisa.id, precio_objetivo=1.0, fx_rate=1.0, fx_fuente='manual')
        self.assertAlmostEqual(mal.fx_rate, 1.0)
        self.assertIn('tipo de cambio es 1.0000', mal.moneda_alerta)
        mal.action_calcular()
        with self.assertRaises(UserError, msg='Divisa con TC 1 no se aprueba'):
            mal.action_enviar_aprobacion()
        # El onchange de la moneda trae el TC de Odoo y el de la compañía lo regresa a 1.
        form = self.env['qb.cotizador.cotizacion'].new({'partner_id': self.partner.id, 'currency_id': divisa.id})
        form._onchange_currency_id()
        self.assertAlmostEqual(form.fx_rate, 18.0, places=2)
        form.currency_id = self.company.currency_id
        form._onchange_currency_id()
        self.assertAlmostEqual(form.fx_rate, 1.0)
        form.currency_id = divisa
        form.fx_rate = 17.0
        form._onchange_fx_rate()
        self.assertEqual(form.fx_fuente, 'manual')

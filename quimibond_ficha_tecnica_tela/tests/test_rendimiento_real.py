# -*- coding: utf-8 -*-
"""2.24.0: un solo rendimiento para planear, de lo fabricado (metros ÷ kilos), y la especificación contra lo real."""
from unittest.mock import patch

from odoo.tests import tagged

from ..models.rendimiento_real import ancho_en_m, es_tela_kg
from .test_tiempo_ficha import TiempoFichaCase


@tagged('post_install', '-at_install')
class TestRendimientoReal(TiempoFichaCase):

    def _con_fabricado(self, datos):
        Acabado = type(self.env['ficha.tecnica.acabado'])
        return patch.object(Acabado, '_ft_fabricado', return_value=datos)

    def test_01_que_cuenta_como_tela(self):
        self.assertTrue(es_tela_kg('WJ044Q22INT235'), "Teñido (I en la posición 9)")
        self.assertTrue(es_tela_kg('WJ044Q22HNT235'), "Crudo")
        self.assertTrue(es_tela_kg('IWJ053Q22JNT160'), "Acabado en kg")
        self.assertFalse(es_tela_kg('ENGOMADO002'), "Los químicos no son tela")
        self.assertEqual((ancho_en_m(160), ancho_en_m(1.6)), (1.6, 1.6))

    def test_02_con_historia_el_rendimiento_es_el_fabricado(self):
        with self._con_fabricado({self.tela_m: (5099.0, 477.0, 4)}):
            self.acabado._ft_actualizar_rendimiento_real()
        self.assertEqual(self.acabado.rendimiento_origen, 'real')
        self.assertAlmostEqual(self.acabado.rendimiento_real, 5099.0 / 477.0, places=4)
        self.assertAlmostEqual(self.acabado.rendimiento_tela_acabada, round(5099.0 / 477.0, 4), places=6,
                               msg="Uno solo: el de la ficha pasa a ser el de lo fabricado")
        self.assertEqual(self.acabado.rendimiento_real_ordenes, 4)
        self.assertEqual(self.tela_m.product_tmpl_id.ft_rendimiento_origen, 'real')

    def test_03_con_pocas_ordenes_se_queda_el_capturado(self):
        with self._con_fabricado({self.tela_m: (1000.0, 100.0, 2)}):
            self.acabado._ft_actualizar_rendimiento_real()
        self.assertEqual(self.acabado.rendimiento_origen, 'ficha')
        self.assertEqual(self.acabado.rendimiento_tela_acabada, 8.0, "Con 2 órdenes no se toca el capturado")
        self.assertAlmostEqual(self.acabado.rendimiento_real, 10.0, places=4, msg="Pero se muestra lo fabricado")

    def test_04_especificacion_contra_lo_real(self):
        self.acabado.write({'ancho_acabado': 165, 'peso_acabado_tol': 3})
        with self._con_fabricado({self.tela_m: (10000.0, 1000.0, 5)}):
            self.acabado._ft_actualizar_rendimiento_real()
        espec = 1000.0 / (53 * 1.65)
        self.assertAlmostEqual(self.acabado.rendimiento_especificacion, espec, places=4)
        self.assertAlmostEqual(self.acabado.rendimiento_diferencia, (10.0 / espec - 1) * 100, places=1)
        peso_real = 1000.0 / (10.0 * 1.65)
        self.assertAlmostEqual(self.acabado.peso_real, peso_real, places=1)
        self.assertEqual(self.acabado.peso_real_resultado, 'no_conforme', "60.6 g/m² contra 53 ± 3")
        self.acabado.peso_acabado_tol = 10
        self.assertEqual(self.acabado.peso_real_resultado, 'cumple')

    def test_05_caracteristicas_con_su_valor_real(self):
        masa = self.env.ref('quimibond_ficha_tecnica_tela.caracteristica_masa', raise_if_not_found=False)
        rend = self.env.ref('quimibond_ficha_tecnica_tela.caracteristica_rendimiento')
        Spec = self.env['ficha.tecnica.spec']
        linea = Spec.create({'acabado_id': self.acabado.id, 'caracteristica_id': rend.id, 'spec_nominal': 11,
                             'spec_tol_minus': 0.5, 'spec_tol_plus': 0.5})
        with self._con_fabricado({self.tela_m: (10000.0, 1000.0, 5)}):
            self.acabado._ft_actualizar_rendimiento_real()
        self.assertAlmostEqual(linea.real_valor, 10.0, places=3)
        self.assertEqual(linea.real_resultado, 'no_conforme', "10 m/kg contra 11 ± 0.5")
        if masa:
            peso = Spec.create({'acabado_id': self.acabado.id, 'caracteristica_id': masa.id})
            self.assertAlmostEqual(peso.real_valor, self.acabado.peso_real, places=3)
            self.assertFalse(peso.real_resultado, "Sin especificación numérica no hay resultado")

    def test_06_la_inspeccion_usa_el_mismo_rendimiento(self):
        with self._con_fabricado({self.tela_m: (9000.0, 1000.0, 3)}):
            self.acabado._ft_actualizar_rendimiento_real()
        self.env.invalidate_all()
        self.assertIn('9 m/kg', self.op_insp.ft_tiempo_calculo or '')

    def test_07_sin_productos_no_hay_fabricado(self):
        Acabado = self.env['ficha.tecnica.acabado']
        self.assertEqual(Acabado._ft_fabricado(self.env['product.product'], '2026-01-01'), {})

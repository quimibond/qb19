# -*- coding: utf-8 -*-
"""2.24.0: un solo rendimiento de tela acabada, calculado con lo fabricado (metros ÷ kilos), y el peso real."""
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

    def test_02_con_ordenes_el_rendimiento_es_el_fabricado(self):
        with self._con_fabricado({self.tela_m: (5099.0, 477.0, 4)}):
            self.acabado._ft_actualizar_rendimiento_real()
        self.assertAlmostEqual(self.acabado.rendimiento_tela_acabada, round(5099.0 / 477.0, 4), places=6)
        self.assertEqual(self.acabado.rendimiento_real_ordenes, 4)
        self.assertAlmostEqual(self.tela_m.product_tmpl_id.ft_rendimiento, round(5099.0 / 477.0, 4), places=6)

    def test_03_sin_ordenes_se_queda_el_de_la_ficha(self):
        with self._con_fabricado({}):
            self.acabado._ft_actualizar_rendimiento_real()
        self.assertEqual(self.acabado.rendimiento_tela_acabada, 8.0)
        self.assertFalse(self.acabado.rendimiento_real_ordenes)
        self.assertFalse(self.acabado.peso_real, "Sin lo fabricado no hay peso real")

    def test_04_peso_real(self):
        self.acabado.ancho_acabado = 165
        with self._con_fabricado({self.tela_m: (10000.0, 1000.0, 5)}):
            self.acabado._ft_actualizar_rendimiento_real()
        self.assertAlmostEqual(self.acabado.peso_real, 1000.0 / (10.0 * 1.65), places=1)

    def test_05_la_inspeccion_usa_el_mismo_rendimiento(self):
        with self._con_fabricado({self.tela_m: (9000.0, 1000.0, 3)}):
            self.acabado._ft_actualizar_rendimiento_real()
        self.env.invalidate_all()
        self.assertIn('9 m/kg', self.op_insp.ft_tiempo_calculo or '')

    def test_06_sin_productos_no_hay_fabricado(self):
        Acabado = self.env['ficha.tecnica.acabado']
        self.assertEqual(Acabado._ft_fabricado(self.env['product.product'], '2026-01-01'), {})

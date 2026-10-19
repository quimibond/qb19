# -*- coding: utf-8 -*-
"""2.10.0: unidades del catálogo en lugar de texto libre (el texto se conserva sincronizado)."""
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestUnidades(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Unidad = cls.env['ficha.tecnica.unidad']
        ref = cls.env.ref
        cls.g_m2 = ref('quimibond_ficha_tecnica_tela.ft_unidad_g_m2')
        cls.cm = ref('quimibond_ficha_tecnica_tela.ft_unidad_cm')
        cls.m = ref('quimibond_ficha_tecnica_tela.ft_unidad_m')
        cls.g = ref('quimibond_ficha_tecnica_tela.ft_unidad_g')
        cls.pto = ref('quimibond_ficha_tecnica_tela.ft_unidad_pto')
        cls.por_in = ref('quimibond_ficha_tecnica_tela.ft_unidad_por_in')
        cls.inch = ref('quimibond_ficha_tecnica_tela.ft_unidad_in')

    def test_01_resolver(self):
        self.assertEqual(self.Unidad._ft_resolver(' G/M2 '), self.g_m2, "Sin importar mayúsculas, espacios ni ²")
        self.assertEqual(self.Unidad._ft_resolver('gr'), self.g, "Por alias (así viene en producción)")
        self.assertEqual(self.Unidad._ft_resolver('in'), self.inch)
        self.assertEqual(self.Unidad._ft_resolver('in', 'columnas_tol_unit'), self.por_in,
                         "En columnas y mallas «in» es por pulgada")
        self.assertFalse(self.Unidad._ft_resolver('zz'))
        self.assertAlmostEqual(self.cm._ft_factor_a(self.env.ref('uom.product_uom_meter')), 0.01)

    def test_02_ficha_sincroniza_texto_y_catalogo(self):
        ficha = self.env['ficha.tecnica.tejido'].create({'articulo': 'ZZ200Q22HNT200', 'tension_tol_unit': 'PTO',
                                                         'columnas_tol_unit': 'in'})
        self.assertEqual(ficha.peso_acondicionado_tol_unit_id, self.g_m2, "La unidad por defecto también se liga")
        self.assertEqual(ficha.tension_tol_unit_id, self.pto)
        self.assertEqual(ficha.columnas_tol_unit_id, self.por_in)
        ficha.ancho_acondicionado_tol_unit_id = self.m
        self.assertEqual(ficha.ancho_acondicionado_tol_unit, 'm', "Elegir la unidad escribe su símbolo (lo lee el SGI)")
        ficha.estiraje_tol_unit = 'zz'
        self.assertFalse(ficha.estiraje_tol_unit_id)
        self.assertIn('«zz»', ficha.unidades_aviso)
        acabado_param = self.env['ficha.tecnica.maquina.parametro'].create({
            'tipo_id': self.env.ref('quimibond_ficha_tecnica_tela.ft_maquina_tipo_acabado').id,
            'code': 'vel_prueba', 'name': 'Velocidad prueba', 'unit': 'm/min', 'ambito': 'articulo'})
        self.assertEqual(acabado_param.unit_id, self.env.ref('quimibond_ficha_tecnica_tela.ft_unidad_m_min'))

    def test_03_rendimiento_con_la_unidad_real(self):
        # Como WJ044Q22HNT235 en producción: 2.18 «cm» que son metros.
        ficha = self.env['ficha.tecnica.tejido'].create({
            'articulo': 'ZZ044Q22HNT235', 'peso_acondicionado': 54.26, 'ancho_acondicionado': 2.18})
        self.assertEqual(ficha._ft_rendimiento(), 0.0, "2.18 cm no es un ancho creíble")
        ficha.ancho_acondicionado_tol_unit_id = self.m
        self.assertAlmostEqual(ficha._ft_rendimiento(), 1000 / (54.26 * 2.18), places=4)

    def test_04_migracion_no_toca_el_texto(self):
        ficha = self.env['ficha.tecnica.tejido'].create({'articulo': 'ZZ300Q22HNT200', 'polea_alimentacion_tol_unit': 'gr'})
        ficha.with_context(ft_unidad_sin_texto=True).polea_alimentacion_tol_unit_id = False
        self.assertEqual(ficha.polea_alimentacion_tol_unit, 'gr')
        sin = self.env['ficha.tecnica.tejido']._ft_ligar_unidades()
        self.assertEqual(ficha.polea_alimentacion_tol_unit_id, self.g)
        self.assertEqual(ficha.polea_alimentacion_tol_unit, 'gr', "El texto de origen se conserva")
        self.assertNotIn('gr', sin)

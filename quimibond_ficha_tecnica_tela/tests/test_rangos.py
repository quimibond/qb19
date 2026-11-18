# -*- coding: utf-8 -*-
"""2.30.0: las medidas de la ficha y las características se capturan también como rango (mínimo a máximo)."""
from odoo.exceptions import ValidationError
from odoo.tests import Form, TransactionCase, tagged

from ..models.rangos import MEDIDAS_ACABADO, MEDIDAS_TEJIDO, rango_a_tolerancias


@tagged('post_install', '-at_install')
class TestRangos(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.tejido = cls.env['ficha.tecnica.tejido'].create({'articulo': 'WJ997Q22HNT160'})
        product = cls.env['product.product'].create({'name': 'Tela prueba rangos'})
        cls.acabado = cls.env['ficha.tecnica.acabado'].create({
            'articulo': 'WJ997Q22JNT160', 'tejido_id': cls.tejido.id, 'product_acabado_id': product.id})
        cls.Spec = cls.env['ficha.tecnica.spec']

    def test_01_funcion(self):
        self.assertEqual(rango_a_tolerancias(0, 50, 70), (60, 10, 10))  # sin nominal: al centro
        self.assertEqual(rango_a_tolerancias(60, 50, 65), (60, 5, 10))  # nominal dentro: se queda
        self.assertEqual(rango_a_tolerancias(80, 50, 70), (60, 10, 10))  # nominal fuera: al centro
        self.assertEqual(rango_a_tolerancias(-3, -5, 0), (-3, 3, 2))  # encogimiento negativo
        self.assertIsNone(rango_a_tolerancias(0, 70, 50))

    def test_02_campos(self):
        for model, medidas in (('ficha.tecnica.acabado', MEDIDAS_ACABADO), ('ficha.tecnica.tejido', MEDIDAS_TEJIDO)):
            fields_ = self.env[model]._fields
            for nominal, plus, minus, _label, _digits in medidas:
                for name in (nominal, plus, minus, nominal + '_min', nominal + '_max'):
                    self.assertIn(name, fields_, name)
        self.assertEqual(self.acabado._fields['espesor_acabado_min'].get_digits(self.env), (16, 3))

    def test_03_calcula_rango(self):
        self.acabado.write({'espesor_acabado': 0.34, 'espesor_acabado_tol': 0.04, 'espesor_acabado_tol_minus': 0.02})
        self.assertAlmostEqual(self.acabado.espesor_acabado_min, 0.32)
        self.assertAlmostEqual(self.acabado.espesor_acabado_max, 0.38)

    def test_04_escribe_rango(self):
        """Jose: elongación de 50 a 70 % sin nominal; ancho 160 de 158 a 165 (asimétrica)."""
        self.acabado.write({'elongacion_largo_acabado_min': 50, 'elongacion_largo_acabado_max': 70,
                            'ancho_acabado': 160, 'ancho_acabado_min': 158, 'ancho_acabado_max': 165})
        a = self.acabado
        self.assertEqual((a.elongacion_largo_acabado, a.elongacion_largo_acabado_tol,
                          a.elongacion_largo_acabado_tol_minus), (60, 10, 10))
        self.assertEqual((a.ancho_acabado, a.ancho_acabado_tol, a.ancho_acabado_tol_minus), (160, 5, 2))
        self.tejido.write({'espesor_acondicionado_min': 0.020, 'espesor_acondicionado_max': 0.024})
        self.assertAlmostEqual(self.tejido.espesor_acondicionado, 0.022)
        self.assertAlmostEqual(self.tejido.espesor_acondicionado_tol_minus, 0.002)

    def test_05_rango_al_reves(self):
        with self.assertRaises(ValidationError):
            self.acabado.write({'espesor_acabado_min': 0.4, 'espesor_acabado_max': 0.3})

    def test_06_pantalla(self):
        """En el formulario, el rango llena nominal y tolerancias al momento; con un solo extremo espera al otro."""
        with Form(self.acabado) as form:
            form.elongacion_ancho_acabado_min = 20
            self.assertEqual(form.elongacion_ancho_acabado, 0)  # 20 a 0: al revés, espera
            form.elongacion_ancho_acabado_max = 40
            self.assertEqual(form.elongacion_ancho_acabado, 30)
            self.assertEqual(form.elongacion_ancho_acabado_tol, 10)
        self.assertEqual(self.acabado.elongacion_ancho_acabado_tol_minus, 10)

    def test_07_otras_medidas_no_cambian(self):
        self.acabado.write({'peso_acabado': 120, 'peso_acabado_tol': 6, 'peso_acabado_tol_minus': 0})
        self.acabado.write({'espesor_acabado_min': 0.3, 'espesor_acabado_max': 0.4})
        self.assertEqual((self.acabado.peso_acabado_tol, self.acabado.peso_acabado_tol_minus), (6, 0))

    def test_08_caracteristica_rango(self):
        line = self.Spec.create({'acabado_id': self.acabado.id, 'name': 'Elongación', 'unit': '%',
                                 'spec_limit': 'range', 'spec_min': 50, 'spec_max': 70})
        self.assertEqual((line.spec_nominal, line.spec_tol_minus, line.spec_tol_plus), (60, 10, 10))
        self.assertEqual(line.spec_label, "50 a 70 %")
        self.assertEqual(line._result_for(55), 'cumple')
        self.assertEqual(line._result_for(72), 'no_conforme')
        line.write({'ctrl_tol_minus': 5, 'ctrl_tol_plus': 5})
        self.assertEqual(line.ctrl_label, "55 a 65 %")
        self.assertEqual(line._result_for(52), 'desviacion')
        line.write({'spec_min': 40})
        self.assertEqual((line.spec_nominal, line.spec_tol_minus, line.spec_tol_plus), (60, 20, 10))
        with self.assertRaises(ValidationError):
            line.write({'spec_min': 80})

    def test_09_caracteristica_pct_a_rango(self):
        """Al pasar de «± %» a rango, la tolerancia queda en unidades y el rango no cambia."""
        line = self.Spec.create({'acabado_id': self.acabado.id, 'name': 'Masa', 'unit': 'g/m²',
                                 'spec_nominal': 200, 'spec_tol_minus': 5, 'spec_tol_plus': 5, 'spec_tol_pct': True})
        self.assertEqual((line.spec_min, line.spec_max), (190, 210))
        with Form(self.acabado) as form:
            with form.spec_line_ids.edit(len(form.spec_line_ids) - 1) as row:
                row.spec_limit = 'range'
        line.invalidate_recordset()
        self.assertFalse(line.spec_tol_pct)
        self.assertEqual((line.spec_min, line.spec_max), (190, 210))
        self.assertEqual(line.spec_label, "190 a 210 g/m²")

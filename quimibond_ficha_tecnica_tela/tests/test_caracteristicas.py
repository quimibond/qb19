# -*- coding: utf-8 -*-
"""2.1.0: catálogo de características, claves de codificación y renglones con
dos juegos de límites en las fichas de tejido y acabado."""
from odoo.exceptions import ValidationError
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestCaracteristicas(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Car = cls.env['ficha.tecnica.caracteristica']
        cls.Spec = cls.env['ficha.tecnica.spec']
        cls.tejido = cls.env['ficha.tecnica.tejido'].create({'articulo': 'WJ053Q22HNT160'})
        product = cls.env['product.product'].create({'name': 'Tela prueba acabada'})
        cls.acabado = cls.env['ficha.tecnica.acabado'].create({
            'articulo': 'WJ053Q22JNT160', 'tejido_id': cls.tejido.id, 'product_acabado_id': product.id})

    def test_01_catalogo_sembrado(self):
        self.assertEqual(self.Car._by_code('masa').unit, "g/m²")
        self.assertEqual(self.Car._by_code('rendimiento').formula, 'rendimiento')
        self.assertEqual(self.Car._by_code('engomado_orillas').kind, 'bool')
        self.assertEqual(self.Car.search_count([]), 38)
        Clave = self.env['ficha.tecnica.clave.codigo']
        counts = {k: Clave.search_count([('kind', '=', k)]) for k in
                  ('composicion', 'dibujo', 'hilo', 'galga', 'operacion', 'color', 'acabado')}
        self.assertEqual(counts, {'composicion': 11, 'dibujo': 15, 'hilo': 3, 'galga': 9, 'operacion': 3,
                                  'color': 20, 'acabado': 12})
        self.assertEqual(Clave.gauge_code(18), '21')
        self.assertEqual(Clave.gauge_from_code('22'), 18)
        self.assertEqual(Clave.gauge_from_code('xx'), 0)
        self.assertIn("Dryfit", Clave.search([('kind', '=', 'acabado'), ('code', '=', 'DI')]).name)
        self.assertEqual(Clave.search([('kind', '=', 'galga'), ('gauge', '=', 14)]).display_name, "Galga 14 (01-10)")
        with self.assertRaises(ValidationError):
            Clave.create({'kind': 'galga', 'code': '96', 'name': 'Galga 34', 'gauge': 34,
                          'range_from': 96, 'range_to': 100})

    def test_02_limites_absolutos_y_porcentaje(self):
        line = self.Spec.create({'acabado_id': self.acabado.id, 'caracteristica_id': self.Car._by_code('masa').id,
                                 'spec_nominal': 53, 'spec_tol_minus': 3, 'spec_tol_plus': 3})
        self.assertEqual((line.name, line.kind, line.unit), ("Masa por unidad de área", 'num', "g/m²"))
        self.assertEqual((line.spec_min, line.spec_max), (50, 56))
        self.assertEqual(line.spec_label, "53 g/m² ± 3 g/m²")
        line.write({'spec_tol_pct': True, 'spec_tol_minus': 5, 'spec_tol_plus': 5})
        self.assertAlmostEqual(line.spec_min, 50.35)
        self.assertAlmostEqual(line.spec_max, 55.65)
        self.assertEqual(line.spec_label, "53 g/m² ± 5%")
        line.write({'spec_tol_pct': False, 'spec_tol_minus': 2, 'spec_tol_plus': 4})
        self.assertEqual(line.spec_label, "53 g/m² +4 / −2 g/m²")
        # Porcentaje sobre un nominal negativo (encogimiento): el margen no se invierte.
        shrink = self.Spec.create({'acabado_id': self.acabado.id,
                                   'caracteristica_id': self.Car._by_code('cambio_dim_calor').id,
                                   'direction': 'largo', 'spec_nominal': -2, 'spec_tol_pct': True,
                                   'spec_tol_minus': 50, 'spec_tol_plus': 50})
        self.assertAlmostEqual(shrink.spec_min, -3)
        self.assertAlmostEqual(shrink.spec_max, -1)

    def test_03_maximo_minimo_y_tres_resultados(self):
        mx = self.Spec.create({'tejido_id': self.tejido.id, 'name': 'Encogimiento', 'spec_limit': 'max',
                               'spec_nominal': 1, 'unit': '%'})
        self.assertEqual(mx.spec_label, "≤ 1 %")
        self.assertEqual(mx._result_for(0.5), 'cumple')
        self.assertEqual(mx._result_for(1.2), 'no_conforme')
        mn = self.Spec.create({'tejido_id': self.tejido.id, 'name': 'Adhesión', 'spec_limit': 'min',
                               'spec_nominal': 8, 'unit': 'N/5 cm'})
        self.assertEqual(mn.spec_label, "≥ 8 N/5 cm")
        self.assertEqual(mn._result_for(8), 'cumple')
        self.assertEqual(mn._result_for(7.9), 'no_conforme')
        line = self.Spec.create({'acabado_id': self.acabado.id, 'caracteristica_id': self.Car._by_code('ancho').id,
                                 'spec_nominal': 1.60, 'spec_tol_minus': 0.05, 'spec_tol_plus': 0.05,
                                 'ctrl_tol_minus': 0.02, 'ctrl_tol_plus': 0.02})
        self.assertTrue(line.ctrl_defined)
        self.assertAlmostEqual(line.ctrl_min, 1.58)
        self.assertEqual(line.ctrl_label, "1.6 m ± 0.02 m")
        self.assertEqual(line._result_for(1.60), 'cumple')
        self.assertEqual(line._result_for(1.63), 'desviacion')
        self.assertEqual(line._result_for(1.70), 'no_conforme')
        with self.assertRaises(ValidationError):
            line.write({'ctrl_tol_plus': 0.10})
        with self.assertRaises(ValidationError):
            line.write({'spec_tol_minus': -1})

    def test_04_cualitativas_y_padre_unico(self):
        line = self.Spec.create({'acabado_id': self.acabado.id, 'caracteristica_id': self.Car._by_code('tacto').id,
                                 'spec_text': 'Suave'})
        self.assertEqual((line.kind, line.spec_label), ('text', 'Suave'))
        self.assertFalse(line._result_for(5))
        yes = self.Spec.create({'acabado_id': self.acabado.id,
                                'caracteristica_id': self.Car._by_code('engomado_orillas').id, 'spec_bool': True})
        self.assertEqual(yes.spec_label, 'Sí')
        self.assertEqual(len(self.acabado.spec_line_ids), 2)
        with self.assertRaises(ValidationError):
            self.Spec.create({'name': 'Sin padre', 'unit': 'm'})
        with self.assertRaises(ValidationError):
            self.Spec.create({'name': 'Dos padres', 'tejido_id': self.tejido.id, 'acabado_id': self.acabado.id})

    def test_05_limit_vals_copia_todo_menos_el_padre(self):
        line = self.Spec.create({'tejido_id': self.tejido.id, 'caracteristica_id': self.Car._by_code('masa').id,
                                 'spec_nominal': 53, 'spec_tol_minus': 3, 'spec_tol_plus': 3,
                                 'ctrl_tol_minus': 1, 'ctrl_tol_plus': 1, 'in_coa': True})
        vals = line._limit_vals()
        self.assertNotIn('tejido_id', vals)
        copy = self.Spec.create(dict(vals, acabado_id=self.acabado.id))
        self.assertEqual((copy.spec_label, copy.ctrl_label, copy.in_coa), (line.spec_label, line.ctrl_label, True))

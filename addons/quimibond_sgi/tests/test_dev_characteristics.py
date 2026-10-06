# -*- coding: utf-8 -*-
"""57.117.0 (C1, bloque 1): tabla numérica de características del desarrollo
y sus catálogos."""
from odoo.exceptions import ValidationError
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestDevCharacteristics(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Type = cls.env['ficha.tecnica.caracteristica']
        cls.Line = cls.env['sgi.dev.characteristic']
        cls.project = cls.env['project.project'].create({'name': 'FT-900-2026 Prueba', 'sgi_dev_type': 'general'})

    def _line(self, code, **vals):
        return self.project.sgi_dev_line_ids.filtered(lambda l: l.caracteristica_code == code and all(
            l[k] == v for k, v in vals.items()))[:1]

    # ---- catálogos ----
    def test_01_catalog_comes_from_ficha_module(self):
        self.assertEqual(self.Type._by_code('masa').unit, "g/m²")
        self.assertEqual(self.Type._by_code('rendimiento').formula, 'rendimiento')
        Tpl = self.env['sgi.dev.characteristic.template']
        counts = {t: Tpl.search_count([('dev_type', '=', t)]) for t in ('general', 'entretelas_v10', 'carda', 'tramado')}
        self.assertEqual(counts, {'general': 26, 'entretelas_v10': 23, 'carda': 15, 'tramado': 15})

    def test_02_load_lines_from_templates(self):
        self.project.action_sgi_dev_load_lines()
        lines = self.project.sgi_dev_line_ids
        self.assertTrue(lines)
        names = lines.mapped('name')
        self.assertIn("Masa por unidad de área", names)
        self.assertIn("Rendimiento", names)
        elong = lines.filtered(lambda l: l.caracteristica_code == 'elongacion_estatica')
        self.assertEqual(set(elong.mapped('direction')), {'largo', 'ancho'})
        masa = self._line('masa')
        self.assertEqual((masa.unit, masa.kind, masa.lab_requested, masa.in_coa), ("g/m²", 'num', True, True))
        self.assertEqual(self._line('engomado_orillas').kind, 'bool')
        n = len(lines)
        self.project.action_sgi_dev_load_lines()
        self.assertEqual(len(self.project.sgi_dev_line_ids), n, "Volver a proponer no duplica renglones.")
        # Entretelas trae la solidez al frote en tres posiciones.
        other = self.env['project.project'].create({'name': 'FT-901-2026 V10', 'sgi_dev_type': 'entretelas_v10'})
        other.action_sgi_dev_load_lines()
        frote = other.sgi_dev_line_ids.filtered(lambda l: l.caracteristica_code == 'solidez_frote')
        self.assertEqual(set(frote.mapped('position')), {'izquierda', 'centro', 'derecha'})

    # ---- límites y resultados ----
    def test_03_limits_absolute_and_percent(self):
        line = self.Line.create({'project_id': self.project.id, 'caracteristica_id': self.Type._by_code('masa').id,
                                 'spec_nominal': 53, 'spec_tol_minus': 3, 'spec_tol_plus': 3})
        self.assertEqual((line.spec_min, line.spec_max), (50, 56))
        self.assertEqual(line.spec_label, "53 g/m² ± 3 g/m²")
        line.write({'spec_tol_pct': True, 'spec_tol_minus': 5, 'spec_tol_plus': 5})
        self.assertAlmostEqual(line.spec_min, 50.35)
        self.assertAlmostEqual(line.spec_max, 55.65)
        self.assertEqual(line.spec_label, "53 g/m² ± 5%")
        # Porcentaje sobre un nominal negativo (encogimiento): el margen no se invierte.
        shrink = self.Line.create({'project_id': self.project.id, 'caracteristica_id': self.Type._by_code('cambio_dim_calor').id,
                                   'direction': 'largo', 'spec_nominal': -2, 'spec_tol_pct': True,
                                   'spec_tol_minus': 50, 'spec_tol_plus': 50})
        self.assertAlmostEqual(shrink.spec_min, -3)
        self.assertAlmostEqual(shrink.spec_max, -1)

    def test_04_max_min_limits(self):
        mx = self.Line.create({'project_id': self.project.id, 'name': 'Encogimiento', 'spec_limit': 'max',
                               'spec_nominal': 1, 'unit': '%'})
        self.assertEqual(mx.spec_label, "≤ 1 %")
        self.assertEqual(mx._result_for(0.5), 'cumple')
        self.assertEqual(mx._result_for(1.2), 'no_conforme')
        mn = self.Line.create({'project_id': self.project.id, 'name': 'Adhesión', 'spec_limit': 'min',
                               'spec_nominal': 8, 'unit': 'N/5 cm'})
        self.assertEqual(mn.spec_label, "≥ 8 N/5 cm")
        self.assertEqual(mn._result_for(8), 'cumple')
        self.assertEqual(mn._result_for(7.9), 'no_conforme')

    def test_05_three_results_and_internal_control(self):
        line = self.Line.create({'project_id': self.project.id, 'caracteristica_id': self.Type._by_code('ancho').id,
                                 'spec_nominal': 1.60, 'spec_tol_minus': 0.05, 'spec_tol_plus': 0.05,
                                 'ctrl_tol_minus': 0.02, 'ctrl_tol_plus': 0.02})
        self.assertTrue(line.ctrl_defined)
        self.assertAlmostEqual(line.ctrl_min, 1.58)
        self.assertEqual(line.ctrl_label, "1.6 m ± 0.02 m")
        self.assertFalse(line.run_result, "Sin lecturas no hay resultado.")
        line.write({'run_1': 1.60, 'run_2': 1.61, 'run_3': 1.59})
        self.assertEqual(line.run_count, 3)
        self.assertAlmostEqual(line.run_avg, 1.60)
        self.assertEqual(line.run_result, 'cumple')
        line.write({'run_1': 1.63, 'run_2': 1.63, 'run_3': 1.63})
        self.assertEqual(line.run_result, 'desviacion', "Dentro del cliente, fuera del control interno.")
        line.write({'run_1': 1.70, 'run_2': 0, 'run_3': 0})
        self.assertEqual(line.run_count, 1)
        self.assertEqual(line.run_result, 'no_conforme')
        self.assertEqual(self.project.sgi_dev_out_of_spec_count, 1)
        line.write({'sample_value': 1.62})
        self.assertEqual(line.sample_result, 'desviacion')
        # El control interno no puede ser más abierto que el del cliente.
        with self.assertRaises(ValidationError):
            line.write({'ctrl_tol_plus': 0.10})
        with self.assertRaises(ValidationError):
            line.write({'spec_tol_minus': -1})

    def test_06_qualitative_lines_have_no_numeric_result(self):
        line = self.Line.create({'project_id': self.project.id, 'caracteristica_id': self.Type._by_code('tacto').id,
                                 'spec_text': 'Suave', 'run_text': 'Suave', 'run_1': 5})
        self.assertEqual(line.kind, 'text')
        self.assertEqual(line.spec_label, 'Suave')
        self.assertFalse(line.run_result)
        yes = self.Line.create({'project_id': self.project.id, 'caracteristica_id': self.Type._by_code('engomado_orillas').id,
                                'spec_bool': True})
        self.assertEqual(yes.spec_label, 'Sí')

    def test_07_yield_is_computed_from_mass_and_width(self):
        self.project.action_sgi_dev_load_lines()
        masa, ancho, rend = self._line('masa'), self._line('ancho'), self._line('rendimiento')
        masa.write({'spec_nominal': 53, 'spec_tol_minus': 3, 'spec_tol_plus': 3, 'sample_value': 54,
                    'run_1': 52, 'run_2': 53, 'run_3': 54})
        self.assertFalse(rend.spec_nominal, "Sin ancho no hay rendimiento.")
        ancho.write({'spec_nominal': 1.60, 'spec_tol_minus': 0.05, 'spec_tol_plus': 0.05, 'sample_value': 1.62,
                     'run_1': 1.60, 'run_2': 1.60, 'run_3': 1.60})
        self.assertAlmostEqual(rend.spec_nominal, 1000 / (53 * 1.60), places=3)
        self.assertAlmostEqual(rend.sample_value, 1000 / (54 * 1.62), places=3)
        self.assertAlmostEqual(rend.run_avg, (1000 / (52 * 1.6) + 1000 / (53 * 1.6) + 1000 / (54 * 1.6)) / 3, places=3)
        # Tolerancia del rendimiento: extremos de masa y ancho.
        self.assertAlmostEqual(rend.spec_min, 1000 / (56 * 1.65), places=3)
        self.assertAlmostEqual(rend.spec_max, 1000 / (50 * 1.55), places=3)
        self.assertEqual(rend.run_result, 'cumple')
        # Carda mide la masa en tres puntos: el rendimiento toma el centro.
        carda = self.env['project.project'].create({'name': 'FT-902-2026 Carda', 'sgi_dev_type': 'carda'})
        carda.action_sgi_dev_load_lines()
        centro = carda.sgi_dev_line_ids.filtered(lambda l: l.caracteristica_code == 'masa' and l.position == 'centro')
        centro.write({'spec_nominal': 100})
        carda.sgi_dev_line_ids.filtered(lambda l: l.caracteristica_code == 'ancho').write({'spec_nominal': 2})
        self.assertAlmostEqual(carda.sgi_dev_line_ids.filtered(lambda l: l.caracteristica_code == 'rendimiento').spec_nominal, 5)

    def test_08_free_line_and_report(self):
        line = self.Line.create({'project_id': self.project.id, 'name': 'Brillo', 'unit': 'GU'})
        self.assertEqual((line.name, line.kind), ('Brillo', 'num'))
        html = self.env['ir.actions.report']._render_qweb_html('quimibond_sgi.report_dev_request_document',
                                                               self.project.ids)[0]
        self.assertIn(b'Brillo', html)
        self.assertNotIn(b'Control interno', html, "El control interno no se imprime al cliente.")

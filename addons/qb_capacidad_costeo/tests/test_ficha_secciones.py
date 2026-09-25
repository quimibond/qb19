# -*- coding: utf-8 -*-
"""Secciones de D&D en la ficha técnica (1.66.0)."""
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestFichaSecciones(TransactionCase):

    def test_renglones_y_pdf(self):
        product = self.env['product.product'].create({'name': 'Tela ZK secciones', 'default_code': 'WR135Q46JNT165'})
        ficha = self.env['qb.producto.ficha'].create({'product_id': product.id, 'source': 'manual'})
        ficha.action_cargar_renglones()
        self.assertTrue(ficha.spec_lines('acabado'))
        self.assertTrue(ficha.spec_lines('especificacion'))
        n = len(ficha.spec_line_ids)
        ficha.action_cargar_renglones()
        self.assertEqual(len(ficha.spec_line_ids), n, "Cargar de nuevo no duplica.")
        ficha.spec_lines('especificacion')[0].write({'value': '135', 'tolerance': '± 5'})
        ficha.write({'uso_principal': 'Automotriz', 'cuidado_lavado': True})
        Report = self.env['ir.actions.report']
        html = Report._render_qweb_html('qb_capacidad_costeo.report_ficha_specs', ficha.ids)[0]
        self.assertIn(b'Product specs', html)
        self.assertIn(b'Automotriz', html)
        html = Report._render_qweb_html('qb_capacidad_costeo.report_ficha_dyd', ficha.ids)[0]
        self.assertIn(b'Acabado', html)

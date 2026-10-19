# -*- coding: utf-8 -*-
"""2.11.0: rendimiento de tela tejida guardado (editable para forzarlo) y ficha de referencia por revisión."""
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestRendimiento(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Ficha = cls.env['ficha.tecnica.tejido']
        cls.circular = cls.env.ref('quimibond_ficha_tecnica_tela.ft_maquina_tipo_tejido_circular')

    def test_01_calculado_y_forzado(self):
        ficha = self.Ficha.create({'articulo': 'ZZ044Q22HNT235', 'peso_acondicionado': 54.26,
                                   'ancho_acondicionado': 218})
        esperado = 1000 / (54.26 * 2.18)
        self.assertAlmostEqual(ficha.rendimiento_tela_tejida, esperado, places=3, msg="218 cm = 2.18 m")
        self.assertAlmostEqual(ficha._ft_rendimiento(), esperado, places=3)
        ficha.peso_acondicionado = 60
        self.assertAlmostEqual(ficha.rendimiento_tela_tejida, 1000 / (60 * 2.18), places=3, msg="Se recalcula")
        ficha.rendimiento_tela_tejida = 8.0
        self.assertTrue(ficha.rendimiento_manual, "Escribirlo a mano lo deja fijo")
        ficha.peso_acondicionado = 70
        self.assertEqual(ficha.rendimiento_tela_tejida, 8.0, "Forzado: el peso ya no lo cambia")
        ficha.rendimiento_manual = False
        self.assertAlmostEqual(ficha.rendimiento_tela_tejida, 1000 / (70 * 2.18), places=3,
                               msg="Al desmarcarlo vuelve al cálculo")
        ficha.ancho_acondicionado = 2.18
        self.assertEqual(ficha.rendimiento_tela_tejida, 0.0, "2.18 «cm» no es un ancho creíble")

    def test_02_ficha_de_referencia_por_revision(self):
        product = self.env['product.product'].create({'name': 'Crudo referencia', 'default_code': 'ZZ047Q23HNT112'})
        wcs = self.env['mrp.workcenter'].create([{'name': 'CIRCULAR 9%d ref prueba' % i, 'ft_tipo_id': self.circular.id}
                                                 for i in range(3)])
        vieja = self.Ficha.create({'articulo': 'ZZ047Q23HNT112', 'revision': '9', 'maquina_tejido': wcs[0].id,
                                   'product_proceso_id': product.id})
        nueva = self.Ficha.create({'articulo': 'ZZ047Q23HNT112', 'revision': '10', 'maquina_tejido': wcs[1].id,
                                   'product_proceso_id': product.id})
        self.Ficha.create({'articulo': 'ZZ047Q23HNT112', 'revision': '10', 'maquina_tejido': wcs[2].id,
                           'active': False})
        self.assertEqual(self.Ficha._ft_de_producto(product), nueva, "La revisión 10 gana a la 9 (no por texto)")
        nueva.active = False
        self.assertEqual(self.Ficha._ft_de_producto(product), vieja, "Solo cuentan las activas")

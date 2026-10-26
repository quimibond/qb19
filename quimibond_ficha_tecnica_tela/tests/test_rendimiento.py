# -*- coding: utf-8 -*-
"""2.11.0: rendimiento de tela tejida guardado (editable para forzarlo) y ficha de referencia por revisión."""
from datetime import date

from odoo.exceptions import ValidationError
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

    def test_03_salida_de_la_tela_de_la_maquina(self):
        """2.14.0: la circular dice si entrega la tela en tubo; con tubular el rendimiento usa el doble del ancho."""
        Config = self.env['ficha.tecnica.maquina.config']
        tubular = self.env['mrp.workcenter'].create({'name': 'CIRCULAR 97 tubular prueba', 'ft_tipo_id': self.circular.id})
        config = Config.create({'workcenter_id': tubular.id, 'date_from': date(2020, 1, 1), 'diametro': 30})
        ficha = self.Ficha.create({'articulo': 'ZZ040Q46HNT098', 'peso_acondicionado': 40,
                                   'ancho_acondicionado': 98, 'maquina_tejido': tubular.id})
        self.assertEqual(ficha.maquina_config_id, config)
        self.assertFalse(ficha.salida_tela, "Sin capturar en la máquina")
        self.assertAlmostEqual(ficha.rendimiento_tela_tejida, 1000 / (40 * 0.98), places=3,
                               msg="Sin dato de la máquina el ancho se toma como abierto")
        linea = config.valor_ids.filtered(lambda v: v.parametro_id.code == 'salida_tela')
        self.assertTrue(linea, "La configuración trae el renglón del parámetro nuevo")
        linea.valor_texto = 'tubular'
        self.assertEqual(ficha.salida_tela, 'Tubular', "La opción se normaliza y la ficha la toma de su máquina")
        self.assertAlmostEqual(ficha.rendimiento_tela_tejida, 1000 / (40 * 1.96), places=3,
                               msg="98 cm de tubo aplanado = 1.96 m de tela abierta")
        linea.valor_texto = 'Abierta'
        self.assertAlmostEqual(ficha.rendimiento_tela_tejida, 1000 / (40 * 0.98), places=3)
        with self.assertRaises(ValidationError):
            linea.valor_texto = 'Doble'

    def test_04_medicion_del_ancho_fijada_en_la_ficha(self):
        """2.15.0: WJ044 (tela abierta) tejida en una circular tubular: la ficha fija «Tela abierta» y el rendimiento
        usa el ancho tal cual; sin fijar, sigue a la máquina."""
        Config = self.env['ficha.tecnica.maquina.config']
        wc = self.env['mrp.workcenter'].create({'name': 'CIRCULAR 98 tubular prueba', 'ft_tipo_id': self.circular.id})
        config = Config.create({'workcenter_id': wc.id, 'date_from': date(2020, 1, 1), 'diametro': 32})
        config.valor_ids.filtered(lambda v: v.parametro_id.code == 'salida_tela').valor_texto = 'Tubular'
        ficha = self.Ficha.create({'articulo': 'ZZ044Q22HNT235', 'peso_acondicionado': 54.26,
                                   'ancho_acondicionado': 218, 'maquina_tejido': wc.id})
        self.assertEqual(ficha.salida_tela, 'Tubular', "Sin fijar, sigue a la máquina")
        self.assertAlmostEqual(ficha.rendimiento_tela_tejida, 1000 / (54.26 * 4.36), places=3)
        ficha.salida_tela_ficha = 'Abierta'
        self.assertEqual(ficha.salida_tela, 'Abierta')
        self.assertAlmostEqual(ficha.rendimiento_tela_tejida, 1000 / (54.26 * 2.18), places=3)
        ficha.salida_tela_ficha = False
        self.assertEqual(ficha.salida_tela, 'Tubular', "Al quitarlo vuelve al dato de la máquina")

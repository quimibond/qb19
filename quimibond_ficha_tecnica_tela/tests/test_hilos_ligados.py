# -*- coding: utf-8 -*-
"""2.9.0: hilos de la ficha de tejido ligados al producto y cotejados con la lista de materiales del crudo,
reporte de ligado del texto anterior y ligado a la configuración vigente de la máquina."""
from datetime import date

from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestHilosLigados(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.kg = cls.env.ref('uom.product_uom_kgm')
        Product = cls.env['product.product']
        # Como en producción (WJ044Q22HNT235): la lista principal y otra alternativa por hilo.
        cls.set100 = Product.create({'name': 'HILO POLIÉSTER 100/36 SET TEXTURIZADO', 'default_code': 'ZZTEX100/36SO',
                                     'uom_id': cls.kg.id})
        cls.str75 = Product.create({'name': 'HILO POLIÉSTER 75/36 STRETCH TORSIÓN Z', 'default_code': 'ZZ75/36Z',
                                    'uom_id': cls.kg.id})
        cls.str100 = Product.create({'name': 'HILO POLIÉSTER 100/36 STRETCH TORSIÓN Z', 'default_code': 'ZZ100/36Z',
                                     'uom_id': cls.kg.id})
        cls.negro = Product.create({'name': 'HILO POLIÉSTER 75/36 NEGRO', 'default_code': 'ZZTEXNG75/36',
                                    'uom_id': cls.kg.id})
        cls.set75 = Product.create({'name': 'HILO POLIÉSTER 75/36', 'default_code': 'ZZTEX75/36', 'uom_id': cls.kg.id})
        cls.crudo = Product.create({'name': 'Tejido circular prueba', 'default_code': 'ZZ044Q22HNT235',
                                    'uom_id': cls.kg.id})
        Bom = cls.env['mrp.bom']
        cls.bom = Bom.create({'product_tmpl_id': cls.crudo.product_tmpl_id.id, 'product_qty': 1,
                              'product_uom_id': cls.kg.id, 'sequence': 1,
                              'bom_line_ids': [(0, 0, {'product_id': cls.set100.id, 'product_qty': 0.58}),
                                               (0, 0, {'product_id': cls.str75.id, 'product_qty': 0.42})]})
        cls.bom_alt = Bom.create({'product_tmpl_id': cls.crudo.product_tmpl_id.id, 'product_qty': 1,
                                  'product_uom_id': cls.kg.id, 'sequence': 2, 'code': '100/36 Z STRETCH',
                                  'bom_line_ids': [(0, 0, {'product_id': cls.str100.id, 'product_qty': 0.5835}),
                                                   (0, 0, {'product_id': cls.set75.id, 'product_qty': 0.4164})]})
        cls.Ficha = cls.env['ficha.tecnica.tejido']

    def _ficha(self, hilos, **vals):
        return self.Ficha.create(dict({
            'articulo': 'ZZ044Q22HNT235', 'product_proceso_id': self.crudo.id,
            'hilo_line_ids': [(0, 0, dict(numero=n, **h)) for n, h in enumerate(hilos, 1)]}, **vals))

    def test_01_texto_a_producto_y_aviso_de_lista(self):
        ficha = self._ficha([{'tipo_hilo': 'SET', 'titulo_hilo': '100/36', 'torsion': 'S', 'porcentaje': 50.85},
                             {'tipo_hilo': 'STRETCH', 'titulo_hilo': '75/36', 'torsion': 'Z', 'porcentaje': 49.15}])
        self.assertEqual(ficha.bom_efectiva_id, self.bom, "Sin lista elegida se coteja la principal")
        self.assertIn('2 hilo(s) sin producto', ficha.bom_aviso)
        uno, dos = ficha.hilo_line_ids
        self.assertEqual(uno._ft_candidatos(), ('unico', self.set100), "SET excluye el 100/36 stretch")
        self.assertEqual(dos._ft_candidatos(), ('unico', self.str75))
        action = ficha.action_ft_reporte_ligado_ficha()
        rows = self.env['ficha.tecnica.hilo.ligado'].search(action['domain'])
        self.assertEqual(rows.mapped('estado'), ['unico', 'unico'])
        rows.action_ligar_unicos()
        self.assertEqual(ficha.hilo_line_ids.product_id, self.set100 | self.str75)
        self.assertEqual(uno.tipo_hilo, 'SET', "El texto anterior se conserva")
        self.assertIn('ZZTEX100/36SO: 50.85 % en la ficha y 58 % en la lista de materiales', ficha.bom_aviso)
        self.assertIn(ficha, self.Ficha.search([('bom_aviso', '!=', False)]))
        uno.porcentaje, dos.porcentaje = 58, 42
        self.assertFalse(ficha.bom_aviso, "Con los % de la lista de materiales no hay aviso")
        self.assertNotIn(ficha, self.Ficha.search([('bom_aviso', '!=', False)]))
        self.assertAlmostEqual(uno.bom_pct, 58)

    def test_02_lista_elegida_y_otras_listas(self):
        ficha = self._ficha([{'tipo_hilo': 'STRETCH', 'titulo_hilo': '100/36', 'torsion': 'Z', 'porcentaje': 58.35},
                             {'tipo_hilo': 'SET', 'titulo_hilo': '75/36', 'torsion': 'S', 'porcentaje': 41.65}])
        uno, dos = ficha.hilo_line_ids
        self.assertEqual(uno._ft_candidatos(), ('otra_lista', self.str100), "Solo está en la otra lista: no se liga solo")
        ficha.bom_id = self.bom_alt
        self.assertEqual(uno._ft_candidatos(), ('unico', self.str100))
        self.assertEqual(dos._ft_candidatos(), ('unico', self.set75))
        ficha.hilo_line_ids._ft_ligar_unicos()
        self.assertFalse(ficha.bom_aviso)

    def test_03_el_porcentaje_desempata(self):
        # Como el WJ035Q22HNN200 (pokayoke): 48.1 % natural, 48.7 % stretch y 3.2 % negro; la ficha no dice «negro».
        pokayoke = self.env['product.product'].create({'name': 'Pokayoke prueba', 'default_code': 'ZZ035Q22HNN200'})
        self.env['mrp.bom'].create({
            'product_tmpl_id': pokayoke.product_tmpl_id.id, 'product_qty': 1, 'product_uom_id': self.kg.id,
            'bom_line_ids': [(0, 0, {'product_id': self.str75.id, 'product_qty': 0.487}),
                             (0, 0, {'product_id': self.set75.id, 'product_qty': 0.481}),
                             (0, 0, {'product_id': self.negro.id, 'product_qty': 0.032})]})
        ficha = self._ficha([{'tipo_hilo': 'SET', 'titulo_hilo': '75/36', 'torsion': 'S', 'porcentaje': 48.1},
                             {'tipo_hilo': 'STRETCH', 'titulo_hilo': '75/36', 'torsion': 'Z', 'porcentaje': 48.7},
                             {'tipo_hilo': 'SET', 'titulo_hilo': '75/36', 'torsion': 'S', 'porcentaje': 3.2}],
                            articulo='ZZ035Q22HNN200', product_proceso_id=pokayoke.id)
        ficha.hilo_line_ids._ft_ligar_unicos()
        self.assertEqual(ficha.hilo_line_ids.mapped('product_id'), self.set75 | self.str75 | self.negro)
        self.assertEqual(ficha.hilo_line_ids[2].product_id, self.negro)
        self.assertFalse(ficha.bom_aviso)
        negra = self._ficha([{'tipo_hilo': 'SET', 'titulo_hilo': '75/36 NG', 'torsion': 'S', 'porcentaje': 100}],
                            articulo='ZZ035Q22HNN200', product_proceso_id=pokayoke.id, revision='2')
        self.assertEqual(negra.hilo_line_ids._ft_candidatos(), ('unico', self.negro), "«NG» solo acepta negros")

    def test_04_faltan_y_sobran(self):
        ficha = self._ficha([{'product_id': self.set100.id, 'porcentaje': 58},
                             {'product_id': self.negro.id, 'porcentaje': 42}])
        self.assertIn('ZZTEXNG75/36 no está en la lista de materiales', ficha.bom_aviso)
        self.assertIn('ZZ75/36Z está en la lista de materiales y no en la ficha', ficha.bom_aviso)
        self.assertEqual(ficha.hilo_line_ids[1]._ft_candidatos(), ('ligado', self.negro))
        doble = self._ficha([{'product_id': self.set100.id, 'porcentaje': 100},
                             {'product_id': self.str75.id, 'porcentaje': 100}], revision='9')
        self.assertIn('suman 200', doble.bom_aviso)

    def test_05_ligar_a_la_configuracion_vigente(self):
        circular = self.env.ref('quimibond_ficha_tecnica_tela.ft_maquina_tipo_tejido_circular')
        wc = self.env['mrp.workcenter'].create({'name': 'CIRCULAR 98 hilos prueba', 'ft_tipo_id': circular.id})
        # Como en producción: las fichas se cargaron antes que las configuraciones (y sin fecha de elaboración).
        buena = self._ficha([], maquina_tejido=wc.id, revision='1', galga=18, diametro=32, no_agujas=1848,
                            no_alimentadores=96)
        mala = self._ficha([], maquina_tejido=wc.id, revision='2', galga=18, diametro=32, no_agujas=1848,
                           no_alimentadores=120)
        config = self.env['ficha.tecnica.maquina.config'].create({
            'workcenter_id': wc.id, 'date_from': date(2026, 10, 8), 'galga': 18, 'diametro': 32,
            'no_alimentadores': 96})
        self.assertFalse(buena.maquina_config_id, "Las fichas de antes no tienen configuración")
        self.assertIn('alimentadores', mala.maquina_config_aviso)
        result = (buena | mala).action_ft_ligar_configuracion()
        self.assertEqual(buena.maquina_config_id, config)
        self.assertEqual(buena.no_agujas, 1848, "Las agujas vacías de la configuración no borran las de la ficha")
        self.assertFalse(mala.maquina_config_id, "Un dato distinto no se resuelve solo")
        self.assertIn('Sin ligar (1)', result['params']['message'])

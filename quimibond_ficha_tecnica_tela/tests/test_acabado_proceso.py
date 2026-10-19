# -*- coding: utf-8 -*-
"""2.12.0: la producción va en kg hasta Inspección; la ficha de acabado liga el acabado en proceso (kg) y la tela
acabada (m) que entrega Inspección."""
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestAcabadoProceso(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.kg = cls.env.ref('uom.product_uom_kgm')
        cls.m = cls.env.ref('uom.product_uom_meter')
        Product = cls.env['product.product']
        cls.crudo = Product.create({'name': 'Crudo inspección', 'default_code': 'ZZ045Q22HNT160',
                                    'uom_id': cls.kg.id})
        # Tela acabada en metros (J en la posición 9) y su acabado en proceso en kg (K, 2.13.0).
        cls.acabado = Product.create({'name': 'Acabado inspección', 'default_code': 'ZZ045Q22JNT160',
                                      'uom_id': cls.m.id})
        cls.proceso = Product.create({'name': 'Acabado en proceso inspección', 'default_code': 'ZZ045Q22KNT160',
                                      'uom_id': cls.kg.id})
        # Los «I…» se venden en kg: no son acabado en proceso.
        Product.create({'name': 'Venta en kg', 'default_code': 'IZZ045Q22JNT160', 'uom_id': cls.kg.id})
        cls.tejido = cls.env['ficha.tecnica.tejido'].create({
            'articulo': 'ZZ045Q22HNT160', 'product_proceso_id': cls.crudo.id, 'peso_acondicionado': 45,
            'ancho_acondicionado': 160})
        Bom = cls.env['mrp.bom']
        Bom.create({'product_tmpl_id': cls.proceso.product_tmpl_id.id, 'product_qty': 1, 'product_uom_id': cls.kg.id,
                    'bom_line_ids': [(0, 0, {'product_id': cls.crudo.id, 'product_qty': 1})]})
        cls.Ficha = cls.env['ficha.tecnica.acabado']

    def _ficha(self, **vals):
        return self.Ficha.create(dict({'articulo': 'ZZ045Q22JNT160', 'tejido_id': self.tejido.id,
                                       'product_acabado_id': self.acabado.id}, **vals))

    def test_01_sugerido_por_codigo_y_aviso(self):
        ficha = self._ficha()
        self.assertEqual(ficha.product_proceso_id, self.proceso, "Al crearse la ficha liga el K que existe")
        ficha.product_proceso_id = False
        self.assertEqual(ficha.product_proceso_sugerido_id, self.proceso)
        self.assertIn('se sugiere ZZ045Q22KNT160', ficha.proceso_aviso)
        self.assertIn(ficha, self.Ficha.search([('product_proceso_id', '=', False)]))
        ficha.action_ft_usar_proceso_sugerido()
        self.assertEqual(ficha.product_proceso_id, self.proceso)
        self.assertFalse(ficha.proceso_aviso, "Sin lista de materiales de la tela acabada no hay más que avisar")

    def test_02_inspeccion_en_la_lista_de_materiales(self):
        ficha = self._ficha(product_proceso_id=self.proceso.id)
        bom = self.env['mrp.bom'].create({
            'product_tmpl_id': self.acabado.product_tmpl_id.id, 'product_qty': 1000, 'product_uom_id': self.m.id,
            'bom_line_ids': [(0, 0, {'product_id': self.crudo.id, 'product_qty': 140})]})
        ficha.invalidate_recordset()
        self.assertIn('no consume ZZ045Q22KNT160', ficha.proceso_aviso)
        bom.bom_line_ids.product_id = self.proceso
        ficha.invalidate_recordset()
        self.assertFalse(ficha.proceso_aviso)
        self.assertEqual(ficha.crudo_id, self.crudo, "El crudo se encuentra bajando por el acabado en proceso")
        self.assertEqual(self.proceso.product_tmpl_id.ficha_tecnica_acabado_id, ficha)
        rendimiento, origen = self.proceso._ft_rendimiento()
        self.assertTrue(rendimiento, origen)

    def test_03_unidad_y_mismo_producto(self):
        ficha = self._ficha(product_proceso_id=self.acabado.id)
        self.assertIn('mismo producto', ficha.proceso_aviso)
        en_metros = self.env['product.product'].create({'name': 'Proceso en m', 'uom_id': self.m.id})
        ficha.product_proceso_id = en_metros
        self.assertIn('hasta Inspección todo va en kg', ficha.proceso_aviso)
        self.assertFalse(ficha._ft_ligar_proceso_sugerido(), "Con un producto ya puesto no se cambia solo")

    def test_04_faltantes(self):
        Product = self.env['product.product']
        nueva = Product.create({'name': 'Tela sin proceso', 'default_code': 'ZZ046Q22JNG160', 'uom_id': self.m.id,
                                'tracking': 'lot', 'sale_ok': True})
        ficha = self._ficha(articulo='ZZ046Q22JNG160', product_acabado_id=nueva.id)
        self.assertIn('ZZ046Q22KNG160 no existe todavía', ficha.proceso_aviso)
        Faltante = self.env['ficha.tecnica.proceso.faltante']
        rows = Faltante.search(Faltante._ft_generar()['domain'])
        self.assertIn('ZZ046Q22KNG160', rows.mapped('codigo'))
        self.assertNotIn('ZZ045Q22KNT160', rows.mapped('codigo'), "El que ya existe no falta")
        rows = Faltante.search(rows.filtered(lambda r: r.acabado_id == nueva).action_crear()['domain'])
        self.assertNotIn('ZZ046Q22KNG160', rows.mapped('codigo'))
        proceso = Product.search([('default_code', '=', 'ZZ046Q22KNG160')])
        self.assertEqual((proceso.uom_id, proceso.tracking, proceso.sale_ok), (self.kg, 'lot', False))
        self.assertEqual(ficha.product_proceso_id, proceso, "Se liga a su ficha de acabado")

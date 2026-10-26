# -*- coding: utf-8 -*-
"""2.9.0: la ficha de acabado muestra su crudo y sus operaciones (producto único de acabado en kg) y los jets
tienen datos del artículo: kg por carga y relación de baño."""
from datetime import date

from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestAcabadoRuta(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.kg = cls.env.ref('uom.product_uom_kgm')
        ref = cls.env.ref
        Wc = cls.env['mrp.workcenter']
        cls.circular = Wc.create({'name': 'CIRCULAR 99 ruta prueba',
                                  'ft_tipo_id': ref('quimibond_ficha_tecnica_tela.ft_maquina_tipo_tejido_circular').id})
        cls.abridora = Wc.create({'name': 'ABRIDORA 99 prueba',
                                  'ft_tipo_id': ref('quimibond_ficha_tecnica_tela.ft_maquina_tipo_abridora').id})
        cls.tintoreria = ref('quimibond_ficha_tecnica_tela.ft_maquina_tipo_tintoreria')
        cls.jet = Wc.create({'name': 'JET 99 prueba', 'ft_tipo_id': cls.tintoreria.id})
        cls.rama = Wc.create({'name': 'RAMA 99 prueba',
                              'ft_tipo_id': ref('quimibond_ficha_tecnica_tela.ft_maquina_tipo_acabado').id})
        if not cls.rama.ft_tipo_id._ft_parametros('articulo'):
            cls.env['ficha.tecnica.maquina.parametro'].create({
                'tipo_id': cls.rama.ft_tipo_id.id, 'code': 'velocidad', 'name': 'Velocidad', 'unit': 'm/min',
                'ambito': 'articulo'})
        Product = cls.env['product.product']
        cls.hilo = Product.create({'name': 'HILO POLIÉSTER 75/36 ruta', 'uom_id': cls.kg.id})
        cls.crudo = Product.create({'name': 'Crudo ruta', 'default_code': 'ZZ035Q22HNT200', 'uom_id': cls.kg.id})
        cls.acabado = Product.create({'name': 'Acabado ruta', 'default_code': 'ZZ035Q22JNT200', 'uom_id': cls.kg.id})
        Bom = cls.env['mrp.bom']
        Bom.create({'product_tmpl_id': cls.crudo.product_tmpl_id.id, 'product_qty': 1, 'product_uom_id': cls.kg.id,
                    'bom_line_ids': [(0, 0, {'product_id': cls.hilo.id, 'product_qty': 1})],
                    'operation_ids': [(0, 0, {'name': 'Tejer', 'workcenter_id': cls.circular.id})]})
        cls.bom = Bom.create({
            'product_tmpl_id': cls.acabado.product_tmpl_id.id, 'product_qty': 1, 'product_uom_id': cls.kg.id,
            'bom_line_ids': [(0, 0, {'product_id': cls.crudo.id, 'product_qty': 1})],
            'operation_ids': [(0, 0, {'name': 'Abrir', 'sequence': 10, 'workcenter_id': cls.abridora.id}),
                              (0, 0, {'name': 'Teñir', 'sequence': 20, 'workcenter_id': cls.jet.id}),
                              (0, 0, {'name': 'Ramear', 'sequence': 30, 'workcenter_id': cls.rama.id})]})
        cls.tejido = cls.env['ficha.tecnica.tejido'].create({'articulo': 'ZZ035Q22HNT200',
                                                              'product_proceso_id': cls.crudo.id})
        cls.Proceso = cls.env['ficha.tecnica.proceso']

    def test_01_crudo_y_operaciones(self):
        rama = self.Proceso.create({'articulo': 'ZZ035Q22JNT200', 'workcenter_id': self.rama.id})
        rama._ft_set_value('velocidad', 18)
        ficha = self.env['ficha.tecnica.acabado'].create({
            'articulo': 'ZZ035Q22JNT200', 'tejido_id': self.tejido.id, 'product_acabado_id': self.acabado.id})
        self.assertEqual(ficha.crudo_id, self.crudo, "El crudo sale de la lista de materiales del acabado")
        self.assertFalse(ficha.crudo_aviso)
        self.assertEqual(ficha.operacion_ids.mapped('name'), ['Abrir', 'Teñir', 'Ramear'],
                         "Las del acabado, no la de tejido del crudo")
        ramear = ficha.operacion_ids.filtered(lambda o: o.name == 'Ramear')
        self.assertEqual(ramear.ft_proceso_id, rama)
        self.assertIn('18 m/min', ramear.ft_proceso_resumen)
        self.assertIn('JET 99 prueba', ficha.operacion_aviso, "El jet pide datos del artículo y no hay proceso")
        self.assertNotIn('ABRIDORA', ficha.operacion_aviso, "La abridora aún no tiene parámetros del artículo")
        otro = self.env['product.product'].create({'name': 'Otro crudo', 'default_code': 'ZZ099Q22HNT200'})
        ficha.tejido_id = self.env['ficha.tecnica.tejido'].create({'articulo': 'ZZ099Q22HNT200',
                                                                    'product_proceso_id': otro.id})
        self.assertIn('ZZ099Q22HNT200', ficha.crudo_aviso)

    def test_02_jet_con_datos_del_articulo(self):
        Param = self.env['ficha.tecnica.maquina.parametro']
        params = {}
        for code, kind in (('capacidad', 'num'), ('relacion_bano', 'text')):
            params[code] = Param.search([('tipo_id', '=', self.tintoreria.id), ('code', '=', code)], limit=1) or \
                Param.create({'tipo_id': self.tintoreria.id, 'code': code, 'name': code, 'kind': kind})
        self.env['ficha.tecnica.maquina.config'].create({
            'workcenter_id': self.jet.id, 'date_from': date(2020, 1, 1),
            'valor_ids': [(0, 0, {'parametro_id': params['capacidad'].id, 'valor': 1000}),
                          (0, 0, {'parametro_id': params['relacion_bano'].id, 'valor_texto': '1:10'})]})
        self.assertEqual(self.jet._ft_capacidad_carga(0.0, None, self.acabado)[0], 1000)
        self.assertEqual(self.jet._ft_relacion_bano(self.acabado), (10.0, 'relación de baño de la máquina'))
        proceso = self.Proceso.create({'articulo': 'ZZ035Q22JNT200', 'workcenter_id': self.jet.id})
        self.assertTrue({'tiempo_ciclo', 'kg_carga', 'relacion_bano_articulo', 'temperatura', 'gradiente_subida',
                         'plegador', 'bomba_presion', 'acumulador_cargado'} <= set(proceso.valor_ids.mapped('code')),
                        "2.16.0: el jet pide también los datos de la ficha de tintorería F-P-D01-33")
        proceso._ft_set_value('kg_carga', 250)
        proceso._ft_set_value('relacion_bano_articulo', 8)
        self.assertEqual(self.jet._ft_capacidad_carga(0.0, None, self.acabado),
                         (250, 'kg por carga del artículo en este jet'))
        self.assertEqual(self.jet._ft_relacion_bano(self.acabado)[0], 8)
        self.assertIn('2 cargas', self.jet._ft_aviso_para(self.acabado, 400))

    def test_03_tintoreria_en_la_ficha_de_acabado(self):
        """2.16.0: la pestaña Tintorería junta el jet de la lista de materiales, el artículo en el jet (con su volumen de
        baño) y la fórmula: la asignada a la operación de tintorería o, con el teñido aparte, la de su lista."""
        Product = self.env['product.product']
        agua = Product.create({'name': 'AGUA ruta', 'default_code': 'AGUA-RUTA'})
        colorante = Product.create({'name': 'NATURAL ruta', 'default_code': 'NATURAL-RUTA', 'uom_id': self.kg.id})
        operacion = self.bom.operation_ids.filtered(lambda o: o.name == 'Teñir')
        self.bom.write({'bom_line_ids': [(0, 0, {'product_id': colorante.id, 'product_qty': 0.013,
                                                  'operation_id': operacion.id})]})
        jet = self.Proceso.create({'articulo': 'ZZ035Q22JNT200', 'workcenter_id': self.jet.id})
        jet._ft_set_value('kg_carga', 300)
        jet._ft_set_value('relacion_bano_articulo', 7)
        ficha = self.env['ficha.tecnica.acabado'].create({
            'articulo': 'ZZ035Q22JNT200', 'tejido_id': self.tejido.id, 'product_acabado_id': self.acabado.id})
        self.assertEqual(ficha.tint_operacion_ids, operacion)
        self.assertEqual(ficha.tint_linea_ids.product_id, colorante, "La fórmula es lo asignado al jet, no el crudo")
        self.assertEqual(ficha.tint_proceso_ids, jet)
        self.assertEqual(jet.ft_volumen_bano, 2100.0, "300 kg × 7 L/kg")
        self.assertFalse(ficha.tint_aviso)
        # Teñido aparte (posición 9 = I): la fórmula sale de su lista, sin la tela.
        tenido = Product.create({'name': 'Teñido ruta', 'default_code': 'ZZ035Q22INT200', 'uom_id': self.kg.id})
        acabado2 = Product.create({'name': 'Acabado ruta 2', 'default_code': 'ZZ035Q22JNT201', 'uom_id': self.kg.id})
        Bom = self.env['mrp.bom']
        Bom.create({'product_tmpl_id': tenido.product_tmpl_id.id, 'product_qty': 1, 'product_uom_id': self.kg.id,
                    'bom_line_ids': [(0, 0, {'product_id': self.crudo.id, 'product_qty': 1}),
                                     (0, 0, {'product_id': agua.id, 'product_qty': 7.09}),
                                     (0, 0, {'product_id': colorante.id, 'product_qty': 0.013})]})
        Bom.create({'product_tmpl_id': acabado2.product_tmpl_id.id, 'product_qty': 1, 'product_uom_id': self.kg.id,
                    'bom_line_ids': [(0, 0, {'product_id': tenido.id, 'product_qty': 1})]})
        ficha2 = self.env['ficha.tecnica.acabado'].create({
            'articulo': 'ZZ035Q22JNT201', 'tejido_id': self.tejido.id, 'product_acabado_id': acabado2.id})
        self.assertEqual(set(ficha2.tint_linea_ids.product_id.ids), {agua.id, colorante.id})
        self.assertFalse(ficha2.tint_operacion_ids)
        self.assertIn('no tiene datos en ningún jet', ficha2.tint_aviso)

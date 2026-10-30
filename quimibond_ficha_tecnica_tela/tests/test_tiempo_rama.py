# -*- coding: utf-8 -*-
"""2.22.0: el tiempo de rama sale de la ficha (peso, ancho bajando de rama, merma y velocidad)."""
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestTiempoRama(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.kg = cls.env.ref('uom.product_uom_kgm')
        cls.tipo = cls.env['ficha.tecnica.maquina.tipo'].create({
            'name': 'Rama tiempo rama prueba', 'code': 'rama_tiempo_rama_prueba',
            'parametro_ids': [
                (0, 0, {'code': 'velocidad', 'name': 'Velocidad', 'unit': 'm/min', 'kind': 'num',
                        'ambito': 'articulo'}),
                (0, 0, {'code': 'merma', 'name': 'Merma', 'unit': '%', 'kind': 'num', 'ambito': 'articulo'}),
                (0, 0, {'code': 'ancho_rama', 'name': 'Ancho bajando de rama', 'unit': 'm', 'kind': 'num',
                        'ambito': 'articulo'})]})
        cls.otro_tipo = cls.env['ficha.tecnica.maquina.tipo'].create({
            'name': 'Abridora tiempo rama prueba', 'code': 'abridora_tiempo_rama_prueba',
            'parametro_ids': [(0, 0, {'code': 'velocidad', 'name': 'Velocidad', 'unit': 'm/min', 'kind': 'num',
                                      'ambito': 'articulo'})]})
        Workcenter = cls.env['mrp.workcenter']
        cls.rama = Workcenter.create({'name': 'RAMA 91 prueba', 'code': 'R91', 'ft_tipo_id': cls.tipo.id})
        cls.abridora = Workcenter.create({'name': 'ABRIDORA 91 prueba', 'code': 'A91', 'ft_tipo_id': cls.otro_tipo.id})
        Product = cls.env['product.product']
        cls.tela = Product.create({'name': 'Tela tiempo rama prueba', 'default_code': 'IZZ053Q22JNT165',
                                   'is_storable': True, 'uom_id': cls.kg.id})
        cls.tela_m = Product.create({'name': 'Tela tiempo rama prueba (m)', 'default_code': 'ZZ053Q22JNT165'})
        cls.crudo = Product.create({'name': 'Crudo tiempo rama prueba', 'default_code': 'ZZ053Q22HNT200',
                                    'uom_id': cls.kg.id})
        cls.bom = cls.env['mrp.bom'].create({
            'product_tmpl_id': cls.tela.product_tmpl_id.id, 'product_qty': 1, 'product_uom_id': cls.kg.id,
            'bom_line_ids': [(0, 0, {'product_id': cls.crudo.id, 'product_qty': 1.1})],
            'operation_ids': [
                (0, 0, {'name': 'Abrir', 'sequence': 10, 'workcenter_id': cls.abridora.id, 'time_mode': 'manual',
                        'time_cycle_manual': 0.1}),
                (0, 0, {'name': 'Rama', 'sequence': 20, 'workcenter_id': cls.rama.id, 'time_mode': 'manual',
                        'time_cycle_manual': 0.3})]})
        cls.op_rama = cls.bom.operation_ids.filtered(lambda o: o.workcenter_id == cls.rama)
        cls.op_abrir = cls.bom.operation_ids - cls.op_rama
        tejido = cls.env['ficha.tecnica.tejido'].create({'articulo': 'ZZ053Q22HNT200',
                                                          'product_proceso_id': cls.crudo.id})
        cls.env['ficha.tecnica.acabado'].create({
            'articulo': 'ZZ053Q22JNT165', 'tejido_id': tejido.id, 'product_acabado_id': cls.tela_m.id,
            'product_proceso_id': cls.tela.id, 'peso_acabado': 53})
        cls.proceso = cls.env['ficha.tecnica.proceso'].create({'articulo': 'IZZ053Q22JNT165',
                                                                'workcenter_id': cls.rama.id})
        cls.proceso._ft_set_value('velocidad', 35)
        cls.proceso._ft_set_value('merma', 10)
        cls.proceso._ft_set_value('ancho_rama', 1.65)

    def test_01_tiempo_de_rama_con_merma(self):
        esperado = 1000.0 / (53 * 1.65) * 0.9 / 35
        # El campo guarda 4 decimales (digits=(16, 4)), como el tiempo que se pasa a la lista.
        self.assertAlmostEqual(self.op_rama.ft_tiempo_ficha, round(esperado, 4), places=6)
        self.assertIn('53 g/m²', self.op_rama.ft_tiempo_calculo)
        self.assertIn('%.4f min/kg' % esperado, self.op_rama.ft_tiempo_calculo)
        self.assertTrue(self.op_rama.ft_tiempo_difiere, "0.3 contra 0.2941 es más de 0.5 %")
        self.op_rama.action_ft_usar_tiempo_ficha()
        self.assertAlmostEqual(self.op_rama.time_cycle_manual, round(esperado, 4), places=6)
        self.assertFalse(self.op_rama.ft_tiempo_difiere)

    def test_02_falta_un_dato(self):
        self.proceso._ft_set_value('merma', 0)
        self.assertFalse(self.op_rama.ft_tiempo_ficha, "Sin merma no hay tiempo: un vacío no es un cero")
        self.assertIn('la merma', self.op_rama.ft_tiempo_calculo)
        self.op_rama.action_ft_usar_tiempo_ficha()
        self.assertEqual(self.op_rama.time_cycle_manual, 0.3, "Sin tiempo de la ficha no se toca la lista")

    def test_03_operacion_que_no_es_de_rama(self):
        self.env['ficha.tecnica.proceso'].create({'articulo': 'IZZ053Q22JNT165', 'workcenter_id': self.abridora.id})
        self.assertFalse(self.op_abrir.ft_tiempo_ficha)
        self.assertFalse(self.op_abrir.ft_tiempo_calculo)

    def test_04_varias_listas_tambien_archivadas(self):
        self.bom.action_archive()
        self.bom.action_ft_tiempos_rama()
        self.assertAlmostEqual(self.op_rama.time_cycle_manual, round(1000.0 / (53 * 1.65) * 0.9 / 35, 4), places=6)
        self.assertEqual(self.op_abrir.time_cycle_manual, 0.1)

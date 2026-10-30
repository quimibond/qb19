# -*- coding: utf-8 -*-
"""2.22.0: la duración de la orden de trabajo sale de la lista (el Gantt no la cambia) y el tiempo de rama de la ficha."""
from datetime import timedelta

from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestDuracionTiempo(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.kg = cls.env.ref('uom.product_uom_kgm')
        cls.tipo = cls.env['ficha.tecnica.maquina.tipo'].create({
            'name': 'Rama duración prueba', 'code': 'rama_duracion_prueba',
            'parametro_ids': [
                (0, 0, {'code': 'velocidad', 'name': 'Velocidad', 'unit': 'm/min', 'kind': 'num',
                        'ambito': 'articulo'}),
                (0, 0, {'code': 'merma', 'name': 'Merma', 'unit': '%', 'kind': 'num', 'ambito': 'articulo'}),
                (0, 0, {'code': 'ancho_rama', 'name': 'Ancho bajando de rama', 'unit': 'm', 'kind': 'num',
                        'ambito': 'articulo'})]})
        cls.otro_tipo = cls.env['ficha.tecnica.maquina.tipo'].create({
            'name': 'Abridora duración prueba', 'code': 'abridora_duracion_prueba',
            'parametro_ids': [(0, 0, {'code': 'velocidad', 'name': 'Velocidad', 'unit': 'm/min', 'kind': 'num',
                                      'ambito': 'articulo'})]})
        Workcenter = cls.env['mrp.workcenter']
        cls.rama = Workcenter.create({'name': 'RAMA 91 prueba', 'code': 'R91', 'ft_tipo_id': cls.tipo.id})
        cls.abridora = Workcenter.create({'name': 'ABRIDORA 91 prueba', 'code': 'A91', 'ft_tipo_id': cls.otro_tipo.id})
        Product = cls.env['product.product']
        cls.tela = Product.create({'name': 'Tela duración prueba', 'default_code': 'IZZ053Q22JNT165',
                                   'is_storable': True, 'uom_id': cls.kg.id})
        cls.tela_m = Product.create({'name': 'Tela duración prueba (m)', 'default_code': 'ZZ053Q22JNT165'})
        cls.crudo = Product.create({'name': 'Crudo duración prueba', 'default_code': 'ZZ053Q22HNT200',
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

    # Tiempo de rama
    def test_01_tiempo_de_rama_con_merma(self):
        esperado = 1000.0 / (53 * 1.65) * 0.9 / 35
        self.assertAlmostEqual(self.op_rama.ft_tiempo_ficha, esperado, places=6)
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

    # Duración de la lista
    def _fin_con_su_duracion(self, wo):
        diferencia = abs((wo.date_finished - wo._calculate_date_finished()).total_seconds())
        self.assertLess(diferencia, 1, "La fecha fin sale de la duración desde la de inicio")

    def _orden_planeada(self):
        production = self.env['mrp.production'].create({'product_id': self.tela.id, 'product_qty': 100,
                                                        'product_uom_id': self.kg.id, 'bom_id': self.bom.id})
        production.action_confirm()
        production.button_plan()
        return production.workorder_ids.filtered(lambda wo: wo.workcenter_id == self.rama)

    def test_05_estirar_no_cambia_la_duracion(self):
        wo = self._orden_planeada()
        self.assertAlmostEqual(wo.duration_expected, 30.0, places=2, msg="100 kg × 0.3 min/kg")
        self.assertAlmostEqual(wo.ft_duracion_lista, 30.0, places=2)
        inicio = wo.date_start
        wo.write({'date_finished': inicio + timedelta(days=5)})
        self.assertAlmostEqual(wo.duration_expected, 30.0, places=2, msg="Estirar la barra no cambia la duración")
        self.assertEqual(wo.date_start, inicio)
        self._fin_con_su_duracion(wo)

    def test_06_cambiar_el_inicio_mueve_la_orden(self):
        wo = self._orden_planeada()
        nuevo = wo.date_start + timedelta(days=7)
        wo.write({'date_start': nuevo})
        self.assertEqual(wo.date_start, nuevo)
        self.assertAlmostEqual(wo.duration_expected, 30.0, places=2)
        self._fin_con_su_duracion(wo)

    def test_07_usar_la_duracion_de_la_lista(self):
        wo = self._orden_planeada()
        wo.write({'duration_expected': 150.0})
        self.assertAlmostEqual(wo.duration_expected, 150.0, places=2, msg="A propósito sí se cambia")
        self.assertTrue(wo.ft_duracion_difiere)
        self.assertIn(wo, self.env['mrp.workorder'].search([('ft_duracion_difiere', '=', True)]))
        wo.action_ft_duracion_lista()
        self.assertAlmostEqual(wo.duration_expected, 30.0, places=2)
        self.assertFalse(wo.ft_duracion_difiere)
        self._fin_con_su_duracion(wo)

    def test_08_clave_de_contexto_deja_lo_nativo(self):
        wo = self._orden_planeada()
        wo.with_context(ft_duracion_libre=True).write({'date_finished': wo.date_start + timedelta(days=5)})
        self.assertGreater(wo.duration_expected, 30.0, "Con ft_duracion_libre, Odoo recalcula la duración")

# -*- coding: utf-8 -*-
"""2.8.0: proceso del artículo en la máquina (velocidad de cada artículo en cada rama y lo que siga)."""
from datetime import date

from odoo.exceptions import ValidationError
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestProcesos(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.tipo = cls.env['ficha.tecnica.maquina.tipo'].create({
            'name': 'Rama prueba', 'code': 'rama_prueba',
            'parametro_ids': [
                (0, 0, {'sequence': 10, 'code': 'ancho_util', 'name': 'Ancho útil', 'unit': 'm', 'kind': 'num'}),
                (0, 0, {'sequence': 20, 'code': 'velocidad', 'name': 'Velocidad', 'unit': 'm/min', 'kind': 'num',
                        'ambito': 'articulo'}),
                (0, 0, {'sequence': 30, 'code': 'merma', 'name': 'Merma', 'unit': '%', 'kind': 'num',
                        'ambito': 'articulo'}),
            ]})
        cls.rama1 = cls.env['mrp.workcenter'].create({'name': 'RAMA 91 prueba', 'code': 'R91',
                                                      'ft_tipo_id': cls.tipo.id})
        cls.rama2 = cls.env['mrp.workcenter'].create({'name': 'RAMA 92 prueba', 'code': 'R92',
                                                      'ft_tipo_id': cls.tipo.id})
        cls.product = cls.env['product.product'].create({'name': 'Tela prueba', 'default_code': 'ZZ140Q21JNT165'})
        cls.Proceso = cls.env['ficha.tecnica.proceso']

    def test_01_valores_y_metros_hora(self):
        proceso = self.Proceso.create({'articulo': ' ZZ140Q21JNT165 ', 'workcenter_id': self.rama1.id})
        self.assertEqual(proceso.articulo, 'ZZ140Q21JNT165')
        self.assertEqual(proceso.product_id, self.product, "Se liga al producto con esa referencia")
        self.assertEqual(proceso.valor_ids.mapped('code'), ['velocidad', 'merma'],
                         "Solo los parámetros del artículo; el ancho útil es de la máquina")
        self.assertFalse(proceso._ft_value('velocidad'), "Sin dato no es cero: queda vacío")
        proceso._ft_set_value('velocidad', 18)
        self.assertEqual(proceso.metros_hora, 1080, "18 m/min = 1,080 m por hora (ejemplo de la planta)")
        self.assertEqual(proceso.metros_hora_neto, 1080)
        proceso._ft_set_value('merma', 10)
        self.assertAlmostEqual(proceso.metros_hora_neto, 972)
        self.assertEqual(proceso.resumen, 'Velocidad: 18 m/min · Merma: 10 %')
        self.assertEqual(self.Proceso._ft_buscar(self.product, self.rama1), proceso)
        self.assertFalse(self.Proceso._ft_buscar(self.product, self.rama2))
        with self.assertRaises(ValidationError):
            proceso._ft_set_value('ancho_util', 2.2)

    def test_02_configuracion_solo_con_parametros_de_la_maquina(self):
        config = self.env['ficha.tecnica.maquina.config'].create({'workcenter_id': self.rama1.id,
                                                                  'date_from': date(2026, 10, 14)})
        self.assertEqual(config.valor_ids.mapped('code'), ['ancho_util'])
        velocidad = self.tipo.parametro_ids.filtered(lambda p: p.code == 'velocidad')
        with self.assertRaises(ValidationError):
            self.env['ficha.tecnica.maquina.valor'].create({'config_id': config.id, 'parametro_id': velocidad.id})

    def test_03_promedio_por_maquina_y_unico(self):
        for code, vel in (('ZZ001', 30), ('ZZ002', 32), ('ZZ003', 0)):
            proceso = self.Proceso.create({'articulo': code, 'workcenter_id': self.rama2.id})
            if vel:
                proceso._ft_set_value('velocidad', vel)
        self.rama2.invalidate_recordset()
        self.assertEqual(self.rama2.ft_proceso_count, 3)
        self.assertEqual(self.rama2.ft_proceso_promedios, 'Velocidad: 31 m/min (2)',
                         "El promedio no cuenta los artículos sin dato")
        with self.assertRaises(Exception):
            with self.env.cr.savepoint():
                self.Proceso.create({'articulo': 'ZZ001', 'workcenter_id': self.rama2.id})

    def test_04_ambito_no_cambia_con_valores(self):
        proceso = self.Proceso.create({'articulo': 'ZZ004', 'workcenter_id': self.rama1.id})
        proceso._ft_set_value('velocidad', 25)
        velocidad = self.tipo.parametro_ids.filtered(lambda p: p.code == 'velocidad')
        with self.assertRaises(ValidationError):
            velocidad.ambito = 'maquina'

    def test_05_importar(self):
        wizard = self.env['ficha.tecnica.proceso.import.wizard'].create({'file_data': b'eA==', 'fuente': 'Prueba'})
        header = ['Artículo', 'Máquina', 'Velocidad (m/min)', 'Merma (%)', 'Peso (g/m2)', 'Notas']
        rows = [('ZZ140Q21JNT165', 'R91', 18, 10, 140, None),
                ('ZZ140Q21JNT165', 'RAMA 92 prueba', None, 10, 140, 'Sin velocidad en la hoja'),
                ('ZZ999', 'RAMA 99', 20, None, None, None)]
        result = wizard._import_rows(header, rows)
        self.assertIn('Procesos creados: 2', result['params']['message'])
        self.assertIn('Peso', result['params']['message'], "Avisa la columna que no es parámetro")
        self.assertIn('RAMA 99', result['params']['message'])
        p1 = self.Proceso._ft_buscar('ZZ140Q21JNT165', self.rama1)
        p2 = self.Proceso._ft_buscar('ZZ140Q21JNT165', self.rama2)
        self.assertEqual((p1._ft_value('velocidad'), p1.fuente), (18, 'Prueba'))
        self.assertFalse(p2._ft_value('velocidad'), "Corre en esa rama, velocidad no dada")
        self.assertEqual(p2.notas, 'Sin velocidad en la hoja')
        # Una celda vacía no borra lo que ya estaba.
        wizard._import_rows(header, [('ZZ140Q21JNT165', 'R91', None, 12, None, None)])
        self.assertEqual((p1._ft_value('velocidad'), p1._ft_value('merma')), (18, 12))

    def test_06_boton_en_la_variante(self):
        """2.8.1: la ficha de la variante (product.product) hereda el botón de la plantilla."""
        proceso = self.Proceso.create({'articulo': 'ZZ140Q21JNT165', 'workcenter_id': self.rama1.id})
        accion = self.product.action_ft_procesos()
        self.assertEqual(accion['res_model'], 'ficha.tecnica.proceso')
        self.assertIn(proceso, self.Proceso.search(accion['domain']))
        self.env.ref('product.product_normal_form_view')._check_xml()

    def test_07_par_kg_metros(self):
        """2.18.0: el proceso va con el código en kg («I» + tela acabada) y la tela acabada en metros lo encuentra."""
        kg = self.env['product.product'].create({'name': 'Tela prueba kg', 'default_code': 'IZZ140Q21JNT165'})
        proceso = self.Proceso.create({'articulo': 'IZZ140Q21JNT165', 'workcenter_id': self.rama1.id})
        self.assertEqual(proceso.product_id, kg)
        self.assertEqual(self.Proceso._ft_codigos('ZZ140Q21JNT165'), ['ZZ140Q21JNT165', 'IZZ140Q21JNT165'])
        self.assertEqual(self.Proceso._ft_codigos('IZZ140Q21JNT165'), ['IZZ140Q21JNT165', 'ZZ140Q21JNT165'])
        self.assertEqual(self.Proceso._ft_codigos('K40GO155'), ['K40GO155'], "Sin par fuera de la regla")
        self.assertEqual(self.Proceso._ft_buscar(self.product, self.rama1), proceso, "Metros → su «I…»")
        self.assertEqual(self.Proceso._ft_buscar(kg, self.rama1), proceso)
        self.assertFalse(self.Proceso._ft_buscar(self.product, self.rama2))
        exacto = self.Proceso.create({'articulo': 'ZZ140Q21JNT165', 'workcenter_id': self.rama1.id})
        self.assertEqual(self.Proceso._ft_buscar(self.product, self.rama1), exacto, "El código exacto manda")
        self.product.product_tmpl_id.invalidate_recordset(['ft_proceso_count'])
        self.assertEqual(self.product.product_tmpl_id.ft_proceso_count, 2, "La tela en metros ve también el «I…»")

    def test_08_ficha_y_unidad_del_proceso(self):
        """2.19.0: el proceso muestra la unidad del producto y lleva a su ficha de acabado."""
        kg = self.env.ref('uom.product_uom_kgm')
        producto_kg = self.env['product.product'].create(
            {'name': 'Tela prueba kg', 'default_code': 'IZZ140Q21JNT165', 'uom_id': kg.id})
        crudo = self.env['product.product'].create({'name': 'Crudo prueba', 'default_code': 'ZZ140Q21HNT165'})
        tejido = self.env['ficha.tecnica.tejido'].create({'articulo': 'ZZ140Q21HNT165', 'product_proceso_id': crudo.id})
        ficha = self.env['ficha.tecnica.acabado'].create({
            'articulo': 'ZZ140Q21JNT165', 'tejido_id': tejido.id, 'product_acabado_id': self.product.id,
            'product_proceso_id': producto_kg.id})
        proceso = self.Proceso.create({'articulo': 'IZZ140Q21JNT165', 'workcenter_id': self.rama1.id})
        self.assertEqual(proceso.product_uom_id, kg)
        self.assertEqual(proceso.ficha_acabado_id, ficha)
        self.assertEqual(proceso.action_ft_ver_ficha_acabado()['res_id'], ficha.id)
        metros = self.Proceso.create({'articulo': 'ZZ140Q21JNT165', 'workcenter_id': self.rama2.id})
        self.assertEqual(metros.ficha_acabado_id, ficha, "También desde la tela acabada en metros")
        self.assertEqual((ficha.bom_proceso_count, ficha.bom_acabado_count), (0, 0))
        archivada = self.env['mrp.bom'].create(
            {'product_tmpl_id': producto_kg.product_tmpl_id.id, 'product_qty': 1, 'active': False})
        ficha.invalidate_recordset(['bom_proceso_count'])
        self.assertEqual(ficha.bom_proceso_count, 1, "Cuenta las listas en kg archivadas")
        self.assertIn(archivada.id, ficha.action_ft_boms_proceso()['domain'][0][2])


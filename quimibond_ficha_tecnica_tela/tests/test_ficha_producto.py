# -*- coding: utf-8 -*-
"""2.23.0: la ficha técnica en las pantallas nativas (producto, operación, centro de trabajo) y el concentrado."""
from odoo.tests import tagged

from .test_tiempo_ficha import TiempoFichaCase


@tagged('post_install', '-at_install')
class TestFichaProducto(TiempoFichaCase):

    def test_01_la_tela_en_su_producto(self):
        tmpl = self.tela_m.product_tmpl_id
        self.assertTrue(tmpl.ft_tiene_ficha and tmpl.ft_es_tela)
        self.assertFalse(tmpl.ft_es_crudo)
        self.assertEqual((tmpl.ft_peso, tmpl.ft_rendimiento), (53.0, 8.0))
        self.assertEqual(tmpl.ft_producto_kg_id, self.tela)
        self.assertEqual(tmpl.ft_crudo_id, self.crudo)
        self.assertEqual(tmpl.ft_crudo_telas_count, 1)
        self.assertEqual(tmpl.ft_bom_kg_id, self.bom)
        self.assertEqual(tmpl.ft_bom_m_id, self.bom_m)
        self.assertEqual(tmpl.ft_operacion_ids, self.bom.operation_ids, "La ruta sale de sus listas de materiales")
        # El producto en kg ve la misma ficha.
        self.assertEqual(self.tela.product_tmpl_id.ft_bom_kg_id, self.bom)

    def test_02_la_tela_en_el_producto_es_de_solo_lectura_y_dice_de_donde_sale(self):
        """2.24.0: el producto no edita la ficha; cada dato dice de qué ficha sale."""
        tmpl = self.tela_m.product_tmpl_id
        for name in ('ft_peso', 'ft_ancho', 'ft_rendimiento', 'ft_crudo_peso', 'ft_crudo_ancho'):
            self.assertTrue(tmpl._fields[name].readonly, name)
            self.assertTrue(tmpl._fields[name].help, "%s dice de dónde sale" % name)
        self.assertEqual(tmpl.ft_acabado_ficha_id, self.acabado)
        self.assertEqual(tmpl.ft_tejido_ficha_id, self.tejido)
        self.acabado.peso_acabado = 54
        self.env.invalidate_all()
        self.assertEqual(tmpl.ft_peso, 54.0, "Lo que se cambia en la ficha de acabado se ve en el producto")

    def test_03_el_crudo_en_su_producto(self):
        tmpl = self.crudo.product_tmpl_id
        self.assertTrue(tmpl.ft_es_crudo and tmpl.ft_tiene_ficha)
        self.assertFalse(tmpl.ft_es_tela)
        self.assertEqual(tmpl.ft_tejido_ids, self.tejido)
        self.assertEqual(tmpl.ft_tela_ids, self.acabado)

    def test_04_concentrado_telas_y_crudos(self):
        Tmpl = self.env['product.template']
        self.assertIn(self.tela_m.product_tmpl_id, Tmpl.search([('ft_es_tela', '=', True)]))
        self.assertNotIn(self.tela.product_tmpl_id, Tmpl.search([('ft_es_tela', '=', True)]),
                         "El concentrado lista la tela acabada, no su producto en kg")
        self.assertIn(self.crudo.product_tmpl_id, Tmpl.search([('ft_es_crudo', '=', True)]))
        self.assertNotIn(self.crudo.product_tmpl_id, Tmpl.search([('ft_es_tela', '=', True)]))

    def test_05_revisar_y_usar_los_datos_desde_el_producto(self):
        tmpl = self.tela_m.product_tmpl_id
        self.assertFalse(tmpl.ft_revision_id)
        tmpl.action_ft_revisar()
        self.env.invalidate_all()
        self.assertTrue(tmpl.ft_revision_id)
        self.assertFalse(tmpl.ft_revision_listo)
        self.tela_m.action_ft_datos_ficha()  # desde la variante: los botones delegan a la plantilla
        self.env.invalidate_all()
        self.assertTrue(tmpl.ft_revision_listo, tmpl.ft_pendiente_ids.mapped('detalle'))

    def test_06_parametros_en_la_operacion_y_telas_en_la_maquina(self):
        self.assertEqual(self.op_tint.ft_param_proceso_ids, self.p_jet | self.env['ficha.tecnica.proceso'].search(
            [('articulo', '=', 'IZZ931Q22JNT165'), ('workcenter_id', '=', self.jet2.id)]),
            "La tela en su jet y en el jet alternativo")
        self.assertEqual(self.op_insp.ft_param_proceso_ids, self.p_insp, "Inspección sin alternativas: su máquina")
        self.assertIn(self.p_jet, self.jet.ft_proceso_ids)
        accion = self.p_jet.action_ft_editar()
        self.assertEqual((accion['res_model'], accion['res_id']), ('ficha.tecnica.proceso', self.p_jet.id))

    def test_07_parametros_del_calculo_sin_duplicar(self):
        """2.23.0: los parámetros que usan los cálculos existen y asegurarlos otra vez no duplica nada."""
        Tipo = self.env['ficha.tecnica.maquina.tipo']
        Tipo._ft_asegurar_parametros()
        self.assertFalse(Tipo._ft_asegurar_parametros(), "La segunda vez no crea nada")
        acabado = self.env.ref('quimibond_ficha_tecnica_tela.ft_maquina_tipo_acabado')
        self.assertEqual(len(acabado.parametro_ids.filtered(lambda p: p.code == 'ancho_rama')), 1)
        jets = self.env.ref('quimibond_ficha_tecnica_tela.ft_maquina_tipo_tintoreria')
        self.assertTrue(jets.parametro_ids.filtered(lambda p: p.code == 'capacidad_grupo_b'))

    def test_08_agregar_los_datos_de_la_tela_desde_la_operacion(self):
        self.p_dev.unlink()
        self.env.invalidate_all()
        self.assertFalse(self.op_dev.ft_proceso_id)
        accion = self.op_dev.action_ft_nuevo_proceso()
        nuevo = self.env['ficha.tecnica.proceso'].browse(accion['res_id'])
        self.assertEqual((nuevo.articulo, nuevo.workcenter_id), ('IZZ931Q22JNT165', self.dev))

    def test_09_la_ficha_de_acabado_sigue_la_lista_del_flujo(self):
        """2.23.0: la ruta de la ficha de acabado sale de la lista del flujo aunque esté archivada."""
        Bom = self.env['mrp.bom']
        Bom.create({'product_tmpl_id': self.tela.product_tmpl_id.id, 'product_qty': 1, 'product_uom_id': self.kg.id,
                    'sequence': 0, 'bom_line_ids': [(0, 0, {'product_id': self.crudo.id, 'product_qty': 1})]})
        self.bom.action_archive()
        self.env.invalidate_all()
        self.assertEqual(self.acabado.crudo_id, self.crudo)
        ops = self.bom.with_context(active_test=False).operation_ids
        self.assertTrue(ops)
        self.assertLessEqual(set(ops.ids), set(self.acabado.operacion_ids.ids),
                             "Archivadas también: al leer el campo Odoo no las descarta")
        self.assertLessEqual(set(ops.ids), set(self.tela_m.product_tmpl_id.ft_operacion_ids.ids),
                             "La ruta en la pestaña del producto, igual")
        self.assertIn(self.op_tint, self.acabado.tint_operacion_ids)

    def test_10_estado_y_filtros_del_concentrado(self):
        """2.23.0: el estado de la tela, los filtros por estado y por área y la búsqueda por crudo."""
        Tmpl = self.env['product.template']
        tmpl = self.tela_m.product_tmpl_id
        self.assertEqual(tmpl.ft_estado, 'sin_revisar')
        self.assertIn(tmpl, Tmpl.search([('ft_estado', '=', 'sin_revisar')]))
        tmpl.action_ft_revisar()
        self.env.invalidate_all()
        self.assertEqual(tmpl.ft_estado, 'activa_pendientes', "La lista en kg de la prueba está activa")
        self.assertIn('Tintorería', tmpl.ft_revision_areas)
        self.assertIn(tmpl, Tmpl.search([('ft_estado', 'in', ['pendientes', 'activa_pendientes'])]))
        self.assertIn(tmpl, Tmpl.search([('ft_area_pendiente', '=', 'tintoreria')]))
        self.assertNotIn(tmpl, Tmpl.search([('ft_area_pendiente', '=', 'acabado')]))
        self.assertIn(tmpl, Tmpl.search([('ft_crudo_id', '=', self.crudo.id)]))
        self.assertIn(tmpl, Tmpl.search([('ft_crudo_id', 'ilike', 'Crudo 931')]))
        self.bom.action_archive()
        tmpl.action_ft_datos_ficha()
        self.env.invalidate_all()
        self.assertEqual(tmpl.ft_estado, 'para_activar', tmpl.ft_pendiente_ids.mapped('detalle'))
        self.assertIn(tmpl, Tmpl.search([('ft_estado', '=', 'para_activar')]))
        self.bom.action_unarchive()
        self.env.invalidate_all()
        self.assertEqual(tmpl.ft_estado, 'activa', "Activar la lista cuenta sin esperar a la revisión de la noche")
        self.assertIn(tmpl, Tmpl.search([('ft_estado', '=', 'activa')]))

    def test_11_validaciones_de_la_ficha(self):
        """2.23.0: ficha de tejido sin rendimiento y ficha de acabado sin peso, avisadas a Ingeniería."""
        self.tejido.rendimiento_tela_tejida = 0
        self.acabado.peso_acabado = 0
        self.env.invalidate_all()
        revision = self.env['ficha.tecnica.revision']._ft_actualizar(self.acabado)
        tipos = {(p.tipo, p.area) for p in revision.pendiente_ids}
        self.assertIn(('datos_tejido', 'ingenieria'), tipos)
        self.assertIn(('datos_acabado', 'ingenieria'), tipos)
        self.assertIn('peso', revision.pendiente_ids.filtered(lambda p: p.tipo == 'datos_acabado').detalle)

    def test_12_crear_la_ficha_desde_el_producto(self):
        """2.23.0: la ruta empieza en el producto: tela, crudo o acabado en kg sin ficha."""
        Product = self.env['product.product']
        meter = self.env.ref('uom.product_uom_meter')
        tela = Product.create({'name': 'Tela 934 prueba (m)', 'default_code': 'ZZ934Q22JNT165', 'uom_id': meter.id})
        kg = Product.create({'name': 'Tela 934 prueba (kg)', 'default_code': 'IZZ934Q22JNT165', 'uom_id': self.kg.id})
        crudo = Product.create({'name': 'Crudo 934 prueba', 'default_code': 'ZZ934Q22HNT200', 'uom_id': self.kg.id})
        self.assertEqual((tela.ft_tipo_codigo, kg.ft_tipo_codigo, crudo.ft_tipo_codigo), ('tela', 'kg', 'crudo'))
        self.assertFalse(tela.ft_tiene_ficha)
        accion = tela.action_ft_crear_ficha()
        self.assertEqual(accion['res_model'], 'ficha.tecnica.acabado')
        self.assertEqual(accion['context'], {'default_articulo': 'ZZ934Q22JNT165', 'default_product_acabado_id': tela.id,
                                             'default_product_proceso_id': kg.id})
        accion = crudo.action_ft_crear_ficha()
        self.assertEqual((accion['res_model'], accion['context']['default_product_proceso_id']),
                         ('ficha.tecnica.tejido', crudo.id))
        accion = kg.action_ft_crear_ficha()
        self.assertEqual((accion['res_model'], accion['res_id']), ('product.template', tela.product_tmpl_id.id))
        contexto = tela.action_ft_crear_ficha()['context']
        ficha = self.env['ficha.tecnica.acabado'].with_context(**contexto).create({'tejido_id': self.tejido.id})
        self.assertEqual((ficha.product_acabado_id, ficha.product_proceso_id), (tela, kg))
        self.env.invalidate_all()
        self.assertTrue(tela.ft_tiene_ficha and tela.product_tmpl_id.ft_es_tela)
        self.assertEqual(tela.product_tmpl_id.ft_estado, 'sin_revisar')

    def test_13_actualizar_la_revision_desde_el_concentrado(self):
        """2.23.0: el botón de la cabecera llega con los renglones seleccionados como argumento."""
        from odoo.service.model import call_kw
        accion = call_kw(self.env['product.template'], 'action_ft_actualizar_revision', [[]], {})
        self.assertEqual(accion['tag'], 'reload')
        self.assertTrue(self.env['ficha.tecnica.revision'].search([('acabado_id', '=', self.acabado.id)]))

    def test_14_la_lista_anterior_activa_no_es_activa(self):
        """2.23.0: si la única lista en kg es la anterior (sin operaciones), la tela no está «Activa con pendientes»:
        le falta la lista del flujo."""
        self.bom.operation_ids.unlink()
        self.env.invalidate_all()
        tmpl = self.tela_m.product_tmpl_id
        tmpl.action_ft_revisar()
        self.env.invalidate_all()
        self.assertEqual(tmpl.ft_estado, 'pendientes')
        tipos = {(p.tipo, p.area) for p in tmpl.ft_pendiente_ids}
        self.assertIn(('lista_kg', 'ingenieria'), tipos)
        self.assertNotIn(('crudo', 'ingenieria'), tipos)

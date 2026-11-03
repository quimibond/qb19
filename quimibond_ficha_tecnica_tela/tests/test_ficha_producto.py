# -*- coding: utf-8 -*-
"""2.25.0: la ficha técnica en las pantallas nativas (producto, operación, centro de trabajo) y el concentrado."""
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

    def test_02_editar_la_tela_desde_el_producto(self):
        tmpl = self.tela_m.product_tmpl_id
        tmpl.ft_peso = 54
        tmpl.ft_rendimiento = 7.5
        self.assertEqual((self.acabado.peso_acabado, self.acabado.rendimiento_tela_acabada), (54.0, 7.5),
                         "Se guarda en su ficha de acabado: el SGI, el costeo y la Programación siguen leyendo de ahí")

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

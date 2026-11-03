# -*- coding: utf-8 -*-
"""2.24.0: «Resolver» en cada pendiente y «Agregar circular» en la ficha de tejido y en el crudo."""
from odoo.tests import tagged

from .test_tiempo_ficha import TiempoFichaCase


@tagged('post_install', '-at_install')
class TestCaptura(TiempoFichaCase):

    def _pendiente(self, tipo):
        revision = self.env['ficha.tecnica.revision']._ft_actualizar(self.acabado)
        pendiente = revision.pendiente_ids.filtered(lambda p: p.tipo == tipo)[:1]
        self.assertTrue(pendiente, "La revisión trae el pendiente %s" % tipo)
        return pendiente

    def test_01_resolver_abre_donde_se_corrige(self):
        tiempo = self._pendiente('tiempo_distinto')
        accion = tiempo.action_ft_resolver()
        self.assertEqual((accion['res_model'], accion['res_id']), ('mrp.routing.workcenter', tiempo.operation_id.id))
        capacidad = self._pendiente('capacidad_falta')
        accion = capacidad.action_ft_resolver()
        self.assertEqual((accion['res_model'], accion['res_id']), ('mrp.workcenter', self.jet.id))
        conversion = self._pendiente('conversion')
        accion = conversion.action_ft_resolver()
        self.assertEqual((accion['res_model'], accion['res_id']), ('mrp.bom', self.bom_m.id))

    def test_02_resolver_maquina_sin_proceso_crea_los_datos_de_la_tela(self):
        self.p_insp.unlink()
        self.env.invalidate_all()
        accion = self._pendiente('sin_proceso').action_ft_resolver()
        self.assertEqual(accion['res_model'], 'ficha.tecnica.proceso')
        proceso = self.env['ficha.tecnica.proceso'].browse(accion['res_id'])
        self.assertEqual((proceso.articulo, proceso.workcenter_id), ('IZZ931Q22JNT165', self.insp))

    def test_03_resolver_ficha_sin_peso_abre_la_ficha_de_acabado(self):
        self.acabado.peso_acabado = 0
        accion = self._pendiente('datos_acabado').action_ft_resolver()
        self.assertEqual((accion['res_model'], accion['res_id']), ('ficha.tecnica.acabado', self.acabado.id))

    def test_04_agregar_circular_copia_todo_menos_la_maquina(self):
        self.tejido.write({
            'velocidad': 20, 'tension': '5',
            'hilo_line_ids': [(0, 0, {'numero': 1, 'tipo_hilo': 'SET', 'titulo_hilo': '75/36', 'porcentaje': 60}),
                              (0, 0, {'numero': 2, 'tipo_hilo': 'STRETCH', 'titulo_hilo': '40', 'porcentaje': 40})]})
        circular = self.env['mrp.workcenter'].create({'name': 'CIRCULAR 931 prueba', 'code': 'C931'})
        accion = self.tejido.action_ft_agregar_circular()
        self.assertEqual(accion['res_model'], 'ficha.tecnica.tejido')
        self.assertNotIn('default_maquina_tejido', accion['context'])
        nueva = self.env['ficha.tecnica.tejido'].with_context(**accion['context']).create(
            {'maquina_tejido': circular.id})
        self.assertNotEqual(nueva, self.tejido)
        self.assertEqual((nueva.articulo, nueva.product_proceso_id, nueva.maquina_tejido),
                         (self.tejido.articulo, self.crudo, circular))
        self.assertEqual((nueva.velocidad, nueva.tension), (20, '5'))
        self.assertEqual(sorted(nueva.hilo_line_ids.mapped('porcentaje')), [40, 60])
        self.assertEqual(len(self.tejido.hilo_line_ids), 2, "La ficha original se queda con sus hilos")
        self.assertIn(nueva, self.crudo.product_tmpl_id.ft_tejido_ids, "El crudo ya la lista como otra circular")

    def test_05_agregar_circular_desde_el_crudo(self):
        accion = self.crudo.product_tmpl_id.action_ft_agregar_circular()
        self.assertEqual(accion['context'].get('default_articulo'), self.tejido.articulo)
        self.assertEqual(accion['context'].get('default_product_proceso_id'), self.crudo.id)

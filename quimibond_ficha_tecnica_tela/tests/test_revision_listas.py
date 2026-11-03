# -*- coding: utf-8 -*-
"""2.24.0: revisión de las listas en kg contra la ficha, por tela y por área."""
from odoo.service.model import call_kw
from odoo.tests import tagged

from .test_tiempo_ficha import TiempoFichaCase


@tagged('post_install', '-at_install')
class TestRevisionListas(TiempoFichaCase):

    def _revision(self, acabado=None):
        return self.env['ficha.tecnica.revision']._ft_actualizar(acabado or self.acabado)

    def _tipos(self, revision):
        return sorted((p.tipo, p.area) for p in revision.pendiente_ids)

    def test_01_pendientes_por_area(self):
        revision = self._revision()
        self.assertEqual(revision.bom_kg_id, self.bom)
        self.assertEqual(revision.bom_m_id, self.bom_m)
        self.assertFalse(revision.listo)
        self.assertEqual(self._tipos(revision), sorted([
            ('tiempo_distinto', 'manufactura'), ('tiempo_distinto', 'manufactura'),  # devanado y abridora
            ('tiempo_distinto', 'inspeccion'),
            ('capacidad_falta', 'tintoreria'),  # el jet no tiene la capacidad para el producto
            ('conversion', 'ingenieria'),       # 0.2 kg por metro y la ficha da 1 ÷ 8
        ]))
        self.assertTrue(revision.pend_tintoreria and revision.pend_inspeccion and revision.pend_ingenieria)
        self.assertFalse(revision.pend_acabado)
        self.assertIn('la ficha da 600', revision.pendiente_ids.filtered(lambda p: p.tipo == 'capacidad_falta').detalle)

    def test_02_usar_los_datos_de_la_ficha_la_deja_lista(self):
        revision = self._revision()
        revision.action_ft_datos_ficha()
        self.env.invalidate_all()
        revision = self.env['ficha.tecnica.revision'].search([('acabado_id', '=', self.acabado.id)])
        self.assertEqual(len(revision), 1, "Una sola fila por tela")
        self.assertTrue(revision.listo, revision.pendiente_ids.mapped('detalle'))
        self.assertFalse(revision.pendiente_ids)

    def test_03_maquina_sin_proceso_y_tiempo_en_cero(self):
        self.p_insp.unlink()
        self.op_dev.time_cycle_manual = 0
        self.env.invalidate_all()
        revision = self._revision()
        tipos = self._tipos(revision)
        self.assertIn(('sin_proceso', 'inspeccion'), tipos)
        self.assertIn(('tiempo_cero', 'manufactura'), tipos)
        self.assertNotIn(('tiempo_distinto', 'inspeccion'), tipos, "Sin proceso no hay tiempo de la ficha")
        cero = revision.pendiente_ids.filtered(lambda p: p.tipo == 'tiempo_cero')
        self.assertIn('la ficha da 0.2200 min', cero.detalle)

    def test_04_ficha_sin_acabado_en_proceso_ni_lista_en_metros(self):
        tela_m = self.env['product.product'].create({'name': 'Tela 933 prueba (m)', 'default_code': 'ZZ933Q22JNT165',
                                                     'uom_id': self.meter.id})
        acabado = self.env['ficha.tecnica.acabado'].create({
            'articulo': 'ZZ933Q22JNT165', 'tejido_id': self.tejido.id, 'product_acabado_id': tela_m.id})
        revision = self._revision(acabado)
        self.assertEqual(self._tipos(revision), [('lista_m', 'ingenieria'), ('sin_producto_kg', 'ingenieria')])
        self.assertEqual(revision.areas, 'Ingeniería (Innovación)')

    def test_05_la_lista_del_flujo_aunque_este_archivada(self):
        """2.25.1: la lista vieja activa (sin operaciones) no gana a la del flujo en kg archivada."""
        Bom = self.env['mrp.bom']
        vieja = Bom.create({'product_tmpl_id': self.tela.product_tmpl_id.id, 'product_qty': 1,
                            'product_uom_id': self.kg.id, 'sequence': 0,
                            'bom_line_ids': [(0, 0, {'product_id': self.crudo.id, 'product_qty': 1})]})
        self.bom.action_archive()
        self.assertEqual(self.env['ficha.tecnica.revision']._ft_bom_de(self.tela), self.bom)
        piloto = self.bom.copy({'code': 'PILOTO TINTORERIA (no usar)', 'active': True, 'sequence': 0})
        self.assertEqual(self.env['ficha.tecnica.revision']._ft_bom_de(self.tela), self.bom,
                         "La que dice «no usar» va al final aunque esté activa y con operaciones")
        self.assertTrue(vieja and piloto)

    def test_06_boton_actualizar_como_lo_llama_la_pantalla(self):
        """2.25.1: el botón de la cabecera de la lista llega con la lista de renglones (vacía) como argumento."""
        accion = call_kw(self.env['ficha.tecnica.revision'], 'action_ft_actualizar', [[]], {})
        self.assertEqual(accion['tag'], 'reload')
        self.assertTrue(self.env['ficha.tecnica.revision'].search([('acabado_id', '=', self.acabado.id)]))

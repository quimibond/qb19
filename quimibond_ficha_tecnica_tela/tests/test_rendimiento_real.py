# -*- coding: utf-8 -*-
"""2.24.0: un solo rendimiento de tela acabada, calculado con lo fabricado, y el peso real. 2.27.0: por kg de tela
terminada (conversiones a kg y rollos de Inspección); estimado con el consumo de crudo si no hay."""
from unittest.mock import patch

from odoo.tests import tagged

from ..models.rendimiento_real import ancho_en_m, es_tela_kg
from .test_tiempo_ficha import TiempoFichaCase


@tagged('post_install', '-at_install')
class TestRendimientoReal(TiempoFichaCase):

    def _con_fabricado(self, datos):
        Acabado = type(self.env['ficha.tecnica.acabado'])
        return patch.object(Acabado, '_ft_fabricado', return_value=datos)

    def _con_terminado(self, datos):
        """2.27.0: lo medido por kg de tela terminada; sin datos de entrada (fabricado vacío)."""
        Acabado = type(self.env['ficha.tecnica.acabado'])
        return patch.multiple(Acabado, _ft_terminado=lambda *a, **k: datos, _ft_fabricado=lambda *a, **k: {})

    def test_01_que_cuenta_como_tela(self):
        self.assertTrue(es_tela_kg('WJ044Q22INT235'), "Teñido (I en la posición 9)")
        self.assertTrue(es_tela_kg('WJ044Q22HNT235'), "Crudo")
        self.assertTrue(es_tela_kg('IWJ053Q22JNT160'), "Acabado en kg")
        self.assertFalse(es_tela_kg('ENGOMADO002'), "Los químicos no son tela")
        self.assertEqual((ancho_en_m(160), ancho_en_m(1.6)), (1.6, 1.6))

    def test_02_con_tela_terminada_el_rendimiento_es_el_medido(self):
        with self._con_terminado({self.acabado: (5099.0, 477.0, 4)}):
            self.acabado._ft_actualizar_rendimiento_real()
        self.assertAlmostEqual(self.acabado.rendimiento_tela_acabada, round(5099.0 / 477.0, 4), places=6)
        self.assertEqual(self.acabado.rendimiento_real_ordenes, 4)
        self.assertEqual(self.acabado.rendimiento_fuente, 'terminado')
        self.assertAlmostEqual(self.tela_m.product_tmpl_id.ft_rendimiento, round(5099.0 / 477.0, 4), places=6)

    def test_03_sin_ordenes_se_queda_el_de_la_ficha(self):
        with self._con_terminado({}):
            self.acabado._ft_actualizar_rendimiento_real()
        self.assertEqual(self.acabado.rendimiento_tela_acabada, 8.0)
        self.assertFalse(self.acabado.rendimiento_real_ordenes)
        self.assertEqual(self.acabado.rendimiento_fuente, 'ficha')
        self.assertFalse(self.acabado.peso_real, "Sin lo fabricado no hay peso real")

    def test_04_peso_real(self):
        self.acabado.ancho_acabado = 165
        with self._con_terminado({self.acabado: (10000.0, 1000.0, 5)}):
            self.acabado._ft_actualizar_rendimiento_real()
        self.assertAlmostEqual(self.acabado.peso_real, 1000.0 / (10.0 * 1.65), places=1)

    def test_05_la_inspeccion_usa_el_mismo_rendimiento(self):
        with self._con_terminado({self.acabado: (9000.0, 1000.0, 3)}):
            self.acabado._ft_actualizar_rendimiento_real()
        self.env.invalidate_all()
        self.assertIn('9 m/kg', self.op_insp.ft_tiempo_calculo or '')

    def test_06_sin_productos_no_hay_fabricado(self):
        Acabado = self.env['ficha.tecnica.acabado']
        self.assertEqual(Acabado._ft_fabricado(self.env['product.product'], '2026-01-01'), {})

    def test_07_los_saldos_se_restan_de_los_kilos(self):
        """Los saldos en kg que salen de la orden en metros (segundas, desperdicio) no son metros de la tela: se
        restan de los kilos consumidos (si no, la primera para planear descontaría la misma merma dos veces)."""
        Product = self.env['product.product']
        saldo = Product.search([('default_code', '=', 'SALDO DE TELA TEJIDO')], limit=1) or Product.create({
            'name': 'SALDO DE TELA TEJIDO', 'default_code': 'SALDO DE TELA TEJIDO', 'uom_id': self.kg.id,
            'is_storable': True})
        self.bom_m.write({'byproduct_ids': [(0, 0, {'product_id': saldo.id, 'product_qty': 0.0,
                                                    'product_uom_id': self.kg.id})]})
        mo = self.env['mrp.production'].create({'product_id': self.tela_m.id, 'bom_id': self.bom_m.id,
                                                'product_qty': 100, 'product_uom_id': self.meter.id})
        mo.action_confirm()
        mo.qty_producing = 100
        mo.move_raw_ids.write({'quantity': 20.0, 'picked': True})
        saldo_move = mo.move_finished_ids.filtered(lambda m: m.product_id == saldo)
        saldo_move.write({'quantity': 1.5, 'picked': True})
        res = mo.button_mark_done()
        if isinstance(res, dict) and res.get('res_model'):
            wiz = self.env[res['res_model']].with_context(**res.get('context', {})).create({})
            getattr(wiz, 'action_confirm', getattr(wiz, 'process', lambda: None))()
        self.assertEqual(mo.state, 'done')
        datos = self.env['ficha.tecnica.acabado']._ft_fabricado(self.tela_m, '2000-01-01')
        metros, kilos, ordenes = datos[self.tela_m]
        self.assertAlmostEqual(metros, 100.0, places=4)
        self.assertAlmostEqual(kilos, 20.0 - 1.5, places=4)
        self.assertEqual(ordenes, 1)

    def test_08_cambio_grande_queda_por_revisar(self):
        """2.26.0: el recálculo nocturno escribe el rendimiento, pero si se mueve más que la alerta queda un pendiente de
        Ingeniería hasta que alguien lo marca revisado."""
        self.assertEqual(self.acabado.rendimiento_tela_acabada, 8.0)
        with self._con_terminado({self.acabado: (9000.0, 1000.0, 3)}):
            self.acabado._ft_actualizar_rendimiento_real()
        self.assertEqual(self.acabado.rendimiento_tela_acabada, 9.0, "El número se escribe igual")
        self.assertTrue(self.acabado.rendimiento_por_revisar)
        self.assertEqual(self.acabado.rendimiento_anterior, 8.0)
        Revision = self.env['ficha.tecnica.revision']
        revision = Revision._ft_actualizar(self.acabado)
        pendiente = revision.pendiente_ids.filtered(lambda p: p.tipo == 'rendimiento_cambio')
        self.assertEqual(pendiente.area, 'ingenieria')
        self.assertIn('+12.5 %', pendiente.detalle)
        self.acabado.action_ft_rendimiento_revisado()
        revision = Revision._ft_actualizar(self.acabado)
        self.assertFalse(revision.pendiente_ids.filtered(lambda p: p.tipo == 'rendimiento_cambio'))

    def test_09_cambio_chico_no_avisa(self):
        with self._con_terminado({self.acabado: (8200.0, 1000.0, 3)}):
            self.acabado._ft_actualizar_rendimiento_real()
        self.assertEqual(self.acabado.rendimiento_tela_acabada, 8.2)
        self.assertFalse(self.acabado.rendimiento_por_revisar, "2.5 % no pasa la alerta de 5 %")
        self.env['ir.config_parameter'].sudo().set_param('quimibond_ficha_tecnica_tela.alerta_rendimiento_pct', '1')
        with self._con_terminado({self.acabado: (8400.0, 1000.0, 3)}):
            self.acabado._ft_actualizar_rendimiento_real()
        self.assertTrue(self.acabado.rendimiento_por_revisar, "Con la alerta en 1 %, 2.4 % sí avisa")

    def test_10_sin_tela_terminada_se_estima_con_el_consumo_de_crudo(self):
        """2.27.0: sin conversiones ni rollos, metros por kg que entró × kg que entran por kg de tela acabada. Sin consumo
        de crudo capturado no se estima: se queda el de la ficha (antes quedaba ≈10 % abajo, por kg de crudo)."""
        Acabado = type(self.env['ficha.tecnica.acabado'])
        with patch.multiple(Acabado, _ft_terminado=lambda *a, **k: {},
                            _ft_fabricado=lambda *a, **k: {self.tela_m: (9000.0, 1000.0, 3)}):
            self.acabado.consumo_crudo = 0.0
            self.acabado._ft_actualizar_rendimiento_real()
            self.assertEqual((self.acabado.rendimiento_tela_acabada, self.acabado.rendimiento_fuente), (8.0, 'ficha'))
            self.acabado.consumo_crudo = 1.1
            self.acabado._ft_actualizar_rendimiento_real()
        self.assertAlmostEqual(self.acabado.rendimiento_tela_acabada, 9.9, places=4)
        self.assertEqual((self.acabado.rendimiento_fuente, self.acabado.rendimiento_real_ordenes), ('estimado', 3))

    def test_11_conversion_de_metros_a_kg(self):
        """Una conversión real (consume la tela en metros y produce su acabado en kg, como TL/CVU/02517: 7,560 m → 470.1
        kg) da el rendimiento por kg de tela terminada; una orden que fabrica la tela en metros no cuenta aquí."""
        # Sin lista de materiales: la lista en metros ya consume el acabado en kg y una lista al revés sería un ciclo.
        mo = self.env['mrp.production'].create({
            'product_id': self.tela.id, 'product_qty': 470.1, 'product_uom_id': self.kg.id, 'bom_id': False,
            'move_raw_ids': [(0, 0, {'product_id': self.tela_m.id, 'product_uom_qty': 7560.0,
                                     'product_uom': self.meter.id})]})
        mo.action_confirm()
        mo.qty_producing = 470.1
        mo.move_raw_ids.write({'quantity': 7560.0, 'picked': True})
        res = mo.button_mark_done()
        if isinstance(res, dict) and res.get('res_model'):
            wiz = self.env[res['res_model']].with_context(**res.get('context', {})).create({})
            getattr(wiz, 'action_confirm', getattr(wiz, 'process', lambda: None))()
        self.assertEqual(mo.state, 'done')
        datos = self.env['ficha.tecnica.acabado']._ft_terminado(self.acabado, '2000-01-01')
        metros, kilos, n = datos[self.acabado]
        self.assertAlmostEqual(metros, 7560.0, places=2)
        self.assertAlmostEqual(kilos, 470.1, places=2)
        self.assertEqual(n, 1)
        self.acabado._ft_actualizar_rendimiento_real()
        self.assertAlmostEqual(self.acabado.rendimiento_tela_acabada, round(7560.0 / 470.1, 4), places=4)
        self.assertEqual(self.acabado.rendimiento_fuente, 'terminado')


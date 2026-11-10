# -*- coding: utf-8 -*-
"""2.24.0: un solo rendimiento de tela acabada, calculado con lo fabricado, y el peso real. 2.27.0: por kg de tela
terminada (conversiones a kg y rollos de Inspección); estimado con el consumo de crudo si no hay. 2.27.2: los rollos
ganan a las conversiones y el rendimiento escrito a mano queda forzado."""
from unittest.mock import patch

from odoo.tests import tagged

from ..models.rendimiento_real import ancho_en_m, es_tela_kg, tipo_tela
from .test_tiempo_ficha import TiempoFichaCase


@tagged('post_install', '-at_install')
class TestRendimientoReal(TiempoFichaCase):

    def _con_fabricado(self, datos):
        Acabado = type(self.env['ficha.tecnica.acabado'])
        return patch.object(Acabado, '_ft_fabricado', return_value=datos)

    def _con_terminado(self, datos, rollos=None, fabricado=None):
        """2.27.0: lo medido por kg de tela terminada (conversiones; 2.27.2: rollos aparte); sin datos de entrada
        (fabricado vacío) salvo que se den."""
        Acabado = type(self.env['ficha.tecnica.acabado'])
        return patch.multiple(Acabado, _ft_conversiones=lambda *a, **k: datos,
                              _ft_rollos=lambda *a, **k: rollos or {},
                              _ft_fabricado=lambda *a, **k: fabricado or {})

    def _recalcular_y_usar(self, ficha=None):
        """2.28.0: el recálculo propone; Ingeniería lo aprueba con «Usar el medido». Las pruebas que miden el cálculo
        hacen las dos cosas."""
        ficha = ficha or self.acabado
        ficha._ft_actualizar_rendimiento_real()
        ficha.action_ft_usar_rendimiento_medido()

    def test_01_que_cuenta_como_tela(self):
        self.assertTrue(es_tela_kg('WJ044Q22INT235'), "Teñido (I en la posición 9)")
        self.assertTrue(es_tela_kg('WJ044Q22HNT235'), "Crudo")
        self.assertTrue(es_tela_kg('IWJ053Q22JNT160'), "Acabado en kg")
        self.assertFalse(es_tela_kg('ENGOMADO002'), "Los químicos no son tela")
        # Revisión 2026-11-06: el reproceso (punto al final) es tela que entró; el «I…» de 15 es tela terminada.
        self.assertEqual(tipo_tela('WD038Q46ING163.'), 'entrada')
        self.assertEqual(tipo_tela('WD040Q46ING098'), 'entrada')
        self.assertEqual(tipo_tela('IWJ053Q22JNT160'), 'terminada')
        self.assertIsNone(tipo_tela('LAVADO001'))
        self.assertEqual((ancho_en_m(160), ancho_en_m(1.6)), (1.6, 1.6))

    def test_02_con_tela_terminada_el_rendimiento_es_el_medido(self):
        with self._con_terminado({self.acabado: (5099.0, 477.0, 4)}):
            self._recalcular_y_usar()
        self.assertAlmostEqual(self.acabado.rendimiento_tela_acabada, round(5099.0 / 477.0, 4), places=6)
        self.assertEqual(self.acabado.rendimiento_real_ordenes, 4)
        self.assertEqual(self.acabado.rendimiento_fuente, 'terminado')
        self.assertAlmostEqual(self.tela_m.product_tmpl_id.ft_rendimiento, round(5099.0 / 477.0, 4), places=6)

    def test_03_sin_ordenes_se_queda_el_de_la_ficha(self):
        with self._con_terminado({}):
            self._recalcular_y_usar()
        self.assertEqual(self.acabado.rendimiento_tela_acabada, 8.0)
        self.assertFalse(self.acabado.rendimiento_real_ordenes)
        self.assertEqual(self.acabado.rendimiento_fuente, 'ficha')
        self.assertFalse(self.acabado.peso_real, "Sin lo fabricado no hay peso real")

    def test_04_peso_real(self):
        self.acabado.ancho_acabado = 165
        with self._con_terminado({self.acabado: (10000.0, 1000.0, 5)}):
            self._recalcular_y_usar()
        self.assertAlmostEqual(self.acabado.peso_real, 1000.0 / (10.0 * 1.65), places=1)

    def test_05_la_inspeccion_usa_el_mismo_rendimiento(self):
        with self._con_terminado({self.acabado: (9000.0, 1000.0, 3)}):
            self._recalcular_y_usar()
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
        metros, kg_entrada, kg_terminada, kg_saldos, ordenes = datos[self.tela_m]
        self.assertAlmostEqual(metros, 100.0, places=4)
        # Revisión 2026-11-06: la orden en metros consume la tela acabada en kg (IZZ931…): son kg de tela terminada, aparte.
        self.assertAlmostEqual(kg_entrada, 0.0, places=4)
        self.assertAlmostEqual(kg_terminada, 20.0, places=4)
        self.assertAlmostEqual(kg_saldos, 1.5, places=4)
        self.assertEqual(ordenes, 1)
        # El estimado: 100 m entre (20 kg de tela terminada − 1.5 de saldos), sin multiplicar por el consumo de crudo.
        self.acabado.consumo_crudo = 1.1
        with patch.multiple(type(self.acabado), _ft_conversiones=lambda *a, **k: {}, _ft_rollos=lambda *a, **k: {}):
            self._recalcular_y_usar()
        self.assertEqual(self.acabado.rendimiento_fuente, 'estimado')
        self.assertAlmostEqual(self.acabado.rendimiento_tela_acabada, round(100.0 / 18.5, 4), places=4)

    def test_08_cambio_grande_se_propone_y_la_ficha_no_cambia(self):
        """2.28.0: el recálculo nocturno PROPONE: lo medido queda aparte y la ficha no cambia hasta que Ingeniería lo usa.
        Si se mueve más que la alerta queda un pendiente de Ingeniería."""
        self.assertEqual(self.acabado.rendimiento_tela_acabada, 8.0)
        with self._con_terminado({self.acabado: (9000.0, 1000.0, 3)}):
            self.acabado._ft_actualizar_rendimiento_real()
        self.assertEqual(self.acabado.rendimiento_tela_acabada, 8.0, "La ficha no cambia sola")
        self.assertEqual((self.acabado.rendimiento_medido, self.acabado.rendimiento_medido_fuente,
                          self.acabado.rendimiento_medido_estado), (9.0, 'terminado', 'propuesto'))
        self.assertTrue(self.acabado.rendimiento_por_revisar)
        Revision = self.env['ficha.tecnica.revision']
        revision = Revision._ft_actualizar(self.acabado)
        pendiente = revision.pendiente_ids.filtered(lambda p: p.tipo == 'rendimiento_cambio')
        self.assertEqual(pendiente.area, 'ingenieria')
        self.assertIn('+12.5 %', pendiente.detalle)
        # «Marcar revisado»: se queda el de la ficha.
        self.acabado.action_ft_rendimiento_revisado()
        self.assertEqual(self.acabado.rendimiento_tela_acabada, 8.0)
        revision = Revision._ft_actualizar(self.acabado)
        self.assertFalse(revision.pendiente_ids.filtered(lambda p: p.tipo == 'rendimiento_cambio'))
        # «Usar el medido»: pasa a la ficha y deja el anterior.
        self.acabado.action_ft_usar_rendimiento_medido()
        self.assertEqual((self.acabado.rendimiento_tela_acabada, self.acabado.rendimiento_fuente,
                          self.acabado.rendimiento_anterior, self.acabado.rendimiento_medido_estado),
                         (9.0, 'terminado', 8.0, 'igual'))

    def test_09_cambio_chico_no_avisa(self):
        with self._con_terminado({self.acabado: (8200.0, 1000.0, 3)}):
            self.acabado._ft_actualizar_rendimiento_real()
        self.assertEqual(self.acabado.rendimiento_tela_acabada, 8.0)
        self.assertEqual(self.acabado.rendimiento_medido_estado, 'igual', "2.5 % no pasa la alerta de 5 %")
        self.assertFalse(self.acabado.rendimiento_por_revisar)
        self.env['ir.config_parameter'].sudo().set_param('quimibond_ficha_tecnica_tela.alerta_rendimiento_pct', '1')
        with self._con_terminado({self.acabado: (8200.0, 1000.0, 3)}):
            self.acabado._ft_actualizar_rendimiento_real()
        self.assertTrue(self.acabado.rendimiento_por_revisar, "Con la alerta en 1 %, 2.5 % sí avisa")

    def test_19_fuera_del_teorico_no_se_propone(self):
        """2.28.0: lo medido que se aleja más de 25 % del teórico (1000 ÷ peso × ancho) es un dato malo: no se propone,
        «Usar el medido» no lo aplica y queda un pendiente para revisar los datos."""
        self.acabado.write({'peso_acabado': 62.5, 'peso_acabado_tol_unit': 'g/m2', 'ancho_acabado': 200})
        self.assertAlmostEqual(self.acabado.rendimiento_teorico, 8.0, places=4)
        with self._con_terminado({self.acabado: (16000.0, 1000.0, 3)}):
            self.acabado._ft_actualizar_rendimiento_real()
        self.assertEqual(self.acabado.rendimiento_medido_estado, 'fuera')
        self.assertFalse(self.acabado.rendimiento_por_revisar)
        self.acabado.action_ft_usar_rendimiento_medido()
        self.assertEqual(self.acabado.rendimiento_tela_acabada, 8.0, "Lo fuera del teórico no se aplica")
        revision = self.env['ficha.tecnica.revision']._ft_actualizar(self.acabado)
        self.assertTrue(revision.pendiente_ids.filtered(lambda p: p.tipo == 'rendimiento_fuera'))
        # Dentro del tope sí se propone.
        with self._con_terminado({self.acabado: (9000.0, 1000.0, 3)}):
            self.acabado._ft_actualizar_rendimiento_real()
        self.assertEqual(self.acabado.rendimiento_medido_estado, 'propuesto')

    def test_10_sin_tela_terminada_se_estima_con_el_consumo_de_crudo(self):
        """2.27.0: sin conversiones ni rollos, metros por kg que entró × kg que entran por kg de tela acabada. Sin consumo
        de crudo capturado no se estima: se queda el de la ficha (antes quedaba ≈10 % abajo, por kg de crudo)."""
        with self._con_terminado({}, fabricado={self.tela_m: (9000.0, 1000.0, 0.0, 0.0, 3)}):
            self.acabado.consumo_crudo = 0.0
            self._recalcular_y_usar()
            self.assertEqual((self.acabado.rendimiento_tela_acabada, self.acabado.rendimiento_fuente), (8.0, 'ficha'))
            self.acabado.consumo_crudo = 1.1
            self._recalcular_y_usar()
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
        datos = self.env['ficha.tecnica.acabado']._ft_conversiones(self.acabado, '2000-01-01')
        metros, kilos, n = datos[self.acabado]
        self.assertAlmostEqual(metros, 7560.0, places=2)
        self.assertAlmostEqual(kilos, 470.1, places=2)
        self.assertEqual(n, 1)
        # Revisión 2026-11-06: una sola conversión no pisa la ficha; con tres, sí.
        self._recalcular_y_usar()
        self.assertEqual((self.acabado.rendimiento_fuente, self.acabado.rendimiento_tela_acabada), ('ficha', 8.0))
        self._conversion(7500.0, 466.0)
        self._conversion(7620.0, 474.0)
        self._recalcular_y_usar()
        self.assertAlmostEqual(self.acabado.rendimiento_tela_acabada,
                               round((7560.0 + 7500.0 + 7620.0) / (470.1 + 466.0 + 474.0), 4), places=4)
        self.assertEqual(self.acabado.rendimiento_fuente, 'terminado')

    def _conversion(self, metros, kilos):
        """Una conversión terminada: consume ``metros`` de la tela y produce ``kilos`` de su acabado en kg."""
        mo = self.env['mrp.production'].create({
            'product_id': self.tela.id, 'product_qty': kilos, 'product_uom_id': self.kg.id, 'bom_id': False,
            'move_raw_ids': [(0, 0, {'product_id': self.tela_m.id, 'product_uom_qty': metros,
                                     'product_uom': self.meter.id})]})
        mo.action_confirm()
        mo.qty_producing = kilos
        mo.move_raw_ids.write({'quantity': metros, 'picked': True})
        res = mo.button_mark_done()
        if isinstance(res, dict) and res.get('res_model'):
            wiz = self.env[res['res_model']].with_context(**res.get('context', {})).create({})
            getattr(wiz, 'action_confirm', getattr(wiz, 'process', lambda: None))()
        self.assertEqual(mo.state, 'done')
        return mo

    def test_12_conversiones_raras_no_cuentan_y_hacen_falta_tres(self):
        """2.27.1: XJ140Q21JNT165 tenía conversiones entre 4.35 y 4.48 m/kg y otras de 0.81 y 2.07 (juntaban kg de otra
        fuente); esas no cuentan. Con menos de 3 muestras buenas se usa el estimado."""
        buenas = [(4518.0, 1013.3), (4403.0, 1011.75), (4433.0, 1002.95)]
        for m, k in buenas + [(1100.0, 1353.95)]:
            self._conversion(m, k)
        metros, kilos, n = self.env['ficha.tecnica.acabado']._ft_conversiones(self.acabado, '2000-01-01')[self.acabado]
        self.assertEqual(n, 3, "La de 0.81 m/kg no cuenta")
        self.assertAlmostEqual(metros, sum(m for m, _k in buenas), places=2)
        self.assertAlmostEqual(kilos, sum(k for _m, k in buenas), places=2)
        # Una sola conversión (WJ060Q21JNT165: 12.05 contra 9.77 real) no gana al estimado.
        self.acabado.consumo_crudo = 1.1
        with self._con_terminado({self.acabado: (1205.0, 100.0, 1)}, fabricado={self.tela_m: (9000.0, 1000.0, 0.0, 0.0, 3)}):
            self._recalcular_y_usar()
        self.assertEqual(self.acabado.rendimiento_fuente, 'estimado')
        self.assertAlmostEqual(self.acabado.rendimiento_tela_acabada, 9.9, places=4)
        # Revisión 2026-11-06: sin estimado, una o dos muestras no pisan la ficha (antes la escribían).
        self.acabado.consumo_crudo = 0.0
        with self._con_terminado({self.acabado: (1205.0, 100.0, 1)}):
            self._recalcular_y_usar()
        self.assertFalse(self.acabado.rendimiento_medido, "Sin medido no hay nada que proponer")
        self.assertAlmostEqual(self.acabado.rendimiento_tela_acabada, 9.9, places=4, msg="Se queda el que tenía")

    def test_13_los_rollos_de_inspeccion_ganan_a_las_conversiones(self):
        """2.27.2: con 3 rollos de Inspección cuentan solo los rollos (kg neto de la estación), aunque haya conversiones;
        con menos de 3 rollos y de 3 conversiones, la ficha."""
        conversiones = {self.acabado: (9000.0, 1000.0, 5)}
        with self._con_terminado(conversiones, rollos={self.acabado: (2550.0, 300.0, 3)}):
            self._recalcular_y_usar()
        self.assertEqual((self.acabado.rendimiento_fuente, self.acabado.rendimiento_real_ordenes), ('rollos', 3))
        self.assertAlmostEqual(self.acabado.rendimiento_tela_acabada, 8.5, places=4)
        with self._con_terminado(conversiones, rollos={self.acabado: (1700.0, 200.0, 2)}):
            self._recalcular_y_usar()
        self.assertEqual(self.acabado.rendimiento_fuente, 'terminado', "Dos rollos no ganan a 5 conversiones")
        # Revisión 2026-11-06: con menos de 3 rollos y de 3 conversiones, y sin estimado, se queda el de la ficha.
        antes = self.acabado.rendimiento_tela_acabada
        with self._con_terminado({self.acabado: (900.0, 100.0, 1)}, rollos={self.acabado: (1700.0, 200.0, 2)}):
            self._recalcular_y_usar()
        self.assertFalse(self.acabado.rendimiento_medido)
        self.assertEqual(self.acabado.rendimiento_tela_acabada, antes)

    def test_14_el_rendimiento_escrito_a_mano(self):
        """2.28.0: lo que Ingeniería escribe en la ficha queda (forzado y revisado); el recálculo solo propone, también
        con 3 rollos de Inspección. Desmarcar «Rendimiento forzado» vuelve a calcular la propuesta."""
        self.acabado.consumo_crudo = 1.1
        with self._con_terminado({self.acabado: (9000.0, 1000.0, 5)}):
            self.acabado._ft_actualizar_rendimiento_real()
        self.assertTrue(self.acabado.rendimiento_por_revisar)
        self.acabado.rendimiento_tela_acabada = 8.1
        self.assertTrue(self.acabado.rendimiento_manual, "Escribirlo a mano lo deja forzado")
        self.assertEqual(self.acabado.rendimiento_fuente, 'ficha')
        self.assertFalse(self.acabado.rendimiento_por_revisar, "Escribirlo a mano cuenta como revisado")
        with self._con_terminado({}, rollos={self.acabado: (2700.0, 300.0, 3)}):
            self.acabado._ft_actualizar_rendimiento_real()
        self.assertEqual(self.acabado.rendimiento_tela_acabada, 8.1, "Ni los rollos cambian la ficha solos")
        self.assertEqual((self.acabado.rendimiento_medido, self.acabado.rendimiento_medido_fuente), (9.0, 'rollos'))
        self.assertTrue(self.acabado.rendimiento_por_revisar, "8.1 → 9.0 pasa la alerta")
        self.acabado.action_ft_usar_rendimiento_medido()
        self.assertEqual((self.acabado.rendimiento_tela_acabada, self.acabado.rendimiento_fuente), (9.0, 'rollos'))
        self.assertFalse(self.acabado.rendimiento_manual, "Lo medido ya no es forzado")
        self.acabado.rendimiento_tela_acabada = 8.1
        with self._con_terminado({self.acabado: (9500.0, 1000.0, 5)}):
            self.acabado.rendimiento_manual = False
        self.assertEqual(self.acabado.rendimiento_medido, 9.5, "Desmarcarlo recalcula la propuesta")

    def test_15_la_importacion_no_fuerza_el_rendimiento(self):
        """2.27.2: el rendimiento del Excel es el teórico; el recálculo lo sigue cambiando."""
        self.acabado.with_context(ft_rendimiento_calculado=True).write({'rendimiento_tela_acabada': 7.0})
        self.assertFalse(self.acabado.rendimiento_manual)
        with self._con_terminado({self.acabado: (9000.0, 1000.0, 5)}):
            self._recalcular_y_usar()
        self.assertEqual(self.acabado.rendimiento_tela_acabada, 9.0)

    def test_16_rollos_finales_de_inspeccion(self):
        """2.27.2: con el módulo de rollos, cada rollo final registrado de primera o FE de la orden en kg de la tela es
        una muestra (metros y kg netos); los inspeccionados sin registrar, la segunda y los raros no cuentan."""
        Final = self.env.get('mrp.rollo.final')
        if Final is None:
            self.skipTest('Sin quimibond_rollos_bitacora')
        orden = self.env['mrp.production'].create({'product_id': self.tela.id, 'product_qty': 100,
                                                   'product_uom_id': self.kg.id, 'bom_id': False})
        ahora = '2026-11-06 12:00:00'
        for i, (m, kg, calidad, estado) in enumerate([
                (850.0, 100.0, 'PQ', 'registrado'), (860.0, 100.0, 'FE', 'registrado'),
                (840.0, 100.0, 'PQ', 'registrado'), (500.0, 100.0, 'PQ', 'registrado'),  # 5 m/kg: raro
                (850.0, 100.0, 'SEGUNDA', 'registrado'), (850.0, 100.0, 'PQ', 'inspeccionado')], 1):
            Final.sudo().create({'name': 'PRUEBA-%04d' % i, 'secuencia': i, 'production_id': orden.id,
                                 'metros': m, 'kg_neto': kg, 'clasificacion': calidad, 'estado': estado,
                                 'fecha': ahora})
        metros, kilos, n = self.env['ficha.tecnica.acabado']._ft_rollos(self.acabado, '2000-01-01')[self.acabado]
        self.assertEqual(n, 3)
        self.assertAlmostEqual(metros / kilos, 8.5, places=4)
        Acabado = type(self.env['ficha.tecnica.acabado'])
        with patch.multiple(Acabado, _ft_conversiones=lambda *a, **k: {self.acabado: (9000.0, 1000.0, 5)},
                            _ft_fabricado=lambda *a, **k: {}):
            self._recalcular_y_usar()
        self.assertEqual((self.acabado.rendimiento_fuente, self.acabado.rendimiento_tela_acabada), ('rollos', 8.5))
        # Con la fecha de arranque del flujo después de esos rollos (pruebas), no cuentan.
        self.env['ir.config_parameter'].sudo().set_param('quimibond_ficha_tecnica_tela.rendimiento_rollos_desde',
                                                         '2026-11-07')
        self.assertFalse(self.env['ficha.tecnica.acabado']._ft_rollos(self.acabado, '2000-01-01'))
        self.env['ir.config_parameter'].sudo().set_param('quimibond_ficha_tecnica_tela.rendimiento_rollos_desde',
                                                         '2026-11-06')
        self.assertEqual(self.env['ficha.tecnica.acabado']._ft_rollos(self.acabado, '2000-01-01')[self.acabado][2], 3)

    def test_17_estimado_con_reprocesos_y_tela_en_kg(self):
        """Revisión 2026-11-06 (revisión 2026-11-06, WD038Q46JNG166 en main a 32.1 m/kg): los kg de reproceso cuentan y el crudo o
        teñido se convierte con el consumo; la tela acabada en kg se suma tal cual. 85,967 m con 3,125 + 2,680.5 +
        297.4 kg de teñido y consumo 1.1669: 16.43 m/kg, no 32.1."""
        self.acabado.consumo_crudo = 1.1669
        with self._con_terminado({}, fabricado={self.tela_m: (85967.0, 3125.0 + 2680.5 + 297.4, 0.0, 0.0, 12)}):
            self._recalcular_y_usar()
        self.assertAlmostEqual(self.acabado.rendimiento_tela_acabada,
                               round(85967.0 / ((3125.0 + 2680.5 + 297.4) / 1.1669), 4), places=4)
        # Tela acabada en kg y teñido mezclados: el «I…» no se multiplica por el consumo.
        with self._con_terminado({}, fabricado={self.tela_m: (16000.0, 550.0, 409.0, 0.0, 2)}):
            self._recalcular_y_usar()
        self.assertAlmostEqual(self.acabado.rendimiento_tela_acabada,
                               round(16000.0 / (409.0 + 550.0 / 1.1669), 4), places=4)
        # Con crudo o teñido y sin consumo de crudo no se estima.
        self.acabado.consumo_crudo = 0.0
        antes = self.acabado.rendimiento_tela_acabada
        with self._con_terminado({}, fabricado={self.tela_m: (16000.0, 550.0, 0.0, 0.0, 2)}):
            self._recalcular_y_usar()
        self.assertFalse(self.acabado.rendimiento_medido)
        self.assertEqual(self.acabado.rendimiento_tela_acabada, antes)

    def test_18_conversion_de_metros_a_metros_no_cuenta(self):
        """Revisión 2026-11-06 (WC090Q11JNT162 en main a 36.7 m/kg): una orden que fabrica la tela consumiendo otra tela
        ya en metros (conversión de artículo) no tiene kilos de tela: sus metros no entran al estimado."""
        otra = self.env['product.product'].create({'name': 'Tela 931 otra (m)', 'default_code': 'ZZ931Q22JNT170',
                                                   'uom_id': self.meter.id, 'is_storable': True})
        mo = self.env['mrp.production'].create({
            'product_id': self.tela_m.id, 'product_qty': 1000.0, 'product_uom_id': self.meter.id, 'bom_id': False,
            'move_raw_ids': [(0, 0, {'product_id': otra.id, 'product_uom_qty': 1000.0, 'product_uom': self.meter.id})]})
        mo.action_confirm()
        mo.qty_producing = 1000.0
        mo.move_raw_ids.write({'quantity': 1000.0, 'picked': True})
        res = mo.button_mark_done()
        if isinstance(res, dict) and res.get('res_model'):
            wiz = self.env[res['res_model']].with_context(**res.get('context', {})).create({})
            getattr(wiz, 'action_confirm', getattr(wiz, 'process', lambda: None))()
        self.assertEqual(mo.state, 'done')
        self.assertEqual(self.env['ficha.tecnica.acabado']._ft_fabricado(self.tela_m, '2000-01-01'), {},
                         "Sin kilos de tela la orden no cuenta")

    def test_20_el_rendimiento_aprobado_pasa_a_la_lista_en_metros(self):
        """Revisión 2026-11-10: «Usar el medido» y el rendimiento escrito a mano ponen al día la conversión kg → m de la
        lista en metros en ese momento (también archivada) y dejan nota; el recálculo que solo propone no la toca."""
        linea = self.bom_m.bom_line_ids
        self.bom_m.action_ft_usar_conversion_ficha()
        self.assertAlmostEqual(linea.product_qty, self._kg_guardables(1 / 8.0), places=6)
        self.bom_m.action_archive()
        self.assertFalse(self.bom_m.ft_conversion_difiere)  # deja el aviso leído, como en una pantalla abierta
        with self._con_terminado({self.acabado: (9000.0, 1000.0, 3)}):
            self.acabado._ft_actualizar_rendimiento_real()
        self.assertAlmostEqual(linea.product_qty, self._kg_guardables(1 / 8.0), places=6,
                               msg="Lo propuesto no cambia la lista")
        notas = len(self.bom_m.message_ids)
        self.acabado.action_ft_usar_rendimiento_medido()
        self.assertEqual(self.acabado.rendimiento_tela_acabada, 9.0)
        self.assertAlmostEqual(linea.product_qty, self._kg_guardables(1 / 9.0), places=6,
                               msg="Aprobar el medido pone al día la lista archivada")
        self.assertEqual(len(self.bom_m.message_ids), notas + 1, "Queda nota en la lista")
        self.env.invalidate_all()
        self.assertFalse(self.bom_m.ft_conversion_difiere)
        self.acabado.rendimiento_tela_acabada = 10.0
        self.assertAlmostEqual(linea.product_qty, self._kg_guardables(1 / 10.0), places=6,
                               msg="Escrito a mano por Ingeniería también")
        self.assertFalse(self.acabado._ft_pasar_conversion_a_listas(), "Sin cambio no escribe ni deja nota")
        self.assertAlmostEqual(self.bom.bom_line_ids.product_qty, 1.1, places=6, msg="La lista en kg no se toca")

    def test_21_el_teorico_usa_el_ancho_bajando_de_rama(self):
        """Revisión 2026-11-10 (Ingeniería, 6-nov): el teórico y el peso real salen del ancho que baja de rama (promedio
        de las ramas del artículo); el ancho de venta solo cuando no está capturado."""
        self.acabado.write({'peso_acabado': 45, 'peso_acabado_tol_unit': 'g/m2', 'ancho_acabado': 160})
        self.assertAlmostEqual(self.acabado.rendimiento_teorico, round(1000 / (45 * 1.6), 4), places=4)
        self.assertIn('de venta', self.acabado.rendimiento_teorico_ancho)
        tipo = self.env.ref('quimibond_ficha_tecnica_tela.ft_maquina_tipo_acabado')
        self._param(tipo, 'ancho_rama', 'm', 'articulo')
        Proceso = self.env['ficha.tecnica.proceso']
        for nombre, ancho in (('RAMA 931 prueba', 1.66), ('RAMA 932 prueba', 1.68), ('RAMA 933 prueba', 0)):
            rama = self.env['mrp.workcenter'].create({'name': nombre, 'code': nombre[:8], 'ft_tipo_id': tipo.id})
            Proceso.create({'articulo': 'IZZ931Q22JNT165', 'workcenter_id': rama.id})._ft_set_value('ancho_rama', ancho)
        self.acabado.invalidate_recordset(['rendimiento_teorico', 'rendimiento_teorico_ancho', 'peso_real'])
        self.assertAlmostEqual(self.acabado.rendimiento_teorico, round(1000 / (45 * 1.67), 4), places=4,
                               msg="Promedio de las ramas con ancho; la que no lo tiene no cuenta")
        self.assertEqual(self.acabado.rendimiento_teorico_ancho, '1.67 m (bajando de rama)')
        # Lo medido de WJ045 (13.19 m/kg) queda a 0.9 % del teórico con rama; con el de venta quedaba a 5 %.
        with self._con_terminado({self.acabado: (13190.0, 1000.0, 3)}):
            self._recalcular_y_usar()
        self.assertAlmostEqual(self.acabado.peso_real, 1000 / (13.19 * 1.67), places=1)


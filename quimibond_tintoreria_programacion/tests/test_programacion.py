# -*- coding: utf-8 -*-
from odoo import Command
from odoo.exceptions import UserError
from odoo.tests import tagged
from odoo.tests.common import TransactionCase

from odoo.addons.quimibond_tintoreria_programacion.models.mrp_production import (
    banda_rendimiento, clave_rollo, rango_rollos, repartir_rollos)


class _Config:
    """Configuración de jet simulada: devuelve el valor por código de parámetro."""

    def __init__(self, **valores):
        self.valores = valores

    def _ft_value(self, codigo):
        return self.valores.get(codigo, 0.0)


CONFIG_HTJ1 = dict(capacidad=1000.0, capacidad_grupo_a=900.0, capacidad_grupo_b=600.0, capacidad_grupo_c=450.0)
CONFIG_HTJ3 = dict(capacidad=600.0, capacidad_grupo_a=600.0)   # bandas B y C: N/A
CONFIG_HTJ5 = dict(capacidad=1200.0)                           # sin bandas: solo capacidad general
# Baño de tejido H13850 de la prueba de Dirección (2026-11-11): 12 rollos, 394.70 kg.
KGS_H13850 = [33.62, 33.42, 33.62, 33.62, 25.08, 33.82, 33.62, 33.62, 33.42, 33.62, 33.42, 33.82]


@tagged('post_install', '-at_install', 'quimibond_tintoreria_programacion')
class TestProgramacionTintoreria(TransactionCase):
    """Caso del cliente: rendimiento de tejido 9 (banda B), HTJ con 600 kg en banda B, MO de 1000 kg
    -> 2 MOs de 500 kg en el mismo HTJ."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.kg = cls.env.ref('uom.product_uom_kgm')
        Workcenter = cls.env['mrp.workcenter']
        cls.htj1 = Workcenter.create({'name': 'Tintoreria TEST 1', 'code': 'HTJT1'})
        cls.htj3 = Workcenter.create({'name': 'Tintoreria TEST 3', 'code': 'HTJT3'})
        cls.htj5 = Workcenter.create({'name': 'Tintoreria TEST 5', 'code': 'HTJT5'})
        cls.configs = {
            cls.htj1.id: CONFIG_HTJ1,
            cls.htj3.id: CONFIG_HTJ3,
            cls.htj5.id: CONFIG_HTJ5,
        }
        cls.producto_kg = cls._producto('IWT105Q56JNT155AF')
        cls.componente = cls._producto('COMP-TEST', storable=False)
        cls.bom = cls.env['mrp.bom'].create({
            'product_tmpl_id': cls.producto_kg.product_tmpl_id.id,
            'product_qty': 1,
            'product_uom_id': cls.kg.id,
            'operation_ids': [Command.create({'name': 'Tintorería', 'workcenter_id': cls.htj1.id})],
            'bom_line_ids': [Command.create({'product_id': cls.componente.id, 'product_qty': 0.1})],
        })

    @classmethod
    def _producto(cls, codigo, storable=True):
        return cls.env['product.product'].create({
            'name': codigo, 'default_code': codigo, 'type': 'consu',
            'is_storable': storable, 'uom_id': cls.kg.id,
        })

    def setUp(self):
        super().setUp()
        self.rendimiento = 9.0
        self.es_kg = True
        configs = self.configs
        # Las fichas técnicas se simulan: configuración vigente del jet, capacidad del artículo,
        # rendimiento de tejido y vínculo kg <-> metros.
        self.patch(self.env.registry['mrp.workcenter'], '_ft_config_at',
                   lambda wc, day=None: _Config(**configs[wc.id]) if wc.id in configs else False)
        self.patch(self.env.registry['mrp.workcenter'], '_ft_capacidad_carga',
                   lambda wc, rendimiento, day=None, product=None: (0.0, 'sin capacidad capturada'))
        self.patch(self.env.registry['product.product'], '_ft_rendimiento',
                   lambda producto: (self.rendimiento, 'prueba'))
        self.patch(self.env.registry['mrp.production'], '_es_producto_acabado_kg',
                   lambda orden: self.es_kg)

    def _crear_mo(self, kg=1000.0):
        mo = self.env['mrp.production'].create({
            'product_id': self.producto_kg.id,
            'product_qty': kg,
            'product_uom_id': self.kg.id,
            'bom_id': self.bom.id,
        })
        mo.action_confirm()
        return mo

    # ------------------------------------------------------------------
    def test_banda_continua(self):
        self.assertFalse(banda_rendimiento(2.9))
        self.assertEqual(banda_rendimiento(3), 'A')
        self.assertEqual(banda_rendimiento(6.5), 'A')
        self.assertEqual(banda_rendimiento(7), 'B')
        self.assertEqual(banda_rendimiento(10.5), 'B')
        self.assertEqual(banda_rendimiento(11), 'C')
        self.assertEqual(banda_rendimiento(15.5), 'C')
        self.assertFalse(banda_rendimiento(16))
        self.assertFalse(banda_rendimiento(0))

    def test_requiere_programacion_por_ficha_no_por_codigo(self):
        mo = self._crear_mo()
        self.assertTrue(mo.requiere_programacion_tintoreria)
        self.assertTrue(mo.tintoreria_pendiente)
        self.es_kg = False
        mo2 = self._crear_mo()
        self.assertFalse(mo2.requiere_programacion_tintoreria)
        with self.assertRaises(UserError):
            mo2._check_programable()

    def test_banda_b_divide_en_dos(self):
        mo = self._crear_mo(1000.0)
        programacion = mo._programar_tintoreria(self.htj1)
        ordenes = programacion.production_ids
        self.assertEqual(programacion.banda, 'B')
        self.assertEqual(programacion.capacidad_kg, 600.0)
        self.assertEqual(len(ordenes), 2)
        self.assertEqual(sorted(ordenes.mapped('product_qty')), [500.0, 500.0])
        self.assertTrue(all(ordenes.mapped('tintoreria_programada')))

    def test_fuera_de_bandas_bloquea(self):
        self.rendimiento = 20.0
        mo = self._crear_mo()
        with self.assertRaises(UserError):
            mo._programar_tintoreria(self.htj1)

    def test_banda_na_en_jet_con_bandas_bloquea(self):
        mo = self._crear_mo()
        with self.assertRaises(UserError):
            mo._programar_tintoreria(self.htj3)

    def test_jet_sin_bandas_usa_capacidad_general_con_aviso(self):
        mo = self._crear_mo(1000.0)
        plan = mo._calcular_plan_tintoreria(self.htj5)
        self.assertEqual(plan['capacidad_kg'], 1200.0)
        self.assertEqual(plan['n_banos'], 1)
        self.assertTrue(plan['aviso'])

    def test_kg_por_carga_del_articulo_manda(self):
        self.patch(self.env.registry['mrp.workcenter'], '_ft_capacidad_carga',
                   lambda wc, rendimiento, day=None, product=None: (
                       400.0, 'kg por carga del artículo en este jet'))
        mo = self._crear_mo(1000.0)
        plan = mo._calcular_plan_tintoreria(self.htj1)
        self.assertEqual(plan['capacidad_kg'], 400.0)
        self.assertEqual(plan['n_banos'], 3)

    def test_arranque_sin_programar_bloquea_y_acepta_argumentos_de_enterprise(self):
        mo = self._crear_mo(100.0)
        orden_trabajo = mo.workorder_ids[:1]
        with self.assertRaises(UserError):
            orden_trabajo.button_start()
        # Regresión: Enterprise llama button_start con argumentos extra (datos demo y Shop Floor).
        with self.assertRaises(UserError):
            orden_trabajo.button_start(True, True)

    def test_terminar_sin_programar_bloquea_igual_que_iniciar(self):
        # 1.8.2: button_finish se saltaba la regla y dejaba la operación «hecha» sin programación.
        mo = self._crear_mo(100.0)
        orden_trabajo = mo.workorder_ids[:1]
        self.assertTrue(mo.requiere_programacion_tintoreria)
        self.assertFalse(mo.tintoreria_programada)
        with self.assertRaises(UserError):
            orden_trabajo.button_finish()
        self.assertNotIn(orden_trabajo.state, ('done', 'cancel'))
        # Una operación ya hecha no se vuelve a revisar (el cierre nativo vuelve a llamar button_finish).
        orden_trabajo.state = 'done'
        orden_trabajo.button_finish()

    def test_tiempos_por_jet_en_la_formula(self):
        """1.4.0: los minutos por jet se capturan en la propia fórmula."""
        formula = self.env['tintoreria.formula'].create({
            'name': 'FORMULA TIEMPOS TEST',
            'tiempo_ids': [Command.create({'workcenter_id': self.htj1.id, 'minutos': 240.0})]})
        tiempo = self.env['tintoreria.formula.tiempo'].search([('formula_id', '=', formula.id)])
        self.assertEqual((tiempo.workcenter_id, tiempo.minutos), (self.htj1, 240.0))
        self.assertEqual(formula.tiempo_ids, tiempo)
        self.assertEqual(tiempo.horas, 4.0)

    def test_tiempo_de_la_operacion_de_la_bom_va_por_kg(self):
        """1.5.0: sin tiempo de la ficha ni de la fórmula, el tiempo de la operación de la BoM es por kg
        (la BoM es de 1 kg) y se multiplica por los kg del baño, como Odoo; antes se tomaba como minutos
        por baño (0.5 min por kg daba baños de medio minuto)."""
        self.bom.operation_ids.write({'time_mode': 'manual', 'time_cycle_manual': 0.5})
        mo = self._crear_mo(1000.0)
        plan = mo._calcular_plan_tintoreria(self.htj1)
        self.assertEqual(plan['n_banos'], 2, "Banda B: 600 kg por baño, 1000 kg son 2 baños de 500")
        self.assertEqual(plan['origen_tiempo'], 'bom')
        self.assertAlmostEqual(plan['minutos'], 250.0, msg="0.5 min por kg × 500 kg del baño")
        self.assertIn('500', plan['aviso_tiempo'])
        programacion = mo._programar_tintoreria(self.htj1)
        self.assertAlmostEqual(programacion.minutos_bano, 250.0)
        for orden in programacion.production_ids:
            self.assertAlmostEqual(orden._workorder_tintoreria().duration_expected, 250.0)

    def test_respaldo_de_la_bom_en_modo_calculado_avisa_en_el_chatter(self):
        """1.6.0: la operación de la BoM en modo calculado (sin historial, Odoo usa el tiempo capturado) no
        bloquea; el tiempo va por kg del baño y el aviso queda en el chatter de cada orden."""
        self.bom.operation_ids.write({'time_mode': 'auto', 'time_cycle_manual': 0.5})
        mo = self._crear_mo(1000.0)
        programacion = mo._programar_tintoreria(self.htj1)
        self.assertEqual(programacion.origen_tiempo, 'bom')
        self.assertAlmostEqual(programacion.minutos_bano, 250.0)
        for orden in programacion.production_ids:
            self.assertTrue(any('operación de Tintorería de la BoM' in (m.body or '') for m in orden.message_ids),
                            "El aviso del respaldo queda en el chatter de la orden")

    def test_jet_programado_no_lo_mueve_el_planificador(self):
        """1.6.0: el HTJ asignado en la programación es a mano; aunque esté en montaje y otro jet sí tenga
        proceso del artículo, al planear la orden de trabajo se queda en él (queda el aviso)."""
        tipo = self.env['ficha.tecnica.maquina.tipo'].create({
            'name': 'Tintorería prueba programación', 'code': 'tint_prog_prueba', 'alternativas_por_tipo': True,
            'parametro_ids': [Command.create({'code': 'tiempo_ciclo', 'name': 'Tiempo de ciclo', 'unit': 'h',
                                              'kind': 'num', 'ambito': 'articulo'})]})
        (self.htj1 | self.htj3).write({'ft_tipo_id': tipo.id})
        self.env['ficha.tecnica.proceso'].create({'articulo': self.producto_kg.default_code,
                                                  'workcenter_id': self.htj3.id})
        self.bom.operation_ids.write({'time_mode': 'manual', 'time_cycle_manual': 0.5})
        mo = self._crear_mo(500.0)
        orden = mo._programar_tintoreria(self.htj1).production_ids
        # 1.7.1: un jet que no opera ya no se programa; el caso real es que pase a montaje después de programado.
        self.htj1.ft_estado = 'montaje'
        workorder = orden._workorder_tintoreria()
        self.assertTrue(workorder._ft_maquina_fija())
        orden.button_plan()
        self.assertEqual(workorder.workcenter_id, self.htj1, "Sin 1.6.0 se iría al HTJ3, el único con proceso")
        self.assertIn('en montaje', workorder.ft_aviso_maquina)

    def test_cancelar_ordenes_cancela_la_programacion(self):
        """1.6.2: al cancelar un baño la programación lo anota; con todos cancelados pasa a «Cancelada»."""
        mo = self._crear_mo(1000.0)
        programacion = mo._programar_tintoreria(self.htj1)
        primera, segunda = programacion.production_ids.sorted('id')
        primera.action_cancel()
        self.assertEqual(programacion.state, 'programada')
        self.assertTrue(programacion.message_ids.filtered(lambda m: 'siguen programadas' in (m.body or '')))
        segunda.action_cancel()
        self.assertEqual(programacion.state, 'cancelada')

    def _crudo_con_rollos(self, kg_por_kg=1.0, kg_orden=381.0, kgs=None, prefijo='H13850', surtir=True, capacidad=None):
        """Orden confirmada con un crudo de categoría de tejido (por lote) y, si ``surtir``, sus rollos ya en la Ubic. 2
        (líneas con lote, como las deja la requisición REQ-MP validada). Devuelve (orden, crudo, rollos)."""
        categoria = self.env['product.category'].search([('name', '=', 'TEJIDO PRUEBA PROGRAMACION')], limit=1) \
            or self.env['product.category'].create({'name': 'TEJIDO PRUEBA PROGRAMACION'})
        crudo = self.env['product.product'].create({
            'name': 'WJ-CRUDO-%s' % prefijo, 'default_code': 'WJ-CRUDO-%s' % prefijo, 'type': 'consu',
            'is_storable': True, 'tracking': 'lot', 'uom_id': self.kg.id, 'categ_id': categoria.id})
        producto = self._producto('IWJ-%s' % prefijo)
        bom = self.env['mrp.bom'].create({
            'product_tmpl_id': producto.product_tmpl_id.id, 'product_qty': 1, 'product_uom_id': self.kg.id,
            'operation_ids': [Command.create({'name': 'Tintorería', 'workcenter_id': self.htj1.id})],
            'bom_line_ids': [Command.create({'product_id': crudo.id, 'product_qty': kg_por_kg}),
                             Command.create({'product_id': self.componente.id, 'product_qty': 0.1})],
        })
        mo = self.env['mrp.production'].create({
            'product_id': producto.id, 'product_qty': kg_orden, 'product_uom_id': self.kg.id, 'bom_id': bom.id})
        mo.action_confirm()
        rollos = self.env['stock.lot']
        if surtir:
            kgs = kgs or KGS_H13850
            move = mo.move_raw_ids.filtered(lambda m: m.product_id == crudo)
            for numero, kg in enumerate(kgs, 1):
                lote = self.env['stock.lot'].create({
                    'name': '%s-%04d' % (prefijo, numero), 'product_id': crudo.id, 'company_id': self.env.company.id})
                self.env['stock.quant']._update_available_quantity(crudo, move.location_id, kg, lot_id=lote)
                self.env['stock.move.line'].create({
                    'move_id': move.id, 'product_id': crudo.id, 'lot_id': lote.id, 'quantity': kg,
                    'product_uom_id': self.kg.id, 'location_id': move.location_id.id,
                    'location_dest_id': move.location_dest_id.id})
                rollos |= lote
            move._recompute_state()
        return mo, crudo, rollos

    def _jet_de(self, capacidad):
        jet = self.env['mrp.workcenter'].create({'name': 'Tintoreria TEST %s' % capacidad, 'code': 'HT%s' % capacidad})
        self.configs[jet.id] = dict(capacidad=capacidad, capacidad_grupo_b=capacidad)
        self.addCleanup(self.configs.pop, jet.id, None)
        return jet

    def test_banos_con_kg_de_crudo(self):
        """1.7.0: el jet se carga con el crudo. 550 kg de tela con 1.15 kg de crudo por kg son 632.5 kg en el jet:
        en banda B (600 kg) son 2 baños. 1.9.0: con los rollos surtidos (4 rollos, 632.5 kg), cada baño lleva rollos
        completos y la orden queda con los kg de sus rollos (316 y 316.5)."""
        mo, crudo, _rollos = self._crudo_con_rollos(
            kg_por_kg=1.15, kg_orden=550.0, kgs=[158.0, 158.0, 158.5, 158.0], prefijo='H20000')
        plan = mo._calcular_plan_tintoreria(self.htj1)
        self.assertEqual(plan['crudo_id'], crudo.id)
        self.assertAlmostEqual(plan['kg_crudo'], 632.5)
        self.assertEqual(plan['n_banos'], 2, "Con los kg de tela (550) cabía en un baño de 600")
        self.assertAlmostEqual(plan['kg_crudo_bano'], 316.25)
        self.assertFalse(plan['aviso_crudo'])
        programacion = mo._programar_tintoreria(self.htj1)
        self.assertEqual(sorted(programacion.production_ids.mapped('product_qty')), [316.0, 316.5])
        self.assertAlmostEqual(programacion.kg_crudo, 632.5)
        self.assertAlmostEqual(programacion.kg_crudo_bano, 316.25)
        self.assertTrue(all(any('316.25 kg de crudo' in (m.body or '') for m in orden.message_ids)
                            for orden in programacion.production_ids))

    def test_rollos_completos_en_dos_baños(self):
        """1.9.0 (Dirección, 2026-11-11): un baño de tejido de 12 rollos (H13850, 394.70 kg) en un jet de 210 kg se reparte
        en 2 órdenes de rollos completos: 1-6 (193.18 kg) y 7-12 (201.52 kg). Cada orden queda con los kg de sus rollos
        y su crudo trae solo sus lotes, sin cortar ninguno; la dosificación se calcula sobre esos kg."""
        mo, crudo, rollos = self._crudo_con_rollos()
        jet = self._jet_de(210.0)
        plan = mo._calcular_plan_tintoreria(jet)
        self.assertEqual(plan['n_banos'], 2)
        self.assertEqual([b['texto'] for b in plan['banos']], ['H13850 rollos 1-6', 'H13850 rollos 7-12'])
        self.assertEqual([b['kg'] for b in plan['banos']], [193.18, 201.52])
        self.assertEqual(plan['lote_crudo'], 'H13850')
        self.assertEqual(plan['n_rollos'], 12)
        programacion = mo._programar_tintoreria(jet)
        primera, segunda = programacion.production_ids.sorted('name')
        self.assertEqual([primera.product_qty, segunda.product_qty], [193.18, 201.52])
        for orden, esperados in ((primera, rollos[:6]), (segunda, rollos[6:])):
            movimiento = orden.move_raw_ids.filtered(lambda m: m.product_id == crudo)
            self.assertEqual(movimiento.move_line_ids.lot_id, esperados, "Solo los rollos de su baño")
            self.assertEqual(movimiento.move_line_ids.mapped('quantity'),
                             [KGS_H13850[int(n.name[-4:]) - 1] for n in movimiento.move_line_ids.lot_id],
                             "Cada rollo completo, sin cortar")
            self.assertAlmostEqual(movimiento.product_uom_qty, orden.product_qty)
            self.assertEqual(movimiento.state, 'assigned')
            self.assertAlmostEqual(orden._kg_carga_jet()[0], orden.product_qty)
            self.assertTrue(orden.tintoreria_programada)
        # El resto de los componentes sigue la cantidad de cada orden (0.1 kg por kg).
        componente = primera.move_raw_ids.filtered(lambda m: m.product_id == self.componente)
        self.assertAlmostEqual(componente.product_uom_qty, 19.318, places=2)
        self.assertAlmostEqual(programacion.kg_total, 394.7)
        self.assertAlmostEqual(programacion.kg_pedido, 381.0)
        self.assertEqual((programacion.lote_crudo, programacion.n_rollos), ('H13850', 12))
        self.assertEqual(programacion.bano_ids.mapped('rollos'), ['H13850 rollos 1-6', 'H13850 rollos 7-12'])
        self.assertEqual(programacion.bano_ids.mapped('production_id'), primera | segunda)
        self.assertTrue(any('H13850 rollos 7-12' in (m.body or '') for m in segunda.message_ids))

    def test_rollos_completos_en_un_solo_jet(self):
        """1.9.0: si el baño completo cabe en el jet, la orden queda con los 12 rollos y 394.70 kg (lo surtido, no los 381
        pedidos)."""
        mo, crudo, rollos = self._crudo_con_rollos()
        programacion = mo._programar_tintoreria(self.htj1)
        self.assertEqual(len(programacion.production_ids), 1)
        orden = programacion.production_ids
        self.assertEqual(orden, mo)
        self.assertEqual(orden.product_qty, 394.7)
        movimiento = orden.move_raw_ids.filtered(lambda m: m.product_id == crudo)
        self.assertEqual(movimiento.move_line_ids.lot_id, rollos)
        self.assertAlmostEqual(movimiento.product_uom_qty, 394.7)
        self.assertAlmostEqual(orden._kg_carga_jet()[0], 394.7)
        self.assertEqual(programacion.bano_ids.rollos, 'H13850 rollos 1-12')

    def test_sin_crudo_surtido_bloquea(self):
        """1.9.0: la programación se arma con los rollos surtidos: sin rollos, o con la requisición pendiente, no se
        programa y el aviso dice qué falta."""
        mo, crudo, _rollos = self._crudo_con_rollos(surtir=False)
        with self.assertRaisesRegex(UserError, 'no tiene rollos'):
            mo._calcular_plan_tintoreria(self.htj1)
        movimiento = mo.move_raw_ids.filtered(lambda m: m.product_id == crudo)
        origen = self.env['stock.move'].create({
            'product_id': crudo.id, 'product_uom': self.kg.id, 'product_uom_qty': 381.0,
            'location_id': movimiento.location_id.id, 'location_dest_id': movimiento.location_id.id,
            'company_id': self.env.company.id})
        origen._action_confirm()
        movimiento.move_orig_ids = [Command.link(origen.id)]
        with self.assertRaisesRegex(UserError, 'Falta surtir el crudo'):
            mo._calcular_plan_tintoreria(self.htj1)
        wizard = self.env['tintoreria.programacion.wizard'].create({
            'line_ids': [Command.create({'production_id': mo.id, 'workcenter_id': self.htj1.id})]})
        self.assertIn('Falta surtir', wizard.line_ids.advertencia)
        with self.assertRaises(UserError):
            wizard.action_confirmar()
        self.assertFalse(mo.tintoreria_programada)

    def test_asistente_muestra_lote_rollos_y_kg_por_baño(self):
        """1.9.0: el asistente muestra el lote de crudo, los rollos y kg surtidos y, al elegir el jet, el rango de rollos y
        los kg de cada baño."""
        mo, _crudo, _rollos = self._crudo_con_rollos()
        jet = self._jet_de(210.0)
        wizard = self.env['tintoreria.programacion.wizard'].create({
            'line_ids': [Command.create({'production_id': mo.id})]})
        linea = wizard.line_ids
        self.assertEqual((linea.lote_crudo, linea.n_rollos), ('H13850', 12))
        self.assertAlmostEqual(linea.kg_rollos, 394.7)
        self.assertFalse(linea.detalle_banos)
        linea.workcenter_id = jet
        self.assertEqual(linea.n_banos, 2)
        detalle = str(linea.detalle_banos)
        self.assertIn('193.18', detalle)
        self.assertIn('H13850 rollos 1-6', detalle)
        self.assertIn('201.52', detalle)
        self.assertIn('H13850 rollos 7-12', detalle)

    def test_repartir_rollos_y_rangos(self):
        """1.9.0: reparto de rollos completos y consecutivos sin pasar la capacidad."""
        self.assertEqual(repartir_rollos(KGS_H13850, 210.0), [(0, 6), (6, 12)])
        self.assertEqual(repartir_rollos(KGS_H13850, 600.0), [(0, 12)])
        # 200 kg no caben en un baño de 150: con 2 baños un rollo de 200 quedaría con otro; son 3.
        self.assertEqual(repartir_rollos([100.0, 100.0, 100.0], 150.0), [(0, 1), (1, 2), (2, 3)])
        with self.assertRaisesRegex(UserError, 'no se cortan'):
            repartir_rollos([100.0, 260.0], 250.0)
        self.assertEqual(clave_rollo('H13850-0007'), ('H13850', 7))
        self.assertEqual(clave_rollo('SINNUMERO'), ('SINNUMERO', 0))
        self.assertEqual(rango_rollos(['H13850-0003', 'H13850-0001', 'H13850-0002', 'H13850-0005']),
                         'H13850 rollos 1-3, 5')
        self.assertEqual(rango_rollos(['H1-0001', 'H2-0001']), 'H1 rollos 1 / H2 rollos 1')

    def test_sin_componente_de_tejido_usa_los_kg_de_la_orden(self):
        """1.7.0: sin componente de una categoría de tejido, los baños salen de los kg de la orden, con aviso."""
        mo = self._crear_mo(1000.0)
        plan = mo._calcular_plan_tintoreria(self.htj1)
        self.assertFalse(plan['crudo_id'])
        self.assertAlmostEqual(plan['kg_crudo'], 1000.0)
        self.assertEqual(plan['n_banos'], 2)
        self.assertTrue(plan['aviso_crudo'])

    def test_jet_que_no_opera_no_se_programa(self):
        """1.7.1: un jet en montaje (o en pruebas, o fuera de servicio) no se programa: el asistente lo avisa y no
        confirma. Ronda 10 en main: TINTORERIA 5, en montaje, se dejaba programar sin aviso."""
        self.htj5.ft_estado = 'montaje'
        mo = self._crear_mo(1000.0)
        with self.assertRaisesRegex(UserError, 'no está operando'):
            mo._calcular_plan_tintoreria(self.htj5)
        wizard = self.env['tintoreria.programacion.wizard'].create({
            'line_ids': [Command.create({'production_id': mo.id, 'workcenter_id': self.htj5.id})]})
        self.assertIn('no está operando', wizard.line_ids.advertencia)
        with self.assertRaises(UserError):
            wizard.action_confirmar()
        self.assertFalse(mo.tintoreria_programada)
        self.htj5.ft_estado = 'operando'
        self.assertEqual(mo._calcular_plan_tintoreria(self.htj5)['n_banos'], 1)

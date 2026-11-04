# -*- coding: utf-8 -*-
"""2.23.0: rutas por familia (tabla de Innovación) y listas generadas desde la ruta y la ficha."""
from odoo.service.model import call_kw
from odoo.tests import tagged

from ..models.ruta_familia import tintoreria_por_codigo
from .test_tiempo_ficha import TiempoFichaCase


@tagged('post_install', '-at_install')
class TestRutaFamilia(TiempoFichaCase):

    def _armar(self):
        """Como el botón de la cabecera: llega con la lista de renglones (vacía)."""
        call_kw(self.env['ficha.tecnica.ruta'], 'action_ft_armar_desde_listas', [[]], {})
        return self.env['ficha.tecnica.ruta'].search([('crudo_id', '=', self.crudo.id)])

    def _ops(self, bom):
        return bom.with_context(active_test=False).operation_ids.sorted(lambda o: (o.sequence, o.id))

    def test_01_lavado_o_tenido_por_codigo(self):
        self.assertEqual(tintoreria_por_codigo('WJ045Q22JNT160'), 'lavado')
        self.assertEqual(tintoreria_por_codigo('IWJ038Q22JOW174'), 'lavado')
        self.assertEqual(tintoreria_por_codigo('WB038Q47JBL172'), 'tenido', "El blanco óptico se tiñe")
        self.assertEqual(tintoreria_por_codigo('WD038Q46JNG163'), 'tenido')
        self.assertEqual(self.acabado.ruta_tintoreria, 'lavado')
        self.acabado.ruta_tintoreria_manual = 'ninguna'
        self.assertEqual(self.acabado.ruta_tintoreria, 'ninguna')

    def test_02_armar_la_ruta_desde_las_listas(self):
        ruta = self._armar()
        self.assertEqual(len(ruta), 1)
        Workcenter = self.env['mrp.workcenter']
        self.assertEqual([(p.paso, p.workcenter_id) for p in ruta.paso_ids.sorted('sequence')],
                         [('devanado', self.dev), ('tintoreria', self.jet), ('abridora', self.abr),
                          ('rama', Workcenter), ('inspeccion', self.insp)],
                         "La rama la llevan todas (tabla de Innovación): sin evidencia, queda sin máquina")
        self.assertIn('Rama (termofijado) sin máquina', ruta.notas)
        self.assertAlmostEqual(self.acabado.consumo_crudo, 1.1, msg="El consumo sale del renglón del crudo")
        self.assertFalse(self.acabado.ruta_rama_id or self.acabado.ruta_jet_id, "Va a las máquinas de su familia")
        self.assertEqual(self.acabado.ruta_familia_id, ruta)
        self.assertEqual(self.acabado.ruta_resumen,
                         'DEV 931 prueba → JET 931 prueba → ABR 931 prueba → Rama (termofijado) (sin máquina) → INSP 931 prueba')
        ruta.paso_ids.filtered(lambda p: p.paso == 'devanado').unlink()
        self._armar()
        self.assertEqual(self.env['ficha.tecnica.ruta'].search_count([('crudo_id', '=', self.crudo.id)]), 1,
                         "Un crudo con ruta no se vuelve a crear")
        self.assertEqual(ruta.paso_ids.filtered(lambda p: p.paso == 'devanado').workcenter_id, self.dev,
                         "Se completa con el paso que faltaba")
        self.assertEqual(len(ruta.paso_ids.filtered(lambda p: p.paso == 'rama')), 1, "Lo que ya existe no se duplica")

    def test_03_generar_en_la_lista_archivada(self):
        ruta = self._armar()
        self.bom.action_archive()
        tipo_rama = self.env.ref('quimibond_ficha_tecnica_tela.ft_maquina_tipo_acabado')
        rama = self.env['mrp.workcenter'].create({'name': 'RAMA 931 prueba', 'code': 'R931', 'ft_tipo_id': tipo_rama.id})
        ruta.paso_ids.filtered(lambda p: p.paso == 'abridora').unlink()
        ruta.paso_ids.filtered(lambda p: p.paso == 'rama').workcenter_id = rama
        self.acabado.action_ft_generar_listas()
        ops = self._ops(self.bom)
        self.assertEqual([o.workcenter_id for o in ops], [self.dev, self.jet, rama, self.insp])
        self.assertFalse(any(ops.mapped('active')), "En una lista archivada las operaciones nacen archivadas")
        self.assertEqual(ops.filtered(lambda o: o.workcenter_id == self.jet).name, 'TINTORERIA LAVADO IZZ931Q22JNT165')
        self.assertAlmostEqual(ops.filtered(lambda o: o.workcenter_id == self.dev).time_cycle_manual, 0.22, places=4,
                               msg="El tiempo sale de la ficha")

    def test_04_la_lista_activa_no_se_toca(self):
        ruta = self._armar()
        ruta.paso_ids.filtered(lambda p: p.paso == 'abridora').unlink()
        antes = self.bom.operation_ids
        accion = self.acabado.action_ft_generar_listas()
        self.assertEqual(self.bom.operation_ids, antes)
        self.assertIn('activa', accion['params']['message'])

    def test_05_jet_propio_y_paso_solo_si_se_tine(self):
        ruta = self._armar()
        (self.bom | self.bom_m).action_archive()
        ruta.paso_ids = [(0, 0, {'paso': 'abridora_volteado', 'sequence': 24, 'workcenter_id': self.abr.id,
                                 'condicion': 'tenido'})]
        self.acabado.ruta_jet_id = self.jet2
        self.acabado.action_ft_generar_listas()
        ops = self._ops(self.bom)
        self.assertIn(self.jet2, ops.workcenter_id)
        self.assertNotIn(self.jet, ops.workcenter_id)
        self.assertNotIn('ABRIDORA VOLTEADO IZZ931Q22JNT165', ops.mapped('name'), "Se lava: el volteado es solo si se tiñe")
        self.acabado.ruta_tintoreria_manual = 'tenido'
        self.acabado.action_ft_generar_listas()
        self.assertIn('ABRIDORA VOLTEADO IZZ931Q22JNT165', self._ops(self.bom).mapped('name'))

    def test_06_crear_las_listas_que_faltan(self):
        self._armar()
        Product = self.env['product.product']
        kg = Product.create({'name': 'Tela 935 prueba (kg)', 'default_code': 'IZZ935Q22JNG165', 'uom_id': self.kg.id,
                             'is_storable': True})
        tela_m = Product.create({'name': 'Tela 935 prueba (m)', 'default_code': 'ZZ935Q22JNG165',
                                 'uom_id': self.meter.id, 'is_storable': True})
        ficha = self.env['ficha.tecnica.acabado'].create({
            'articulo': 'ZZ935Q22JNG165', 'tejido_id': self.tejido.id, 'product_acabado_id': tela_m.id,
            'product_proceso_id': kg.id, 'peso_acabado': 53, 'rendimiento_tela_acabada': 8.0})
        accion = ficha.action_ft_generar_listas()
        self.assertIn('consumo de crudo', accion['params']['message'], "Sin consumo no se inventa la lista en kg")
        ficha.consumo_crudo = 1.12
        ficha.action_ft_generar_listas()
        Revision = self.env['ficha.tecnica.revision']
        bom_kg, bom_m = Revision._ft_bom_de(kg), Revision._ft_bom_de(tela_m)
        self.assertTrue(bom_kg and bom_m)
        self.assertFalse(bom_kg.active or bom_m.active, "Lo generado nace archivado")
        self.assertEqual(bom_kg.code, 'Flujo en kg (borrador)')
        self.assertEqual(bom_kg.bom_line_ids.product_id, self.crudo)
        self.assertAlmostEqual(bom_kg.bom_line_ids.product_qty, 1.12)
        self.assertIn('TINTORERIA TEÑIDO IZZ935Q22JNG165', self._ops(bom_kg).mapped('name'))
        self.assertEqual(bom_m.bom_line_ids.product_id, kg)
        self.assertAlmostEqual(bom_m.bom_line_ids.product_qty, self._kg_guardables(0.125), places=6)

    def test_07_botones_de_la_lista_de_rutas(self):
        ruta = self._armar()
        accion = call_kw(self.env['ficha.tecnica.ruta'], 'action_ft_generar_listas', [ruta.ids], {})
        self.assertEqual(accion['tag'], 'display_notification')
        self.assertIn(self.acabado, ruta.acabado_ids)

    def test_08_habilitar_mcp(self):
        """2.23.0: si el servidor MCP está instalado, las rutas quedan habilitadas una sola vez."""
        Ruta = self.env['ficha.tecnica.ruta']
        if 'mcp.enabled.model' not in self.env:
            self.assertFalse(Ruta._ft_habilitar_mcp())
            return
        Ruta._ft_habilitar_mcp()
        self.assertFalse(Ruta._ft_habilitar_mcp(), "La segunda vez no crea nada")
        Enabled = self.env['mcp.enabled.model'].sudo()
        self.assertTrue(Enabled.search([('model_id.model', '=', 'ficha.tecnica.ruta')]))

    def test_09_lista_anterior_activa_sin_operaciones(self):
        """2.23.0: si la única lista activa es la anterior (sin operaciones), la del flujo se crea archivada junto a
        ella y la anterior no se toca."""
        self._armar()
        self.bom.operation_ids.unlink()
        self.acabado.action_ft_generar_listas()
        self.assertTrue(self.bom.active and not self.bom.operation_ids, "La lista anterior queda como estaba")
        nueva = self.env['ficha.tecnica.revision']._ft_bom_de(self.tela)
        self.assertNotEqual(nueva, self.bom)
        self.assertFalse(nueva.active)
        self.assertEqual(nueva.code, 'Flujo en kg (borrador)')
        self.assertEqual([o.workcenter_id for o in self._ops(nueva)], [self.dev, self.jet, self.abr, self.insp])

    def test_10_desperdicio_de_rama_en_la_lista_en_kg(self):
        """2.25.0: la lista en kg generada lleva el subproducto «Desperdicio Tejido» en la Rama, con cantidad 0 (lo pesa
        Rama con quimibond_rollos_bitacora); al volver a generar no se duplica y sigue en la Rama."""
        ruta = self._armar()
        self.bom.action_archive()
        Product = self.env['product.product']
        desperdicio = Product.search([('default_code', '=', 'DESPERDICIO TEJIDO')], limit=1)
        if not desperdicio:
            accion = self.acabado.action_ft_generar_listas()
            self.assertIn('DESPERDICIO TEJIDO', accion['params']['message'], "Sin el artículo, avisa y no inventa")
            self.assertFalse(self.bom.byproduct_ids)
            desperdicio = Product.create({'name': 'Desperdicio tejido prueba', 'default_code': 'DESPERDICIO TEJIDO',
                                          'uom_id': self.kg.id, 'is_storable': True})
        tipo_rama = self.env.ref('quimibond_ficha_tecnica_tela.ft_maquina_tipo_acabado')
        rama = self.env['mrp.workcenter'].create({'name': 'RAMA 931 prueba', 'code': 'R931', 'ft_tipo_id': tipo_rama.id})
        ruta.paso_ids.filtered(lambda p: p.paso == 'rama').workcenter_id = rama
        self.acabado.action_ft_generar_listas()
        sub = self.bom.byproduct_ids
        self.assertEqual(sub.product_id, desperdicio)
        self.assertEqual(sub.product_qty, 0.0, "La cantidad no es estándar: se pesa en cada orden")
        self.assertEqual(sub.operation_id.workcenter_id, rama)
        ruta.paso_ids.filtered(lambda p: p.paso == 'abridora').unlink()
        self.acabado.action_ft_generar_listas()
        self.assertEqual(len(self.bom.byproduct_ids), 1, "No se duplica")
        self.assertEqual(self.bom.byproduct_ids.operation_id.workcenter_id, rama,
                         "Al rehacer las operaciones vuelve a la Rama nueva")

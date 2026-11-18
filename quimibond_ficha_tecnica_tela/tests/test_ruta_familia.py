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
        self.assertEqual([(p.paso, p.workcenter_id) for p in ruta.paso_ids.sorted('sequence')
                          if p.paso != 'clasificacion'],
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

    def test_06b_saldos_en_la_lista_en_metros(self):
        """2.24.0: la lista en metros lleva los saldos de segundas y desperdicio (kg) con cantidad 0, una sola vez;
        si la referencia no existe, avisa y no la inventa."""
        Product = self.env['product.product']
        saldos = Product
        for codigo in ('SALDO DE TELA TEJIDO', 'SALDO TEJIDO D'):
            saldos |= Product.search([('default_code', '=', codigo)], limit=1) or Product.create({
                'name': codigo, 'default_code': codigo, 'uom_id': self.kg.id, 'is_storable': True})
        self._armar()
        (self.bom | self.bom_m).action_archive()
        self.acabado.action_ft_generar_listas()
        self.acabado.action_ft_generar_listas()
        bom_m = self.env['ficha.tecnica.revision']._ft_bom_de(self.acabado.product_acabado_id)
        self.assertFalse(bom_m.active)
        self.assertEqual(bom_m.byproduct_ids.product_id, saldos)
        self.assertEqual(len(bom_m.byproduct_ids), 2, "Generar dos veces no repite los saldos")
        self.assertEqual(set(bom_m.byproduct_ids.mapped('product_qty')), {0.0})
        self.assertEqual(bom_m.byproduct_ids.product_uom_id, self.kg)
        self.env['ir.config_parameter'].sudo().set_param('quimibond_ficha_tecnica_tela.saldo_segunda', 'ZZ-NO-EXISTE')
        avisos = self.acabado._ft_poner_saldos(bom_m)
        self.assertTrue(any('ZZ-NO-EXISTE' in a for a in avisos))

    def _borrador_m(self):
        return self.env['mrp.bom'].with_context(active_test=False).search([
            ('product_tmpl_id', '=', self.tela_m.product_tmpl_id.id), ('code', '=', 'Clasificación final (borrador)')])

    def _empaque(self):
        return self.env['mrp.workcenter'].with_context(active_test=False).search([('name', '=ilike', 'EMPAQUE')], limit=1)

    def test_06c_clasificacion_en_empaque(self):
        """2.28.0: armar la ruta crea el paso «Clasificación final» en EMPAQUE (el centro se crea si no existe, se
        reactiva si está archivado) y «Generar» pone la operación CLASIFICACION y la capacidad de 400 m."""
        self.assertFalse(self._empaque().filtered('active'))
        antiguo = self.env['mrp.workcenter'].create({'name': 'CIRCULAR 931 prueba', 'code': 'C931'})
        self.env['mrp.routing.workcenter'].create({'bom_id': self.bom_m.id, 'name': 'CLASIFICACION',
                                                   'workcenter_id': antiguo.id})
        (self.bom | self.bom_m).action_archive()
        ruta = self._armar()
        paso = ruta.paso_ids.filtered(lambda p: p.paso == 'clasificacion')
        self.assertEqual(len(paso), 1)
        self.assertEqual(paso.workcenter_id, self._empaque(), "Siempre EMPAQUE, no la máquina de la lista vieja")
        self.assertEqual(paso.tiempo, 60.0)
        self.assertIn('EMPAQUE', ruta.resumen)
        self._armar()
        self.assertEqual(len(ruta.paso_ids.filtered(lambda p: p.paso == 'clasificacion')), 1, "No se duplica")
        self.acabado.action_ft_generar_listas()
        self.acabado.action_ft_generar_listas()
        bom_m = self.env['ficha.tecnica.revision']._ft_bom_de(self.acabado.product_acabado_id)
        self.assertFalse(bom_m.active)
        ops = self._ops(bom_m)
        self.assertEqual(ops.mapped('name'), ['CLASIFICACION %s' % self.tela_m.default_code])
        self.assertEqual(ops.workcenter_id, self._empaque())
        self.assertEqual(ops.time_cycle_manual, 60.0)
        cap = self._empaque().capacity_ids.filtered(lambda c: c.product_id == self.tela_m)
        self.assertEqual(len(cap), 1, "Generar dos veces no repite la capacidad")
        self.assertEqual((cap.capacity, cap.product_uom_id), (400.0, self.meter))

    def test_06d_capacidad_y_duracion(self):
        """La capacidad dada de alta manda: 400 m por hora; sin tocar la que ya existe."""
        self._armar()
        (self.bom | self.bom_m).action_archive()
        self.acabado.action_ft_generar_listas()
        bom_m = self.env['ficha.tecnica.revision']._ft_bom_de(self.acabado.product_acabado_id)
        capacidad, _ini, _fin = self._empaque()._get_capacity(self.tela_m, self.meter, 1)
        self.assertEqual(capacidad, 400.0)
        op = self._ops(bom_m)
        ciclos = -(-1000 // int(capacidad))
        self.assertEqual(ciclos * op.time_cycle_manual, 180.0)
        cap = self._empaque().capacity_ids.filtered(lambda c: c.product_id == self.tela_m)
        cap.capacity = 300.0
        self.acabado.action_ft_generar_listas()
        self.assertEqual(cap.capacity, 300.0, "Una capacidad ya capturada no se pisa")

    def test_06e_activar_el_borrador_completa_la_clasificacion(self):
        """Al activar la lista en metros (visto bueno de Ingeniería) se completa la operación si faltaba."""
        self._armar()
        self.bom_m.code = 'Clasificación final (borrador)'
        (self.bom | self.bom_m).action_archive()
        self.acabado.action_ft_generar_listas()
        bom_m = self._borrador_m()
        bom_m.with_context(active_test=False).operation_ids.unlink()
        self.assertFalse(bom_m.with_context(active_test=False).operation_ids)
        bom_m.active = True
        ops = bom_m.operation_ids
        self.assertEqual(ops.workcenter_id, self._empaque())
        self.assertEqual(ops.time_cycle_manual, 60.0)

    def test_06f_migracion_archivadas_si_activas_no(self):
        """La migración pone el paso en las rutas, la operación en las listas archivadas y la capacidad de cada tela;
        una lista activa no se toca (se arregla con el botón)."""
        ruta = self._armar()
        ruta.paso_ids.filtered(lambda p: p.paso == 'clasificacion').unlink()
        self.bom_m.code = 'Clasificación final (borrador)'
        (self.bom | self.bom_m).action_archive()
        self.acabado.action_ft_generar_listas()
        bom_m = self._borrador_m()
        bom_m.with_context(active_test=False).operation_ids.unlink()
        self._empaque().capacity_ids.unlink()
        res = self.env['ficha.tecnica.ruta']._ft_poner_clasificacion_rutas()
        self.assertEqual((res['rutas'], res['listas'], res['capacidades']), (1, 1, 1))
        self.assertEqual(bom_m.with_context(active_test=False).operation_ids.workcenter_id, self._empaque())
        res = self.env['ficha.tecnica.ruta']._ft_poner_clasificacion_rutas()
        self.assertEqual((res['rutas'], res['listas'], res['capacidades']), (0, 0, 0), "Repetirla no cambia nada")
        # una lista en metros activa no se toca
        bom_m.with_context(active_test=False).operation_ids.unlink()
        bom_m.active = False
        bom_m.write({'active': True})
        bom_m.operation_ids.unlink()
        self.env['ficha.tecnica.ruta']._ft_poner_clasificacion_rutas()
        self.assertFalse(bom_m.operation_ids, "La lista activa queda como estaba")
        accion = bom_m.action_ft_poner_clasificacion()
        self.assertEqual(accion['tag'], 'display_notification')
        self.assertEqual(bom_m.operation_ids.workcenter_id, self._empaque(), "El botón sí la completa")

    def test_06h_normaliza_tiempos_viejos_y_da_capacidad_a_telas_sin_ficha(self):
        """Operaciones y pasos de «0.05 min por metro» pasan a 60 min por ciclo; una tela sin ficha con la operación
        también queda con su capacidad de 400 m."""
        ruta = self._armar()
        paso = ruta.paso_ids.filtered(lambda p: p.paso == 'clasificacion')
        paso.tiempo = 0.05
        empaque = self._empaque()
        sin_ficha = self.env['product.product'].create({
            'name': 'Tela sin ficha 931', 'default_code': 'ZZ931SINFICHA', 'uom_id': self.meter.id,
            'is_storable': True})
        bom = self.env['mrp.bom'].create({
            'product_tmpl_id': sin_ficha.product_tmpl_id.id, 'product_qty': 1, 'product_uom_id': self.meter.id,
            'code': 'Clasificación final (borrador)', 'active': False,
            'bom_line_ids': [(0, 0, {'product_id': self.tela.id, 'product_qty': 0.2})]})
        op = self.env['mrp.routing.workcenter'].with_context(active_test=False).create({
            'bom_id': bom.id, 'name': 'CLASIFICACION', 'workcenter_id': empaque.id, 'time_cycle_manual': 0.05})
        res = self.env['ficha.tecnica.ruta']._ft_poner_clasificacion_rutas()
        self.assertEqual(paso.tiempo, 60.0)
        self.assertEqual(op.time_cycle_manual, 60.0)
        cap = empaque.capacity_ids.filtered(lambda c: c.product_id == sin_ficha)
        self.assertEqual((cap.capacity, cap.product_uom_id), (400.0, self.meter))
        self.assertGreaterEqual(res['tiempos'], 2)
        res = self.env['ficha.tecnica.ruta']._ft_poner_clasificacion_rutas()
        self.assertEqual((res['rutas'], res['listas'], res['tiempos'], res['capacidades']), (0, 0, 0, 0))

    def test_06g_sin_paso_en_la_ruta_no_quita_operaciones(self):
        """Sin el paso en la ruta, el botón avisa y no quita lo que alguien puso a mano."""
        ruta = self._armar()
        ruta.paso_ids.filtered(lambda p: p.paso == 'clasificacion').unlink()
        circular = self.env['mrp.workcenter'].create({'name': 'CIRCULAR 931 prueba', 'code': 'C931'})
        self.env['mrp.routing.workcenter'].create({'bom_id': self.bom_m.id, 'name': 'CLASIFICACION',
                                                   'workcenter_id': circular.id})
        avisos = self.bom_m._ft_poner_clasificacion()
        self.assertTrue(any('Clasificación final' in a for a in avisos))
        self.assertEqual(self.bom_m.operation_ids.workcenter_id, circular)

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

    def test_11_tipo_de_operacion_del_flujo(self):
        """2.25.0: las listas que crea «Generar listas» llevan su tipo de operación (sin él la orden nacía como
        «Re-proceso Tintorería»), y la revisión avisa a Ingeniería si una lista del flujo no lo tiene."""
        self._armar()
        Product = self.env['product.product']
        kg = Product.create({'name': 'Tela 936 prueba (kg)', 'default_code': 'IZZ936Q22JNG165', 'uom_id': self.kg.id,
                             'is_storable': True})
        tela_m = Product.create({'name': 'Tela 936 prueba (m)', 'default_code': 'ZZ936Q22JNG165',
                                 'uom_id': self.meter.id, 'is_storable': True})
        ficha = self.env['ficha.tecnica.acabado'].create({
            'articulo': 'ZZ936Q22JNG165', 'tejido_id': self.tejido.id, 'product_acabado_id': tela_m.id,
            'product_proceso_id': kg.id, 'peso_acabado': 53, 'rendimiento_tela_acabada': 8.0, 'consumo_crudo': 1.1})
        ficha.action_ft_generar_listas()
        Revision = self.env['ficha.tecnica.revision']
        bom_kg, bom_m = Revision._ft_bom_de(kg), Revision._ft_bom_de(tela_m)
        self.assertEqual(bom_kg.picking_type_id, self.tipo_kg)
        self.assertEqual(bom_m.picking_type_id, self.tipo_m)
        tipos = Revision.search([('acabado_id', '=', ficha.id)]).pendiente_ids.mapped('tipo')
        self.assertNotIn('tipo_operacion', tipos)
        bom_kg.picking_type_id = False
        revision = Revision._ft_actualizar(ficha)
        pendiente = revision.pendiente_ids.filtered(lambda p: p.tipo == 'tipo_operacion')
        self.assertEqual((pendiente.area, pendiente.bom_id), ('ingenieria', bom_kg))
        self.assertEqual(pendiente.action_ft_resolver()['res_id'], bom_kg.id, "«Resolver» abre la lista")
        ficha.action_ft_generar_listas()
        self.assertEqual(bom_kg.picking_type_id, self.tipo_kg, "Al volver a generar se completa")


    def test_12_consumo_flexible_activacion_y_revision(self):
        """2.25.0 (piloto en el build de main, 2026-11-04): las listas del flujo nacen en consumo flexible; al activar
        la lista en kg el «I…» recibe la ruta MTO (y la lista su desperdicio y tipo); la revisión avisa si a la lista
        «Flujo en kg» le falta el desperdicio de Rama o, ya activa, al «I…» la ruta MTO."""
        ruta_fam = self._armar()
        tipo_rama = self.env.ref('quimibond_ficha_tecnica_tela.ft_maquina_tipo_acabado')
        rama = self.env['mrp.workcenter'].create({'name': 'RAMA 937 prueba', 'code': 'R937', 'ft_tipo_id': tipo_rama.id})
        ruta_fam.paso_ids.filtered(lambda p: p.paso == 'rama').workcenter_id = rama
        Product = self.env['product.product']
        if not Product.search([('default_code', '=', 'DESPERDICIO TEJIDO')], limit=1):
            Product.create({'name': 'Desperdicio tejido prueba', 'default_code': 'DESPERDICIO TEJIDO',
                            'uom_id': self.kg.id, 'is_storable': True})
        almacen = self.env['stock.warehouse'].search([('company_id', '=', self.env.company.id)], limit=1)
        Bom = self.env['mrp.bom']
        ruta_mto = Bom._ft_ruta_mto_kg()
        if not ruta_mto:
            ruta_mto = self.env['stock.route'].create({'name': 'Fabricar Tela kg prueba (MTO)', 'product_selectable': True})
            self.env['stock.rule'].create({
                'name': 'Fabricar Tela kg prueba', 'route_id': ruta_mto.id, 'action': 'manufacture',
                'picking_type_id': self.tipo_kg.id, 'location_dest_id': almacen.lot_stock_id.id,
                'procure_method': 'make_to_order'})
        kg = Product.create({'name': 'Tela 937 prueba (kg)', 'default_code': 'IZZ937Q22JNG165', 'uom_id': self.kg.id,
                             'is_storable': True})
        tela_m = Product.create({'name': 'Tela 937 prueba (m)', 'default_code': 'ZZ937Q22JNG165',
                                 'uom_id': self.meter.id, 'is_storable': True})
        ficha = self.env['ficha.tecnica.acabado'].create({
            'articulo': 'ZZ937Q22JNG165', 'tejido_id': self.tejido.id, 'product_acabado_id': tela_m.id,
            'product_proceso_id': kg.id, 'peso_acabado': 53, 'rendimiento_tela_acabada': 8.0, 'consumo_crudo': 1.1})
        ficha.action_ft_generar_listas()
        Revision = self.env['ficha.tecnica.revision']
        bom_kg, bom_m = Revision._ft_bom_de(kg), Revision._ft_bom_de(tela_m)
        self.assertEqual((bom_kg.consumption, bom_m.consumption), ('flexible', 'flexible'))
        self.assertTrue(bom_kg.byproduct_ids)
        bom_kg.consumption = 'warning'
        bom_kg.byproduct_ids.unlink()
        tipos = Revision._ft_actualizar(ficha).pendiente_ids.mapped('tipo')
        self.assertIn('desperdicio', tipos)
        self.assertNotIn('ruta_mto', tipos, "Archivada, la ruta todavía no hace falta")
        self.assertNotIn(ruta_mto, kg.product_tmpl_id.route_ids)
        bom_kg.action_unarchive()
        self.assertIn(ruta_mto, kg.product_tmpl_id.route_ids, "Al activar, el «I…» recibe la ruta MTO")
        self.assertEqual(bom_kg.consumption, 'flexible')
        self.assertTrue(bom_kg.byproduct_ids, "Al activar se completa el desperdicio")
        self.assertNotIn('desperdicio', Revision._ft_actualizar(ficha).pendiente_ids.mapped('tipo'))
        kg.product_tmpl_id.route_ids = [(3, ruta_mto.id)]
        revision = Revision._ft_actualizar(ficha)
        pendiente = revision.pendiente_ids.filtered(lambda p: p.tipo == 'ruta_mto')
        self.assertEqual(pendiente.area, 'ingenieria')
        self.assertEqual(pendiente.action_ft_resolver()['res_model'], 'product.template')

    def test_13_ruta_de_la_lista_en_metros_al_activar(self):
        """2.25.0: al activar la lista en metros del flujo, la tela recibe la ruta que fabrica con el tipo OP-M
        («Fabricar Tela m»); la revisión avisa si a una lista en metros activa le falta."""
        self._armar()
        almacen = self.env['stock.warehouse'].search([('company_id', '=', self.env.company.id)], limit=1)
        Bom = self.env['mrp.bom']
        ruta_m = Bom._ft_ruta_flujo('m')
        if not ruta_m:
            ruta_m = self.env['stock.route'].create({'name': 'Fabricar Tela m prueba', 'product_selectable': True})
            self.env['stock.rule'].create({
                'name': 'Fabricar Tela m prueba', 'route_id': ruta_m.id, 'action': 'manufacture',
                'picking_type_id': self.tipo_m.id, 'location_dest_id': almacen.lot_stock_id.id,
                'procure_method': 'make_to_order'})
        Product = self.env['product.product']
        kg = Product.create({'name': 'Tela 938 prueba (kg)', 'default_code': 'IZZ938Q22JNG165', 'uom_id': self.kg.id,
                             'is_storable': True})
        tela_m = Product.create({'name': 'Tela 938 prueba (m)', 'default_code': 'ZZ938Q22JNG165',
                                 'uom_id': self.meter.id, 'is_storable': True})
        ficha = self.env['ficha.tecnica.acabado'].create({
            'articulo': 'ZZ938Q22JNG165', 'tejido_id': self.tejido.id, 'product_acabado_id': tela_m.id,
            'product_proceso_id': kg.id, 'peso_acabado': 53, 'rendimiento_tela_acabada': 8.0, 'consumo_crudo': 1.1})
        ficha.action_ft_generar_listas()
        Revision = self.env['ficha.tecnica.revision']
        bom_m = Revision._ft_bom_de(tela_m)
        self.assertNotIn(ruta_m, tela_m.product_tmpl_id.route_ids)
        bom_m.action_unarchive()
        self.assertIn(ruta_m, tela_m.product_tmpl_id.route_ids, "Al activar, la tela recibe la ruta del flujo")
        tela_m.product_tmpl_id.route_ids = [(3, ruta_m.id)]
        pendiente = Revision._ft_actualizar(ficha).pendiente_ids.filtered(
            lambda p: p.tipo == 'ruta_mto' and p.bom_id == bom_m)
        self.assertTrue(pendiente, "Lista en metros activa sin la ruta: pendiente de Ingeniería")

    def test_14_la_lista_activada_va_primero(self):
        """Revisión 2026-11-11: al activar la lista en metros del flujo, queda antes que la lista anterior del producto
        (que sigue activa, de respaldo): una orden nueva toma la del flujo. Activarla otra vez no mueve la secuencia."""
        self._armar()
        Product = self.env['product.product']
        Bom = self.env['mrp.bom']
        kg = Product.create({'name': 'Tela 939 prueba (kg)', 'default_code': 'IZZ939Q22JNG165', 'uom_id': self.kg.id,
                             'is_storable': True})
        tela_m = Product.create({'name': 'Tela 939 prueba (m)', 'default_code': 'ZZ939Q22JNG165',
                                 'uom_id': self.meter.id, 'is_storable': True})
        anterior = Bom.create({'product_tmpl_id': tela_m.product_tmpl_id.id, 'product_id': tela_m.id, 'product_qty': 1,
                               'product_uom_id': self.meter.id, 'sequence': 0, 'bom_line_ids': [
                                   (0, 0, {'product_id': self.crudo.id, 'product_qty': 0.1})]})
        ficha = self.env['ficha.tecnica.acabado'].create({
            'articulo': 'ZZ939Q22JNG165', 'tejido_id': self.tejido.id, 'product_acabado_id': tela_m.id,
            'product_proceso_id': kg.id, 'peso_acabado': 53, 'rendimiento_tela_acabada': 8.0, 'consumo_crudo': 1.1})
        ficha.action_ft_generar_listas()
        bom_m = Bom.with_context(active_test=False).search([
            ('product_tmpl_id', '=', tela_m.product_tmpl_id.id), ('code', '=', 'Clasificación final (borrador)')])
        self.assertEqual(len(bom_m), 1)
        self.assertFalse(bom_m.active, "Lo generado nace archivado")
        self.assertEqual(Bom._bom_find(tela_m, bom_type='normal')[tela_m], anterior)
        bom_m.action_unarchive()
        self.assertTrue(anterior.active, "La lista anterior sigue activa, de respaldo")
        self.assertLess(bom_m.sequence, anterior.sequence)
        self.assertEqual(Bom._bom_find(tela_m, bom_type='normal')[tela_m], bom_m,
                         "Una orden nueva toma la lista del flujo")
        secuencia = bom_m.sequence
        bom_m.action_archive()
        bom_m.action_unarchive()
        self.assertEqual(bom_m.sequence, secuencia, "Si ya va primero no se mueve")


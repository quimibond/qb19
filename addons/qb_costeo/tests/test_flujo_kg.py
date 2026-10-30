# -*- coding: utf-8 -*-
"""1.7.0: el flujo de tela en dos niveles (diseño técnico v5.4).

Pedido en metros de la tela acabada → clasificación final (lista «B», en m,
consume la tela en proceso en kg a 1 ÷ rendimiento de la ficha) → por MTO la
orden en kg (lista «A»: crudo × factor real, devanado, tintorería por baño,
rama e inspección). Caso del piloto de qbtesting (30-oct-2026):
WJ053Q22JNT160 ← IWJ053Q22JNT160 ← WJ044Q22HNT235.
"""
from datetime import date, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import patch

from odoo.tests import tagged

from .common import CosteoCase

KG_M_FICHA = 1 / 11.7925      # 0.0848 kg/m: 1 ÷ rendimiento de la ficha
KG_M_LISTA = 0.0885           # lo que trae la lista B de qbtesting


@tagged('post_install', '-at_install')
class TestFlujoKg(CosteoCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # Producción guarda 0.0885 y 1.113 en las listas; la base de pruebas
        # redondea a 2 decimales.
        cls.env['decimal.precision'].search(
            [('name', 'in', ('Product Unit', 'Product Unit of Measure'))]).digits = 5
        kg = cls.kg = cls.env.ref('uom.product_uom_kgm')
        m = cls.m = cls.env.ref('uom.product_uom_meter')
        cal = cls.wc_tin.resource_calendar_id
        WC = cls.env['mrp.workcenter']
        cls.wc_dev = WC.create({'name': 'DEVANADORA 2', 'resource_calendar_id': cal.id})
        cls.wc_rama = WC.create({'name': 'RAMA 2', 'resource_calendar_id': cal.id})
        cls.wc_insp = WC.create({'name': 'INSPECCION 2', 'resource_calendar_id': cal.id})
        cls.wc_emp = WC.create({'name': 'EMPAQUE', 'resource_calendar_id': cal.id})
        # Devanadoras en tintorería (decisión 2026-10-30).
        cls.c_tin.workcenter_ids = [(4, cls.wc_dev.id)]
        cls.c_aca.workcenter_ids = [(4, cls.wc_rama.id)]
        cls.c_insp = cls.env['qb.centro'].create({
            'code': 'INSP', 'name': 'Inspección y Empaque', 'nature': 'directo',
            'driver': 'fijo_unidad',
            'workcenter_ids': [(6, 0, [cls.wc_insp.id, cls.wc_emp.id])]})

        def prod(code, uom, price=0.0):
            return cls.env['product.product'].create({
                'name': code, 'default_code': code, 'type': 'consu',
                'is_storable': True, 'uom_id': uom.id, 'standard_price': price})
        cls.crudo = prod('WJ044Q22HNT235', kg, 50.0)
        cls.proceso = prod('IWJ053Q22JNT160', kg)
        cls.tela = prod('WJ053Q22JNT160', m)
        cls.env['mrp.workcenter.capacity'].create({
            'workcenter_id': cls.wc_tin.id, 'product_id': cls.proceso.id,
            'product_uom_id': kg.id, 'capacity': 600})

        wh = cls.env['stock.warehouse'].search(
            [('company_id', '=', cls.company.id)], limit=1)
        Loc = cls.env['stock.location']
        cls.loc_insp = Loc.create({'name': '10 INSPECCION', 'usage': 'internal',
                                   'location_id': wh.lot_stock_id.id})
        cls.loc_pt = Loc.create({'name': '14 PT', 'usage': 'internal',
                                 'location_id': wh.lot_stock_id.id})
        cls.loc_seg = Loc.create({'name': 'SEGUNDAS', 'usage': 'internal',
                                  'location_id': wh.lot_stock_id.id})
        Tipo = cls.env['stock.picking.type']
        cls.t_kg = Tipo.create({
            'name': 'Fabricación Tela en kg', 'code': 'mrp_operation',
            'sequence_code': 'QBKG', 'warehouse_id': wh.id,
            'default_location_src_id': wh.lot_stock_id.id,
            'default_location_dest_id': cls.loc_insp.id})
        cls.t_clf = Tipo.create({
            'name': 'Clasificación final', 'code': 'mrp_operation',
            'sequence_code': 'QBCLF', 'warehouse_id': wh.id,
            'default_location_src_id': cls.loc_insp.id,
            'default_location_dest_id': cls.loc_pt.id})
        P = cls.env['qb.parametro']
        for key, val in (('tipos_flujo_nuevo', '%d,%d' % (cls.t_kg.id, cls.t_clf.id)),
                         ('tipos_clasificacion', str(cls.t_clf.id)),
                         ('tipos_kg', str(cls.t_kg.id))):
            P._rec(key).value_text = val

        Bom = cls.env['mrp.bom']
        # Lista vieja en metros: sin operaciones, con historia (abajo).
        cls.bom_vieja = Bom.create({
            'product_tmpl_id': cls.tela.product_tmpl_id.id, 'product_qty': 1,
            'product_uom_id': m.id, 'sequence': 0,
            'bom_line_ids': [(0, 0, {'product_id': cls.crudo.id, 'product_qty': 0.1,
                                     'product_uom_id': kg.id})]})
        # Lista A (kg): crudo × 1.113 con la merma de proceso, 5 operaciones.
        cls.bom_a = Bom.create({
            'product_tmpl_id': cls.proceso.product_tmpl_id.id, 'product_qty': 1,
            'product_uom_id': kg.id, 'sequence': -10, 'picking_type_id': cls.t_kg.id,
            'bom_line_ids': [(0, 0, {'product_id': cls.crudo.id, 'product_qty': 1.113,
                                     'product_uom_id': kg.id})],
            'operation_ids': [
                (0, 0, {'name': 'DEVANADO', 'workcenter_id': cls.wc_dev.id,
                        'time_cycle_manual': 0.413, 'sequence': 10}),
                (0, 0, {'name': 'TINTORERIA', 'workcenter_id': cls.wc_tin.id,
                        'time_cycle_manual': 210, 'sequence': 20}),
                (0, 0, {'name': 'RAMA', 'workcenter_id': cls.wc_rama.id,
                        'time_cycle_manual': 0.3178, 'sequence': 30}),
                (0, 0, {'name': 'INSPECCION', 'workcenter_id': cls.wc_insp.id,
                        'time_cycle_manual': 0.9497, 'sequence': 40})]})
        # Lista de clasificación final (m) como la B de José: 0.0885 kg/m.
        cls.bom_b = Bom.create({
            'product_tmpl_id': cls.tela.product_tmpl_id.id, 'product_qty': 1,
            'product_uom_id': m.id, 'sequence': -10, 'picking_type_id': cls.t_clf.id,
            'bom_line_ids': [(0, 0, {'product_id': cls.proceso.id,
                                     'product_qty': KG_M_LISTA, 'product_uom_id': kg.id})],
            'operation_ids': [(0, 0, {'name': 'CLASIFICACION', 'workcenter_id': cls.wc_emp.id,
                                      'time_cycle_manual': 0.05})]})
        cls.H = cls.env['qb.producto.horas']
        cls.ficha = SimpleNamespace(
            display_name='WJ053Q22JNT160', rendimiento_tela_acabada=11.7925,
            product_acabado_id=cls.tela, product_proceso_id=cls.proceso)

    def _con_ficha(self):
        """La ficha de acabado de Consolti sin depender de su módulo."""
        ficha = self.ficha

        def _ficha_acabado(self_, product):
            return ficha if product in (ficha.product_acabado_id, ficha.product_proceso_id) else None
        return patch.object(type(self.H), '_ficha_acabado', _ficha_acabado)

    def _mo(self, bom, qty, fin, picking_type=None, dest=None, wos=()):
        product = bom.product_id or bom.product_tmpl_id.product_variant_id
        vals = {'product_id': product.id, 'product_qty': qty,
                'product_uom_id': bom.product_uom_id.id, 'bom_id': bom.id}
        if picking_type:
            vals['picking_type_id'] = picking_type.id
        if dest:
            vals['location_dest_id'] = dest.id
        mo = self.env['mrp.production'].create(vals)
        mo.action_confirm()
        mo.qty_producing = qty
        mo.move_raw_ids.picked = True
        res = mo.button_mark_done()
        if isinstance(res, dict) and res.get('res_model'):
            # Asistente de consumo o de pendientes: se confirma tal cual.
            wiz = self.env[res['res_model']].with_context(**res.get('context', {})).create({})
            getattr(wiz, 'action_confirm', getattr(wiz, 'process', lambda: None))()
        self.assertEqual(mo.state, 'done')
        mo.write({'date_finished': fin})
        # Odoo 19 crea las órdenes de trabajo de la lista y las cierra solas
        # al terminar; la prueba deja solo las que dice `wos`.
        self.env.cr.execute('DELETE FROM mrp_workorder WHERE production_id = %s', (mo.id,))
        self.env.invalidate_all()
        for wc, horas in wos:
            wo = self.env['mrp.workorder'].create({
                'name': 'WO', 'production_id': mo.id, 'workcenter_id': wc.id,
                'product_uom_id': product.uom_id.id})
            wo.write({'state': 'done', 'duration': horas * 60, 'date_finished': fin,
                      'costs_hour': wc.costs_hour})
        return mo

    def _fila(self, product, centro):
        return self.H.search([('product_id', '=', product.id), ('centro_id', '=', centro.id)])

    # ------------------------------------------------------------------
    def test_lista_vigente_es_la_del_flujo_nuevo(self):
        """La lista vieja en metros con más historia NO es la vigente."""
        for _ in range(3):
            self._mo(self.bom_vieja, 1000, datetime(2026, 9, 10, 12))
        self.assertEqual(self.H._bom_vigente(self.tela), self.bom_b)
        self.assertEqual(self.H._bom_vigente(self.proceso), self.bom_a)
        # Sin tipo de operación del flujo nuevo, la de menor secuencia.
        self.bom_b.picking_type_id = False
        self.assertEqual(self.H._bom_vigente(self.tela), self.bom_b)
        self.bom_b.sequence = 5
        self.assertEqual(self.H._bom_vigente(self.tela), self.bom_vieja)

    def test_tintoreria_por_bano_y_herencia_en_metros(self):
        """Tintorería: 210 min por baño ÷ 600 kg; el devanado va en el mismo
        centro. La tela en metros hereda kg/m de la ficha × horas por kg."""
        h_tin = 210 / 600.0 / 60.0
        h_dev = 0.413 / 60.0
        op_tin = self.bom_a.operation_ids.filtered(lambda o: o.name == 'TINTORERIA')
        m, det = self.H._minutos_operacion(self.proceso, self.bom_a, op_tin, self.c_tin)
        self.assertAlmostEqual(m / 60.0, h_tin, 9)
        self.assertIn('600', det)
        with self._con_ficha():
            self.H.recalcular(self.proceso | self.tela)
        f = self._fila(self.proceso, self.c_tin)
        self.assertEqual(f.fuente, 'estandar')
        self.assertAlmostEqual(f.horas_propias, h_dev + h_tin, 6)
        h_rama = 0.3178 / 60.0
        h_insp = 0.9497 / 60.0
        self.assertAlmostEqual(self._fila(self.proceso, self.c_aca).horas_propias, h_rama, 6)
        self.assertAlmostEqual(self._fila(self.proceso, self.c_insp).horas_propias, h_insp, 6)
        # Metros: solo clasificación propia; lo demás, heredado a 0.0848 kg/m.
        ft = self._fila(self.tela, self.c_tin)
        self.assertFalse(ft.horas_propias)
        self.assertAlmostEqual(ft.horas_heredadas, KG_M_FICHA * (h_dev + h_tin), 6)
        fa = self._fila(self.tela, self.c_aca)
        self.assertFalse(fa.horas_propias)
        self.assertAlmostEqual(fa.horas_unidad, KG_M_FICHA * h_rama, 6)
        fi = self._fila(self.tela, self.c_insp)
        self.assertAlmostEqual(fi.horas_propias, 0.05 / 60.0, 6)
        self.assertAlmostEqual(fi.horas_heredadas, KG_M_FICHA * h_insp, 6)

    def test_tiempo_de_la_formula_manda_sobre_la_operacion(self):
        """Con tiempo por baño de la fórmula en ese jet (programación), ese
        manda sobre la operación de la lista."""
        if 'tintoreria.formula.tiempo' not in self.env:
            return
        formula = self.env['tintoreria.formula'].create({'name': 'NATURAL COSTEO'})
        self.bom_a.formula_tintoreria_id = formula.id
        self.env['tintoreria.formula.tiempo'].create({
            'formula_id': formula.id, 'workcenter_id': self.wc_tin.id, 'minutos': 300})
        op_tin = self.bom_a.operation_ids.filtered(lambda o: o.name == 'TINTORERIA')
        m, det = self.H._minutos_operacion(self.proceso, self.bom_a, op_tin, self.c_tin)
        self.assertAlmostEqual(m, 300 / 600.0, 9)
        self.assertIn('fórmula', det)

    def test_historia_vieja_no_carga_acabado_a_los_metros(self):
        """Las órdenes viejas en metros con rama medida no le cargan acabado
        propio a la tela: la rama la carga la de kg (no se duplica)."""
        self._mo(self.bom_vieja, 1000, datetime(2026, 9, 10, 12),
                 wos=[(self.wc_rama, 1.0)])
        with self._con_ficha():
            self.H.recalcular(self.proceso | self.tela)
        fa = self._fila(self.tela, self.c_aca)
        self.assertFalse(fa.horas_propias)
        self.assertAlmostEqual(fa.horas_unidad, KG_M_FICHA * 0.3178 / 60.0, 6)

    def test_etapas_y_centros_esperados(self):
        esperados = self.H.centros_esperados(
            self.tela, self.c_tin | self.c_aca | self.c_insp)
        self.assertEqual(esperados, self.c_tin | self.c_aca | self.c_insp)
        self.assertEqual(self.H._drivers_por_codigo(self.proceso, self.env['mrp.bom']),
                         {'kg_ciclo', 'm_velocidad', 'fijo_unidad'})
        self.assertEqual(self.H._drivers_por_codigo(self.tela, self.bom_b), {'fijo_unidad'})
        self.assertEqual(self.H._drivers_por_codigo(self.tela, self.bom_vieja), {'m_velocidad'})
        importado = self.env['product.product'].create({
            'name': 'imp', 'default_code': 'IWJ053Q22JNT160', 'type': 'consu',
            'uom_id': self.m.id})
        self.assertIsNone(self.H._drivers_por_codigo(importado, self.env['mrp.bom']))
        # La operación real manda: la de kg no pasa por tejido ni por empaque solo
        # por su código.
        self.assertFalse(self.H._centro_aplica(self.tela, self.bom_b, self.c_aca))
        self.assertTrue(self.H._centro_aplica(self.tela, self.bom_b, self.c_insp))

    def test_conversion_y_merma_una_sola_vez(self):
        """MP de la tela = kg/m de la ficha × 1.113 kg de crudo × precio: la
        merma de proceso va una vez (en la lista A) y la conversión es la de
        la ficha, no la de la lista B. El peso por metro es el mismo."""
        Mp = self.env['qb.producto.mp']
        with self._con_ficha():
            mp, hojas = Mp.explotar(self.tela)
            self.assertAlmostEqual(mp, KG_M_FICHA * 1.113 * 50.0, 6)
            self.assertAlmostEqual(hojas[self.crudo.id][0], KG_M_FICHA * 1.113, 9)
            kg, fuente, _d = self.env['qb.producto.kg'].resolver(self.tela)
            self.assertEqual(fuente, 'ficha')
            self.assertAlmostEqual(kg, KG_M_FICHA, 9)
            self.assertEqual(self.env['qb.producto.kg'].resolver(self.proceso)[1], 'kg')
        # Sin ficha, la de la lista.
        mp, _h = Mp.explotar(self.tela)
        self.assertAlmostEqual(mp, KG_M_LISTA * 1.113 * 50.0, 6)

    def test_validacion_de_conversion(self):
        Val = self.env['qb.producto.validacion']
        with self._con_ficha():
            Val.revisar(self.tela)
            v = Val.search([('product_id', '=', self.tela.id),
                            ('regla', '=', 'conversion_clasificacion')])
            self.assertEqual(v.estado, 'abierta')
            self.assertEqual(v.componente_id, self.proceso)
            self.assertAlmostEqual(v.valor, 100 * (KG_M_LISTA / KG_M_FICHA - 1), 2)
            self.bom_b.bom_line_ids.product_qty = round(KG_M_FICHA, 5)
            Val.revisar(self.tela)
            self.assertEqual(v.estado, 'corregida')

    def test_rendimiento_solo_desde_clasificacion(self):
        """El rendimiento vendible cuenta lo que sale de la clasificación
        (primera y segundas); lo que sale de la orden en kg no cuenta aunque
        caiga en una ubicación de primera."""
        P = self.env['qb.parametro']
        prod_loc = self.proceso.property_stock_production
        for key, val in (('calidad_locs_vendible', str(self.loc_pt.id)),
                         ('calidad_locs_merma', str(self.loc_seg.id)),
                         ('calidad_locs_origen', ','.join(str(i) for i in (
                             self.env['stock.location'].search([('usage', '=', 'production')])
                             | prod_loc | self.loc_insp).ids))):
            P._rec(key).value_text = val
        hoy = datetime.now()
        self._mo(self.bom_a, 100, hoy, picking_type=self.t_kg, dest=self.loc_pt)
        self._mo(self.bom_b, 900, hoy, picking_type=self.t_clf, dest=self.loc_pt)
        move = self.env['stock.move'].create({
            'product_id': self.tela.id, 'product_uom_qty': 100,
            'product_uom': self.m.id, 'location_id': self.loc_insp.id,
            'location_dest_id': self.loc_seg.id})
        move._action_confirm()
        move.quantity = 100
        move.picked = True
        move._action_done()
        datos, _planta = self.env['qb.producto.rendimiento'].medir()
        self.assertNotIn(self.proceso.id, datos)
        self.assertEqual(datos[self.tela.id], (900.0, 1000.0))

    def test_horas_reales_mixtas_y_conciliacion(self):
        """Primer mes: una orden en kg con órdenes de trabajo y otra sin
        ellas → órdenes de trabajo + estándar. La mano de obra capitalizada
        de la orden en kg y la de metros no se suman dos veces."""
        self.wc_tin.costs_hour = 146.71
        self.wc_dev.costs_hour = 146.71
        self.wc_emp.costs_hour = 338.51
        sept = datetime(2026, 9, 12, 10)
        self._mo(self.bom_a, 354, sept, wos=[(self.wc_dev, 2.437), (self.wc_tin, 3.5)])
        self._mo(self.bom_a, 600, sept)
        self._mo(self.bom_b, 4000, sept, wos=[(self.wc_emp, 3.333)])
        horas, fuente = self.c_tin.horas_reales(date(2026, 9, 1), date(2026, 10, 1))
        self.assertEqual(fuente, 'mixta')
        std = 600 * (0.413 / 60.0 + 210 / 600.0 / 60.0)
        self.assertAlmostEqual(horas, 2.437 + 3.5 + std, 4)
        P = self.env['qb.parametro']
        P.set_float('horas_reales_completar', 0)
        horas, fuente = self.c_tin.horas_reales(date(2026, 9, 1), date(2026, 10, 1))
        self.assertEqual(fuente, 'workorder')
        self.assertAlmostEqual(horas, 2.437 + 3.5, 6)
        self.periodo.action_calcular()
        conc = self.periodo.conciliacion_mano_de_obra()
        self.assertAlmostEqual(conc['TIN']['horas'], 2.437 + 3.5, 4)
        self.assertAlmostEqual(conc['TIN']['capitalizado'], (2.437 + 3.5) * 146.71, 2)
        self.assertAlmostEqual(conc['INSP']['horas'], 3.333, 4)
        total_wo = sum(d['horas'] for d in conc.values())
        self.assertAlmostEqual(total_wo, 2.437 + 3.5 + 3.333, 4)
        # La tela en kg es semielaborado: su costo no se divide entre el
        # rendimiento (la merma de proceso ya va en el crudo × 1.113).
        self.periodo.action_calcular_costos()
        fk = self.periodo.costo_ids.filtered(lambda r: r.product_id == self.proceso)
        self.assertEqual(fk.rendimiento, 1.0)
        self.assertEqual(fk.rendimiento_fuente, 'semielaborado')
        self.assertAlmostEqual(fk.costo_vendible, fk.costo_produccion, 4)

    def test_banda_por_bano_en_tintoreria(self):
        """Tintorería: banda en horas por orden (baño) para los kg; una orden
        con el cronómetro abierto se descarta y se ve como fuera de rango."""
        self.c_tin.banda_ids = [(0, 0, {'uom_id': self.kg.id, 'por': 'orden',
                                        'minimo': 2, 'maximo': 8})]
        fin = datetime.now() - timedelta(days=1)
        bien = self._mo(self.bom_a, 354, fin, wos=[(self.wc_tin, 3.5)])
        largo = self._mo(self.bom_a, 354, fin, wos=[(self.wc_tin, 30)])
        medido = self.H._medido(self.c_tin, *self.H._ventana())
        h, n, qty = medido[(self.proceso.id, self.c_tin.id)]
        self.assertEqual(n, 1)
        self.assertAlmostEqual(h, 3.5 / 354, 9)
        X = self.env['qb.ot.excepcion']
        self.assertFalse(X.search([('production_id', '=', bien.id)]))
        xl = X.search([('production_id', '=', largo.id)])
        self.assertEqual(xl.tipo, 'timer_largo')
        self.assertEqual(xl.por, 'orden')
        self.assertAlmostEqual(xl.horas_desviadas, 30 - 8, 4)

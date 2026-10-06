# -*- coding: utf-8 -*-
"""La conversión absorbida llega al costo unitario (v1.69).

Desde que TEJIDO capitaliza por workcenter, su costo sale del pool fabril.
Sin esta capa la receta se explotaba hasta el hilo y el tejido se perdía:
una tela de 40 g cargaba la misma fabricación que una de 135 g, y los totales
del período no traían el tejido que sí estaba en el costo de ventas.
"""
from datetime import date, datetime

from dateutil.relativedelta import relativedelta

from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestConversionAbsorbida(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.env = cls.env(context=dict(cls.env.context, tracking_disable=True))
        dp = cls.env['decimal.precision'].search(
            [('name', '=', 'Product Unit')], limit=1)
        if dp and dp.digits < 4:
            dp.digits = 4
        cls.uom_kg = cls.env.ref('uom.product_uom_kgm')
        cls.uom_m = cls.env.ref('uom.product_uom_meter')
        cls.Costo = cls.env['qb.costo.producto']
        cls.period = date(2028, 3, 1)
        cls.fin = cls.period + relativedelta(months=1)
        Product = cls.env['product.product']
        # $60/h en la máquina absorbida
        cls.wc = cls.env['mrp.workcenter'].create(
            {'name': 'CIRCULAR CONV TEST', 'costs_hour': 60.0})
        cls.centro = cls.env['qb.costeo.centro'].create({
            'code': 'TEST_CONV', 'name': 'Tejido conversión test',
            'nature': 'fabril_directo', 'driver_principal': 'peso',
            'modo_costeo': 'absorcion_odoo', 'fecha_absorcion': cls.period,
            'workcenter_ids': [(6, 0, [cls.wc.id])]})
        cls.hilo = Product.create({
            'name': 'HILO CONV TEST', 'is_storable': True,
            'uom_id': cls.uom_kg.id, 'standard_price': 40.0})
        cls.crudo = Product.create({
            'name': 'CRUDO CONV TEST', 'default_code': 'CRCONV01',
            'is_storable': True, 'tracking': 'lot', 'uom_id': cls.uom_kg.id})
        cls.pesada = Product.create({
            'name': 'TELA 135 G TEST', 'default_code': 'WK135CONV160',
            'is_storable': True, 'tracking': 'lot', 'uom_id': cls.uom_m.id,
            'sale_ok': True})
        cls.ligera = Product.create({
            'name': 'TELA 40 G TEST', 'default_code': 'NN040CONV160',
            'is_storable': True, 'uom_id': cls.uom_m.id, 'sale_ok': True})
        cls._bom(cls.crudo, cls.uom_kg, [(cls.hilo, 1.0, cls.uom_kg)])
        # 135 g/m² × 1.60 m ≈ 0.216 kg/m; 40 g/m² ≈ 0.064 kg/m
        cls._bom(cls.pesada, cls.uom_m, [(cls.crudo, 0.216, cls.uom_kg)])
        cls._bom(cls.ligera, cls.uom_m, [(cls.crudo, 0.064, cls.uom_kg)])
        Loc = cls.env['stock.location']
        cls.loc = cls.env.ref('stock.stock_location_stock',
                              raise_if_not_found=False) \
            or Loc.search([('usage', '=', 'internal')], limit=1)
        cls.loc_prod = Loc.search([('usage', '=', 'production')], limit=1)
        cls.cliente = Loc.search([('usage', '=', 'customer')], limit=1)

    @classmethod
    def _bom(cls, product, uom, lineas):
        return cls.env['mrp.bom'].create({
            'product_tmpl_id': product.product_tmpl_id.id,
            'product_qty': 1.0, 'product_uom_id': uom.id,
            'bom_line_ids': [(0, 0, {
                'product_id': comp.id, 'product_qty': qty,
                'product_uom_id': u.id}) for comp, qty, u in lineas],
        })

    def _mo(self, producto, qty, uom, fin, lot=None, minutos=0.0):
        """Orden terminada por SQL (como en la traza): una línea de salida,
        con lote si se pide, y una orden de trabajo en la máquina absorbida
        si lleva minutos."""
        mo = self.env['mrp.production'].create({
            'name': 'CONV/%s' % producto.default_code,
            'product_id': producto.id,
            'product_qty': qty, 'product_uom_id': uom.id})
        if minutos:
            wo = self.env['mrp.workorder'].create({
                'name': 'tejido', 'production_id': mo.id,
                'workcenter_id': self.wc.id})
            self.env.flush_all()
            self.env.cr.execute(
                "UPDATE mrp_workorder SET duration = %s, state = 'done' "
                "WHERE id = %s", (minutos, wo.id))
        move = self.env['stock.move'].create({
            'product_id': producto.id, 'product_uom': uom.id,
            'product_uom_qty': qty, 'location_id': self.loc_prod.id,
            'location_dest_id': self.loc.id, 'production_id': mo.id})
        line = self.env['stock.move.line'].create({
            'move_id': move.id, 'product_id': producto.id,
            'product_uom_id': uom.id, 'quantity': qty,
            'lot_id': lot.id if lot else False,
            'location_id': self.loc_prod.id, 'location_dest_id': self.loc.id})
        self.env.flush_all()
        self.env.cr.execute(
            "UPDATE mrp_production SET state = 'done', date_finished = %s, "
            "qty_producing = %s WHERE id = %s", (fin, qty, mo.id))
        self.env.cr.execute(
            "UPDATE stock_move SET state = 'done', date = %s WHERE id = %s",
            (fin, move.id))
        self.env.cr.execute(
            "UPDATE stock_move_line SET state = 'done', date = %s "
            "WHERE id = %s", (fin, line.id))
        self.env.invalidate_all()
        return mo

    def _linea(self, producto, qty, uom, lot, src, dst, fecha, raw_mo=None):
        vals = {'product_id': producto.id, 'product_uom': uom.id,
                'product_uom_qty': qty, 'location_id': src.id,
                'location_dest_id': dst.id}
        if raw_mo is not None:
            vals['raw_material_production_id'] = raw_mo.id
        move = self.env['stock.move'].create(vals)
        line = self.env['stock.move.line'].create({
            'move_id': move.id, 'product_id': producto.id,
            'product_uom_id': uom.id, 'quantity': qty, 'lot_id': lot.id,
            'location_id': src.id, 'location_dest_id': dst.id})
        self.env.flush_all()
        self.env.cr.execute(
            "UPDATE stock_move SET state = 'done', date = %s WHERE id = %s",
            (fecha, move.id))
        self.env.cr.execute(
            "UPDATE stock_move_line SET state = 'done', date = %s "
            "WHERE id = %s", (fecha, line.id))
        self.env.invalidate_all()

    def _factores(self, **extra):
        vals = {'period': self.period, 'window_months': 12,
                'factor_fab_kg': 0.0, 'factor_fab_m': 2.0,
                'energia_por_kg': 5.0, 'op_pct': 0.15,
                'centros_absorbidos': 'TEST_CONV',
                'conv_tarifa_kg_centro': 1.0, 'conv_energia_share': 0.25}
        vals.update(extra)
        return self.env['qb.costo.factores'].create(vals)

    # ------------------------------------------------------------------
    def test_la_conversion_sube_con_el_gramaje(self):
        """Tarifa del crudo = lo abonado en sus órdenes ÷ sus kilos: 200 min
        × $60/h = $200 sobre 100 kg = $2/kg. La tela pesada consume 0.216 kg
        por metro y la ligera 0.064: cargan $0.432 y $0.128. Antes las dos
        cargaban la misma fabricación por metro."""
        self._mo(self.crudo, 100.0, self.uom_kg,
                 datetime(2028, 3, 5, 12), minutos=200.0)
        f = self._factores()
        conv_p, var_p, fuente_p = self.Costo._conv_unit(self.pesada, f)
        conv_l, _var_l, fuente_l = self.Costo._conv_unit(self.ligera, f)
        self.assertAlmostEqual(conv_p, 0.216 * 2.0, places=6)
        self.assertAlmostEqual(conv_l, 0.064 * 2.0, places=6)
        self.assertAlmostEqual(var_p, conv_p * 0.25, places=6,
                               msg='la parte de energía es variable')
        self.assertEqual((fuente_p, fuente_l), ('op', 'op'))
        self.assertGreater(conv_p, 3 * conv_l)
        # El hilo no carga conversión: va separada de la MP
        self.assertEqual(self.Costo._conv_unit(self.hilo, f)[0], 0.0)

    def test_sin_centro_absorbido_no_hay_capa(self):
        """En un período de capa (hasta agosto de 2026) el tejido ya viaja en
        `fab_unit`: agregarle conversión lo cobraría dos veces."""
        self._mo(self.crudo, 100.0, self.uom_kg,
                 datetime(2028, 3, 5, 12), minutos=200.0)
        f = self._factores(centros_absorbidos=False)
        self.assertEqual(self.Costo._conv_unit(self.pesada, f),
                         (0.0, 0.0, False))

    def test_crudo_sin_ordenes_toma_su_familia_y_luego_el_centro(self):
        """Producto sin historial en el período: la familia de máquinas da
        $/h ÷ su velocidad; sin familia, el promedio del centro marcado como
        estimado. El crudo se reconoce por haber corrido alguna vez en las
        máquinas absorbidas."""
        nuevo = self.env['product.product'].create({
            'name': 'CRUDO NUEVO TEST', 'default_code': 'CRCONV02',
            'is_storable': True, 'uom_id': self.uom_kg.id})
        # Corrió hace un año: es crudo, pero no tiene órdenes en el período
        self._mo(nuevo, 10.0, self.uom_kg, datetime(2027, 3, 5, 12),
                 minutos=30.0)
        f = self._factores(conv_tarifa_kg_centro=1.7)
        self.assertEqual(self.Costo._conv_unit(nuevo, f)[0::2],
                         (1.7, 'centro'))
        familia = self.env['qb.costeo.familia'].create({
            'code': 'TEST_CONV_F', 'name': 'Galga 24 test',
            'centro_id': self.centro.id,
            'machine_names': 'CIRCULAR CONV TEST', 'machine_count': 1,
            'hours_per_week': 144.0, 'std_output_per_hour': 12.0})
        self.env['qb.familia.producto'].create({
            'familia_id': familia.id, 'product_code': 'CRCONV02',
            'std_output_per_hour': 15.0})
        # $60/h ÷ 15 kg/h del artículo en esa familia = $4/kg
        self.assertEqual(self.Costo._conv_unit(nuevo, f)[0::2],
                         (4.0, 'familia'))
        # Una especificación nueva usa la velocidad de la familia: 60 ÷ 12
        self.assertEqual(
            self.Costo.tarifa_conversion_familia(familia, f), (5.0, 'familia'))
        self.assertEqual(
            self.Costo.tarifa_conversion_familia(
                self.env['qb.costeo.familia'], f), (1.7, 'centro'))

    def test_crudo_sin_workorder_se_reconoce_por_el_nombre_de_su_orden(self):
        """El crudo del WK135B66JNG165 solo tiene dos órdenes de 2025 sin
        orden de trabajo: sin el patrón del centro explotaba hasta el hilo y
        la tela salía sin tejido."""
        viejo = self.env['product.product'].create({
            'name': 'CRUDO VIEJO TEST', 'default_code': 'CRCONV04',
            'is_storable': True, 'uom_id': self.uom_kg.id})
        self._bom(viejo, self.uom_kg, [(self.hilo, 1.0, self.uom_kg)])
        self._mo(viejo, 8.0, self.uom_kg, datetime(2025, 6, 13, 12))
        f = self._factores(conv_tarifa_kg_centro=1.5)
        self.assertEqual(self.Costo._conv_unit(viejo, f)[0], 0.0)
        self.centro.mo_name_pattern = 'CONV/CRCONV04%'
        self.assertEqual(self.Costo._conv_unit(viejo, f)[0::2],
                         (1.5, 'centro'))

    def test_el_estimado_se_hereda_por_la_receta(self):
        """Si un componente sale estimado, el producto también."""
        nuevo = self.env['product.product'].create({
            'name': 'CRUDO EST TEST', 'default_code': 'CRCONV03',
            'is_storable': True, 'uom_id': self.uom_kg.id})
        self._mo(nuevo, 10.0, self.uom_kg, datetime(2027, 3, 5, 12),
                 minutos=30.0)
        tela = self.env['product.product'].create({
            'name': 'TELA EST TEST', 'default_code': 'WJ050CONV160',
            'is_storable': True, 'uom_id': self.uom_m.id, 'sale_ok': True})
        self._bom(tela, self.uom_m, [(nuevo, 0.1, self.uom_kg),
                                     (self.crudo, 0.1, self.uom_kg)])
        self._mo(self.crudo, 100.0, self.uom_kg,
                 datetime(2028, 3, 5, 12), minutos=200.0)
        f = self._factores(conv_tarifa_kg_centro=1.0)
        conv, _v, fuente = self.Costo._conv_unit(tela, f)
        self.assertAlmostEqual(conv, 0.1 * 1.0 + 0.1 * 2.0, places=6)
        self.assertEqual(fuente, 'centro')

    def test_los_totales_cargan_lo_que_de_verdad_llego_a_ventas(self):
        """El período carga la conversión que la traza encontró en las
        entregas, no tarifa × qty: en el mes del corte parte de lo vendido
        se tejió antes y su tejido ya se fue a gasto. La suma de las filas
        es exactamente la conversión absorbida vendida del período, y la
        diferencia contra tarifa × qty queda como efecto de transición."""
        Lot = self.env['stock.lot']
        lot_c = Lot.create({'name': 'CR-CONV-1', 'product_id': self.crudo.id})
        lot_t = Lot.create({'name': 'TL-CONV-1',
                            'product_id': self.pesada.id})
        # 100 kg tejidos con $200 de conversión; 21.6 kg hacen 100 m
        self._mo(self.crudo, 100.0, self.uom_kg, datetime(2028, 3, 5, 12),
                 lot=lot_c, minutos=200.0)
        mo_t = self._mo(self.pesada, 100.0, self.uom_m,
                        datetime(2028, 3, 10, 12), lot=lot_t)
        self._linea(self.crudo, 21.6, self.uom_kg, lot_c, self.loc,
                    self.loc_prod, datetime(2028, 3, 10, 12), raw_mo=mo_t)
        # Se entregan 50 de los 100 m: $43.20 × 50/100 = $21.60
        self._linea(self.pesada, 50.0, self.uom_m, lot_t, self.loc,
                    self.cliente, datetime(2028, 3, 20, 12))
        self.Costo.action_recompute_period(self.period)
        f = self.env['qb.costo.factores'].search(
            [('period', '=', self.period)], limit=1)
        self.assertAlmostEqual(f.absorcion_vendida_month, 21.6, places=4)
        self.assertAlmostEqual(f.conv_tarifa_kg_centro, 2.0, places=6)
        self.assertAlmostEqual(f.conv_kg_month, 100.0, places=4)
        filas = self.Costo.search([('period', '=', self.period)])
        self.assertAlmostEqual(sum(filas.mapped('conv_total')), 21.6,
                               places=4)
        fila = filas.filtered(lambda r: r.product_id == self.pesada)
        self.assertAlmostEqual(fila.conv_total, 21.6, places=4)
        self.assertAlmostEqual(fila.conv_unit, 0.432, places=6)
        self.assertAlmostEqual(
            fila.costo_produccion,
            fila.mp_unit + fila.energia_unit + fila.fab_unit + fila.conv_unit,
            places=6)
        self.assertAlmostEqual(
            fila.costo_variable,
            fila.mp_unit + fila.energia_unit + fila.conv_var_unit, places=6)
        # Sin factura no hay qty vendida: todo el cargo es transición negativa
        self.assertAlmostEqual(f.conv_transicion_month,
                               f.conv_unitaria_vendida_month - 21.6, places=4)
        # Identidad ventas − costo = margen, con la conversión dentro
        self.assertAlmostEqual(
            fila.margen_neto_total,
            fila.ventas_total - fila.costo_absorbido_total, places=4)
        self.assertAlmostEqual(
            fila.costo_produccion_total,
            fila.mp_total + fila.energia_total + fila.fab_total
            + fila.conv_total, places=4)

    def test_cotizar_usa_el_ultimo_periodo_cerrado(self):
        """El mes en curso tiene el pool a medio llenar: un piso no puede
        depender del día en que se cotizó."""
        Fac = self.env['qb.costo.factores']
        cerrado = Fac.create({'period': date(2028, 1, 1), 'window_months': 12,
                              'energia_por_kg': 5.0, 'op_pct': 0.1,
                              'state': 'cerrado'})
        Fac.create({'period': date(2028, 2, 1), 'window_months': 12,
                    'energia_por_kg': 5.0, 'op_pct': 0.1})
        factores, aviso = Fac.para_cotizar()
        self.assertEqual(factores, cerrado)
        self.assertFalse(aviso)
        cerrado.state = 'borrador'
        factores, aviso = Fac.para_cotizar()
        self.assertEqual(factores.period, date(2028, 2, 1))
        self.assertIn('BORRADOR', aviso)

    def test_energia_en_cero_con_pool_no_se_cotiza(self):
        """Las cotizaciones 89, 93 y 94 salieron con energía $0.00/kg porque
        el denominador de kilos estaba en cero."""
        f = self._factores(energia_por_kg=0.0, energia_pool_month=900000.0,
                           centros_absorbidos=False)
        with self.assertRaises(UserError):
            self.Costo.quote_product(self.pesada, f)
        # Sin pool de energía no hay nada que cobrar: se cotiza
        f.energia_pool_month = 0.0
        self.assertTrue(self.Costo.quote_product(self.pesada, f))

    def test_pisos_con_conversion_y_merma(self):
        """Piso lleno = producción vendible ÷ (1 − op), con la conversión
        completa; piso ocioso = solo la energía de la conversión. Los dos
        divididos entre el rendimiento de primera."""
        p = self.Costo._pisos(mp=10.0, energia=1.0, fab=2.0, conv=4.0,
                              conv_var=1.0, rend=0.8, op=0.2, mercado=0.0)
        self.assertAlmostEqual(p['piso_ocioso'], (10 + 1 + 1) / 0.8,
                               places=6)
        self.assertAlmostEqual(p['produccion'], (10 + 1 + 2 + 4) / 0.8,
                               places=6)
        self.assertAlmostEqual(p['piso_lleno'], p['produccion'] / 0.8,
                               places=6)

    def test_el_cotizador_carga_la_conversion(self):
        """La calculadora suma la capa al costo de producción y al piso, la
        muestra como renglón propio y la guarda en la cotización."""
        self._mo(self.crudo, 100.0, self.uom_kg,
                 datetime(2028, 3, 5, 12), minutos=200.0)
        f = self._factores(state='cerrado')
        wiz = self.env['qb.cotizador.wizard'].create({
            'product_id': self.pesada.id, 'volumen': 1000.0})
        self.assertEqual(wiz.factores_id, f)
        self.assertAlmostEqual(wiz.conv_unit, 0.432, places=6)
        self.assertFalse(wiz.factores_aviso)
        cot = wiz._save_cotizacion()
        self.assertAlmostEqual(cot.conv_unit, 0.432, places=6)
        self.assertAlmostEqual(
            cot.costo_absorbido_sin_op,
            cot.mp_unit + cot.energia_unit + cot.fab_unit + cot.conv_unit,
            places=4)
        self.assertAlmostEqual(
            cot.piso_lleno, cot.costo_absorbido_sin_op / (1 - 0.15), places=4)
        self.assertIn('Conversión absorbida', cot.supuestos)

    def test_receta_de_la_ultima_op_sin_crudo_usa_la_otra_receta(self):
        """WJ060Q21JNT165 tiene dos recetas activas: la de su última OP baja
        a un crudo de 2022 (órdenes OP-DES, sin máquina ni patrón) y la otra
        al crudo tejido hoy. Un producto que se teje no puede salir con
        conversión cero: si la receta de la última OP no llega a ningún
        crudo, se toma la más cara de sus recetas."""
        legado = self.env['product.product'].create({
            'name': 'CRUDO 2022 TEST', 'default_code': 'CRCONV05',
            'is_storable': True, 'uom_id': self.uom_kg.id})
        self._bom(legado, self.uom_kg, [(self.hilo, 1.0, self.uom_kg)])
        tela = self.env['product.product'].create({
            'name': 'JERSEY DOS RECETAS TEST', 'default_code': 'WJ060CONV165',
            'is_storable': True, 'uom_id': self.uom_m.id, 'sale_ok': True})
        self._bom(tela, self.uom_m, [(self.crudo, 0.1166, self.uom_kg)])
        bom_vieja = self._bom(tela, self.uom_m,
                              [(legado, 0.1166, self.uom_kg)])
        mo = self._mo(tela, 10.0, self.uom_m, datetime(2028, 3, 24, 12))
        mo.bom_id = bom_vieja
        self._mo(self.crudo, 100.0, self.uom_kg,
                 datetime(2028, 3, 5, 12), minutos=200.0)
        f = self._factores()
        conv, _v, fuente = self.Costo._conv_unit(tela, f)
        self.assertAlmostEqual(conv, 0.1166 * 2.0, places=6)
        self.assertEqual(fuente, 'op')

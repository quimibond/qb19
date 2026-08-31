# -*- coding: utf-8 -*-
from datetime import date, datetime

from dateutil.relativedelta import relativedelta

from odoo.exceptions import UserError, ValidationError
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestQbCosteo(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.env = cls.env(context=dict(cls.env.context, tracking_disable=True))
        uom_m = cls.env.ref('uom.product_uom_meter')
        uom_kg = cls.env.ref('uom.product_uom_kgm')

        cls.hilo = cls.env['product.product'].create({
            'name': 'HILO TEST', 'is_storable': True,
            'uom_id': uom_kg.id, 'purchase_ok': True,
            'standard_price': 50.0,
        })
        cls.tela = cls.env['product.product'].create({
            'name': 'TELA TEST', 'default_code': 'WJ045NT160',
            'is_storable': True, 'uom_id': uom_m.id, 'sale_ok': True,
        })
        cls.saldo = cls.env['product.product'].create({
            'name': 'SALDO TELA TEST', 'default_code': 'SALDO WJ045',
            'is_storable': True, 'uom_id': uom_kg.id, 'sale_ok': True,
            'standard_price': 30.0,
        })
        cls.importado = cls.env['product.product'].create({
            'name': 'TELA IMPORTADA TEST', 'default_code': 'WM4032OW152 I',
            'is_storable': True, 'uom_id': uom_m.id, 'sale_ok': True,
            'standard_price': 7.51,
        })
        # BOM: 1 m de tela consume 0.072 kg de hilo (45 g/m² × 1.60 m)
        cls.env['mrp.bom'].create({
            'product_tmpl_id': cls.tela.product_tmpl_id.id,
            'product_qty': 1.0,
            'product_uom_id': uom_m.id,
            'bom_line_ids': [(0, 0, {
                'product_id': cls.hilo.id,
                'product_qty': 0.072,
                'product_uom_id': uom_kg.id,
            })],
        })
        cls.Costo = cls.env['qb.costo.producto']
        cls.Peso = cls.env['qb.producto.peso']
        cls.Ruteo = cls.env['qb.producto.ruteo']

    def test_gramaje_from_ref(self):
        """El parser de gramaje: exactamente 3 dígitos = g/m²; 4 dígitos es
        código de resina y NO debe interpretarse como gramaje."""
        kg = self.Peso._gramaje_from_ref('WJ045NT160')
        self.assertAlmostEqual(kg, 0.045 * 1.60, places=4)
        self.assertEqual(self.Peso._gramaje_from_ref('WM4032OW152'), 0.0)

    def test_mp_recursiva_ultimo_costo(self):
        """MP de la tela = receta × costo del hilo (fallback avg sin compras)."""
        mp = self.Costo._mp_cost_unit(self.tela)
        self.assertAlmostEqual(mp, 0.072 * 50.0, places=4)

    def test_mp_receta_ambigua_no_usa_avco_de_fabricado(self):
        """Semiterminado FABRICADO con varias BOMs activas: el AVCO de Odoo
        trae las capas de conversión de las MOs (horas × tarifa), no solo
        materiales, y el modelo ya cobra la conversión vía fab_unit —
        usarlo como MP la cobraba DOS veces. El caso real: la cruda de
        WC090 (3 BOMs) con AVCO $107/kg cuando el hilo cuesta ~$40/kg →
        CONTITECH y todo el segmento industrial salían con margen neto
        negativo por costo fantasma. La regla: explotar TODAS las recetas
        y tomar la MÁS CARA — nunca el AVCO de un fabricado."""
        uom_m = self.env.ref('uom.product_uom_meter')
        uom_kg = self.env.ref('uom.product_uom_kgm')
        hilo_z = self.env['product.product'].create({
            'name': 'HILO PES Z TEST', 'is_storable': True,
            'uom_id': uom_kg.id, 'standard_price': 40.0,
        })
        hilo_set = self.env['product.product'].create({
            'name': 'HILO PES SET TEST', 'is_storable': True,
            'uom_id': uom_kg.id, 'standard_price': 30.0,
        })
        cruda = self.env['product.product'].create({
            'name': 'CRUDA CREP TEST', 'is_storable': True,
            'uom_id': uom_kg.id,
            'standard_price': 107.0,  # AVCO con conversión adentro
        })
        # DOS recetas (mezclas de hilo distintas) → receta ambigua
        for lineas in ([(hilo_z, 1.0)], [(hilo_z, 0.5), (hilo_set, 0.5)]):
            self.env['mrp.bom'].create({
                'product_tmpl_id': cruda.product_tmpl_id.id,
                'product_id': cruda.id,
                'product_qty': 1.0, 'product_uom_id': uom_kg.id,
                'bom_line_ids': [(0, 0, {
                    'product_id': comp.id, 'product_qty': qty,
                    'product_uom_id': uom_kg.id}) for comp, qty in lineas],
            })
        self.assertTrue(self.Costo._has_multiple_boms(cruda))
        # 1×40 vs 0.5×40+0.5×30=35 → la más cara (40), NUNCA el AVCO 107
        self.assertAlmostEqual(self.Costo._mp_cost_unit(cruda), 40.0,
                               places=4)
        # Un producto final que consume 0.167 kg de la cruda hereda el
        # costo de hilo, no el AVCO inflado
        tela2 = self.env['product.product'].create({
            'name': 'CREP TERMINADA TEST', 'default_code': 'WD080Q46JNT161',
            'is_storable': True, 'uom_id': uom_m.id, 'sale_ok': True,
        })
        self.env['mrp.bom'].create({
            'product_tmpl_id': tela2.product_tmpl_id.id,
            'product_qty': 1.0, 'product_uom_id': uom_m.id,
            'bom_line_ids': [(0, 0, {
                'product_id': cruda.id, 'product_qty': 0.167,
                'product_uom_id': uom_kg.id})],
        })
        self.assertAlmostEqual(
            self.Costo._mp_cost_unit(tela2), 0.167 * 40.0, places=3)
        # El camino batch (ctx precomputa multi_bom_ids) da lo mismo
        ctx = self.Costo._engine_ctx([tela2.id])
        self.assertIn(cruda.id, ctx['multi_bom_ids'])
        self.assertAlmostEqual(
            self.Costo._mp_cost_unit(tela2, ctx=ctx), 0.167 * 40.0, places=3)
        # Y el desglose explicado sigue a la receta más cara, no al AVCO
        rows = self.Costo.mp_breakdown(tela2)
        self.assertAlmostEqual(sum(r['total'] for r in rows), 0.167 * 40.0,
                               places=3)
        self.assertTrue(all('AVCO de Odoo' not in r['fuente'] for r in rows))
        # Receta ÚNICA sigue explotando normal (no se toca ese camino)
        self.assertFalse(self.Costo._has_multiple_boms(self.tela))
        self.assertAlmostEqual(
            self.Costo._mp_cost_unit(self.tela), 0.072 * 50.0, places=4)

    def test_mp_receta_ambigua_degenerada_avisa(self):
        """Recetas ambiguas DEGENERADAS (genéricos 'MUESTRA PILOTO' con
        recetas de relleno que explotan a casi nada frente al AVCO): el
        costo se queda el explotado — el AVCO de un fabricado no es MP —
        pero el motor AVISA en el log para que alguien arregle las BOMs.
        Ya no se tapa el hoyo con el AVCO (así entró el doble conteo)."""
        uom_kg = self.env.ref('uom.product_uom_kgm')
        barato = self.env['product.product'].create({
            'name': 'INSUMO BARATO', 'is_storable': True,
            'uom_id': uom_kg.id, 'standard_price': 1.0,
        })
        semi = self.env['product.product'].create({
            'name': 'MUESTRA PILOTO TEST', 'is_storable': True,
            'uom_id': uom_kg.id, 'standard_price': 88.0,
        })
        for _i in range(2):
            self.env['mrp.bom'].create({
                'product_tmpl_id': semi.product_tmpl_id.id,
                'product_id': semi.id,
                'product_qty': 1.0, 'product_uom_id': uom_kg.id,
                'bom_line_ids': [(0, 0, {
                    'product_id': barato.id, 'product_qty': 0.01,
                    'product_uom_id': uom_kg.id})],
            })
        with self.assertLogs(
                'odoo.addons.qb_capacidad_costeo.models.costeo',
                level='WARNING') as capturado:
            self.assertAlmostEqual(self.Costo._mp_cost_unit(semi), 0.01,
                                   places=4)
        self.assertTrue(any('degeneradas' in m for m in capturado.output))

    def test_peso_estimado_se_marca(self):
        """El peso adivinado del código (ref_gramaje) o del weight de Odoo se
        marca como estimado; un peso capturado a mano NO. Cierra #1/#2."""
        # self.tela = WJ045NT160, sin registro de peso → gramaje del código
        self.assertEqual(self.Peso.resolve_kg_source(self.tela), 'ref_gramaje')
        self.assertIn('ref_gramaje', self.Peso.PESO_SOURCES_ESTIMADAS)
        # capturar el peso real a mano → fuente 'manual', ya no estimado
        self.Peso.create({
            'product_id': self.tela.id, 'kg_per_unit': 0.30,
            'source': 'manual'})
        self.assertEqual(self.Peso.resolve_kg_source(self.tela), 'manual')
        self.assertNotIn('manual', self.Peso.PESO_SOURCES_ESTIMADAS)
        self.assertAlmostEqual(
            self.Peso.resolve_kg_per_unit(self.tela), 0.30, places=4)

    def test_gramaje_ancho_distinto_del_gramaje(self):
        """El ancho se busca sólo DESPUÉS del gramaje: 'WD080' sin ancho
        explícito usa el default 1.5 m, no toma sus propios '080'. Cierra #3."""
        # sin ancho → default 1.5 m (no 0.80)
        self.assertAlmostEqual(
            self.Peso._gramaje_from_ref('WD080'), 0.080 * 1.5, places=4)
        # con ancho explícito → lo respeta
        self.assertAlmostEqual(
            self.Peso._gramaje_from_ref('WD080Q46JNT160'), 0.080 * 1.60,
            places=4)

    def test_receta_ambigua_sin_avco_toma_la_mas_cara(self):
        """Semiterminado con varias BOMs y SIN AVCO: en vez de explotar una al
        azar (colapso), toma la receta MÁS CARA (conservador). Cierra #6."""
        uom_kg = self.env.ref('uom.product_uom_kgm')
        barato = self.env['product.product'].create({
            'name': 'INSUMO BARATO 2', 'is_storable': True,
            'uom_id': uom_kg.id, 'standard_price': 10.0})
        caro = self.env['product.product'].create({
            'name': 'INSUMO CARO', 'is_storable': True,
            'uom_id': uom_kg.id, 'standard_price': 100.0})
        semi = self.env['product.product'].create({
            'name': 'SEMI SIN AVCO', 'is_storable': True,
            'uom_id': uom_kg.id, 'standard_price': 0.0})  # sin AVCO
        for comp in (barato, caro):
            self.env['mrp.bom'].create({
                'product_tmpl_id': semi.product_tmpl_id.id,
                'product_id': semi.id, 'product_qty': 1.0,
                'product_uom_id': uom_kg.id,
                'bom_line_ids': [(0, 0, {
                    'product_id': comp.id, 'product_qty': 1.0,
                    'product_uom_id': uom_kg.id})]})
        self.assertTrue(self.Costo._has_multiple_boms(semi))
        # 1×10 vs 1×100 → toma la más cara (100), no una al azar
        self.assertAlmostEqual(self.Costo._mp_cost_unit(semi), 100.0, places=4)

    def test_receta_ambigua_prefiere_bom_de_ultima_op(self):
        """Receta ambigua CON historial de fabricación: el costo sigue a la
        BOM de la última OP terminada (cómo se fabrica hoy), no a la
        explosión más cara — los genéricos de prueba ('MUESTRA PILOTO')
        colgados de BOMs activas inflaban la MP (caso TJ085Q22JNT157:
        11.30/m por el camino piloto cuando su receta real, la de 53 de
        sus 55 OPs, da ~6.2). Sin OPs sigue el criterio conservador de la
        más cara (test de arriba)."""
        uom_kg = self.env.ref('uom.product_uom_kgm')
        real = self.env['product.product'].create({
            'name': 'HILO RECETA REAL', 'is_storable': True,
            'uom_id': uom_kg.id, 'standard_price': 10.0})
        piloto = self.env['product.product'].create({
            'name': 'COMODIN PILOTO CARO', 'is_storable': True,
            'uom_id': uom_kg.id, 'standard_price': 100.0})
        semi = self.env['product.product'].create({
            'name': 'TELA CON PILOTO TEST', 'is_storable': True,
            'uom_id': uom_kg.id, 'standard_price': 0.0})
        boms = {}
        for key, comp in (('real', real), ('piloto', piloto)):
            boms[key] = self.env['mrp.bom'].create({
                'product_tmpl_id': semi.product_tmpl_id.id,
                'product_id': semi.id, 'product_qty': 1.0,
                'product_uom_id': uom_kg.id,
                'bom_line_ids': [(0, 0, {
                    'product_id': comp.id, 'product_qty': 1.0,
                    'product_uom_id': uom_kg.id})]})
        mo = self.env['mrp.production'].create({
            'product_id': semi.id, 'product_qty': 5.0,
            'product_uom_id': uom_kg.id, 'bom_id': boms['real'].id})
        # 'done' directo por SQL: el flujo completo de una OP arrastra
        # movimientos de stock que este test no necesita.
        self.env.cr.execute(
            "UPDATE mrp_production SET state = 'done' WHERE id = %s",
            (mo.id,))
        self.env.invalidate_all()
        self.assertAlmostEqual(self.Costo._mp_cost_unit(semi), 10.0,
                               places=4)
        # El camino batch (mapa precomputado en el ctx) da lo mismo
        ctx = self.Costo._engine_ctx([semi.id])
        self.assertEqual(ctx['last_mo_bom'].get(semi.id), boms['real'].id)
        self.assertAlmostEqual(
            self.Costo._mp_cost_unit(semi, ctx=ctx), 10.0, places=4)
        # Archivada la BOM de la última OP, vuelve el criterio conservador
        boms['real'].active = False
        self.assertAlmostEqual(self.Costo._mp_cost_unit(semi), 100.0,
                               places=4)

    def test_avco_negativo_no_da_mp_negativa(self):
        """Un AVCO negativo (herida de valuación de inventario, caso
        PESFCHMO1.5X2.0 en -0.30/kg) no es un costo: la hoja se acota a 0
        y la MP de la receta que la consume nunca sale negativa."""
        uom_m = self.env.ref('uom.product_uom_meter')
        uom_kg = self.env.ref('uom.product_uom_kgm')
        fibra = self.env['product.product'].create({
            'name': 'FIBRA AVCO NEGATIVO', 'is_storable': True,
            'uom_id': uom_kg.id, 'standard_price': -0.30})
        velo = self.env['product.product'].create({
            'name': 'ENTRETELA VELO TEST', 'default_code': 'P19BL155',
            'is_storable': True, 'uom_id': uom_m.id, 'sale_ok': True})
        self.env['mrp.bom'].create({
            'product_tmpl_id': velo.product_tmpl_id.id,
            'product_qty': 1.0, 'product_uom_id': uom_m.id,
            'bom_line_ids': [(0, 0, {
                'product_id': fibra.id, 'product_qty': 0.017,
                'product_uom_id': uom_kg.id})]})
        self.assertEqual(self.Costo._mp_cost_unit(velo), 0.0)

    def test_importado_sin_costo_usa_compra_del_it_de_su_bom(self):
        """Un ' I' sin AVCO ni compra propia toma la última compra del
        gemelo ' IT' de su BOM de conversión — el código del IT puede NO
        compartir prefijo con el ' I' (KP2032T11GO152 I se produce del
        KP4032T11GO152 IT), así que buscar por ref no basta."""
        uom_m = self.env.ref('uom.product_uom_meter')
        it = self.env['product.product'].create({
            'name': 'ENTRETELA IT TEST', 'default_code': 'KP9032GO152 IT',
            'is_storable': True, 'uom_id': uom_m.id, 'purchase_ok': True})
        imp = self.env['product.product'].create({
            'name': 'ENTRETELA I TEST', 'default_code': 'KP1032GO152 I',
            'is_storable': True, 'uom_id': uom_m.id, 'sale_ok': True,
            'standard_price': 0.0})
        self.env['mrp.bom'].create({
            'product_tmpl_id': imp.product_tmpl_id.id,
            'product_qty': 1.0, 'product_uom_id': uom_m.id,
            'bom_line_ids': [(0, 0, {
                'product_id': it.id, 'product_qty': 1.0,
                'product_uom_id': uom_m.id})]})
        proveedor = self.env['res.partner'].create({'name': 'PROV IT TEST'})
        po = self.env['purchase.order'].create({
            'partner_id': proveedor.id,
            'order_line': [(0, 0, {
                'product_id': it.id, 'product_qty': 1000.0,
                'price_unit': 6.10})]})
        po.button_confirm()
        self.assertEqual(self.Costo._it_twin(imp), it)
        self.assertAlmostEqual(self.Costo._mp_cost_unit(imp), 6.10, places=4)

    def test_mp_historica_usa_precio_de_la_epoca(self):
        """«Si tomamos la última compra para todos los períodos no vamos a
        saber la realidad de a cuánto compré»: cada período costea la MP
        con la última compra CONOCIDA A SU CORTE — marzo con el precio de
        marzo, julio con el de julio — y el cotizador (sin período) sigue
        a reposición de hoy. Un producto comprado por primera vez DESPUÉS
        del corte usa esa primera compra (el precio más cercano a su
        época), nunca el de hoy ni el AVCO."""
        uom_m = self.env.ref('uom.product_uom_meter')
        uom_kg = self.env.ref('uom.product_uom_kgm')
        hilo = self.env['product.product'].create({
            'name': 'HILO EPOCA TEST', 'is_storable': True,
            'uom_id': uom_kg.id, 'purchase_ok': True,
            'standard_price': 0.0})
        tela = self.env['product.product'].create({
            'name': 'TELA EPOCA TEST', 'default_code': 'WJ060NT160',
            'is_storable': True, 'uom_id': uom_m.id, 'sale_ok': True})
        self.env['mrp.bom'].create({
            'product_tmpl_id': tela.product_tmpl_id.id,
            'product_qty': 1.0, 'product_uom_id': uom_m.id,
            'bom_line_ids': [(0, 0, {
                'product_id': hilo.id, 'product_qty': 0.1,
                'product_uom_id': uom_kg.id})]})
        prov = self.env['res.partner'].create({'name': 'PROV EPOCA'})
        for fecha, precio in ((datetime(2031, 1, 15), 50.0),
                              (datetime(2031, 6, 10), 80.0)):
            po = self.env['purchase.order'].create({
                'partner_id': prov.id,
                'order_line': [(0, 0, {
                    'product_id': hilo.id, 'product_qty': 100.0,
                    'price_unit': precio})]})
            po.button_confirm()
            # confirmar pisa date_order con «ahora»: regresarla a su época
            po.date_order = fecha
        Factores = self.env['qb.costo.factores']
        # Períodos ficticios lejanos para no chocar con los reales de la
        # base de test (period es único por compañía)
        marzo = Factores.create({'period': date(2031, 3, 1),
                                 'window_months': 12})
        julio = Factores.create({'period': date(2031, 7, 1),
                                 'window_months': 12})
        antes = Factores.create({'period': date(2030, 11, 1),
                                 'window_months': 12})
        ctx = self.Costo._engine_ctx([tela.id], marzo)
        self.assertAlmostEqual(
            self.Costo._mp_cost_unit(tela, ctx=ctx), 0.1 * 50.0, places=4)
        ctx = self.Costo._engine_ctx([tela.id], julio)
        self.assertAlmostEqual(
            self.Costo._mp_cost_unit(tela, ctx=ctx), 0.1 * 80.0, places=4)
        # Cotizador, sin período: reposición de HOY = la última compra
        self.assertAlmostEqual(
            self.Costo._mp_cost_unit(tela), 0.1 * 80.0, places=4)
        # Período ANTERIOR a la primera compra del hilo: usa la primera
        # (50), no el AVCO (0) ni el precio de hoy (80)
        ctx = self.Costo._engine_ctx([tela.id], antes)
        self.assertAlmostEqual(
            self.Costo._mp_cost_unit(tela, ctx=ctx), 0.1 * 50.0, places=4)
        # Y el camino suelto (sin ctx) con corte explícito da lo mismo
        self.assertAlmostEqual(
            self.Costo._last_purchase_cost(
                hilo, cutoff=date(2031, 4, 1)), 50.0, places=4)
        self.assertAlmostEqual(
            self.Costo._last_purchase_cost(
                hilo, cutoff=date(2030, 12, 1)), 50.0, places=4)

    def test_qty_neta_negativa_no_da_precio_negativo(self):
        """Devoluciones > ventas (qty neta ≤ 0) → precio 0, sin alerta falsa
        de 'bajo costo variable'. Cierra #10."""
        period = date(2026, 12, 1)
        factores = self.env['qb.costo.factores'].create({
            'period': period, 'window_months': 12,
            'factor_fab_kg': 30.0, 'factor_fab_m': 3.0,
            'energia_por_kg': 4.0, 'op_pct': 0.18})
        ctx = self.Costo._engine_ctx([self.tela.id])
        # qty neta negativa (devoluciones > ventas)
        sales = {self.tela.id: {'qty': -5.0, 'revenue': -100.0,
                                'divisas': ''}}
        vals, _ = self.Costo._compute_product_vals(
            self.tela, period, factores, sales, ctx, self.Ruteo, self.Peso)
        self.assertEqual(vals['precio_prom'], 0.0)
        self.assertNotEqual(vals['alerta'], 'bajo_variable')
        # El monto facturado SÍ se conserva (es un hecho contable), pero sin
        # precio unitario válido no hay costo ni margen que totalizar.
        self.assertEqual(vals['ventas_total'], -100.0)
        self.assertEqual(vals['costo_absorbido_total'], 0.0)
        self.assertEqual(vals['margen_bruto_total'], 0.0)

    def test_precio_sugerido_con_margen_meta(self):
        """El precio sugerido = costo_producción ÷ (1 − op − margen_meta),
        nunca por debajo del piso lleno ni del mercado. Revive target_margin."""
        Config = self.env['qb.costeo.factor.config']
        Config.set_param('target_margin', 0.30)
        factores = self.env['qb.costo.factores'].create({
            'period': date(2027, 1, 1), 'window_months': 12,
            'factor_fab_kg': 30.0, 'factor_fab_m': 3.0,
            'energia_por_kg': 4.0, 'op_pct': 0.18})
        q = self.Costo.quote_product(self.tela, factores)
        prod = q['variable'] + q['fab']
        esperado = prod / (1.0 - 0.18 - 0.30)  # op + margen meta al denominador
        self.assertAlmostEqual(q['precio_sugerido'], esperado, places=3)
        # a ese precio el margen NETO ≈ el meta (30%)
        neto = 100.0 * (q['precio_sugerido'] - q['variable'] - q['fab']
                        - 0.18 * q['precio_sugerido']) / q['precio_sugerido']
        self.assertAlmostEqual(neto, 30.0, places=1)
        self.assertGreaterEqual(q['precio_sugerido'], q['piso_lleno'])

    def test_maestro_pesos_nativo(self):
        """El maestro de pesos nativo (CSV, sin Supabase) llena/corrige por
        código de producto y NO pisa un peso ya autoritativo."""
        p = self.env['product.product'].create({
            'name': 'RESINA TEST', 'default_code': 'WM4032OW152',
            'is_storable': True, 'uom_id': self.tela.uom_id.id,
            'sale_ok': True})
        # código de resina (4032) → no da gramaje → sin peso resuelto
        self.assertEqual(self.Peso.resolve_kg_source(p), 'sin_peso')
        creados, _corr, _sp = self.Peso.load_weight_master()
        self.assertGreaterEqual(creados, 1)
        # tras cargar: peso medido real y fuente confiable
        self.assertAlmostEqual(
            self.Peso.resolve_kg_per_unit(p), 0.0654, places=4)
        self.assertEqual(self.Peso.resolve_kg_source(p), 'manual')
        self.assertNotIn('manual', self.Peso.PESO_SOURCES_ESTIMADAS)
        # idempotente: una edición manual del usuario se respeta
        rec = self.Peso.search([('product_id', '=', p.id)])
        rec.kg_per_unit = 0.99
        self.Peso.load_weight_master()
        self.assertAlmostEqual(rec.kg_per_unit, 0.99, places=2)

    def test_cotizacion_ciclo_de_vida(self):
        """La cotización es un documento nativo: estados borrador→presentada→
        ganada/perdida por botón, con actividades y chatter (mixins)."""
        self.env['qb.costo.factores'].create({
            'period': date(2027, 2, 1), 'window_months': 12,
            'factor_fab_kg': 30.0, 'factor_fab_m': 3.0,
            'energia_por_kg': 4.0, 'op_pct': 0.18})
        wiz = self.env['qb.cotizador.wizard'].create({
            'product_id': self.tela.id, 'volumen': 1000,
            'precio_objetivo': 100.0})
        cot = self.env['qb.cotizacion'].browse(
            wiz.action_cotizar()['res_id'])
        self.assertEqual(cot.state, 'draft')
        # mixins nativos presentes
        self.assertTrue(hasattr(cot, 'activity_ids'))
        self.assertTrue(hasattr(cot, 'message_ids'))
        cot.action_marcar_presentada()
        self.assertEqual(cot.state, 'done')
        cot.action_marcar_ganada()
        self.assertEqual(cot.state, 'won')
        cot.action_reabrir()
        self.assertEqual(cot.state, 'draft')

    def test_subproducto_mp_cero(self):
        """SALDO*: MP $0 — su materia ya está en la receta del principal."""
        bucket, _ = self.Ruteo.resolve(self.saldo)
        self.assertEqual(bucket, 'subproducto')
        self.assertEqual(self.Costo._mp_cost_unit(self.saldo), 0.0)

    def test_importado_landed_sin_fab(self):
        """Importados (' I'): MP = landed (avg) y NO cargan la fabricación
        de los procesos (tejido/teñido/rama) — solo su inspección (test
        aparte)."""
        bucket, _ = self.Ruteo.resolve(self.importado)
        self.assertEqual(bucket, 'importado')
        self.assertAlmostEqual(
            self.Costo._mp_cost_unit(self.importado), 7.51, places=2)
        factores = self.env['qb.costo.factores'].create({
            'period': date(2026, 1, 1), 'factor_fab_kg': 30.0,
            'factor_fab_m': 3.0, 'entretela_factor_m': 2.3,
        })
        # Sin factor de inspección calculado, sigue en 0 — nunca hereda el
        # factor fabril de los procesos.
        fab = self.Costo._fab_unit('importado', False, 0.1, 10.0, factores)
        self.assertEqual(fab, 0.0)

    def test_importado_carga_inspeccion_por_metro(self):
        """«Todo lo importado se inspecciona»: las OPs TL/CONV las trabaja
        el centro Inspección y Empaque, cuya nómina (501.06) entraba
        completa al pool que solo absorben los FABRICADOS — las telas
        pagaban la inspección de la reventa. El importado ahora carga
        fabricación = inspección por metro; subproducto y servicio siguen
        en $0, y esa parte se resta del pool fabril (no se cobra doble)."""
        factores = self.env['qb.costo.factores'].create({
            'period': date(2031, 9, 1), 'window_months': 12,
            'factor_fab_kg': 40.0, 'factor_fab_m': 2.0,
            'factor_inspeccion_m': 0.52})
        # vendido en metros: el factor tal cual
        self.assertAlmostEqual(
            self.Costo._fab_unit('importado', False, 0.09, 10.4, factores),
            0.52, places=4)
        # vendido en kg: por los metros que trae el kilo
        self.assertAlmostEqual(
            self.Costo._fab_unit('importado', True, 1.0, 10.4, factores),
            10.4 * 0.52, places=4)
        self.assertEqual(
            self.Costo._fab_unit('subproducto', False, 0.1, 10.0, factores),
            0.0)
        self.assertEqual(
            self.Costo._fab_unit('servicio', False, 0.1, 10.0, factores),
            0.0)
        # Los insumos del factor son datos vivos y quedan acotados aunque
        # la base esté vacía
        share = self.Costo._inspeccion_headcount_share()
        self.assertGreaterEqual(share, 0.0)
        self.assertLessEqual(share, 1.0)
        self.assertGreaterEqual(
            self.Costo._conv_import_m_avg(
                date(2026, 1, 1), date(2026, 8, 1), 7), 0.0)

    def test_recargo_aduana_sigue_a_la_compra_no_al_producto(self):
        """El mismo hilo se compra a veces importado y a veces a un
        comerciante NACIONAL cuyo precio ya trae el arancel adentro (caso
        HP65P35A22/1: FILAFIL MX a $65 ≈ IG TEXTILE US a $48.8 × 1.32).
        `import_ids` dice qué productos PUEDEN llevar recargo; la COMPRA
        usada decide si esta vez lo lleva — recargar la compra nacional lo
        contaba dos veces."""
        uom_kg = self.env.ref('uom.product_uom_kgm')
        mx = self.env.ref('base.mx')
        us = self.env.ref('base.us')
        self.env.company.partner_id.country_id = mx
        hilo = self.env['product.product'].create({
            'name': 'HILO MIXTO TEST', 'is_storable': True,
            'uom_id': uom_kg.id, 'purchase_ok': True,
            'standard_price': 0.0})
        prov_mx = self.env['res.partner'].create({
            'name': 'COMERCIANTE MX', 'country_id': mx.id})
        prov_us = self.env['res.partner'].create({
            'name': 'PROVEEDOR US', 'country_id': us.id})
        po_mx = self.env['purchase.order'].create({
            'partner_id': prov_mx.id,
            'order_line': [(0, 0, {
                'product_id': hilo.id, 'product_qty': 1000.0,
                'price_unit': 65.0})]})
        po_mx.button_confirm()
        # Última compra NACIONAL: el arancel ya viene en el precio → sin
        # recargo aunque el producto esté en import_ids
        self.assertAlmostEqual(
            self.Costo._costo_de_compra(hilo, None, 0.32, {hilo.id}),
            65.0, places=2)
        # Llega una compra IMPORTADA más nueva: esa SÍ lleva el recargo
        po_us = self.env['purchase.order'].create({
            'partner_id': prov_us.id,
            'order_line': [(0, 0, {
                'product_id': hilo.id, 'product_qty': 1000.0,
                'price_unit': 48.8})]})
        po_us.button_confirm()
        self.assertAlmostEqual(
            self.Costo._costo_de_compra(hilo, None, 0.32, {hilo.id}),
            48.8 * 1.32, places=2)
        # Proveedor SIN país capturado: no se inventa importación
        prov_us.country_id = False
        self.assertAlmostEqual(
            self.Costo._costo_de_compra(hilo, None, 0.32, {hilo.id}),
            48.8, places=2)

    def test_nomina_diseno_se_mueve_a_operacion(self):
        """La nómina de DISEÑO cobra por cuentas de fábrica (bucket mod →
        pool fabril) pero desarrollar producto es gasto del período. Las
        pólizas de nómina se postean por departamento y el departamento
        solo queda escrito en la REFERENCIA (el concepto dice «Sueldos y
        salarios» en todas): el motor las detecta por ahí y las mueve al
        pool de operación — las pagan todos los productos, no solo los
        fabricados."""
        Account = self.env['account.account']
        cuenta = Account.create({
            'code': '501.06.99T', 'name': 'SUELDOS TEST DISEÑO',
            'account_type': 'expense_direct_cost'})
        contra = Account.search(
            [('account_type', '=', 'liability_payable')], limit=1)
        self.env['qb.costeo.cuenta.class'].create({
            'name': 'test nómina diseño', 'account_id': cuenta.id,
            'bucket': 'mod'})
        journal = self.env['account.journal'].search(
            [('type', '=', 'general'),
             ('company_id', '=', self.env.company.id)], limit=1)
        move = self.env['account.move'].create({
            'journal_id': journal.id, 'date': date(2031, 10, 15),
            'ref': 'NOMINA 99, QNAL TOLUCA, DISEÑO, TEST',
            'line_ids': [
                (0, 0, {'account_id': cuenta.id, 'debit': 43000.0,
                        'name': 'Sueldos y salarios'}),
                (0, 0, {'account_id': contra.id, 'credit': 43000.0,
                        'name': 'Sueldos por pagar'})]})
        move.action_post()
        self.assertAlmostEqual(
            self.Costo._nomina_por_ref(
                'DISEÑO', date(2031, 10, 1), date(2031, 11, 1), 1),
            43000.0, places=2)
        # Una póliza fabril normal (sin DISEÑO en la referencia) NO se
        # mueve, y fuera de la ventana tampoco
        self.assertAlmostEqual(
            self.Costo._nomina_por_ref(
                'DISEÑO', date(2031, 11, 1), date(2031, 12, 1), 1),
            0.0, places=2)
        self.assertAlmostEqual(
            self.Costo._nomina_por_ref(
                'TINTORERIA', date(2031, 10, 1), date(2031, 11, 1), 1),
            0.0, places=2)

    def test_fab_hibrida_tela(self):
        """Tela en m: fab = kg/m × factor_peso + factor_largo.
        Tela en kg: fab = factor_peso + m/kg × factor_largo."""
        factores = self.env['qb.costo.factores'].create({
            'period': date(2026, 2, 1), 'factor_fab_kg': 30.0,
            'factor_fab_m': 3.0,
        })
        fab_m = self.Costo._fab_unit('tela', False, 0.072, 13.9, factores)
        self.assertAlmostEqual(fab_m, 0.072 * 30.0 + 3.0, places=4)
        fab_kg = self.Costo._fab_unit('tela', True, 1.0, 13.9, factores)
        self.assertAlmostEqual(fab_kg, 30.0 + 13.9 * 3.0, places=4)

    def test_engine_ctx_equivale_a_camino_directo(self):
        """El camino batch (ctx con pol_map/reglas/cachés prefetcheados)
        da exactamente el mismo MP que el camino directo del cotizador."""
        directo = self.Costo._mp_cost_unit(self.tela)
        ctx = self.Costo._engine_ctx([self.tela.id])
        batch = self.Costo._mp_cost_unit(self.tela, ctx=ctx)
        self.assertAlmostEqual(directo, batch, places=6)
        self.assertIn(self.tela.id, ctx['mp_cache'])

    def test_cotizador_calculadora_viva(self):
        """Los resultados del wizard se computan en vivo (sin botón). Sin
        margen meta: la evaluación cae en cascada objetivo → mercado →
        piso lleno, y los márgenes se calculan al precio evaluado."""
        self.env['qb.costo.factores'].create({
            'period': date(2026, 3, 1), 'window_months': 12,
            'factor_fab_kg': 30.0, 'factor_fab_m': 3.0,
            'energia_por_kg': 4.0, 'op_pct': 0.18,
        })
        wiz = self.env['qb.cotizador.wizard'].create({
            'product_id': self.tela.id, 'volumen': 1000,
        })
        # MP 3.6 + energía 4×0.072 = variable; fab por híbrida en metros
        self.assertAlmostEqual(wiz.mp_unit, 0.072 * 50.0, places=4)
        self.assertAlmostEqual(wiz.energia_unit, 4.0 * 0.072, places=4)
        self.assertAlmostEqual(
            wiz.costo_variable, wiz.mp_unit + wiz.energia_unit, places=4)
        self.assertAlmostEqual(wiz.fab_unit, 0.072 * 30.0 + 3.0, places=4)
        self.assertEqual(wiz.piso_ocioso, wiz.costo_variable)
        self.assertAlmostEqual(
            wiz.piso_lleno,
            (wiz.costo_variable + wiz.fab_unit) / (1 - 0.18), places=3)
        # Sin objetivo ni ventas: se evalúa el PISO LLENO → neto exacto 0
        self.assertEqual(wiz.precio_mercado, 0.0)
        self.assertIn('piso a planta llena', wiz.evaluado_info)
        self.assertAlmostEqual(wiz.margen_neto_pct, 0.0, places=3)
        self.assertAlmostEqual(wiz.margen_bruto_pct, 18.0, places=3)
        # Con objetivo capturado: los márgenes se evalúan a ESE precio
        wiz.precio_objetivo = 100.0
        self.assertIn('objetivo', wiz.evaluado_info)
        bruto = 100.0 * (100.0 - wiz.costo_variable - wiz.fab_unit) / 100.0
        self.assertAlmostEqual(wiz.margen_bruto_pct, bruto, places=3)
        self.assertAlmostEqual(wiz.margen_neto_pct, bruto - 18.0, places=3)
        # Guardar produce la cotización con los mismos números
        action = wiz.action_cotizar()
        cot = self.env['qb.cotizacion'].browse(action['res_id'])
        self.assertAlmostEqual(cot.costo_variable, wiz.costo_variable, places=4)
        self.assertAlmostEqual(cot.piso_lleno, wiz.piso_lleno, places=4)
        self.assertEqual(cot.evaluado_fuente, 'precio objetivo')
        self.assertAlmostEqual(cot.precio_evaluado, 100.0, places=2)

    def test_cotizador_desde_orden_aplicar_precio(self):
        """Lanzado desde una sale.order: prefillea cliente/línea/producto,
        el semáforo evalúa el precio vs pisos, y 'aplicar a la línea'
        escribe el precio en el pedido."""
        self.env['qb.costo.factores'].create({
            'period': date(2026, 4, 1), 'window_months': 12,
            'factor_fab_kg': 30.0, 'factor_fab_m': 3.0,
            'energia_por_kg': 4.0, 'op_pct': 0.18,
        })
        partner = self.env['res.partner'].create({'name': 'Cliente Test'})
        order = self.env['sale.order'].create({
            'partner_id': partner.id,
            'order_line': [(0, 0, {
                'product_id': self.tela.id,
                'product_uom_qty': 500,
                'price_unit': 1.0,  # precio absurdo: debajo del variable
            })],
        })
        wiz = self.env['qb.cotizador.wizard'].with_context(
            active_model='sale.order', active_id=order.id,
        ).create({})
        self.assertEqual(wiz.sale_order_id, order)
        self.assertEqual(wiz.product_id, self.tela)
        self.assertEqual(wiz.semaforo, 'rojo')  # $1 < costo variable
        # Precio arriba del piso lleno → verde, y se aplica a la línea
        wiz.precio_objetivo = 100.0
        self.assertEqual(wiz.semaforo, 'verde')
        wiz.action_aplicar_precio()
        self.assertEqual(order.order_line[0].price_unit, 100.0)
        cot = self.env['qb.cotizacion'].search(
            [('sale_order_id', '=', order.id)], limit=1)
        self.assertTrue(cot)
        self.assertEqual(cot.semaforo, 'verde')

    def test_matematicas_identidades(self):
        """Las fórmulas cumplen su álgebra exacta:
        - Al piso LLENO, el margen absorbido == 0 (cubre todo, gana nada).
        - Al piso OCIOSO, la contribución == 0.
        - El semáforo cambia exactamente en los pisos."""
        factores = self.env['qb.costo.factores'].create({
            'period': date(2026, 5, 1), 'window_months': 12,
            'factor_fab_kg': 30.0, 'factor_fab_m': 3.0,
            'energia_por_kg': 4.0, 'op_pct': 0.18,
        })
        q = self.Costo.quote_product(self.tela, factores)
        v, f, op = q['variable'], q['fab'], q['op_pct']

        # margen absorbido en el piso lleno = 0
        p2 = q['piso_lleno']
        self.assertAlmostEqual(p2 - v - f - op * p2, 0.0, places=6)
        # contribución en el piso ocioso = 0
        self.assertAlmostEqual(q['piso_ocioso'] - v, 0.0, places=6)
        # semáforo exacto en las fronteras
        eps = 0.001
        self.assertEqual(self.Costo.semaforo_for(v - eps, v, p2), 'rojo')
        self.assertEqual(self.Costo.semaforo_for(v + eps, v, p2), 'ambar')
        self.assertEqual(self.Costo.semaforo_for(p2 + eps, v, p2), 'verde')
        # jerarquía de pisos y referencia de mercado presente (0 sin ventas)
        self.assertLess(q['piso_ocioso'], q['piso_lleno'])
        self.assertEqual(q['precio_mercado'], 0.0)

    def test_cotizador_orden_multilinea(self):
        """Orden con varios productos: una fila por línea con su semáforo,
        pre-marcado lo rojo, y aplicar escribe los precios en lote."""
        self.env['qb.costo.factores'].create({
            'period': date(2026, 6, 1), 'window_months': 12,
            'factor_fab_kg': 30.0, 'factor_fab_m': 3.0,
            'energia_por_kg': 4.0, 'op_pct': 0.18,
        })
        partner = self.env['res.partner'].create({'name': 'Cliente Multi'})
        order = self.env['sale.order'].create({
            'partner_id': partner.id,
            'order_line': [
                (0, 0, {'product_id': self.tela.id,
                        'product_uom_qty': 100, 'price_unit': 1.0}),   # rojo
                (0, 0, {'product_id': self.importado.id,
                        'product_uom_qty': 50, 'price_unit': 100.0}),  # verde
            ],
        })
        wiz = self.env['qb.cotizador.orden.wizard'].with_context(
            active_model='sale.order', active_id=order.id).create({})
        self.assertEqual(len(wiz.line_ids), 2)
        l_tela = wiz.line_ids.filtered(lambda l: l.product_id == self.tela)
        l_imp = wiz.line_ids.filtered(lambda l: l.product_id == self.importado)
        self.assertEqual(l_tela.semaforo, 'rojo')
        self.assertTrue(l_tela.aplicar, 'lo rojo se pre-marca')
        self.assertEqual(l_imp.semaforo, 'verde')
        self.assertFalse(l_imp.aplicar)
        # sin ventas 12m el default de corrección es el PISO LLENO
        self.assertEqual(l_tela.precio_mercado, 0.0)
        self.assertAlmostEqual(
            l_tela.nuevo_precio, l_tela.piso_lleno, places=2)
        # aplicar en lote: solo la marcada cambia
        wiz.action_aplicar_seleccionados()
        self.assertAlmostEqual(
            order.order_line[0].price_unit, l_tela.piso_lleno, places=2)
        self.assertEqual(order.order_line[1].price_unit, 100.0)

    def test_moneda_extranjera_semaforo(self):
        """Pedido en divisa: el precio se compara CONVERTIDO con el TC de
        Odoo — un precio de exportación razonable en USD/EUR no debe salir
        'destruye valor' por compararlo crudo contra pisos MXN."""
        self.env['qb.costo.factores'].create({
            'period': date(2026, 7, 1), 'window_months': 12,
            'factor_fab_kg': 30.0, 'factor_fab_m': 3.0,
            'energia_por_kg': 4.0, 'op_pct': 0.18,
        })
        eur = self.env.ref('base.EUR')
        eur.active = True
        self.env['res.currency.rate'].create({
            'currency_id': eur.id,
            'rate': 0.05,  # 1 moneda cía = 0.05 EUR → 1 EUR = 20 cía
            'name': date.today().replace(day=1),
        })
        Costo = self.env['qb.costo.producto']
        self.assertAlmostEqual(Costo.to_mxn_rate(eur), 20.0, places=2)
        self.assertEqual(
            Costo.to_mxn_rate(self.env.company.currency_id), 1.0)

        pricelist = self.env['product.pricelist'].create({
            'name': 'Export EUR', 'currency_id': eur.id})
        partner = self.env['res.partner'].create({'name': 'Cliente Export'})
        order = self.env['sale.order'].create({
            'partner_id': partner.id,
            'pricelist_id': pricelist.id,
            'order_line': [(0, 0, {
                'product_id': self.tela.id,
                'product_uom_qty': 100,
                'price_unit': 3.0,  # 3 EUR = 60 MXN — precio razonable
            })],
        })
        wiz = self.env['qb.cotizador.orden.wizard'].with_context(
            active_model='sale.order', active_id=order.id).create({})
        line = wiz.line_ids[0]
        self.assertTrue(wiz.is_foreign)
        self.assertAlmostEqual(line.precio_actual_mxn, 60.0, places=2)
        # 60 MXN vs variable ~7.9: NO es rojo (antes salía rojo falso)
        self.assertNotEqual(line.semaforo, 'rojo')
        # Sin ventas 12m el default es el PISO LLENO convertido a la divisa
        self.assertEqual(line.precio_mercado, 0.0)
        self.assertAlmostEqual(
            line.nuevo_precio, line.piso_lleno / 20.0, places=2)
        # Aplicar escribe en EUR (moneda del pedido), no en MXN
        line.aplicar = True
        wiz.action_aplicar_seleccionados()
        self.assertAlmostEqual(
            order.order_line[0].price_unit, line.piso_lleno / 20.0,
            places=2)

        # Wizard individual: el precio objetivo se captura EN EUR y el
        # modelo lo convierte; los espejos en divisa cuadran con el TC
        wiz_ind = self.env['qb.cotizador.wizard'].with_context(
            active_model='sale.order', active_id=order.id).create({})
        self.assertEqual(wiz_ind.currency_id, eur)
        self.assertAlmostEqual(wiz_ind.fx_rate, 20.0, places=2)
        wiz_ind.precio_objetivo = 3.0  # EUR (= 60 MXN)
        self.assertNotEqual(wiz_ind.semaforo, 'rojo')
        # El espejo en MXN del precio objetivo hace explícita la conversión
        self.assertAlmostEqual(wiz_ind.precio_objetivo_mxn, 60.0, places=2)
        self.assertAlmostEqual(
            wiz_ind.piso_lleno_divisa, wiz_ind.piso_lleno / 20.0, places=3)
        self.assertAlmostEqual(
            wiz_ind.piso_ocioso_divisa, wiz_ind.piso_ocioso / 20.0, places=3)
        # Con precio 0.5 EUR (= 10 MXN < variable) sí es rojo
        wiz_ind.precio_objetivo = 0.5
        self.assertEqual(wiz_ind.semaforo, 'rojo')
        # Al guardar: la moneda queda en la cotización y el precio para el
        # PDF del cliente sale en SU divisa (0.5 EUR), aunque el interno
        # guarde MXN (10.0)
        action = wiz_ind.action_cotizar()
        cot_eur = self.env['qb.cotizacion'].browse(action['res_id'])
        self.assertEqual(cot_eur.currency_id, eur)
        self.assertTrue(cot_eur.es_divisa)
        self.assertAlmostEqual(cot_eur.precio_objetivo, 10.0, places=2)
        self.assertAlmostEqual(cot_eur.precio_cliente_mxn, 10.0, places=2)
        self.assertAlmostEqual(cot_eur.precio_cliente_divisa, 0.5, places=3)

        # Guardián de moneda: 1.68 "USD" tecleado con moneda MXN → alerta
        wiz_mxn = self.env['qb.cotizador.wizard'].create({
            'product_id': self.tela.id, 'precio_objetivo': 1.68,
        })
        self.assertTrue(wiz_mxn.moneda_alerta)
        self.assertIn('dólares', wiz_mxn.moneda_alerta)
        # Mismo precio con la moneda correcta (EUR) → sin alerta
        wiz_mxn.currency_id = eur
        self.assertFalse(wiz_mxn.moneda_alerta)
        # Inverso: 60 "MXN" tecleados con moneda EUR (60×20=1200 MXN ≫ piso)
        wiz_mxn.precio_objetivo = 60.0
        self.assertTrue(wiz_mxn.moneda_alerta)
        self.assertIn('pesos', wiz_mxn.moneda_alerta)

        # La moneda sigue al CLIENTE sin pedido: partner con pricelist EUR
        partner_eur = self.env['res.partner'].create({
            'name': 'Cliente Export EUR',
            'property_product_pricelist': pricelist.id,
        })
        wiz_std = self.env['qb.cotizador.wizard'].new({
            'product_id': self.tela.id})
        wiz_std.partner_id = partner_eur
        wiz_std._onchange_partner()
        self.assertEqual(wiz_std.currency_id, eur,
                         'la moneda debe derivarse de la lista de precios '
                         'del cliente aunque no venga de un pedido')

    def test_desglose_explicado_consistente(self):
        """El desglose (mp_breakdown) suma EXACTO lo mismo que el motor
        (_mp_cost_unit) — si divergen, la explicación miente."""
        factores = self.env['qb.costo.factores'].create({
            'period': date(2026, 8, 1), 'window_months': 12,
            'fab_pool_month': 5000000, 'energia_pool_month': 600000,
            'op_pool_month': 3000000, 'ventas_pool_month': 17000000,
            'kg_denom_month': 90000, 'm_denom_month': 900000,
            'fab_weight_share': 0.67,
            'factor_fab_kg': 30.0, 'factor_fab_m': 3.0,
            'energia_por_kg': 4.0, 'op_pct': 0.18,
        })
        rows = self.Costo.mp_breakdown(self.tela)
        self.assertTrue(rows)
        suma = sum(r['total'] for r in rows)
        self.assertAlmostEqual(
            suma, self.Costo._mp_cost_unit(self.tela), places=6)
        self.assertTrue(all(r['fuente'] for r in rows),
                        'cada hoja debe decir de dónde viene su costo')
        # Subproducto explicado en $0
        rows_saldo = self.Costo.mp_breakdown(self.saldo)
        self.assertEqual(rows_saldo[0]['total'], 0.0)
        self.assertIn('Subproducto', rows_saldo[0]['fuente'])
        # El HTML trae las 4 capas y el resumen
        html = self.Costo.explain_quote_html(self.tela, factores)
        for seccion in ('Materia prima', 'Energía', 'Fabricación',
                        'Operación', 'Costo completo'):
            self.assertIn(seccion, html)

    def test_ficha_parser_nomenclatura(self):
        """El parser lee la nomenclatura: WR135Q46JNT165 → WR, 135 g/m²,
        Q46, terminado, NT, 1.65 m. Los códigos de resina (4 dígitos) no
        se confunden con gramaje."""
        Ficha = self.env['qb.producto.ficha']
        v = Ficha.parse_ref('WR135Q46JNT165')
        self.assertEqual(v['familia'], 'WR')
        self.assertEqual(v['gramaje_g_m2'], 135.0)
        self.assertEqual(v['calidad'], 'Q46')
        self.assertEqual(v['estado'], 'terminado')
        self.assertEqual(v['color'], 'NT')
        self.assertEqual(v['ancho_m'], 1.65)
        self.assertFalse(v['parse_warning'])
        v2 = Ficha.parse_ref('WM4032OW152 I')
        self.assertEqual(v2['resina_code'], '4032')
        self.assertNotIn('gramaje_g_m2', v2)
        self.assertEqual(v2['color'], 'OW')
        self.assertEqual(v2['ancho_m'], 1.52)
        self.assertTrue(v2['es_importado'])
        v3 = Ficha.parse_ref('SALDO WJ045')
        self.assertEqual(v3['familia'], 'SUBPRODUCTO')
        # Generación masiva: crea fichas y respeta las manuales
        Ficha.action_generar_fichas()
        ficha = Ficha.search([('product_id', '=', self.tela.id)], limit=1)
        self.assertTrue(ficha)
        self.assertEqual(ficha.gramaje_g_m2, 45.0)  # WJ045NT160
        self.assertEqual(ficha.ancho_m, 1.60)
        ficha.write({'gramaje_g_m2': 47.0, 'source': 'manual'})
        Ficha.action_generar_fichas()
        self.assertEqual(ficha.gramaje_g_m2, 47.0, 'manual no se pisa')

    def test_related_presentations_kg_metros_importado(self):
        """El prefijo 'I' empareja la presentación en kilos con la de
        metros (WJ045NT160 ↔ IWJ045NT160) y el sufijo ' I' con el gemelo
        nacional — en AMBAS direcciones y solo si el ref hermano existe."""
        uom_kg = self.env.ref('uom.product_uom_kgm')
        kg_twin = self.env['product.product'].create({
            'name': 'TELA TEST EN KILOS', 'default_code': 'IWJ045NT160',
            'is_storable': True, 'uom_id': uom_kg.id, 'sale_ok': True,
        })
        rels = {p.id: label
                for p, label in self.Costo.related_presentations(self.tela)}
        self.assertIn(kg_twin.id, rels,
                      'la tela en metros debe encontrar su presentación en kg')
        self.assertIn('KILOS', rels[kg_twin.id])
        rels_inv = self.Costo.related_presentations(kg_twin)
        self.assertIn(self.tela.id, [p.id for p, _l in rels_inv],
                      'la presentación en kg debe encontrar la de metros')
        # Importado ' I' ↔ gemelo nacional
        nacional = self.env['product.product'].create({
            'name': 'TELA NACIONAL TEST', 'default_code': 'WM4032OW152',
            'is_storable': True, 'uom_id': self.tela.uom_id.id,
            'sale_ok': True,
        })
        rels_imp = self.Costo.related_presentations(self.importado)
        self.assertIn(nacional.id, [p.id for p, _l in rels_imp])
        labels = [l for _p, l in rels_imp]
        self.assertTrue(any('nacional' in l for l in labels))
        # Sin referencia → sin variantes (no truena)
        sin_ref = self.env['product.product'].create({
            'name': 'SIN REF', 'is_storable': True, 'sale_ok': True})
        self.assertEqual(self.Costo.related_presentations(sin_ref), [])

    def test_comparativa_y_glosario(self):
        """La comparativa lista las otras presentaciones con su margen y el
        glosario aparece en wizard, cotización guardada y con los términos
        clave definidos. Sin ventas: lo dice claro en vez de inventar."""
        uom_kg = self.env.ref('uom.product_uom_kgm')
        kg_twin = self.env['product.product'].create({
            'name': 'TELA TEST EN KILOS', 'default_code': 'IWJ045NT160',
            'is_storable': True, 'uom_id': uom_kg.id, 'sale_ok': True,
        })
        factores = self.env['qb.costo.factores'].create({
            'period': date(2026, 9, 1), 'window_months': 12,
            'factor_fab_kg': 30.0, 'factor_fab_m': 3.0,
            'energia_por_kg': 4.0, 'op_pct': 0.18,
        })
        html = self.Costo.comparativa_html(self.tela, factores)
        self.assertIn('Sin ventas', html, 'sin facturas debe decirlo claro')
        self.assertIn('IWJ045NT160', html,
                      'la presentación en kg debe listarse')
        self.assertIn('KILOS', html)
        self.assertIn('sin ventas 12m', html)
        self.assertIn('piso lleno', html,
                      'sin ventas la referencia mostrada es el piso lleno')
        # En el wizard se computa sola y se guarda como foto en la cotización
        wiz = self.env['qb.cotizador.wizard'].create({
            'product_id': self.tela.id, 'volumen': 1000,
        })
        self.assertTrue(wiz.comparativa_html)
        self.assertIn('IWJ045NT160', wiz.comparativa_html)
        # MXN: el precio objetivo y su espejo coinciden (TC = 1)
        wiz.precio_objetivo = 100.0
        self.assertAlmostEqual(wiz.precio_objetivo_mxn, 100.0, places=2)
        action = wiz.action_cotizar()
        cot = self.env['qb.cotizacion'].browse(action['res_id'])
        self.assertIn('IWJ045NT160', cot.comparativa_html)
        # Glosario: mismos términos en wizard y cotización — sin margen meta
        for termino in ('Precio objetivo', 'Precio de mercado',
                        'Precio evaluado', 'Tipo de cambio', 'Margen bruto',
                        'Margen neto', 'Ociosidad',
                        'Piso con capacidad ociosa', 'Piso a planta llena',
                        'Capacidad'):
            self.assertIn(termino, wiz.glosario_html)
            self.assertIn(termino, cot.glosario_html)
        self.assertNotIn('margen meta', wiz.glosario_html)
        self.assertTrue(kg_twin.exists())

    def test_precio_cliente_mxn(self):
        """El PDF comercial presenta UN solo precio: el objetivo si se
        capturó; sin objetivo ni ventas cae al piso a planta llena. En MXN
        la parte divisa queda en 0."""
        self.env['qb.costo.factores'].create({
            'period': date(2026, 10, 1), 'window_months': 12,
            'factor_fab_kg': 30.0, 'factor_fab_m': 3.0,
            'energia_por_kg': 4.0, 'op_pct': 0.18,
        })
        wiz = self.env['qb.cotizador.wizard'].create({
            'product_id': self.tela.id, 'volumen': 1000,
            'precio_objetivo': 100.0,
        })
        cot = self.env['qb.cotizacion'].browse(
            wiz.action_cotizar()['res_id'])
        self.assertEqual(cot.currency_id, self.env.company.currency_id)
        self.assertFalse(cot.es_divisa)
        self.assertAlmostEqual(cot.precio_cliente_mxn, 100.0, places=2)
        self.assertEqual(cot.precio_cliente_divisa, 0.0)
        # Sin precio objetivo ni ventas → cae al piso a planta llena
        wiz2 = self.env['qb.cotizador.wizard'].create({
            'product_id': self.tela.id, 'volumen': 1000,
        })
        cot2 = self.env['qb.cotizacion'].browse(
            wiz2.action_cotizar()['res_id'])
        self.assertEqual(cot2.evaluado_fuente, 'piso a planta llena')
        self.assertAlmostEqual(
            cot2.precio_cliente_mxn, cot2.piso_lleno, places=4)

    def test_escalera_volumen(self):
        """La escalera estandariza el descuento por volumen con sus dos
        reglas duras: nunca debajo del piso lleno y contribución total
        $/mes que nunca baja. Se guarda como tramos en la cotización y el
        PDF comercial solo ofrece los que caben en capacidad."""
        self.env['qb.costo.factores'].create({
            'period': date(2026, 11, 1), 'window_months': 12,
            'factor_fab_kg': 30.0, 'factor_fab_m': 3.0,
            'energia_por_kg': 4.0, 'op_pct': 0.18,
        })
        wiz = self.env['qb.cotizador.wizard'].create({
            'product_id': self.tela.id, 'volumen': 1000,
            'precio_objetivo': 100.0,
        })
        tramos = wiz._escalera_tramos(wiz._calc())
        self.assertEqual([t['volumen'] for t in tramos],
                         [500.0, 1000.0, 2000.0, 4000.0])
        base = next(t for t in tramos if t['es_base'])
        self.assertAlmostEqual(base['precio_mxn'], 100.0, places=2)
        precios = [t['precio_mxn'] for t in tramos]
        # ½× cobra premium; de ahí en adelante el precio no sube
        self.assertGreater(precios[0], precios[1])
        self.assertGreaterEqual(precios[1], precios[2])
        self.assertGreaterEqual(precios[2], precios[3])
        # 3% por duplicación (default del seed no existe en test DB → 0.03)
        self.assertAlmostEqual(precios[2], 97.0, places=2)
        self.assertAlmostEqual(precios[3], 94.0, places=2)
        piso = wiz.piso_lleno
        contribs = [t['contrib_total_mes'] for t in tramos]
        for t in tramos:
            self.assertGreaterEqual(t['precio_mxn'], round(piso, 2) - 0.01,
                                    'ningún tramo debajo del piso lleno')
        self.assertEqual(contribs, sorted(contribs),
                         'la contribución total nunca baja')
        self.assertTrue(wiz.escalera_html)
        self.assertIn('duplicación', wiz.escalera_html)
        self.assertIn('cotizado', wiz.escalera_html)
        # Guardar congela los tramos en la cotización
        cot = self.env['qb.cotizacion'].browse(
            wiz.action_cotizar()['res_id'])
        self.assertEqual(len(cot.tramo_ids), 4)
        self.assertEqual(cot.tramo_ids.filtered('es_base').volumen, 1000.0)
        # Piso duro: objetivo AL piso lleno → los tramos ≥1× no descuentan
        wiz2 = self.env['qb.cotizador.wizard'].create({
            'product_id': self.tela.id, 'volumen': 1000,
        })  # sin objetivo ni ventas: base = piso lleno
        tramos2 = wiz2._escalera_tramos(wiz2._calc())
        for t in tramos2:
            if t['multiplo'] >= 1:
                self.assertAlmostEqual(
                    t['precio_mxn'], round(wiz2.piso_lleno, 2), places=2)

    def test_recompute_invariante_costo_total(self):
        """Las identidades de costos y márgenes, exactas por producto:
        absorbido = MP + energía + fab + op; producción = variable + fab;
        bruto = precio − producción; neto (absorbido) = bruto − op."""
        period = date.today().replace(day=1)
        self.Costo.action_recompute_period(period)
        recs = self.Costo.search([('period', '=', period)])
        self.assertTrue(recs, 'El recompute debe generar registros')
        for rec in recs:
            self.assertAlmostEqual(
                rec.costo_absorbido,
                rec.mp_unit + rec.energia_unit + rec.fab_unit + rec.op_unit,
                places=3,
                msg='Invariante roto en %s' % rec.product_id.display_name)
            self.assertAlmostEqual(
                rec.costo_produccion, rec.costo_variable + rec.fab_unit,
                places=4)
            if rec.product_bucket in ('importado', 'subproducto'):
                self.assertEqual(rec.fab_unit, 0.0)
            self.assertTrue(rec.alerta, 'alerta debe poblarse siempre')
            self.assertAlmostEqual(
                rec.contrib_total,
                (rec.margen_contribucion * rec.qty_vendida
                 if rec.precio_prom else 0.0), places=2)
            if rec.precio_prom:
                # bruto = precio − producción; neto = bruto − operación
                self.assertAlmostEqual(
                    rec.margen_bruto, rec.precio_prom - rec.costo_produccion,
                    places=3)
                self.assertAlmostEqual(
                    rec.margen_absorbido, rec.margen_bruto - rec.op_unit,
                    places=3)
                # jerarquía: contribución ≥ bruto ≥ neto (op ≥ 0)
                self.assertGreaterEqual(
                    rec.margen_contribucion + 0.001, rec.margen_bruto)
                self.assertGreaterEqual(
                    rec.margen_bruto + 0.001, rec.margen_absorbido)
                self.assertAlmostEqual(
                    rec.margen_bruto_total,
                    rec.margen_bruto * rec.qty_vendida, places=2)
                self.assertAlmostEqual(
                    rec.margen_neto_total,
                    rec.margen_absorbido * rec.qty_vendida, places=2)
                # Los totales del período cuadran con lo facturado: el
                # revenue de la contabilidad menos el costo de lo vendido
                # ES el margen total (nada se calcula dos veces por caminos
                # distintos).
                self.assertAlmostEqual(
                    rec.ventas_total, rec.precio_prom * rec.qty_vendida,
                    places=2)
                self.assertAlmostEqual(
                    rec.costo_absorbido_total,
                    rec.mp_total + rec.energia_total + rec.fab_total
                    + rec.op_total, places=2)
                self.assertAlmostEqual(
                    rec.margen_bruto_total,
                    rec.ventas_total - rec.costo_produccion_total, places=2)
                self.assertAlmostEqual(
                    rec.margen_neto_total,
                    rec.ventas_total - rec.costo_absorbido_total, places=2)
                self.assertAlmostEqual(
                    rec.contrib_total,
                    rec.ventas_total - rec.costo_variable_total, places=2)
            else:
                self.assertEqual(rec.margen_bruto, 0.0)
                self.assertEqual(rec.margen_bruto_total, 0.0)
                self.assertEqual(rec.margen_neto_total, 0.0)

    def test_margen_objetivo_fija_precio(self):
        """Cotizar por MARGEN objetivo: el precio sugerido = costo_producción
        ÷ (1 − op − margen), y se usa como precio evaluado (sin precio
        objetivo capturado)."""
        self.env['qb.costo.factores'].create({
            'period': date(2026, 9, 1), 'window_months': 12,
            'factor_fab_kg': 30.0, 'factor_fab_m': 3.0,
            'energia_por_kg': 4.0, 'op_pct': 0.18,
        })
        wiz = self.env['qb.cotizador.wizard'].create({
            'product_id': self.tela.id, 'volumen': 1000,
            'margen_objetivo': 25.0,
        })
        calc = wiz._calc()
        esperado = (calc['variable'] + calc['fab']) / (1 - calc['op_pct'] - 0.25)
        esperado = max(esperado, calc['piso_lleno'], calc['precio_mercado'])
        self.assertAlmostEqual(calc['precio_sugerido'], esperado, places=3)
        self.assertAlmostEqual(calc['precio_ref'], esperado, places=3)
        self.assertIn('margen objetivo', calc['evaluado_fuente'])
        # El margen neto realizado a ese precio ≈ 25% (no clampeado)
        neto = 100.0 * (esperado - calc['variable'] - calc['fab']
                        - calc['op_pct'] * esperado) / esperado
        self.assertAlmostEqual(neto, 25.0, places=1)
        # Precio objetivo captura manda sobre el margen
        wiz.precio_objetivo = 999.0
        calc2 = wiz._calc()
        self.assertAlmostEqual(calc2['precio_ref'], 999.0, places=2)

    def test_tejido_produccion_por_orden_no_workorder(self):
        """El centro TEJIDO mide producción por patrón de ORDEN (TL/OP-TE),
        no por workorder (mal registrado): así el promedio no lo arrastra un
        mes con workorders sin cerrar. La migración/seed le pone el patrón."""
        tejido = self.env.ref('qb_capacidad_costeo.centro_tejido')
        tint = self.env.ref('qb_capacidad_costeo.centro_tintoreria')
        acab = self.env.ref('qb_capacidad_costeo.centro_acabado')
        ent = self.env.ref('qb_capacidad_costeo.centro_entretelas')
        self.assertEqual(tejido.mo_name_pattern, 'TL/OP-TE%',
                         'TEJIDO debe medirse por orden, no por workorder')
        # Tintorería tenía producción CERO (sin patrón); ahora TL/OP-TIN.
        self.assertEqual(tint.mo_name_pattern, 'TL/OP-TIN%')
        # V10 es entretelas/resina, NO acabado: acabado solo TL/OP-ACA.
        self.assertNotIn('V10', acab.mo_name_pattern or '')
        # Entretelas suma la resina V10 (patrón múltiple por coma).
        self.assertIn('TL/OP-V10%', ent.mo_name_pattern)
        self.assertIn('TL/OP-CAR%', ent.mo_name_pattern)
        # Con patrón, _production_month_avg NO usa el conteo por workorder
        # para este centro (que tenga o no workcenters ligados). El patrón
        # múltiple (coma) tampoco truena.
        Costo = self.env['qb.costo.producto']
        from datetime import date as _d
        for c in (tejido, tint, acab, ent):
            avg = Costo._production_month_avg(c, _d(2026, 1, 1), _d(2026, 8, 1))
            self.assertGreaterEqual(avg, 0.0)  # no truena; sin datos MO → 0

    def test_ventas_solo_cuentas_income(self):
        """Una factura contra una cuenta que NO es de ventas de producto
        (utilidad en venta de activo fijo = income_other, como la rama
        Icomatex de $11.3M; o anticipos = liability) NO entra al costeo:
        no infla qty, precio ni contribución del mes."""
        journal = self.env['account.journal'].search(
            [('type', '=', 'sale')], limit=1)
        if not journal:
            self.skipTest('sin plan contable en la DB de test')
        period = date.today().replace(day=1)
        cuenta_activo = self.env['account.account'].create({
            'code': '704.23.T99', 'name': 'UTILIDAD VENTA ACTIVO TEST',
            'account_type': 'income_other'})
        maquina = self.env['product.product'].create({
            'name': 'RAMA TEST', 'is_storable': False, 'sale_ok': True})
        partner = self.env['res.partner'].create({'name': 'Leasing Test'})
        # Venta de la máquina contra la cuenta de activo fijo
        move = self.env['account.move'].create({
            'move_type': 'out_invoice', 'partner_id': partner.id,
            'invoice_date': period,
            'invoice_line_ids': [(0, 0, {
                'product_id': maquina.id, 'quantity': 1,
                'price_unit': 1000000.0,
                'account_id': cuenta_activo.id})]})
        move.action_post()
        # Venta normal de producto (cuenta income por defecto del diario)
        move2 = self.env['account.move'].create({
            'move_type': 'out_invoice', 'partner_id': partner.id,
            'invoice_date': period,
            'invoice_line_ids': [(0, 0, {
                'product_id': self.tela.id, 'quantity': 5,
                'price_unit': 10.0})]})
        move2.action_post()
        sales = self.Costo._sales_by_product(period)
        self.assertNotIn(maquina.id, sales,
                         'venta de activo fijo no debe entrar al costeo')
        self.assertIn(self.tela.id, sales,
                      'la venta normal sí debe entrar')
        # Tampoco al precio de mercado / ventas por cliente del cotizador
        self.assertEqual(self.Costo.sales_by_customer(maquina), [])
        self.assertEqual(self.Costo.market_price(maquina), 0.0)
        # Una fila VIEJA de la máquina (calculada antes del filtro) no debe
        # sobrevivir el recálculo: el upsert solo tocaba productos en alcance
        # y la huérfana se quedaba con sus millones (rama Icomatex 2026-03).
        self.Costo.create({
            'period': period, 'product_id': maquina.id,
            'qty_vendida': 1, 'precio_prom': 1000000.0,
            'contrib_total': 1000000.0})
        self.Costo.action_recompute_period(period)
        self.assertFalse(
            self.Costo.search([('period', '=', period),
                               ('product_id', '=', maquina.id)]),
            'la fila huérfana debe eliminarse en el recálculo')
        # y el producto con venta válida conserva su fila
        self.assertTrue(
            self.Costo.search([('period', '=', period),
                               ('product_id', '=', self.tela.id)]))

    def test_rentabilidad_cliente_lee_sin_error(self):
        """La vista SQL de rentabilidad por cliente compila y expone la
        cobertura de costo (revenue en MXN vía balance, no price_subtotal)."""
        Rent = self.env['qb.cliente.rentabilidad']
        # _table_query pasa por formateo estilo printf: un '%' literal (aun
        # en un comentario SQL) truena TODA lectura de la vista con "not
        # enough arguments for format string" — pasó en producción con un
        # comentario que decía "op%".
        self.assertNotIn('%', Rent._table_query)
        # No debe tronar aunque no haya facturas en la DB de test.
        recs = Rent.search([], limit=5)
        self.assertIn('costo_cobertura_pct', Rent._fields)
        # Los tres márgenes por cliente están expuestos
        for f in ('margen_bruto_12m', 'margen_bruto_pct',
                  'margen_neto_12m', 'margen_neto_pct'):
            self.assertIn(f, Rent._fields)
        for r in recs:
            # cobertura es un %; el revenue en MXN no explota a negativos raros
            self.assertGreaterEqual(r.costo_cobertura_pct, -0.01)
            # neto ≤ bruto siempre (op% ≥ 0 sobre el facturado)
            self.assertGreaterEqual(
                r.margen_bruto_12m + 0.01, r.margen_neto_12m)

    def _ventana(self, period, window=12):
        return (period + relativedelta(months=1) - relativedelta(months=window),
                period + relativedelta(months=1))

    def test_dos_rollos_iguales_cuentan_como_dos(self):
        """Dos líneas del mismo producto con la misma cantidad en una factura
        son dos rollos, no un triplete de facturación.

        El dedup viejo colapsaba cualquier repetición: la cantidad se partía a
        la mitad y el precio promedio salía al doble. La regla nueva mira el
        TAMAÑO del grupo — un triplete son exactamente tres líneas."""
        journal = self.env['account.journal'].search(
            [('type', '=', 'sale')], limit=1)
        if not journal:
            self.skipTest('sin plan contable en la DB de test')
        period = date(2027, 9, 1)
        partner = self.env['res.partner'].create({'name': 'Cliente Rollos'})
        linea = {'product_id': self.tela.id, 'quantity': 100,
                 'price_unit': 20.0}
        self.env['account.move'].create({
            'move_type': 'out_invoice', 'partner_id': partner.id,
            'invoice_date': period,
            'invoice_line_ids': [(0, 0, dict(linea)), (0, 0, dict(linea))],
        }).action_post()

        venta = self.Costo._sales_by_product(period)[self.tela.id]
        self.assertAlmostEqual(venta['qty'], 200.0, places=2,
                               msg='dos rollos de 100 m son 200 m')
        self.assertAlmostEqual(venta['revenue'], 4000.0, places=2)
        # …y por lo tanto el precio promedio es el real, no el doble
        self.assertAlmostEqual(
            venta['revenue'] / venta['qty'], 20.0, places=2)

    def test_triplete_de_tres_lineas_si_se_colapsa(self):
        """Tres líneas con la misma cantidad sí son un triplete: la cantidad
        cuenta una vez y el revenue suma las tres (cancelan aritméticamente),
        que es justo para lo que existe el dedup."""
        journal = self.env['account.journal'].search(
            [('type', '=', 'sale')], limit=1)
        if not journal:
            self.skipTest('sin plan contable en la DB de test')
        period = date(2027, 10, 1)
        partner = self.env['res.partner'].create({'name': 'Cliente Triplete'})
        self.env['account.move'].create({
            'move_type': 'out_invoice', 'partner_id': partner.id,
            'invoice_date': period,
            'invoice_line_ids': [
                (0, 0, {'product_id': self.tela.id, 'quantity': 100,
                        'price_unit': 25.0}),      # lista
                (0, 0, {'product_id': self.tela.id, 'quantity': 100,
                        'price_unit': -5.0}),      # descuento
                (0, 0, {'product_id': self.tela.id, 'quantity': 100,
                        'price_unit': 0.0}),       # neta
            ]}).action_post()

        venta = self.Costo._sales_by_product(period)[self.tela.id]
        self.assertAlmostEqual(venta['qty'], 100.0, places=2,
                               msg='el triplete es UNA venta de 100 m')
        self.assertAlmostEqual(venta['revenue'], 2000.0, places=2)

    def test_receta_con_atributos_no_carga_lo_de_las_hermanas(self):
        """Una línea de receta que solo aplica a otra variante NO debe entrar
        al costo del producto. Sin el filtro, la MP salía inflada por todo lo
        de sus variantes hermanas."""
        uom_kg = self.env.ref('uom.product_uom_kgm')
        attr = self.env['product.attribute'].create({
            'name': 'COLOR TEST',
            'value_ids': [(0, 0, {'name': 'ROJO TEST'}),
                          (0, 0, {'name': 'AZUL TEST'})]})
        tmpl = self.env['product.template'].create({
            'name': 'TELA CON COLOR TEST', 'is_storable': True,
            'uom_id': uom_kg.id, 'sale_ok': True,
            'attribute_line_ids': [(0, 0, {
                'attribute_id': attr.id,
                'value_ids': [(6, 0, attr.value_ids.ids)]})]})
        rojo, azul = tmpl.product_variant_ids[0], tmpl.product_variant_ids[1]
        ptav_rojo = rojo.product_template_attribute_value_ids[:1]

        pigmento = self.env['product.product'].create({
            'name': 'PIGMENTO TEST', 'is_storable': True,
            'uom_id': uom_kg.id, 'standard_price': 900.0})
        self.env['mrp.bom'].create({
            'product_tmpl_id': tmpl.id, 'product_qty': 1.0,
            'product_uom_id': uom_kg.id,
            'bom_line_ids': [
                (0, 0, {'product_id': self.hilo.id, 'product_qty': 1.0,
                        'product_uom_id': uom_kg.id}),
                # Solo para la variante ROJO
                (0, 0, {'product_id': pigmento.id, 'product_qty': 0.1,
                        'product_uom_id': uom_kg.id,
                        'bom_product_template_attribute_value_ids':
                            [(6, 0, ptav_rojo.ids)]}),
            ]})

        costo_rojo = self.Costo._mp_cost_unit(rojo)
        costo_azul = self.Costo._mp_cost_unit(azul)
        self.assertAlmostEqual(costo_rojo, 50.0 + 0.1 * 900.0, places=4)
        self.assertAlmostEqual(
            costo_azul, 50.0, places=4,
            msg='AZUL no lleva pigmento: no debe cargar el de ROJO')

    def test_operacion_no_depende_del_precio(self):
        """El costo reportado deja de moverse con el descuento del vendedor.

        Con `op = op_pct × precio`, el mismo producto vendido a la mitad
        «costaba» la mitad de operación y su margen se veía sano. Con driver
        de producción la operación se reparte sobre lo que cuesta fabricar,
        que no se mueve con el precio."""
        period = date(2027, 6, 1)
        factores = self.env['qb.costo.factores'].create({
            'period': period, 'window_months': 12,
            'factor_fab_kg': 30.0, 'factor_fab_m': 3.0,
            'energia_por_kg': 4.0, 'op_pct': 0.18, 'op_rate': 0.10})
        ctx = self.Costo._engine_ctx([self.tela.id], factores)

        def vals_a(precio):
            ventas = {self.tela.id: {
                'qty': 100.0, 'revenue': precio * 100.0, 'divisas': ''}}
            v, _f = self.Costo._compute_product_vals(
                self.tela, period, factores, ventas, ctx, self.Ruteo, self.Peso)
            return v

        caro, barato = vals_a(50.0), vals_a(25.0)
        self.assertAlmostEqual(caro['op_unit'], barato['op_unit'], places=4,
                               msg='la operación no debe seguir al precio')
        self.assertAlmostEqual(
            caro['op_unit'], caro['costo_produccion'] * 0.10, places=4)
        self.assertAlmostEqual(caro['costo_absorbido'],
                               barato['costo_absorbido'], places=4)
        # Y la identidad de capas sigue exacta
        self.assertAlmostEqual(
            caro['costo_absorbido'],
            caro['mp_unit'] + caro['energia_unit'] + caro['fab_unit']
            + caro['op_unit'], places=4)

    def test_operacion_driver_legacy_sigue_sobre_ventas(self):
        """Con `op_rate` en 0 (driver «ventas») se mantiene el reparto viejo:
        el cambio es reversible desde Parámetros."""
        period = date(2027, 7, 1)
        factores = self.env['qb.costo.factores'].create({
            'period': period, 'window_months': 12,
            'factor_fab_kg': 30.0, 'factor_fab_m': 3.0,
            'energia_por_kg': 4.0, 'op_pct': 0.18, 'op_rate': 0.0})
        ctx = self.Costo._engine_ctx([self.tela.id], factores)
        ventas = {self.tela.id: {
            'qty': 100.0, 'revenue': 5000.0, 'divisas': ''}}
        vals, _f = self.Costo._compute_product_vals(
            self.tela, period, factores, ventas, ctx, self.Ruteo, self.Peso)
        self.assertAlmostEqual(vals['op_unit'], 0.18 * 50.0, places=4)

    def test_capas_produccion_es_la_misma_cuenta_en_los_dos_caminos(self):
        """El reporte y la base de la tasa de operación tienen que salir del
        MISMO cálculo: si divergieran, la tasa no cuadraría contra el costo al
        que se aplica."""
        period = date(2027, 8, 1)
        factores = self.env['qb.costo.factores'].create({
            'period': period, 'window_months': 12,
            'factor_fab_kg': 30.0, 'factor_fab_m': 3.0,
            'energia_por_kg': 4.0, 'op_pct': 0.18, 'mp_ajuste': 0.9})
        ctx = self.Costo._engine_ctx([self.tela.id], factores)
        _b, _c, _kg, _mkg, _ik, mp, energia, fab = \
            self.Costo._capas_produccion(
                self.tela, factores, ctx, self.Ruteo, self.Peso)
        vals, _f = self.Costo._compute_product_vals(
            self.tela, period, factores, {}, ctx, self.Ruteo, self.Peso)
        self.assertAlmostEqual(vals['mp_unit'], mp, places=6)
        self.assertAlmostEqual(vals['energia_unit'], energia, places=6)
        self.assertAlmostEqual(vals['fab_unit'], fab, places=6)
        self.assertAlmostEqual(
            vals['costo_produccion'], mp + energia + fab, places=6)

    def test_denominador_usa_capacidad_normal_no_produccion(self):
        """El pool fijo se divide entre capacidad NORMAL, no entre producción
        real (IAS 2). Con producción en el denominador, un mes flojo encarece
        el producto y el modelo recomienda subir el precio justo cuando lo que
        hace falta es vender más."""
        Centro = self.env['qb.costeo.centro']
        centro = Centro.create({
            'code': 'TEST_CAP', 'name': 'Centro de prueba capacidad',
            'nature': 'fabril_directo', 'driver_principal': 'peso',
            'capacidad_normal': 50000.0, 'std_output_per_hour': 10.0,
            'es_denominador_kg': True,
        })
        period = date.today().replace(day=1)
        date_to = period + relativedelta(months=1)
        date_from = date_to - relativedelta(months=12)
        denom = self.Costo._denominador_capacidad(centro, date_from, date_to)
        self.assertAlmostEqual(
            denom, 50000.0, places=2,
            msg='con capacidad normal capturada, el denominador es esa')

        # Apagando el costeo normal vuelve a producción real (que sin órdenes
        # en el centro de prueba es 0)
        self.env['qb.costeo.factor.config'].create({
            'key': 'denominador_capacidad_normal', 'value': 0.0})
        self.assertAlmostEqual(
            self.Costo._denominador_capacidad(centro, date_from, date_to),
            self.Costo._production_month_avg(centro, date_from, date_to),
            places=2)
        centro.unlink()

    def test_energia_se_divide_entre_produccion_real(self):
        """La energía es VARIABLE: su $/kg va sobre los kilos que de verdad se
        produjeron. Con capacidad normal en el denominador, un mes al 60% de
        utilización daría una energía por kilo 40% baja — al revés de la
        realidad física."""
        Config = self.env['qb.costeo.factor.config']
        if Config.get_param('energia_por_kg', 0.0) or \
                Config.get_param('denominador_kg_override', 0.0):
            self.skipTest('energía por kg fijada a mano por parámetro')
        period = date.today().replace(day=1)
        factores = self.Costo._compute_factores(period)
        if not factores.energia_pool_month or not factores.kg_produccion_month:
            self.skipTest('sin pool de energía ni producción en la DB de test')
        self.assertAlmostEqual(
            factores.energia_por_kg,
            factores.energia_pool_month / factores.kg_produccion_month,
            places=4)

    def test_ociosidad_no_absorbida_es_la_parte_del_pool_que_falta(self):
        """`fab_ocioso_month` = pool fijo − lo que la producción real alcanza a
        absorber. Es la diferencia DELIBERADA entre el modelo y el gasto: bajo
        IAS 2 la capacidad ociosa va al resultado del período."""
        period = date.today().replace(day=1)
        f = self.Costo._compute_factores(period)
        util = (f.fab_weight_share * f.utilizacion_kg_pct / 100.0
                + (1 - f.fab_weight_share) * f.utilizacion_m_pct / 100.0)
        self.assertAlmostEqual(
            f.fab_ocioso_month, max(f.fab_pool_month * (1 - util), 0.0),
            places=2)
        self.assertGreaterEqual(f.fab_ocioso_month, 0.0)

    def test_mp_ajuste_inerte_sin_cuentas_clasificadas(self):
        """Sin cuentas en el bucket «mp» no hay contra qué conciliar: el
        ajuste vale 1.0 y el costo no se mueve. El ajuste solo existe cuando
        hay un número duro del mayor enfrente."""
        Clase = self.env['qb.costeo.cuenta.class']
        Clase.search([('bucket', '=', 'mp')]).write({'active': False})
        period = date.today().replace(day=1)
        ctx = self.Costo._engine_ctx([self.tela.id])
        gl, modelada, factor = self.Costo._mp_ajuste(
            *self._ventana(period), ctx)
        self.assertEqual((gl, modelada, factor), (0.0, 0.0, 1.0))

    def test_mp_ajuste_es_el_cociente_y_respeta_la_banda(self):
        """El factor es MP consumida ÷ MP modelada, y se recorta a la banda
        de cordura: un factor disparado casi siempre significa una cuenta mal
        clasificada, no una receta equivocada por 3×."""
        journal = self.env['account.journal'].search(
            [('type', '=', 'general')], limit=1)
        if not journal:
            self.skipTest('sin plan contable en la DB de test')
        period = date.today().replace(day=1)
        Clase = self.env['qb.costeo.cuenta.class']
        Account = self.env['account.account']
        cuenta_mp = Account.create({
            'name': 'COSTO PRIMO TEST', 'code': 'QBMP.0001',
            'account_type': 'expense_direct_cost'})
        contra = Account.create({
            'name': 'CONTRA TEST', 'code': 'QBMP.0002',
            'account_type': 'expense'})
        Clase.create({'account_id': cuenta_mp.id, 'bucket': 'mp'})
        # Un monto absurdo contra la MP modelada: debe recortarse, no pasar
        self.env['account.move'].create({
            'move_type': 'entry', 'journal_id': journal.id, 'date': period,
            'line_ids': [
                (0, 0, {'account_id': cuenta_mp.id, 'debit': 99000000.0,
                        'credit': 0.0}),
                (0, 0, {'account_id': contra.id, 'debit': 0.0,
                        'credit': 99000000.0}),
            ]}).action_post()

        ctx = self.Costo._engine_ctx([self.tela.id])
        gl, modelada, factor = self.Costo._mp_ajuste(
            *self._ventana(period), ctx)
        f_max = self.env['qb.costeo.factor.config'].get_param(
            'mp_ajuste_max', 1.5) or 1.5
        if modelada > 0:
            self.assertEqual(factor, f_max,
                             'un cociente disparado debe recortarse a la banda')
            # gl y modelada se devuelven sobre el MISMO conjunto de meses, así
            # que su cociente es el factor SIN recortar
            self.assertGreater(gl / modelada, f_max)
        else:
            # Sin ventas en la ventana no hay con qué comparar
            self.assertEqual(factor, 1.0)

    def test_mp_ajuste_solo_toca_al_nacional(self):
        """El ajuste acerca la receta al consumo real de la planta. El
        importado no consume materia prima de la planta (su MP es precio de
        compra más aduana) y el subproducto tiene MP $0: ninguno se escala."""
        period = date(2027, 5, 1)
        factores = self.env['qb.costo.factores'].create({
            'period': period, 'window_months': 12,
            'factor_fab_kg': 30.0, 'factor_fab_m': 3.0,
            'energia_por_kg': 4.0, 'op_pct': 0.18,
            'mp_ajuste': 0.80})
        ctx = self.Costo._engine_ctx(
            [self.tela.id, self.importado.id, self.saldo.id], factores)

        base_tela = self.Costo._mp_cost_unit(self.tela, ctx=ctx)
        vals, _f = self.Costo._compute_product_vals(
            self.tela, period, factores, {}, ctx, self.Ruteo, self.Peso)
        self.assertAlmostEqual(vals['mp_unit'], base_tela * 0.80, places=4)

        vals_imp, _f = self.Costo._compute_product_vals(
            self.importado, period, factores, {}, ctx, self.Ruteo, self.Peso)
        self.assertAlmostEqual(
            vals_imp['mp_unit'], self.importado.standard_price, places=4,
            msg='el importado no se ajusta contra el costo primo de la planta')

        vals_sub, _f = self.Costo._compute_product_vals(
            self.saldo, period, factores, {}, ctx, self.Ruteo, self.Peso)
        self.assertEqual(vals_sub['mp_unit'], 0.0)

    def test_reconocedor_de_cuentas_de_materia_prima(self):
        """Reconoce consumo, no inventario: 'INVENTARIO DE MATERIA PRIMA' es
        un activo y matchea el patrón, pero no es consumo — meterlo al bucket
        falsearía la conciliación."""
        Clase = self.env['qb.costeo.cuenta.class']
        Account = self.env['account.account']

        def cuenta(name, code, tipo='expense_direct_cost'):
            return Account.create({'name': name, 'code': code,
                                   'account_type': tipo})

        self.assertTrue(Clase._es_cuenta_de_materia_prima(
            cuenta('COSTO PRIMO TEST', 'QBR.0001')))
        self.assertTrue(Clase._es_cuenta_de_materia_prima(
            cuenta('COSTO POR AJUSTES A CANTIDAD TEST', 'QBR.0002')))
        self.assertTrue(Clase._es_cuenta_de_materia_prima(
            cuenta('DIFERENCIAS POR CONTEO TEST', 'QBR.0003')))
        self.assertFalse(Clase._es_cuenta_de_materia_prima(
            cuenta('INVENTARIO DE MATERIA PRIMA TEST', 'QBR.0004',
                   tipo='asset_current')),
            'el inventario es un activo, no consumo')
        self.assertFalse(Clase._es_cuenta_de_materia_prima(
            cuenta('SUELDOS Y SALARIOS TEST', 'QBR.0005')))

        fuera = Clase.create({
            'account_id': cuenta('COSTO PRIMO MOVER TEST', 'QBR.0006').id,
            'bucket': 'no_costeo'})
        Clase.reclasificar_cuentas_de_materia_prima()
        self.assertEqual(fuera.bucket, 'mp')

    def test_importacion_entra_al_costo_del_importado(self):
        """Los impuestos y gastos de aduana se cargan al valor importado.

        El AVCO de Odoo NO los trae: IGI, DTA, PRV y el agente aduanal se
        postean directo a resultados. Antes quedaban en `no_costeo` y el
        importado se veía más barato de lo que es."""
        factores = self.env['qb.costo.factores'].create({
            'period': date(2027, 3, 1), 'window_months': 12,
            'factor_fab_kg': 30.0, 'factor_fab_m': 3.0,
            'energia_por_kg': 4.0, 'op_pct': 0.18,
            'factor_importacion': 0.20})
        base = self.importado.standard_price  # 7.51

        q = self.Costo.quote_product(self.importado, factores)
        self.assertAlmostEqual(q['mp'], base * 1.20, places=4,
                               msg='el importado debe cargar su aduana')
        # Y sigue sin cargar fabricación: solo pasa por inspección
        self.assertEqual(q['fab'], 0.0)

        # El nacional NO se toca: su MP no lleva aduana
        q_nac = self.Costo.quote_product(self.tela, factores)
        factores.factor_importacion = 0.0
        self.assertAlmostEqual(
            q_nac['mp'], self.Costo.quote_product(self.tela, factores)['mp'],
            places=4, msg='la aduana no debe tocar al producto nacional')

    def test_aduana_del_hilo_importado_llega_a_la_tela(self):
        """El pedimento del hilo lo carga el hilo, y la receta lo arrastra a
        cada tela que lo consume.

        La versión anterior repartía TODA la aduana sobre la familia de
        reventa, que es ~9% del valor importado; el hilo —que es ~83%— no
        cargaba nada. El resultado era un factor once veces alto sobre el
        producto equivocado."""
        ctx = self.Costo._engine_ctx([self.tela.id])
        base = self.Costo._mp_cost_unit(self.tela, ctx=ctx)

        ctx_imp = dict(ctx, import_factor=0.20,
                       import_ids={self.hilo.id}, mp_cache={})
        con_aduana = self.Costo._mp_cost_unit(self.tela, ctx=ctx_imp)
        self.assertAlmostEqual(con_aduana, base * 1.20, places=4)
        # y la tela NO se vuelve importada por eso: sigue cargando fabricación
        self.assertNotEqual(
            self.Ruteo.resolve(self.tela, ctx['rules'])[0], 'importado')

    def test_aduana_no_se_aplica_dos_veces_en_la_receta(self):
        """Un componente importado ya trae su aduana dentro; el total de la
        receta no se vuelve a multiplicar."""
        ctx = self.Costo._engine_ctx([self.tela.id])
        ctx_imp = dict(ctx, import_factor=0.20,
                       import_ids={self.hilo.id, self.tela.id}, mp_cache={})
        # Aunque la TELA también esté marcada como compra importada, su costo
        # sale de la receta (tiene BOM), no de una compra: el recargo entra
        # una sola vez, por el hilo.
        base = self.Costo._mp_cost_unit(
            self.tela, ctx=dict(ctx, mp_cache={}))
        self.assertAlmostEqual(
            self.Costo._mp_cost_unit(self.tela, ctx=ctx_imp),
            base * 1.20, places=4)

    def test_driver_de_aduana_por_default_no_prorratea(self):
        """El default es «landed»: el módulo NO inventa un prorrateo de
        pedimentos. Mide cuánta aduana se quedó en resultados y espera que se
        capitalice con el landed cost de Odoo sobre cada recepción."""
        Config = self.env['qb.costeo.factor.config']
        self.assertEqual(
            Config.get_param_text('importacion_driver', 'landed'), 'landed')
        period = date.today().replace(day=1)
        factores = self.Costo._compute_factores(period)
        self.assertEqual(
            factores.factor_importacion, 0.0,
            'con driver «landed» no debe haber factor de prorrateo')
        self.assertEqual(factores.importacion_base_month, 0.0)

    def test_importacion_se_reporta_dentro_de_la_mp(self):
        """`importacion_unit` es informativo: la parte de la MP que es aduana.
        No es una capa aparte — si lo fuera, la cascada del cotizador y del
        PDF (MP → +energía → =variable) dejaría de cuadrar."""
        period = date(2027, 4, 1)
        factores = self.env['qb.costo.factores'].create({
            'period': period, 'window_months': 12,
            'factor_fab_kg': 30.0, 'factor_fab_m': 3.0,
            'energia_por_kg': 4.0, 'op_pct': 0.18,
            'factor_importacion': 0.25})
        ctx = self.Costo._engine_ctx([self.importado.id], factores)
        vals, _fab = self.Costo._compute_product_vals(
            self.importado, period, factores, {}, ctx, self.Ruteo, self.Peso)
        # 25% sobre el valor de compra → 20% del costo final es aduana
        self.assertAlmostEqual(
            vals['importacion_unit'], vals['mp_unit'] * 0.25 / 1.25, places=4)
        # y la identidad de capas sigue intacta (la aduana NO se suma aparte)
        self.assertAlmostEqual(
            vals['costo_variable'], vals['mp_unit'] + vals['energia_unit'],
            places=4)

    def test_reconocedor_de_cuentas_de_importacion(self):
        """Distingue aduana de exportación y no se dispara con subcadenas:
        'DTAS' o 'DIGITALIZACION' no son cuentas de importación."""
        Clase = self.env['qb.costeo.cuenta.class']
        Account = self.env['account.account']

        def cuenta(name, code):
            return Account.create({'name': name, 'code': code,
                                   'account_type': 'expense'})

        self.assertTrue(Clase._es_cuenta_de_importacion(
            cuenta('GASTOS DE IMPORTACION TEST', 'QBI.0001')))
        self.assertTrue(Clase._es_cuenta_de_importacion(
            cuenta('IGI TEST', 'QBI.0002')))
        self.assertFalse(Clase._es_cuenta_de_importacion(
            cuenta('GASTOS POR EXPORTACIONES TEST', 'QBI.0003')))
        self.assertFalse(Clase._es_cuenta_de_importacion(
            cuenta('VENTA DTAS TEST', 'QBI.0004')))

        # Solo reclasifica lo que hoy está fuera de costeo
        fuera = Clase.create({
            'account_id': cuenta('IMPORTACION FLETES TEST', 'QBI.0005').id,
            'bucket': 'no_costeo'})
        en_operacion = Clase.create({
            'account_id': cuenta('IMPORTACION AGENTE TEST', 'QBI.0006').id,
            'bucket': 'operacion'})
        Clase.reclasificar_cuentas_de_importacion()
        self.assertEqual(fuera.bucket, 'importacion')
        self.assertEqual(en_operacion.bucket, 'operacion',
                         'mover un bucket ya activo es decisión del usuario')
        self.assertIn(en_operacion,
                      Clase.cuentas_de_importacion_mal_ubicadas())

    def test_smooth_divide_entre_meses_de_ventana_no_de_facturas(self):
        """Un gasto que se registra al PAGARSE aparece en unos meses sí y
        otros no. Dividir entre los meses en que apareció da el cargo por
        factura, no el costo mensual: energía en 53k/65k/173k son $97k por
        recibo pero $41k al mes si la ventana es de siete."""
        Costo = self.Costo
        por_mes = {date(2026, 1, 1): 53000.0,
                   date(2026, 3, 1): 65000.0,
                   date(2026, 6, 1): 173000.0}
        self.assertAlmostEqual(
            Costo._smooth(por_mes, meses=7), 291000.0 / 7, places=2)
        # Sin `meses` se conserva el comportamiento viejo (por factura)
        self.assertAlmostEqual(
            Costo._smooth(por_mes), 291000.0 / 3, places=2)

    def test_smooth_descarta_el_reverso_de_cierre_de_los_dos_lados(self):
        """El reverso del cierre anual es un mes negativo. Dejarlo en el
        numerador hundiría el promedio; dejarlo en el denominador lo
        subvaluaría igual. Sale de los dos."""
        por_mes = {date(2025, 11, 1): 100.0,
                   date(2025, 12, 1): -5000.0,
                   date(2026, 1, 1): 200.0}
        # 3 meses de ventana, uno descartado -> 300 / 2
        self.assertAlmostEqual(
            self.Costo._smooth(por_mes, meses=3), 150.0, places=2)

    def test_ventana_fabril_arranca_en_el_corte_de_absorcion(self):
        """Promediar meses del régimen viejo con meses del nuevo mezcla dos
        cosas distintas: los anteriores al corte llevan el gasto del centro
        completo. El factor del mes tiene que describir a ese mes."""
        Centro = self.env['qb.costeo.centro']
        period = date.today().replace(day=1)
        corte = period - relativedelta(months=1)
        centro = Centro.create({
            'code': 'TEST_VENTANA', 'name': 'Corte de prueba',
            'nature': 'fabril_directo', 'driver_principal': 'peso',
            'modo_costeo': 'absorcion_odoo', 'fecha_absorcion': corte,
        })
        factores = self.Costo._compute_factores(period)
        self.assertEqual(factores.fab_ventana_desde, corte,
                         'la ventana fabril arranca en el corte')
        self.assertLessEqual(factores.fab_ventana_meses,
                             factores.window_months)
        centro.unlink()

        # Sin centros absorbidos, la ventana fabril es la de suavizado
        factores = self.Costo._compute_factores(period)
        esperado = (period + relativedelta(months=1)
                    - relativedelta(months=factores.window_months))
        self.assertEqual(factores.fab_ventana_desde, esperado)

    def test_periodo_cerrado_no_se_recalcula(self):
        """Un período cerrado es un snapshot: ni el cron ni un recálculo
        manual lo tocan. Sin esto, el número que presentaste el mes pasado
        cambia solo la próxima vez que alguien recalcula."""
        period = date(2027, 11, 1)
        factores = self.env['qb.costo.factores'].create({
            'period': period, 'window_months': 12,
            'factor_fab_kg': 30.0, 'op_pct': 0.18})
        fila = self.Costo.create({
            'period': period, 'product_id': self.tela.id,
            'qty_vendida': 10.0, 'precio_prom': 50.0, 'mp_unit': 3.6})

        factores.action_cerrar()
        self.assertEqual(factores.state, 'cerrado')
        self.assertEqual(factores.cerrado_por, self.env.user)

        # El recálculo se rehúsa y lo dice
        self.assertFalse(self.Costo.action_recompute_period(period))
        # y la fila no se puede escribir ni borrar por la puerta de atrás
        with self.assertRaises(UserError):
            fila.write({'precio_prom': 999.0})
        with self.assertRaises(UserError):
            fila.unlink()
        self.assertEqual(fila.precio_prom, 50.0)

        # Reabrir exige motivo, y queda contado
        with self.assertRaises(UserError):
            factores.action_reabrir()
        factores.motivo_reapertura = 'Se reclasificó una cuenta de renta.'
        factores.action_reabrir()
        self.assertEqual(factores.state, 'borrador')
        self.assertEqual(factores.reaperturas, 1)
        fila.write({'precio_prom': 60.0})
        self.assertEqual(fila.precio_prom, 60.0)

    def test_centro_absorbido_sale_del_pool(self):
        """Un centro cuyos workcenters ya capitalizan NO puede seguir en el
        pool: Odoo mete su costo al AVCO del producto y la venta lo libera.
        Repartirlo además con los factores lo cobraría dos veces."""
        Centro = self.env['qb.costeo.centro']
        centro = Centro.create({
            'code': 'TEST_ABS', 'name': 'Centro de prueba absorción',
            'nature': 'fabril_directo', 'driver_principal': 'peso',
            'renta_contractual_mxn': 80000.0,
        })
        period = date.today().replace(day=1)
        pool_capa = self.Costo._compute_factores(period).fab_pool_month

        centro.write({'modo_costeo': 'absorcion_odoo',
                      'fecha_absorcion': period})
        factores = self.Costo._compute_factores(period)
        self.assertIn('TEST_ABS', factores.centros_absorbidos or '')
        self.assertNotIn('TEST_ABS', factores.centros_capa or '')
        self.assertAlmostEqual(
            pool_capa - factores.fab_pool_month, 80000.0, places=2,
            msg='su renta contractual debe salir del pool')

        # La fecha de corte se compara contra el PERÍODO: un mes anterior
        # conserva el régimen de capa y el histórico no se reescribe.
        anterior = period - relativedelta(months=1)
        previos = self.Costo._compute_factores(anterior)
        self.assertNotIn('TEST_ABS', previos.centros_absorbidos or '')
        centro.unlink()

    def test_absorcion_requiere_fecha_de_corte(self):
        """Sin fecha no se sabe desde qué período sacar el gasto del pool."""
        Centro = self.env['qb.costeo.centro']
        with self.assertRaises(ValidationError):
            Centro.create({
                'code': 'TEST_SIN_FECHA', 'name': 'Sin fecha',
                'nature': 'fabril_directo', 'modo_costeo': 'absorcion_odoo'})

    def test_share_se_apaga_cuando_su_lado_queda_sin_centros(self):
        """Al absorberse el único centro que define los kilos, el share de
        peso debe irse a 0: repartirle pool a un factor sin denominador
        dejaría dinero sin absorber."""
        Centro = self.env['qb.costeo.centro']
        period = date(2027, 12, 1)
        kg = Centro.create({
            'code': 'TEST_KG', 'name': 'Denominador kg de prueba',
            'nature': 'fabril_directo', 'driver_principal': 'peso',
            'es_denominador_kg': True, 'std_output_per_hour': 10.0})
        m = Centro.create({
            'code': 'TEST_M', 'name': 'Denominador m de prueba',
            'nature': 'fabril_directo', 'driver_principal': 'largo',
            'es_denominador_m': True, 'std_output_per_hour': 100.0})
        # Los centros reales de la DB pueden aportar denominador también, así
        # que se apagan para aislar el caso.
        otros = Centro.search([
            '|', ('es_denominador_kg', '=', True), ('es_denominador_m', '=', True),
            ('id', 'not in', (kg | m).ids)])
        otros.write({'active': False})

        self.assertGreater(
            self.Costo._compute_factores(period).fab_weight_share, 0.0)
        kg.write({'modo_costeo': 'absorcion_odoo', 'fecha_absorcion': period})
        self.assertEqual(
            self.Costo._compute_factores(period).fab_weight_share, 0.0,
            'sin centro de kilos en capa, todo el pool va por metros')
        otros.write({'active': True})
        (kg | m).unlink()

    def test_renta_contractual_entra_al_pool_fabril(self):
        """La renta contractual de TODOS los centros fabriles llega al costo
        del producto, no solo la de entretelas.

        Era un bug con dinero real: la cuenta de renta del GL se excluía
        (`no_costeo`) argumentando que en su lugar se usaba la renta
        contractual, pero el código solo la aplicaba dentro del bloque de
        entretelas. Tejido, tintorería y acabado tenían su renta capturada y
        nunca llegaba al costo."""
        Centro = self.env['qb.costeo.centro']
        centro = Centro.create({
            'code': 'TEST_RENTA', 'name': 'Centro de prueba renta',
            'nature': 'fabril_directo', 'driver_principal': 'peso',
            'renta_contractual_mxn': 100000.0,
        })
        period = date.today().replace(day=1)
        factores = self.Costo._compute_factores(period)
        self.assertGreaterEqual(
            factores.renta_contractual_pool, 100000.0,
            'la renta contractual del centro fabril debe entrar al pool')
        pool_con = factores.fab_pool_month

        # Sin renta contractual el pool baja exactamente en esos $100k
        centro.renta_contractual_mxn = 0.0
        factores = self.Costo._compute_factores(period)
        pool_sin = factores.fab_pool_month
        if pool_sin > 0:
            # (con el pool en 0 el max(..., 0) del motor recorta y la resta
            # exacta deja de aplicar)
            self.assertAlmostEqual(pool_con - pool_sin, 100000.0, places=2)
        else:
            self.assertAlmostEqual(pool_con, 100000.0, places=2)
        centro.unlink()

    def test_renta_de_entretelas_no_le_cuesta_a_tela(self):
        """La renta contractual de entretelas entra al total y sale otra vez
        con el pool propio de entretelas: el pool de TELA no se mueve.

        Restarle a tela una renta que nunca se le sumó le quitaría dinero que
        tela no tuvo, y su factor $/kg saldría bajo."""
        Centro = self.env['qb.costeo.centro']
        centro = Centro.create({
            'code': 'ENTRETELA TEST', 'name': 'Entretela de prueba',
            'nature': 'fabril_directo', 'driver_principal': 'largo',
            'renta_contractual_mxn': 0.0,
        })
        period = date.today().replace(day=1)
        pool_sin = self.Costo._compute_factores(period).fab_pool_month

        centro.renta_contractual_mxn = 70000.0
        factores = self.Costo._compute_factores(period)
        self.assertAlmostEqual(
            factores.fab_pool_month, pool_sin, places=2,
            msg='la renta de entretelas no debe tocar el pool de tela')
        self.assertGreaterEqual(factores.entretela_pool_month, 70000.0,
                                'pero sí debe financiar su propio factor')
        centro.unlink()

    def test_renta_del_gl_marcada_sale_del_pool(self):
        """Una cuenta marcada «es renta de inmueble» se saca del pool fabril:
        de otro modo la renta se contaría dos veces (GL + contrato)."""
        Clase = self.env['qb.costeo.cuenta.class']
        Account = self.env['account.account']
        renta = Account.create({
            'name': 'RENTA DEL LOCAL (PLANTA) TEST', 'code': 'QBT.45.0001',
            'account_type': 'expense'})
        maquina = Account.create({
            'name': 'ARRENDAMIENTO DE MAQUINARIA TEST', 'code': 'QBT.20.0001',
            'account_type': 'expense'})
        self.assertTrue(Clase._es_cuenta_de_renta(renta))
        self.assertFalse(
            Clase._es_cuenta_de_renta(maquina),
            'el arrendamiento de maquinaria NO se sustituye por contrato')

        # Y el marcado automático las distingue igual: la de inmueble queda
        # fuera del pool fabril, la de maquinaria se queda dentro.
        clase_renta = Clase.create({'account_id': renta.id,
                                    'bucket': 'overhead_fab'})
        clase_maq = Clase.create({'account_id': maquina.id,
                                  'bucket': 'arrend_maquinaria'})
        Clase.marcar_cuentas_de_renta()
        self.assertTrue(clase_renta.es_renta)
        self.assertFalse(clase_maq.es_renta)

    def test_conciliacion_modelo_vs_mayor(self):
        """La conciliación compila, cuadra con el motor y expone la brecha.

        Es el control de calidad del costeo: el modelo reparte costos con
        recetas y pools, y esta vista lo confronta contra el mayor. Si las
        ventas del modelo no empatan con las del mayor, o el costo repartido
        no se parece al gasto real, aquí se ve — antes se decidían precios
        sin saberlo."""
        period = date.today().replace(day=1)
        self.Costo.action_recompute_period(period)
        Conc = self.env['qb.costo.conciliacion']
        rows = Conc.search([])
        # La vista lee sin reventar y trae todas sus columnas
        rows.read([
            'period', 'gl_ventas', 'modelo_ventas', 'ventas_dif',
            'gl_costo_ventas', 'gl_gastos_operacion', 'gl_gasto_total',
            'modelo_mp', 'modelo_energia', 'modelo_fab', 'modelo_op',
            'modelo_costo_total', 'gl_mp', 'gl_no_costeo',
            'gl_sin_clasificar', 'resultado_gl', 'resultado_modelo',
            'ociosidad_ias2', 'resultado_par',
            'brecha', 'brecha_pct', 'cobertura_pct'])
        # El par indivisible existe en TODOS los períodos: el resultado es
        # margen de productos − ociosidad, fila por fila. Es lo que grafica
        # "Margen vs ociosidad por mes".
        for r in rows:
            self.assertAlmostEqual(
                r.resultado_par,
                r.resultado_modelo - r.ociosidad_ias2, places=2)
        row = Conc.search([('period', '=', period)], limit=1)
        if not row:
            self.skipTest('sin movimientos de resultados en el período')
        # El lado "modelo" es exactamente lo que suma qb.costo.producto:
        # si esto se desalinea, la brecha deja de significar algo.
        recs = self.Costo.search([('period', '=', period)])
        self.assertAlmostEqual(
            row.modelo_ventas, sum(recs.mapped('ventas_total')), places=2)
        self.assertAlmostEqual(
            row.modelo_costo_total,
            sum(recs.mapped('costo_absorbido_total')), places=2)
        self.assertAlmostEqual(
            row.resultado_modelo,
            sum(recs.mapped('margen_neto_total')), places=2)
        # Y la brecha es, por definición, modelo − mayor
        self.assertAlmostEqual(
            row.brecha, row.resultado_modelo - row.resultado_gl, places=2)
        self.assertAlmostEqual(
            row.gl_gasto_total,
            row.gl_costo_ventas + row.gl_gastos_operacion, places=2)

    def test_cuenta_especifica_gana_sobre_patron(self):
        """Una clase de CUENTA ESPECIFICA gana sobre una de patrón para la
        misma cuenta. Antes '(c.account_id = rel.account_id) DESC' daba
        NULL para las clases de patrón y Postgres ordena NULL antes que
        TRUE en DESC → el patrón ganaba (bug real: reclasificar la renta
        de planta 603.45.0001 a overhead_fab no surtía efecto porque el
        patrón '603 por ciento' → operación seguía mandando)."""
        cuenta = self.env['account.account'].create({
            'code': '603.99.T88', 'name': 'RENTA PLANTA TEST',
            'account_type': 'expense'})
        self.env['qb.costeo.cuenta.class'].create({
            'code_pattern': '603.99%', 'bucket': 'operacion'})
        self.env['qb.costeo.cuenta.class'].create({
            'account_id': cuenta.id, 'bucket': 'overhead_fab'})
        row = self.env['qb.costeo.cuenta.map'].search(
            [('account_id', '=', cuenta.id)])
        self.assertEqual(len(row), 1, 'una sola fila por cuenta')
        self.assertEqual(row.bucket, 'overhead_fab',
                         'la cuenta específica debe ganar sobre el patrón')

    def test_rh_mod_prorrateo_por_nomina(self):
        """El MOD por centro sale del GL (la nómina que de verdad se pagó)
        repartido por masa salarial de RH normalizada a mensual: un sueldo
        capturado SEMANAL pesa ×4.33 frente al mismo número capturado
        mensual. Antes la columna GL salía $0 (las cuentas de nómina no
        tienen centro asignado) y los sueldos crudos mezclaban semanales
        con mensuales."""
        journal = self.env['account.journal'].search(
            [('type', '=', 'general')], limit=1)
        if not journal:
            self.skipTest('sin plan contable en la DB de test')
        if 'hr.version' not in self.env \
                or 'wage' not in self.env['hr.version']._fields \
                or 'schedule_pay' not in self.env['hr.version']._fields:
            self.skipTest('hr.version sin parte contractual en esta DB')
        # Cuenta de nómina clasificada mod SIN centro → entra al pool
        cuenta = self.env['account.account'].create({
            'code': '501.06.T77', 'name': 'SUELDOS TEST',
            'account_type': 'expense'})
        contra = self.env['account.account'].create({
            'code': '102.01.T77', 'name': 'BANCO TEST',
            'account_type': 'asset_cash'})
        self.env['qb.costeo.cuenta.class'].create({
            'account_id': cuenta.id, 'bucket': 'mod'})
        fecha = date.today().replace(day=1) - relativedelta(months=1)
        move = self.env['account.move'].create({
            'move_type': 'entry', 'journal_id': journal.id, 'date': fecha,
            'line_ids': [
                (0, 0, {'account_id': cuenta.id, 'debit': 300000.0,
                        'name': 'nomina test'}),
                (0, 0, {'account_id': contra.id, 'credit': 300000.0,
                        'name': 'nomina test'}),
            ]})
        move.action_post()
        # Dos centros, un empleado cada uno, MISMO número capturado pero
        # distinta periodicidad: semanal pesa 4.33× vs mensual
        tejido = self.env.ref('qb_capacidad_costeo.centro_tejido')
        acabado = self.env.ref('qb_capacidad_costeo.centro_acabado')
        d1 = self.env['hr.department'].create({'name': 'Tejido RH Test'})
        d2 = self.env['hr.department'].create({'name': 'Acabado RH Test'})
        tejido.department_ids = [(6, 0, [d1.id])]
        acabado.department_ids = [(6, 0, [d2.id])]
        e1 = self.env['hr.employee'].create({
            'name': 'Op Semanal', 'department_id': d1.id})
        e2 = self.env['hr.employee'].create({
            'name': 'Op Mensual', 'department_id': d2.id})
        e1.current_version_id.write(
            {'wage': 1000.0, 'schedule_pay': 'weekly'})
        e2.current_version_id.write(
            {'wage': 1000.0, 'schedule_pay': 'monthly'})
        Rh = self.env['qb.rh.centro']
        r1 = Rh.search([('centro_id', '=', tejido.id)])
        r2 = Rh.search([('centro_id', '=', acabado.id)])
        # Normalización: 1000 semanal → 4330/mes; 1000 mensual → 1000/mes
        self.assertAlmostEqual(r1.wage_month_total, 4330.0, places=1)
        self.assertAlmostEqual(r2.wage_month_total, 1000.0, places=1)
        # El pool (300k/3 = 100k/mes) se reparte 4.33:1 entre los dos
        # únicos centros con empleados mapeados
        self.assertGreater(r1.gl_mod_prorrateado, 0)
        self.assertGreater(r2.gl_mod_prorrateado, 0)
        self.assertAlmostEqual(
            r1.gl_mod_prorrateado / r2.gl_mod_prorrateado, 4.33, places=2)
        self.assertGreaterEqual(
            r1.gl_mod_prorrateado + r2.gl_mod_prorrateado, 99999.0)
        # el total del centro = directo (0 aquí) + prorrateado
        self.assertAlmostEqual(
            r1.gl_mod_month, r1.gl_mod_directo + r1.gl_mod_prorrateado,
            places=2)
        # y las participaciones suman 100 entre todos los centros
        total_share = sum(Rh.search([]).mapped('nomina_share_pct'))
        self.assertAlmostEqual(total_share, 100.0, places=1)

    def test_cron_refresca_mes_en_curso(self):
        """El cron semanal recalcula el mes EN CURSO (sin esperar al cierre),
        así el reporte no requiere 'Recalcular' a mano entre cierres."""
        from datetime import date as _d
        self.Costo.cron_recompute_current_month()
        today = _d.today()
        recs = self.Costo.search([('period', '=', _d(today.year, today.month, 1))])
        self.assertTrue(recs, 'debe existir el mes en curso tras el cron')

    def test_recompute_year_todos_los_meses(self):
        """Recalcular año en curso genera filas de varios meses (enero → mes
        actual), no sólo uno — para ver el reporte del año completo."""
        from datetime import date as _d
        today = _d.today()
        self.Costo.action_recompute_year(today.year)
        periods = self.Costo.search([]).mapped('period')
        meses = {p.month for p in periods if p and p.year == today.year}
        # Al menos enero y el mes en curso (si estamos en enero, sólo uno).
        self.assertIn(1, meses)
        if today.month > 1:
            self.assertGreaterEqual(len(meses), 2,
                                    'debe cubrir varios meses del año')

    def test_comparador_productos(self):
        """El comparador pone productos lado a lado: uno con fila del período
        (del reporte) y uno sin ventas (costo en vivo)."""
        period = date.today().replace(day=1)
        self.Costo.action_recompute_period(period)
        wiz = self.env['qb.comparador.wizard'].create({
            'period': period,
            'product_ids': [(6, 0, [self.tela.id, self.importado.id])],
        })
        html = wiz.comparativa_html
        self.assertIn('Costo variable', html)
        self.assertIn('Costo absorbido', html)
        self.assertIn('Precio sugerido', html)
        self.assertIn(self.tela.default_code, html)
        self.assertIn(self.importado.default_code, html)
        # Con margen objetivo la fila cambia de etiqueta y muestra el precio
        wiz.margen_objetivo = 30.0
        self.assertIn('margen 30%', wiz.comparativa_html)
        # El precio sugerido de la tela deja ~30% neto (no clampeado por piso)
        m = wiz._metrics(self.tela)
        self.assertAlmostEqual(m['sug_neto'], 30.0, delta=0.5)
        # Menos de 2 productos → mensaje, no tabla
        wiz.product_ids = [(6, 0, [self.tela.id])]
        self.assertIn('al menos 2', wiz.comparativa_html)

    def test_reporte_revenue_en_mxn_no_divisa_cruda(self):
        """El reporte toma el revenue de aml.balance (MXN), NO de
        price_subtotal (moneda del documento): una factura en EUR entra con su
        valor REAL en pesos y marca la divisa. Antes sumaba euros crudos contra
        pesos y el precio salía basura (~1/TC)."""
        journal = self.env['account.journal'].search(
            [('type', '=', 'sale')], limit=1)
        if not journal:
            self.skipTest('sin plan contable en la DB de test')
        period = date.today().replace(day=1)
        eur = self.env.ref('base.EUR')
        eur.active = True
        self.env['res.currency.rate'].create({
            'currency_id': eur.id,
            'rate': 0.05,  # 1 cía = 0.05 EUR → 1 EUR = 20 cía
            'name': period,
        })
        partner = self.env['res.partner'].create({'name': 'Cliente Export'})
        move = self.env['account.move'].create({
            'move_type': 'out_invoice',
            'partner_id': partner.id,
            'currency_id': eur.id,
            'invoice_date': period,
            'invoice_line_ids': [(0, 0, {
                'product_id': self.tela.id,
                'quantity': 100,
                'price_unit': 5.0,   # 5 EUR = 100 cía por unidad
            })],
        })
        move.action_post()

        sales = self.Costo._sales_by_product(period)
        venta = sales[self.tela.id]
        qty, revenue, divisa = (venta['qty'], venta['revenue'],
                                venta['divisas'])
        self.assertAlmostEqual(qty, 100.0, places=2)
        # 100 u × 5 EUR × 20 = 10,000 en moneda cía — NO 500 (el crudo EUR)
        self.assertAlmostEqual(revenue, 10000.0, places=0,
                               msg='revenue debe venir de balance (MXN)')
        self.assertIn('EUR', divisa, 'debe marcar la divisa de la factura')

        # El recompute deja precio_prom en moneda cía y puebla divisa_venta
        self.Costo.action_recompute_period(period)
        rec = self.Costo.search([('period', '=', period),
                                 ('product_id', '=', self.tela.id)])
        self.assertAlmostEqual(rec.precio_prom, 100.0, places=0)
        self.assertIn('EUR', rec.divisa_venta)
        self.assertEqual(rec.company_currency_id,
                         self.env.company.currency_id)
        # …y ADEMÁS el precio en la divisa original, sin convertir: 5 EUR/u
        self.assertEqual(rec.divisa_id, eur)
        self.assertAlmostEqual(rec.qty_divisa, 100.0, places=2)
        self.assertAlmostEqual(rec.ventas_total_divisa, 500.0, places=2)
        self.assertAlmostEqual(rec.precio_prom_divisa, 5.0, places=2)
        self.assertAlmostEqual(rec.tc_prom, 20.0, places=2)
        # Ventas del período = el facturado real en pesos
        self.assertAlmostEqual(rec.ventas_total, 10000.0, places=0)

    def test_ajuste_de_metros_sale_del_denominador(self):
        """Encogimiento y estiramiento no destruyen ni crean material: la
        misma tela mide menos o mide más.

        Los dos ocurren después de la orden, en Inspección, así que el
        denominador los ignora. El encogimiento tiene que restarse —el pool
        sí se gastó pero se recupera sobre menos metros vendibles— y el
        estiramiento sumarse, por la razón inversa. Si sólo se resta uno, la
        corrección queda sesgada hacia arriba.
        """
        Costo = self.env['qb.costo.producto']
        desde, hasta = date(2026, 1, 1), date(2026, 9, 1)

        # La consulta corre contra la BD real: lo que se prueba es que no
        # truene y devuelva metros por mes (el riesgo del cambio es el SQL).
        ajuste = Costo._ajuste_metros_by_month(desde, hasta)
        self.assertIsInstance(ajuste, dict)
        for mes, metros in ajuste.items():
            self.assertIsInstance(metros, float)

        # El signo lo da la dirección del movimiento, no una constante: el
        # ajuste puede ser negativo si en un mes el estiramiento supera al
        # encogimiento, y entonces devuelve metros al denominador.
        estira = self.env['stock.picking.type'].with_context(
            active_test=False).search(
                [('sequence_code', 'like', 'OP-EST')])
        encoge = self.env['stock.picking.type'].with_context(
            active_test=False).search(
                [('sequence_code', 'like', 'ENC')])
        # 'EST' a secas matchearía 'DEST-' (DESTRUCCIÓN), que es material
        # perdido de verdad y no debe entrar al ajuste.
        self.assertFalse(
            estira.filtered(lambda t: 'DEST' in (t.sequence_code or '')))
        self.assertFalse(encoge & estira)

        acabado = self.env.ref('qb_capacidad_costeo.centro_acabado')
        base = Costo._production_month_avg(acabado, desde, hasta)

        # Descontar más metros de los producidos no puede dar un denominador
        # negativo: los meses que quedan en cero o abajo se excluyen del
        # promedio, igual que los meses sin producción.
        exagerado = Costo._production_month_avg(
            acabado, desde, hasta,
            restar_by_month={date(2026, m, 1): 1e12 for m in range(1, 9)})
        self.assertGreaterEqual(exagerado, 0.0)
        self.assertLessEqual(exagerado, base if base else 0.0)

        # Un ajuste negativo (estiramiento neto) sube el denominador, no lo
        # baja: es la mitad de la simetría que este cambio agrega.
        if base:
            devuelto = Costo._production_month_avg(
                acabado, desde, hasta,
                restar_by_month={date(2026, m, 1): -1.0
                                 for m in range(1, 9)})
            self.assertGreaterEqual(devuelto, base)

        # Sin resta, el resultado es idéntico al de la firma de 3 argumentos:
        # el parámetro es opcional y no altera el camino existente.
        self.assertEqual(
            Costo._production_month_avg(acabado, desde, hasta,
                                        restar_by_month={}),
            base)

    def test_absorcion_resta_solo_lo_que_el_pool_todavia_trae(self):
        """La resta del absorbido quita del pool lo que ese pool contiene del
        centro, ni un peso más.

        Cuando un centro pasa a absorción, tres mecanismos distintos le sacan
        su costo al pool y sólo uno de ellos es la resta del abono:

          · `excluir_centros` saca las cuentas ETIQUETADAS al centro,
          · `renta_centros` saca su renta contractual,
          · la resta del abono a «costos fabriles aplicados» tiene que sacar
            el REMANENTE — lo que el centro aportaba por cuentas sin etiquetar.

        La tarifa por hora capitaliza el costo COMPLETO del centro, así que
        restar el abono entero quitaba las dos primeras partidas por segunda
        vez. Con datos de producción eran ~$463k/mes: la renta contractual de
        TEJIDO ($284,269) más sus energéticos y agujados ($179k), etiquetados
        a su centro. El `max(…, 0)` no lo veía porque el pool seguía positivo.
        """
        journal = self.env['account.journal'].search(
            [('type', '=', 'general')], limit=1)
        if not journal:
            self.skipTest('sin plan contable en la DB de test')
        Centro = self.env['qb.costeo.centro']
        Clase = self.env['qb.costeo.cuenta.class']
        Account = self.env['account.account']
        period = date(2027, 3, 1)

        # Componentes conocidos, uno por comportamiento
        etiquetada, sin_etiqueta = 30000.0, 200000.0  # overhead fabril
        deprec, renta_gl, renta_contractual = 12000.0, 44000.0, 80000.0
        absorbido = 140000.0                          # lo que Odoo capitalizó

        centro = Centro.create({
            'code': 'TEST_ABS_NETO', 'name': 'Centro absorbido de prueba',
            'nature': 'fabril_directo', 'driver_principal': 'peso',
            'renta_contractual_mxn': renta_contractual})
        # Centro sin costo alguno: sirve para fijar la MISMA ventana fabril en
        # los dos escenarios (la ventana arranca en el corte de absorción), de
        # forma que la única diferencia medida sea el régimen del centro real.
        neutro = Centro.create({
            'code': 'TEST_ABS_NEUTRO', 'name': 'Centro neutro de prueba',
            'nature': 'fabril_indirecto', 'driver_principal': 'largo',
            'modo_costeo': 'absorcion_odoo', 'fecha_absorcion': period})

        def cuenta(code, name, bucket, **kw):
            acc = Account.create({'name': name, 'code': code,
                                  'account_type': 'expense_direct_cost'})
            Clase.create(dict(account_id=acc.id, bucket=bucket, **kw))
            return acc

        c_etq = cuenta('QBAB.0001', 'OVERHEAD DEL CENTRO TEST',
                       'overhead_fab', centro_id=centro.id)
        c_sin = cuenta('QBAB.0002', 'OVERHEAD GENERICO TEST', 'overhead_fab')
        c_dep = cuenta('QBAB.0003', 'DEPRECIACION TEST', 'depreciacion')
        c_ren = cuenta('QBAB.0004', 'RENTA DEL LOCAL TEST', 'no_costeo')
        c_abs = cuenta('QBAB.0005', 'COSTOS FABRILES APLICADOS TEST',
                       'absorcion_odoo')
        contra = Account.create({'name': 'CONTRA ABS TEST', 'code': 'QBAB.0009',
                                 'account_type': 'expense'})
        cargos = etiquetada + sin_etiqueta + deprec + renta_gl
        self.env['account.move'].create({
            'move_type': 'entry', 'journal_id': journal.id, 'date': period,
            'line_ids': [
                (0, 0, {'account_id': c_etq.id, 'debit': etiquetada}),
                (0, 0, {'account_id': c_sin.id, 'debit': sin_etiqueta}),
                (0, 0, {'account_id': c_dep.id, 'debit': deprec}),
                (0, 0, {'account_id': c_ren.id, 'debit': renta_gl}),
                (0, 0, {'account_id': c_abs.id, 'credit': absorbido}),
                (0, 0, {'account_id': contra.id, 'debit': absorbido,
                        'credit': 0.0}),
                (0, 0, {'account_id': contra.id, 'credit': cargos}),
            ]}).action_post()

        # Escenario A: el centro real sigue en capa (sólo el neutro absorbe)
        en_capa = self.Costo._compute_factores(period).fab_pool_month

        # Escenario B: el centro real pasa a absorción
        centro.write({'modo_costeo': 'absorcion_odoo',
                      'fecha_absorcion': period})
        f = self.Costo._compute_factores(period)

        # El bruto es el hecho contable: el abono a la cuenta, tal cual
        self.assertAlmostEqual(f.absorcion_bruta_month, absorbido, places=2)
        # Ya fuera = cuentas etiquetadas al centro + su renta contractual.
        # Ni la depreciación ni la renta del GL entran: la primera sigue
        # DENTRO del pool (no tiene centro) y la segunda nunca estuvo en él
        # (`no_costeo`). Meterlas aquí subvaluaría la resta.
        self.assertAlmostEqual(f.absorcion_ya_fuera_month,
                               etiquetada + renta_contractual, places=2)
        # Y la resta neta es el remanente, no el abono entero
        self.assertAlmostEqual(
            f.absorcion_pool_month,
            absorbido - etiquetada - renta_contractual, places=2)

        # La consecuencia que importa: el pool cae por el mismo importe con
        # el centro etiquetado que sin etiquetar. Restar el abono completo lo
        # dejaba $110,000 más abajo (la etiquetada + la renta contractual) y
        # ese hueco lo pagaban los centros que siguen en capa.
        self.assertAlmostEqual(f.fab_pool_month, en_capa, places=2)

        (centro | neutro).unlink()

    def test_absorcion_neta_no_baja_de_cero_y_lo_avisa(self):
        """Si la tarifa absorbe MENOS que lo que el centro ya tenía
        etiquetado, la resta neta se queda en 0 —nunca devuelve dinero al
        pool— y el bruto queda guardado para que el panel lo pueda avisar."""
        journal = self.env['account.journal'].search(
            [('type', '=', 'general')], limit=1)
        if not journal:
            self.skipTest('sin plan contable en la DB de test')
        Centro = self.env['qb.costeo.centro']
        Clase = self.env['qb.costeo.cuenta.class']
        Account = self.env['account.account']
        period = date(2027, 4, 1)

        centro = Centro.create({
            'code': 'TEST_ABS_CORTA', 'name': 'Centro absorbido corto',
            'nature': 'fabril_directo', 'driver_principal': 'peso',
            'renta_contractual_mxn': 90000.0,
            'modo_costeo': 'absorcion_odoo', 'fecha_absorcion': period})
        c_abs = Account.create({
            'name': 'COSTOS FABRILES APLICADOS CORTO TEST',
            'code': 'QBAC.0001', 'account_type': 'expense_direct_cost'})
        Clase.create({'account_id': c_abs.id, 'bucket': 'absorcion_odoo'})
        contra = Account.create({'name': 'CONTRA CORTO TEST',
                                 'code': 'QBAC.0009', 'account_type': 'expense'})
        self.env['account.move'].create({
            'move_type': 'entry', 'journal_id': journal.id, 'date': period,
            'line_ids': [
                (0, 0, {'account_id': c_abs.id, 'credit': 10000.0}),
                (0, 0, {'account_id': contra.id, 'debit': 10000.0}),
            ]}).action_post()

        f = self.Costo._compute_factores(period)
        self.assertAlmostEqual(f.absorcion_bruta_month, 10000.0, places=2)
        self.assertAlmostEqual(f.absorcion_ya_fuera_month, 90000.0, places=2)
        self.assertEqual(f.absorcion_pool_month, 0.0)
        centro.unlink()

    def test_los_totales_cumplen_ventas_menos_costo_igual_margen(self):
        """En TODA fila: ventas_total − costo_X_total = margen_X_total.

        La identidad se rompía en las filas con cantidad neta ≤ 0 —cuando las
        devoluciones del período superan a las ventas—. Esa fila se trata
        como «sin ventas» para no generar un precio negativo que envenene los
        márgenes unitarios y las alertas, pero `ventas_total` sí conserva el
        ingreso negativo, que es el hecho contable. Los totales de margen se
        calculaban desde `precio × qty`, así que salían en 0 contra un
        ingreso que no era 0.

        No es un detalle: 11 filas metieron $561,866 de residuo en la
        conciliación entre enero y julio de 2026 —marzo solo, $242,363— y ese
        residuo no correspondía a ninguna causa real. Se leía como gasto sin
        explicar.
        """
        Costo = self.Costo
        period = date(2027, 6, 1)
        factores = self.env['qb.costo.factores'].create({
            'period': period, 'window_months': 12,
            'factor_fab_kg': 30.0, 'factor_fab_m': 3.0,
            'energia_por_kg': 4.0, 'op_pct': 0.18})
        ctx = Costo._engine_ctx([self.tela.id], factores)

        def fila(qty, revenue):
            vals, _f = Costo._compute_product_vals(
                self.tela, period, factores,
                {self.tela.id: {'qty': qty, 'revenue': revenue}},
                ctx, self.Ruteo, self.Peso)
            return vals

        for qty, revenue, caso in (
                (1000.0, 50000.0, 'venta normal'),
                (0.0, 0.0, 'sin movimiento'),
                (500.0, 0.0, 'muestra sin cargo'),
                (-2.0, -242363.52, 'devolución neta'),
                (-5716.8, -157560.37, 'devolución neta de volumen')):
            v = fila(qty, revenue)
            self.assertAlmostEqual(
                v['ventas_total'] - v['costo_absorbido_total'],
                v['margen_neto_total'], places=2, msg=caso)
            self.assertAlmostEqual(
                v['ventas_total'] - v['costo_produccion_total'],
                v['margen_bruto_total'], places=2, msg=caso)
            self.assertAlmostEqual(
                v['ventas_total'] - v['costo_variable_total'],
                v['contrib_total'], places=2, msg=caso)

        # Y la venta normal no cambia de valor: el arreglo es una identidad
        # algebraica para toda fila con precio válido, no un criterio nuevo.
        v = fila(1000.0, 50000.0)
        self.assertAlmostEqual(
            v['margen_neto_total'],
            (v['precio_prom'] - v['costo_absorbido']) * 1000.0, places=2)

        # La devolución neta sigue SIN precio ni margen unitario: el guard
        # que evita el precio negativo se conserva, es solo el total el que
        # ahora refleja el ingreso.
        d = fila(-2.0, -242363.52)
        self.assertEqual(d['precio_prom'], 0.0)
        self.assertEqual(d['margen_absorbido'], 0.0)
        self.assertEqual(d['costo_absorbido_total'], 0.0)
        self.assertAlmostEqual(d['margen_neto_total'], -242363.52, places=2)

    def test_patron_amplio_con_una_renta_dentro_no_se_marca(self):
        """Una clase de PATRÓN que abarca una cuenta de renta entre muchas
        que no lo son NO se marca `es_renta`.

        La bandera vive en la clase y el motor saca del pool todo lo que la
        clase abarca. Con la regla vieja —marcar si ALGUNA cuenta era renta—
        bastaba que `504.01%` incluyera a `504.01.0008 RENTA DEL LOCAL` para
        que los cuarenta gastos de overhead de fábrica salieran del pool. En
        producción quedaron marcadas 38 cuentas fabriles, de las que una sola
        era renta de inmueble: el motor sacaba $1,534,140/mes que nada
        reponía.
        """
        Clase = self.env['qb.costeo.cuenta.class']
        Account = self.env['account.account']

        def cuenta(name, code):
            return Account.create({'name': name, 'code': code,
                                   'account_type': 'expense_direct_cost'})

        renta = cuenta('RENTA DEL LOCAL TEST', 'QBRP.01.0008')
        mtto = cuenta('MANTENIMIENTOS FABRICA TEST', 'QBRP.01.0005')
        herr = cuenta('HERRAMIENTAS Y EQUIPO MENOR TEST', 'QBRP.01.0042')
        patron = Clase.create({'code_pattern': 'QBRP.01%',
                               'bucket': 'overhead_fab'})
        self.assertEqual(set(patron.account_ids.ids),
                         {renta.id, mtto.id, herr.id})

        Clase.marcar_cuentas_de_renta()
        self.assertFalse(
            patron.es_renta,
            'marcar el patrón sacaría del pool el mantenimiento y las '
            'herramientas, que nada repone')
        # Y el panel lo dice, en vez de pedir que se marque
        self.assertIn(patron, Clase.clases_con_renta_mezclada())

        # La salida correcta: la cuenta de renta con clasificación propia.
        # Gana por más específica, y esa SÍ se marca.
        propia = Clase.create({'account_id': renta.id,
                               'bucket': 'no_costeo'})
        Clase.marcar_cuentas_de_renta()
        self.assertTrue(propia.es_renta)
        self.assertFalse(patron.es_renta)
        # Y el aviso se apaga: la cuenta de renta ya no la resuelve el
        # patrón, aunque su `code_pattern` la siga matcheando. Mirar
        # `account_ids` en vez de lo resuelto dejaba el aviso prendido para
        # siempre, mandando a arreglar algo que ya estaba bien — es lo que
        # pasó en producción con `504.01%` y `504.01.0008 RENTA DEL LOCAL`.
        self.assertIn(renta, patron.account_ids,
                      'el patrón la sigue matcheando...')
        self.assertNotIn(patron, Clase.clases_con_renta_mezclada(),
                         '...pero ya no se la queda, y el aviso se apaga')

    def test_arrendamiento_de_maquinaria_nunca_sale_del_pool(self):
        """El arrendamiento de maquinaria es costo de producción: no hay renta
        contractual de centro que lo sustituya, así que sacarlo del pool es
        quitarlo y ya.

        El reconocedor mira el NOMBRE de la cuenta, y una cuenta que se llama
        solo «ARRENDAMIENTO FINANCIERO» matchea sin decir maquinaria por
        ningún lado. En producción eso sacó $867,721/mes del pool fabril. El
        bucket manda sobre el nombre: `arrend_maquinaria` nunca es renta de
        inmueble.
        """
        Clase = self.env['qb.costeo.cuenta.class']
        Account = self.env['account.account']
        generica = Account.create({
            'name': 'ARRENDAMIENTO FINANCIERO TEST', 'code': 'QBAF.11.0001',
            'account_type': 'expense_direct_cost'})
        # El reconocedor por nombre sí matchea: no dice «maquinaria»
        self.assertTrue(Clase._es_cuenta_de_renta(generica))

        clase = Clase.create({'code_pattern': 'QBAF.11%',
                              'bucket': 'arrend_maquinaria'})
        Clase.marcar_cuentas_de_renta()
        self.assertFalse(clase.es_renta, 'el bucket manda sobre el nombre')
        self.assertNotIn(clase, Clase.clases_con_renta_mezclada())

    def test_marcar_rentas_tambien_desmarca(self):
        """`marcar_cuentas_de_renta` corre la regla VIGENTE sobre todas las
        clases: si una quedó marcada por una regla anterior, la desmarca. Sin
        eso, arreglar la regla no arregla los datos."""
        Clase = self.env['qb.costeo.cuenta.class']
        Account = self.env['account.account']
        mtto = Account.create({'name': 'MANTENIMIENTOS TEST', 'code': 'QBDM.01',
                               'account_type': 'expense_direct_cost'})
        clase = Clase.create({'account_id': mtto.id, 'bucket': 'overhead_fab'})
        clase.es_renta = True          # como lo dejó la regla vieja
        Clase.marcar_cuentas_de_renta()
        self.assertFalse(clase.es_renta)

    def test_la_aduana_de_una_maquina_no_la_paga_el_hilo(self):
        """El activo fijo y los servicios se quedan en la BASE del factor de
        importación, pero nunca reciben el recargo.

        Quedarse en la base es correcto: su pedimento existe y diluye el
        factor. Recibir el recargo no lo sería: una máquina se deprecia, no se
        vende, y un seguro no tiene inventario que cargar. Esa parte del pool
        se queda en resultados a propósito.

        En producción la ventana sep-2025/ago-2026 traía una ROPE OPENER AND
        SLITTING LINE de €95,000 y una decena de seguros, fletes y licencias
        dentro del conjunto que recibe recargo.
        """
        Costo = self.Costo
        Categ = self.env['product.category']
        Product = self.env['product.product']
        fijo = Categ.create({'name': 'Maquinaria', 'parent_id': Categ.create(
            {'name': 'Activo Fijo'}).id})
        mp = Categ.create({'name': 'Hilo QB TEST'})

        maquina = Product.create({
            'name': 'ROPE OPENER TEST', 'is_storable': True,
            'categ_id': fijo.id})
        seguro = Product.create({
            'name': 'SEGURO TEST', 'type': 'service', 'categ_id': mp.id})
        hilo = Product.create({
            'name': 'HILO IMPORTADO TEST', 'is_storable': True,
            'categ_id': mp.id})

        self.assertTrue(Costo._es_importado_costeable(hilo))
        self.assertFalse(Costo._es_importado_costeable(maquina),
                         'una máquina se deprecia, no se vende')
        self.assertFalse(Costo._es_importado_costeable(seguro),
                         'un servicio no tiene inventario que cargar')

    def test_base_de_importacion_separa_lo_costeable(self):
        """La base total incluye todo lo importado; la costeable, solo lo que
        puede recibir el recargo. Las dos se promedian sobre los MISMOS meses,
        o el porcentaje entre ellas mentiría."""
        base, ids, costeable = self.Costo._import_purchase_base(
            date(2025, 9, 1), date(2026, 9, 1))
        self.assertGreaterEqual(base, costeable,
                                'lo costeable es un subconjunto de la base')
        self.assertGreaterEqual(costeable, 0.0)
        Product = self.env['product.product']
        for pid in list(ids)[:25]:
            self.assertTrue(
                self.Costo._es_importado_costeable(Product.browse(pid)),
                'solo productos costeables reciben el recargo')
        if not base:
            self.skipTest('sin compras de importación en la ventana')

    def test_wizard_de_recalculo_cubre_el_rango_y_respeta_lo_cerrado(self):
        """El asistente de rango es lo que permite ver años anteriores.

        El motor siempre supo costear cualquier período, pero desde la UI solo
        se podía pedir el mes anterior o el año EN CURSO: el menú llamaba
        `action_recompute_year()` sin argumento. Para ver 2025 había que entrar
        al shell, así que en la práctica no se veía.
        """
        Wizard = self.env['qb.recalculo.wizard']
        w = Wizard.create({'desde': date(2025, 3, 1), 'hasta': date(2025, 6, 30)})
        # El rango incluye los dos extremos y normaliza al día 1
        self.assertEqual(
            w._meses(),
            [date(2025, m, 1) for m in (3, 4, 5, 6)])

        # Un solo mes es un rango válido de un elemento
        self.assertEqual(
            Wizard.create({'desde': date(2025, 3, 15),
                           'hasta': date(2025, 3, 20)})._meses(),
            [date(2025, 3, 1)])

        # Al revés no: es casi siempre un dedazo, y correrlo daría cero meses
        # en silencio en vez de avisar.
        with self.assertRaises(UserError):
            Wizard.create({'desde': date(2025, 6, 1), 'hasta': date(2025, 3, 1)})

        # Por defecto propone el año ANTERIOR completo, que es justo lo que el
        # menú de «año en curso» no alcanza.
        d = Wizard.default_get(['desde', 'hasta'])
        self.assertEqual(d['desde'].month, 1)
        self.assertEqual(d['hasta'].month, 12)
        self.assertEqual(d['desde'].year, date.today().year - 1)
        self.assertEqual(d['hasta'].year, date.today().year - 1)

    def test_wizard_de_recalculo_no_pisa_un_periodo_cerrado(self):
        """Un período cerrado se congeló a propósito: el asistente lo salta y
        lo dice, en vez de reescribir un número que ya se reportó."""
        period = date(2027, 9, 1)
        factores = self.env['qb.costo.factores'].create({
            'period': period, 'window_months': 12,
            'factor_fab_kg': 30.0, 'factor_fab_m': 3.0})
        factores.action_cerrar()
        self.assertEqual(factores.state, 'cerrado')

        w = self.env['qb.recalculo.wizard'].create({
            'desde': period, 'hasta': period})
        w.action_recalcular()
        self.assertIn('0 meses recalculados', w.resultado)
        self.assertIn('2027-09', w.resultado)
        self.assertIn('CERRADOS', w.resultado)

    def test_la_poliza_de_cierre_anual_no_entra_a_los_pools(self):
        """El asiento de CIERRE ANUAL reversa las cuentas de resultados del año
        entero contra una sola póliza de diciembre. En producción es
        `Dr/2025/12/32`, «POLIZA DE CIERRE ANUAL», $190,684,760.

        Dejarla dentro hace dos daños: la conciliación de diciembre sale sin
        sentido (−$163M de "ventas", −$147M de "gasto") y el promedio de cada
        pool pierde diciembre entero, porque `_smooth` descarta el mes por
        salir negativo. Cada año que se quiera ver pierde un mes real.
        """
        journal = self.env['account.journal'].search(
            [('type', '=', 'general')], limit=1)
        if not journal:
            self.skipTest('sin plan contable en la DB de test')
        Clase = self.env['qb.costeo.cuenta.class']
        Account = self.env['account.account']
        mes = date(2027, 11, 1)

        cuenta = Account.create({
            'name': 'OVERHEAD CIERRE TEST', 'code': 'QBCI.0001',
            'account_type': 'expense_direct_cost'})
        Clase.create({'account_id': cuenta.id, 'bucket': 'overhead_fab'})
        contra = Account.create({'name': 'CONTRA CIERRE TEST',
                                 'code': 'QBCI.0009',
                                 'account_type': 'expense'})

        def poliza(monto, ref=False):
            self.env['account.move'].create({
                'move_type': 'entry', 'journal_id': journal.id,
                'date': mes, 'ref': ref,
                'line_ids': [
                    (0, 0, {'account_id': cuenta.id,
                            'debit': monto if monto > 0 else 0.0,
                            'credit': -monto if monto < 0 else 0.0}),
                    (0, 0, {'account_id': contra.id,
                            'credit': monto if monto > 0 else 0.0,
                            'debit': -monto if monto < 0 else 0.0}),
                ]}).action_post()

        poliza(400000.0)                                 # el gasto real del mes
        Costo = self.Costo
        solo_real = Costo._pool_by_month(
            ('overhead_fab',), mes, mes + relativedelta(months=1),
            es_variable=False).get(mes, 0.0)

        # Y ahora el cierre, que reversa mucho más que el mes
        poliza(-9000000.0, ref='POLIZA DE CIERRE ANUAL')
        con_cierre = Costo._pool_by_month(
            ('overhead_fab',), mes, mes + relativedelta(months=1),
            es_variable=False).get(mes, 0.0)

        self.assertAlmostEqual(
            con_cierre, solo_real, places=2,
            msg='la póliza de cierre no debe mover el pool del mes')
        self.assertGreater(
            con_cierre, 0.0,
            'y el mes conserva su gasto real en vez de irse a negativo y '
            'caerse del promedio')

    def test_periodo_con_ventana_sin_produccion_se_marca_no_comparable(self):
        """Un período cuya ventana produjo muy por debajo de la capacidad NO
        es comparable con uno normal, y el reporte tiene que decirlo.

        La fabricación se divide entre capacidad normal, así que no se mueve
        con la producción. La ENERGÍA sí: es variable y se divide entre los
        kilos REALES, que es lo correcto físicamente. Pero si los kilos de la
        ventana están muy abajo, su $/kg se infla en esa proporción y el
        producto sale caro por una razón que no es su costo.

        Medido en producción: enero-2024 dio energía a $34.22/kg y
        diciembre-2024 a $11.09/kg —3.1×— porque la ventana de los primeros
        meses cae en 2023, cuando las órdenes todavía no se registraban en
        Odoo (372 en todo 2023 contra 4,715 en 2024). El margen de esos meses
        salía negativo por eso, no por el negocio.
        """
        Costo = self.Costo
        Factores = self.env['qb.costo.factores']
        Config = self.env['qb.costeo.factor.config']
        parcial = Config.get_param('utilizacion_min_comparable', 0.70)
        mala = Config.get_param('utilizacion_min_utilizable', 0.40)
        self.assertGreater(parcial, mala, 'la banda debe ir de menor a mayor')

        # Los períodos ya calculados de la DB: si alguno tiene la ventana
        # floja, tiene que estar marcado; si está a capacidad, no.
        for f in Factores.search([], limit=25):
            util = (f.utilizacion_pond_pct or 0.0) / 100.0
            if not util:
                continue
            if util < mala:
                self.assertEqual(f.confiabilidad, 'mala', f.period)
            elif util < parcial:
                self.assertEqual(f.confiabilidad, 'parcial', f.period)
            else:
                self.assertEqual(f.confiabilidad, 'ok', f.period)
            # Y el marcado siempre viene con su explicación, o sin ella
            self.assertEqual(bool(f.confiabilidad_detalle),
                             f.confiabilidad != 'ok', f.period)

        # La ponderada usa el share, no solo los kilos: cuando un lado se
        # queda sin centros en capa su share es 0, y mirar solo la de kg
        # marcaría el período como malo sin serlo.
        period = date(2027, 10, 1)
        f = Costo._compute_factores(period)
        ws = f.fab_weight_share
        esperado = (ws * (f.utilizacion_kg_pct or 0.0)
                    + (1 - ws) * (f.utilizacion_m_pct or 0.0))
        self.assertAlmostEqual(f.utilizacion_pond_pct, esperado, places=4)

    def test_filtro_de_etiqueta_deja_pasar_solo_la_merma(self):
        """Una cuenta que mezcla naturalezas deja pasar al pool solo las
        líneas cuyo concepto lo diga.

        `501.01.02 COSTO POR AJUSTES A CANTIDAD` junta la merma real
        —etiquetada `SP/`, el scrap de Odoo— con embarques (`TL/EMB/`),
        encogimiento (`TL/ENC/`) y entradas de refacciones (`TVAR/ENT-REF/`).
        Solo la merma es costo del producto; los ajustes de cantidad no.

        Sin el filtro, un asiento de regularización de $5,822,686 en diciembre
        de 2025 —1,136 scraps sin asiento, revueltos con ajustes— entraba
        entero al ajuste de MP y subía el costo de materia prima de TODOS los
        productos.
        """
        journal = self.env['account.journal'].search(
            [('type', '=', 'general')], limit=1)
        if not journal:
            self.skipTest('sin plan contable en la DB de test')
        Clase = self.env['qb.costeo.cuenta.class']
        Account = self.env['account.account']
        mes = date(2027, 7, 1)

        cuenta = Account.create({
            'name': 'AJUSTES A CANTIDAD TEST', 'code': 'QBSP.0001',
            'account_type': 'expense_direct_cost'})
        contra = Account.create({'name': 'CONTRA SP TEST', 'code': 'QBSP.0009',
                                 'account_type': 'expense'})
        clase = Clase.create({'account_id': cuenta.id, 'bucket': 'mp'})

        self.env['account.move'].create({
            'move_type': 'entry', 'journal_id': journal.id, 'date': mes,
            'line_ids': [
                (0, 0, {'account_id': cuenta.id, 'debit': 30000.0,
                        'name': 'SP/10758 - TC210X5.2X0.2X 3.99'}),
                (0, 0, {'account_id': cuenta.id, 'debit': 70000.0,
                        'name': 'TL/EMB/04840 - Entretela no tejida'}),
                (0, 0, {'account_id': cuenta.id, 'debit': 50000.0,
                        'name': 'TL/ENC//00103 - TEJIDO CIRCULAR'}),
                (0, 0, {'account_id': contra.id, 'credit': 150000.0}),
            ]}).action_post()

        ventana = (mes, mes + relativedelta(months=1))
        sin_filtro = self.Costo._pool_by_month(('mp',), *ventana).get(mes, 0.0)
        self.assertAlmostEqual(sin_filtro, 150000.0, places=2,
                               msg='sin filtro entra la cuenta completa')

        clase.filtro_etiqueta = 'SP/'
        con_filtro = self.Costo._pool_by_month(('mp',), *ventana).get(mes, 0.0)
        self.assertAlmostEqual(
            con_filtro, 30000.0, places=2,
            msg='con filtro entra solo la merma, no los ajustes')

        # Vaciarlo lo apaga: el filtro es opt-in, no un default escondido
        clase.filtro_etiqueta = False
        self.assertAlmostEqual(
            self.Costo._pool_by_month(('mp',), *ventana).get(mes, 0.0),
            150000.0, places=2)

    def test_refs_fuera_de_costeo_es_configurable_y_no_inyecta(self):
        """La lista de referencias fuera del costeo sale de un parámetro, y su
        valor se INTERPOLA en el SQL porque `_table_query` no admite
        parámetros. La lista blanca de caracteres es lo que sostiene eso."""
        from odoo.addons.qb_capacidad_costeo.models.cuenta_map import (
            excluir_refs_sql)
        Config = self.env['qb.costeo.factor.config']

        sql = excluir_refs_sql(self.env)
        self.assertNotIn('%', sql, 'un porcentaje rompe el formateo printf')
        self.assertIn('CIERRE ANUAL', sql)
        self.assertIn('ENAJENACI', sql)

        def poner(texto):
            rec = Config.search([('key', '=', 'refs_fuera_de_costeo')], limit=1)
            if rec:
                rec.value_text = texto
            else:
                Config.create({'key': 'refs_fuera_de_costeo',
                               'value_text': texto})

        poner("X'; DROP TABLE x; --")
        sql = excluir_refs_sql(self.env)
        # Lo que hace segura la interpolación es que la comilla no pasa: sin
        # ella el valor no puede salirse de su literal. El punto y coma
        # tampoco, así que no hay forma de encadenar otra sentencia. Un `--`
        # sobrevive y da igual: dentro de comillas es texto, no comentario.
        self.assertNotIn(';', sql,
                         'la lista blanca no deja pasar el punto y coma')
        self.assertEqual(
            sql.count("'") % 2, 0,
            'las comillas quedan balanceadas: el valor no se sale del literal')
        self.assertNotIn('%', sql)

        poner('')
        self.assertEqual(
            excluir_refs_sql(self.env), 'TRUE',
            'vacío = no se excluye nada, y el SQL sigue siendo válido')

    def test_la_baja_de_activo_vendido_no_entra_al_pool(self):
        """La depreciación de un activo que salió no es costo del período
        cuando su reemplazo ya está en el pool.

        En dic-2025 se cargaron $5,827,157 a `504.08.0001 DEPRECIACIÓN
        MAQUINARIA` por dos máquinas —FONGS JET y CIRCULAR INTERLOCK— que se
        vendieron. Tres razones para que no sea costo, y apuntan al mismo lado:

          1. Ya está compensado: `704.23.0003 UTILIDAD EN VENTA DE ACTIVO
             FIJO` trae $5,896,997 el mismo mes, en una cuenta `income_other`
             que el costeo no mira ni debe mirar. El módulo veía media
             operación.
          2. Es un evento único, y el suavizado a 12 meses lo vuelve
             recurrente: $485,596/mes, el 7.8% del pool fabril.
          3. Fue una venta con arrendamiento en reversa. Esas máquinas hoy se
             pagan como renta y la renta YA está en el pool —`701.11%` saltó
             de 10 a 16 contratos justo en dic-2025—. Repartir además su
             depreciación de cuando eran propias le cobra a cada producto la
             misma máquina dos veces.
        """
        journal = self.env['account.journal'].search(
            [('type', '=', 'general')], limit=1)
        if not journal:
            self.skipTest('sin plan contable en la DB de test')
        Clase = self.env['qb.costeo.cuenta.class']
        Account = self.env['account.account']
        mes = date(2027, 8, 1)

        deprec = Account.create({
            'name': 'DEPRECIACION MAQUINARIA TEST', 'code': 'QBEN.0001',
            'account_type': 'expense_direct_cost'})
        Clase.create({'account_id': deprec.id, 'bucket': 'depreciacion'})
        acum = Account.create({'name': 'DEPRE ACUM TEST', 'code': 'QBEN.0002',
                               'account_type': 'asset_fixed'})

        def poliza(monto, ref):
            self.env['account.move'].create({
                'move_type': 'entry', 'journal_id': journal.id,
                'date': mes, 'ref': ref,
                'line_ids': [
                    (0, 0, {'account_id': deprec.id, 'debit': monto}),
                    (0, 0, {'account_id': acum.id, 'credit': monto}),
                ]}).action_post()

        poliza(90000.0, 'DEPRECIACION DEL MES')
        ventana = (mes, mes + relativedelta(months=1))
        normal = self.Costo._pool_by_month(
            ('depreciacion',), *ventana, es_variable=False).get(mes, 0.0)
        self.assertAlmostEqual(normal, 90000.0, places=2)

        poliza(5827156.83, 'REGISTRO ENAJENACIÓN DE ACTIVO MAQUINA')
        con_baja = self.Costo._pool_by_month(
            ('depreciacion',), *ventana, es_variable=False).get(mes, 0.0)
        self.assertAlmostEqual(
            con_baja, 90000.0, places=2,
            msg='la baja del activo vendido no mueve el pool; la '
                'depreciación del mes sí sigue contando')

    def test_la_conciliacion_ve_el_costeo_que_vive_en_otras_cuentas(self):
        """La conciliación filtraba por TIPO de cuenta; el motor filtra por
        BUCKET. Esa asimetría dejaba fuera del mayor gasto que el modelo sí le
        cobraba al producto.

        El caso real: `701.11.0001 ARRENDAMIENTO FINANCIERO` tiene
        `account_type = income_other` pero está en el bucket
        `arrend_maquinaria`. Son las máquinas con las que se produce —una
        venta con arrendamiento en reversa—, así que el modelo lo cobra bien;
        pero el mayor no lo contaba como gasto y la brecha salía baja por ese
        lado: $13,907,465 entre 2025 y 2026.

        Lo demás que vive en `income_other` —cambiaria, intereses, comisiones,
        utilidad en venta de activo— no es costo de producto ni debe serlo,
        pero sí es resultado de la empresa: va en su propia línea para que el
        resultado del mayor sea el de la empresa.
        """
        Concil = self.env['qb.costo.conciliacion']
        campos = Concil._fields
        self.assertIn('gl_otros_costeo', campos)
        self.assertIn('gl_resultado_integral', campos)

        # La identidad que debe cumplirse en toda fila
        for c in Concil.search([], limit=24):
            self.assertAlmostEqual(
                c.gl_gasto_total,
                c.gl_costo_ventas + c.gl_gastos_operacion + c.gl_otros_costeo,
                places=2, msg='%s: el gasto total suma sus tres partes'
                              % c.period)
            self.assertAlmostEqual(
                c.resultado_gl,
                c.gl_ventas - c.gl_gasto_total - c.gl_resultado_integral,
                places=2, msg='%s: el resultado del mayor es ventas menos '
                              'todo lo que se gastó' % c.period)

    def test_el_margen_de_una_cotizacion_sigue_al_precio(self):
        """Editar el precio objetivo de una cotización recalcula margen y
        semáforo. Antes eran floats sueltos que el cotizador escribía una
        vez: al rebajar después el precio, el margen se quedaba con el del
        precio anterior.

        El caso real: una cotización a $16.00 presumía 5.0% de margen cuando
        a ese precio el real era 1.5% — el 5.0% correspondía a $16.72, el
        precio de antes de la rebaja. Prácticamente en el piso, presentada
        como si tuviera colchón.
        """
        cot = self.env['qb.cotizacion'].create({
            'name': 'COT MARGEN VIVO TEST',
            'costo_variable': 6.3254,
            'costo_absorbido_sin_op': 12.9706,
            'op_pct': 17.38926082589547,
            'piso_ocioso': 6.3254,
            'piso_lleno': 15.7009,
            'precio_objetivo': 16.7214,
        })
        self.assertAlmostEqual(cot.margen_neto_pct, 5.04, places=1)
        self.assertEqual(cot.semaforo, 'verde')

        # La rebaja que en producción dejó el margen viejo
        cot.precio_objetivo = 16.0
        self.assertAlmostEqual(
            cot.margen_neto_pct, 1.54, places=1,
            msg='el margen debe seguir al precio, no quedarse con el viejo')
        self.assertAlmostEqual(cot.margen_contribucion, 16.0 - 6.3254,
                               places=4)
        self.assertEqual(cot.semaforo, 'verde')

        # Y el semáforo también es vivo: entre pisos = ámbar, bajo variable
        # = rojo
        cot.precio_objetivo = 10.0
        self.assertEqual(cot.semaforo, 'ambar')
        self.assertLess(cot.margen_neto_pct, 0.0)
        cot.precio_objetivo = 5.0
        self.assertEqual(cot.semaforo, 'rojo')

        # Sin precio objetivo cae al mercado, y sin mercado al piso lleno —
        # el mismo fallback del precio evaluado
        cot.precio_objetivo = 0.0
        cot.precio_mercado = 18.0
        self.assertAlmostEqual(
            cot.margen_bruto_pct, 100.0 * (18.0 - 12.9706) / 18.0, places=2)

    def test_capacidad_sin_datos_no_reprueba(self):
        """Un centro sin workcenters NI turnos no tiene dato de capacidad
        práctica: el check dice «no se puede validar» y NO reprueba.

        Antes caía a `libres = 0` y cualquier volumen reprobaba por ese
        centro: las 15 cotizaciones de agosto salieron «no cabe» porque a
        ACABADO le faltaba 1 hora contra un cero inventado. Un check que
        siempre dice que no es ruido, no una validación.
        """
        Centro = self.env['qb.costeo.centro']
        sin_datos = Centro.create({
            'code': 'TEST_CAP_SIN', 'name': 'Sin datos de capacidad',
            'nature': 'fabril_directo', 'driver_principal': 'largo',
            'std_output_per_hour': 1779.0})
        wiz = self.env['qb.cotizador.wizard'].new({})
        ok, detail = wiz._check_capacity(
            sin_datos, is_kg=False, kg=0.22, m_per_kg=4.5, volumen=2500.0)
        self.assertTrue(ok, 'sin dato de capacidad no se puede reprobar')
        self.assertIn('no se puede validar', detail)
        self.assertNotIn('FALTAN', detail)
        sin_datos.unlink()

    def test_recalculo_diferido_vacia_la_cola_y_apaga_su_cron(self):
        """El cron de recálculo diferido procesa la cola por lotes y se apaga
        solo al terminar.

        Existe porque la migración que recalculaba TODOS los períodos pasó de
        ~80 s (8 períodos) a 5-6 minutos (32, al cargar 2024 y 2025) y todo
        build de migración los pagaba. Ahora la migración recalcula síncrono
        solo el año corriente y difiere la historia a esta cola.
        """
        Config = self.env['qb.costeo.factor.config']
        Costo = self.env['qb.costo.producto']
        cron = self.env.ref('qb_capacidad_costeo.cron_recalculo_pendientes')
        cron.active = True

        # Dos períodos sintéticos en la cola
        pendientes = '2028-03-01,2028-02-01'
        rec = Config.search([('key', '=', 'recalculo_pendiente')], limit=1)
        if rec:
            rec.value_text = pendientes
        else:
            rec = Config.create({'key': 'recalculo_pendiente', 'value': 0,
                                 'value_text': pendientes})

        Costo.cron_recompute_pendientes()

        # La cola quedó vacía, los períodos existen y el cron se apagó solo
        self.assertFalse(rec.value_text)
        Factores = self.env['qb.costo.factores']
        self.assertTrue(Factores.search_count(
            [('period', '=', date(2028, 3, 1))]))
        self.assertTrue(Factores.search_count(
            [('period', '=', date(2028, 2, 1))]))
        self.assertFalse(cron.active, 'sin pendientes, el cron se apaga solo')

        # Con la cola vacía es un no-op inofensivo
        Costo.cron_recompute_pendientes()
        self.assertFalse(rec.value_text)

    def test_recalculo_diferido_respeta_el_lote(self):
        """Un lote procesa a lo más 6 períodos y deja el resto en la cola,
        con el cron todavía prendido."""
        Config = self.env['qb.costeo.factor.config']
        cron = self.env.ref('qb_capacidad_costeo.cron_recalculo_pendientes')
        cron.active = True
        # 7 períodos: 6 entran al lote, 1 se queda
        meses = ['2028-%02d-01' % m for m in range(11, 4, -1)]
        rec = Config.search([('key', '=', 'recalculo_pendiente')], limit=1)
        if rec:
            rec.value_text = ','.join(meses)
        else:
            rec = Config.create({'key': 'recalculo_pendiente', 'value': 0,
                                 'value_text': ','.join(meses)})
        self.env['qb.costo.producto'].cron_recompute_pendientes()
        self.assertEqual(rec.value_text, '2028-05-01',
                         'el séptimo se queda para el siguiente lote')
        self.assertTrue(cron.active,
                        'con pendientes, el cron sigue prendido')

    def test_historial_de_revisiones_encadena_al_crear(self):
        """Cotizar el MISMO producto al MISMO cliente otra vez crea la
        revisión siguiente, liga a la anterior y reemplaza solo ofertas
        VIVAS (borrador/presentada); una ganada o perdida es historia del
        trato y conserva su estado."""
        partner = self.env['res.partner'].create({'name': 'CLIENTE REV TEST'})
        Cot = self.env['qb.cotizacion']
        base = {'partner_id': partner.id, 'product_id': self.tela.id,
                'costo_variable': 6.0, 'costo_absorbido_sin_op': 12.0,
                'op_pct': 15.0, 'piso_ocioso': 6.0, 'piso_lleno': 14.0}
        a = Cot.create(dict(base, name='REV A', precio_objetivo=16.0))
        self.assertEqual(a.revision, 1)
        self.assertFalse(a.revision_anterior_id)

        b = Cot.create(dict(base, name='REV B', precio_objetivo=15.5))
        self.assertEqual(b.revision, 2)
        self.assertEqual(b.revision_anterior_id, a)
        self.assertEqual(a.state, 'superseded',
                         'el borrador anterior queda reemplazado')
        self.assertEqual(a.revision_siguiente_ids, b)

        # Una GANADA no se reemplaza al recotizar: es historia del trato
        b.action_marcar_ganada()
        c = Cot.create(dict(base, name='REV C', precio_objetivo=15.0))
        self.assertEqual(c.revision, 3)
        self.assertEqual(c.revision_anterior_id, b)
        self.assertEqual(b.state, 'won',
                         'una ganada conserva su estado al ser recotizada')
        self.assertEqual(a.historial_count, 3)
        self.assertEqual(c.historial_count, 3)

        # Folio estilo formato viejo: año + consecutivo («20260096»)
        self.assertEqual(c.folio,
                         '%s%04d' % (c.create_date.year, c.id))

        # El smart button abre la cadena completa cliente+producto
        accion = c.action_ver_historial()
        registros = Cot.search(accion['domain'])
        self.assertEqual(registros, a + b + c)

    def test_historial_sin_cliente_o_producto_no_encadena(self):
        """Una cotización de especificación nueva (sin producto) o sin
        cliente no participa en cadenas: siempre es revisión 1 suelta."""
        Cot = self.env['qb.cotizacion']
        s1 = Cot.create({'name': 'SPEC SUELTA 1', 'piso_lleno': 10.0,
                         'spec_descripcion': 'tela nueva 45 g'})
        s2 = Cot.create({'name': 'SPEC SUELTA 2', 'piso_lleno': 10.0,
                         'spec_descripcion': 'tela nueva 45 g'})
        self.assertEqual(s1.revision, 1)
        self.assertEqual(s2.revision, 1)
        self.assertFalse(s2.revision_anterior_id)
        self.assertNotEqual(s1.state, 'superseded')

    def test_peso_m2_usa_gramaje_sin_ancho(self):
        """Producto vendido en m²: el peso por unidad ES el gramaje — el
        ancho no juega (un m² pesa lo mismo a cualquier ancho). Antes el
        parser no encontraba ancho en refs '...M2' y aplicaba el default
        1.5 m: toda la familia m² (54 productos) salía +50 por ciento de
        peso, con su energía y fabricación infladas igual (FXI: una tela a
        −35 por ciento de bruto y su gemela a +46)."""
        uom_m2 = self.env['uom.uom'].search(
            [('name', 'in', ('m2', 'm²'))], limit=1)
        if not uom_m2:
            uom_m2 = self.env['uom.uom'].create({'name': 'm2'})
        tela_m2 = self.env['product.product'].create({
            'name': 'TELA M2 TEST', 'default_code': 'WJ038Q22JNT160M2',
            'is_storable': True, 'uom_id': uom_m2.id, 'sale_ok': True,
        })
        kg, src = self.Peso._resolve_kg_source(tela_m2)
        self.assertAlmostEqual(kg, 0.038, places=6,
                               msg='en m² el peso es gramaje/1000, sin ancho')
        self.assertEqual(src, 'ref_gramaje')
        # El teórico de la auditoría dice lo mismo
        self.assertAlmostEqual(
            self.env['qb.peso.auditoria']._kg_teorico(tela_m2), 0.038,
            places=6)
        # Y en metros lineales el ancho SÍ juega (no se toca ese camino)
        self.assertAlmostEqual(
            self.Peso._gramaje_from_ref('WJ045NT160'), 0.045 * 1.60,
            places=6)

    def test_reportes_por_producto_leen_y_cuadran(self):
        """Los tres reportes por producto leen sin error, y la identidad
        que los ata: producto y cliente agrupan EL MISMO universo de
        líneas, así que sus totales de venta y margen neto son iguales."""
        Prod = self.env['qb.producto.rentabilidad']
        Cli = self.env['qb.cliente.rentabilidad']
        ProdCli = self.env['qb.producto.cliente']
        Mensual = self.env['qb.producto.mensual']
        prods = Prod.search([])
        clientes = Cli.search([])
        parejas = ProdCli.search([])
        meses = Mensual.search([])
        self.assertAlmostEqual(
            sum(prods.mapped('revenue_12m')),
            sum(clientes.mapped('revenue_12m')), delta=1.0,
            msg='mismo universo: la venta por producto = venta por cliente')
        self.assertAlmostEqual(
            sum(prods.mapped('margen_neto_12m')),
            sum(clientes.mapped('margen_neto_12m')), delta=1.0)
        self.assertAlmostEqual(
            sum(parejas.mapped('revenue_12m')),
            sum(prods.mapped('revenue_12m')), delta=1.0)
        self.assertAlmostEqual(
            sum(meses.mapped('revenue')),
            sum(prods.mapped('revenue_12m')), delta=1.0)

    def test_drill_down_producto_y_cliente(self):
        """Desde el renglón de un producto o cliente se navega a todo lo
        suyo: los botones devuelven acciones filtradas al registro."""
        prod = self.env['qb.producto.rentabilidad'].search([], limit=1)
        if prod:
            # Ficha 360 del producto: semáforo coherente y pestañas con
            # contenido
            esperado = ('rojo' if prod.margen_neto_pct < 0
                        else 'ambar' if prod.margen_neto_pct < 5
                        else 'verde')
            self.assertEqual(prod.semaforo, esperado)
            self.assertTrue(prod.veredicto)
            self.assertIn('<table', prod.clientes_html)
            self.assertIn('<table', prod.tendencia_html)
            self.assertTrue(prod.cotizaciones_html)
            acc = prod.action_ver_clientes()
            self.assertEqual(acc['res_model'], 'qb.producto.cliente')
            self.assertIn(('product_id', '=', prod.product_id.id),
                          acc['domain'])
            acc = prod.action_ver_programa()
            self.assertEqual(acc['res_model'], 'qb.producto.mensual')
            acc = prod.action_ver_costos()
            self.assertEqual(acc['res_model'], 'qb.costo.producto')
            acc = prod.action_abrir_producto()
            self.assertEqual(acc['res_id'], prod.product_id.id)
        cli = self.env['qb.cliente.rentabilidad'].search([], limit=1)
        if cli:
            acc = cli.action_ver_productos()
            self.assertEqual(acc['res_model'], 'qb.producto.cliente')
            self.assertIn(('partner_id', '=', cli.partner_id.id),
                          acc['domain'])
            acc = cli.action_ver_facturas()
            self.assertEqual(acc['res_model'], 'account.move')
            acc = cli.action_abrir_cliente()
            self.assertEqual(acc['res_id'], cli.partner_id.id)
            # La situación completa: semáforo coherente con el margen y
            # las tres pestañas renderean con contenido
            esperado = ('rojo' if cli.margen_neto_pct < 0
                        else 'ambar' if cli.margen_neto_pct < 5
                        else 'verde')
            self.assertEqual(cli.semaforo, esperado)
            self.assertTrue(cli.veredicto)
            self.assertIn('<table', cli.productos_html)
            self.assertIn('<table', cli.tendencia_html)
            self.assertTrue(cli.cotizaciones_html)

    def test_reventa_no_carga_fabricacion_ni_energia(self):
        """Un producto COMPRADO que se revende (fibra PES1.4NG1.5, hilo,
        servicio facturado) no pasa por planta: solo su costo de compra +
        operación. El fallback del ruteo lo mandaba a 'tela' y le cargaba
        $66/kg de energía y fabricación que no lleva — por eso la fibra
        salía con margen negativo gigante en rentabilidad por producto."""
        uom_kg = self.env.ref('uom.product_uom_kgm')
        fibra = self.env['product.product'].create({
            'name': 'PES9.9NG9.9', 'default_code': 'PES9.9NG9.9',
            'is_storable': True, 'uom_id': uom_kg.id,
            'sale_ok': True, 'purchase_ok': True, 'standard_price': 31.0})
        self.env['qb.producto.ruteo'].create({
            'name_pattern': '^PES[0-9]', 'product_bucket': 'servicio',
            'sequence': 5})
        factores = self.env['qb.costo.factores'].create({
            'period': date(2027, 6, 1), 'window_months': 12,
            'factor_fab_kg': 30.0, 'factor_fab_m': 3.0,
            'energia_por_kg': 9.0, 'op_pct': 0.15, 'mp_ajuste': 0.9})
        Costo = self.env['qb.costo.producto']
        ctx = Costo._engine_ctx([fibra.id])
        bucket, _c, _kg, _mpk, _iskg, mp, energia, fab = \
            Costo._capas_produccion(
                fibra, factores, ctx,
                self.env['qb.producto.ruteo'], self.env['qb.producto.peso'])
        self.assertEqual(bucket, 'servicio')
        self.assertEqual(energia, 0.0, 'reventa no consume energía de planta')
        self.assertEqual(fab, 0.0, 'reventa no absorbe fabricación')
        self.assertAlmostEqual(
            mp, 31.0, places=4,
            msg='costo de compra SIN ajuste de merma (no hay producción)')
        # Y una tela con nombre parecido ("PES CREP ...") NO cae en la regla
        bucket_tela, _ = self.env['qb.producto.ruteo'].resolve(self.tela)
        self.assertNotEqual(bucket_tela, 'servicio')

    def test_formato_de_fichas_360(self):
        """El formato compartido de las fichas: signo ANTES del símbolo
        (−$947,106, no $-947,106) y meses/fechas en español — strftime
        usaba el locale C y pintaba «Aug 2025»."""
        from odoo.addons.qb_capacidad_costeo.models.producto_reportes \
            import fecha_es, mes_es, money
        self.assertEqual(money(-947106.4), '-$947,106')
        self.assertEqual(money(947106.4), '$947,106')
        self.assertEqual(money(-16.984, 2), '-$16.98')
        self.assertEqual(money(0), '$0')
        self.assertEqual(mes_es(date(2025, 8, 1)), 'ago 2025')
        self.assertEqual(mes_es(date(2025, 12, 1)), 'dic 2025')
        self.assertEqual(fecha_es(date(2026, 8, 26)), '26 ago 2026')
        self.assertEqual(fecha_es(None), '')
        # Y ningún «$-» debe quedar en una ficha real
        cli = self.env['qb.cliente.rentabilidad'].search([], limit=1)
        if cli:
            self.assertNotIn('$-', cli.veredicto or '')
            self.assertNotIn('$-', cli.productos_html)
            self.assertNotIn('$-', cli.tendencia_html)
            self.assertNotIn('Aug ', cli.tendencia_html)

    def test_unico_comprador_en_vez_de_delta_cero(self):
        """Cuando un cliente es el ÚNICO comprador de un producto, el Δ
        contra el promedio es 0 por construcción: la ficha dice «único
        comprador» en vez de un «+0.0» que confunde (caso BLANCOS
        MILENIUM y su WN075)."""
        Prod = self.env['qb.producto.rentabilidad']
        solo = Prod.search([('n_clientes', '=', 1)], limit=1)
        if not solo:
            self.skipTest('sin productos monocomprador en la base de test')
        pareja = self.env['qb.producto.cliente'].search(
            [('product_id', '=', solo.product_id.id)], limit=1)
        cli = self.env['qb.cliente.rentabilidad'].browse(
            pareja.partner_id.id)
        self.assertIn('único comprador', cli.productos_html)
        self.assertIn('único comprador', solo.clientes_html)

    def test_panel_negocio_primero_config_colapsada(self):
        """El panel abre con el negocio (mes, quién deja y quién cuesta,
        acciones) y la configuración queda SIEMPRE colapsada con resumen —
        es de la puesta a punto, no del día a día."""
        panel = self.env['qb.costeo.panel'].create({})
        self.assertIn('¿Cómo va el negocio?', panel.negocio_html)
        self.assertIn('<details', panel.estado_html,
                      'la configuración siempre va colapsada')
        self.assertIn('Configuración', panel.estado_html)
        # Con capacidad normal honesta, el margen de productos NUNCA se lee
        # solo: el par margen − ociosidad = resultado va siempre junto
        # (un +11M de productos con −13M de ociosidad al lado es un año en
        # tablas, no una utilidad).
        if 'Margen de productos (mes)' in panel.negocio_html:
            self.assertIn('Ociosidad del mes', panel.negocio_html)
            self.assertIn('Resultado del mes (modelo)', panel.negocio_html)
            self.assertIn('margen de productos − ociosidad',
                          panel.negocio_html)

    def test_panel_detecta_periodos_desfasados_y_cola_atorada(self):
        """Los dos candados del caso WD3846NT163m2: (1) un período abierto
        calculado ANTES del último cambio de pesos mezcla criterios en la
        ventana de 12 meses — el panel lo marca; (2) una cola de recálculo
        con el cron apagado quedó atorada 2 días sin que nadie lo viera —
        el panel la marca en rojo."""
        panel = self.env['qb.costeo.panel'].create({})
        Config = self.env['qb.costeo.factor.config']
        factores = self.env['qb.costo.factores'].search(
            [('state', '=', 'borrador')], limit=1)
        if factores:
            # Tocar un peso AHORA deja a todo período existente como
            # anterior al cambio → el check debe avisar
            self.env['qb.producto.peso'].create({
                'product_id': self.tela.id, 'kg_per_unit': 0.072,
                'source': 'manual'})
            estado = panel._build_estado()
            self.assertIn('Períodos vs maestro de pesos', estado)
            self.assertIn('ANTES del último cambio', estado)

        # Cola con períodos y cron apagado → atorada, en rojo
        cron = self.env.ref(
            'qb_capacidad_costeo.cron_recalculo_pendientes',
            raise_if_not_found=False)
        if cron:
            cron.active = False
        rec = Config.search([('key', '=', 'recalculo_pendiente')], limit=1)
        if rec:
            rec.value_text = '2024-01-01,2024-02-01'
        else:
            Config.create({'key': 'recalculo_pendiente', 'value': 0,
                           'value_text': '2024-01-01,2024-02-01'})
        estado = panel._build_estado()
        self.assertIn('Cola de recálculo diferido', estado)
        self.assertIn('atorada', estado)
        # Con el cron prendido, la misma cola es «convergiendo», no error
        if cron:
            cron.active = True
            estado = panel._build_estado()
            self.assertIn('convergiendo', estado)

    def test_panel_avisa_avco_importado_divergente(self):
        """El AVCO de un ' I' duplica un dato vivo (la compra de su gemelo
        IT + gastos de conversión) y debe validarse contra la fuente: el
        KP2032T11GO152 I traía 9.39 calcado a mano del gemelo nacional
        cuando su IT real se compró a ~6.10 (+54%). El panel compara cada
        importado vendido del período contra la última compra de su IT y
        avisa cuando diverge más de ±35%."""
        uom_m = self.env.ref('uom.product_uom_meter')
        it = self.env['product.product'].create({
            'name': 'IT PANEL TEST', 'default_code': 'KX9032GO152 IT',
            'is_storable': True, 'uom_id': uom_m.id, 'purchase_ok': True})
        imp = self.env['product.product'].create({
            'name': 'I PANEL TEST', 'default_code': 'KX2032GO152 I',
            'is_storable': True, 'uom_id': uom_m.id, 'sale_ok': True,
            'standard_price': 9.39})
        self.env['mrp.bom'].create({
            'product_tmpl_id': imp.product_tmpl_id.id,
            'product_qty': 1.0, 'product_uom_id': uom_m.id,
            'bom_line_ids': [(0, 0, {
                'product_id': it.id, 'product_qty': 1.0,
                'product_uom_id': uom_m.id})]})
        proveedor = self.env['res.partner'].create({'name': 'PROV PANEL'})
        po = self.env['purchase.order'].create({
            'partner_id': proveedor.id,
            'order_line': [(0, 0, {
                'product_id': it.id, 'product_qty': 500.0,
                'price_unit': 6.10})]})
        po.button_confirm()
        # El período más nuevo manda en el check: uno propio, con la fila
        # del importado vendida y su MP divergente (9.39 vs 6.10 = +54%)
        periodo = date(2027, 1, 1)
        self.env['qb.costo.factores'].create({
            'period': periodo, 'window_months': 12})
        self.Costo.create({
            'period': periodo, 'product_id': imp.id,
            'product_bucket': 'importado', 'qty_vendida': 100.0,
            'mp_unit': 9.39})
        panel = self.env['qb.costeo.panel'].create({})
        estado = panel._build_estado()
        self.assertIn('AVCO de importados vs compra IT', estado)
        self.assertIn('KX2032GO152 I', estado)
        self.assertIn('+54%', estado)

    def test_panel_avisa_bom_desviada_del_consumo_real(self):
        """La receta duplica un dato vivo: lo que las OPs done consumieron.
        El caso X140 (ago-2026): la BOM decía 0.2674 kg de tejido por
        metro y las OPs reales consumían 0.2474 — 8% de hilo fantasma,
        $1/m de MP inflada, un producto pintado en rojo sin estarlo. El
        panel compara cada receta kg→m con volumen contra el consumo real
        de 12 meses y avisa cuando divergen más de ±5%."""
        uom_m = self.env.ref('uom.product_uom_meter')
        uom_kg = self.env.ref('uom.product_uom_kgm')
        tejido = self.env['product.product'].create({
            'name': 'TEJIDO CONSUMO TEST', 'default_code': 'XT130TEST',
            'is_storable': True, 'uom_id': uom_kg.id})
        tela = self.env['product.product'].create({
            'name': 'TELA CONSUMO TEST', 'default_code': 'XT140TEST',
            'is_storable': True, 'uom_id': uom_m.id, 'sale_ok': True})
        self.env['mrp.bom'].create({
            'product_tmpl_id': tela.product_tmpl_id.id,
            'product_qty': 1.0, 'product_uom_id': uom_m.id,
            'bom_line_ids': [(0, 0, {
                'product_id': tejido.id, 'product_qty': 0.30,
                'product_uom_id': uom_kg.id})]})
        mo = self.env['mrp.production'].create({
            'product_id': tela.id, 'product_qty': 60000.0,
            'product_uom_id': uom_m.id})
        loc = self.env.ref('stock.stock_location_stock',
                           raise_if_not_found=False) \
            or self.env['stock.location'].search([], limit=1)
        move = self.env['stock.move'].create({
            'name': 'consumo test', 'product_id': tejido.id,
            'product_uom': uom_kg.id, 'quantity': 14400.0,
            'location_id': loc.id, 'location_dest_id': loc.id,
            'raw_material_production_id': mo.id})
        # 'done' directo por SQL: el flujo completo de la OP arrastra
        # reservas y validaciones que este check no necesita.
        self.env.cr.execute(
            "UPDATE mrp_production SET state = 'done' WHERE id = %s",
            (mo.id,))
        self.env.cr.execute(
            "UPDATE stock_move SET state = 'done' WHERE id = %s",
            (move.id,))
        self.env.invalidate_all()
        panel = self.env['qb.costeo.panel'].create({})
        estado = panel._build_estado()
        self.assertIn('Consumo de BOM vs OPs reales', estado)
        # real = 14,400 / 60,000 = 0.24; BOM 0.30 = +25%
        self.assertIn('XT140TEST', estado)
        self.assertIn('+25%', estado)

    def test_rendimiento_vendible_desde_lineas_de_almacen(self):
        """Memo 31-ago, bloque A: el costo era por metro PRODUCIDO y la
        merma no existía en el modelo. El rendimiento se mide de las
        LÍNEAS de almacén (C1), FE cuenta como merma (decisión 31-ago:
        no hay canal directo, se recupera resinado), los tipos CVU y
        cambio de artículo se excluyen (C2/C3), los reingresos no cuentan
        (C6) y abajo del umbral manda la tasa de planta (A4)."""
        Loc = self.env['stock.location']
        base = self.env.ref('stock.stock_location_stock',
                            raise_if_not_found=False) \
            or Loc.search([('usage', '=', 'internal')], limit=1)
        origen = Loc.create({'name': 'TESTCAL ORIGEN', 'usage': 'internal',
                             'location_id': base.id})
        reingreso = Loc.create({'name': 'TESTCAL REINGRESO',
                                'usage': 'internal', 'location_id': base.id})
        vend = Loc.create({'name': 'TESTCAL PQ', 'usage': 'internal',
                           'location_id': base.id})
        fe = Loc.create({'name': 'TESTCAL FE', 'usage': 'internal',
                         'location_id': base.id})
        Config = self.env['qb.costeo.factor.config']
        for key, vt in (('calidad_locs_vendible', str(vend.id)),
                        ('calidad_locs_merma', str(fe.id)),
                        ('calidad_locs_origen', str(origen.id))):
            rec = Config.search([('key', '=', key)], limit=1)
            if rec:
                rec.write({'value_text': vt, 'active': True})
            else:
                Config.create({'key': key, 'value': 0, 'value_text': vt})
        rec_min = Config.search([('key', '=', 'rendimiento_min_m')], limit=1)
        if rec_min:
            rec_min.write({'value': 1000.0})
        else:
            Config.create({'key': 'rendimiento_min_m', 'value': 1000.0})
        uom_m = self.env.ref('uom.product_uom_meter')
        grande = self.env['product.product'].create({
            'name': 'TELA RENDIMIENTO GRANDE', 'is_storable': True,
            'uom_id': uom_m.id})
        chica = self.env['product.product'].create({
            'name': 'TELA RENDIMIENTO CHICA', 'is_storable': True,
            'uom_id': uom_m.id})
        excl_ids = [int(x) for x in Config.get_param_text(
            'calidad_picking_excluir', '77,147').split(',') if x.strip()]
        Move = self.env['stock.move']
        datos = [
            (grande, origen, vend, 18000.0, False),
            (grande, origen, fe, 2000.0, False),      # 10% de merma
            (chica, origen, vend, 300.0, False),
            (chica, origen, fe, 200.0, False),        # chica: < umbral
            # C2/C3: un reetiquetado gigante por CVU no infla el rendimiento
            (grande, origen, vend, 99999.0, excl_ids[0] if excl_ids else 0),
            # C6: un reingreso (origen no productivo) no cuenta
            (grande, reingreso, fe, 5000.0, False),
        ]
        moves = Move.browse()
        for prod, src, dst, qty, ptype in datos:
            vals = {'name': 'testcal', 'product_id': prod.id,
                    'product_uom': uom_m.id, 'quantity': qty,
                    'location_id': src.id, 'location_dest_id': dst.id}
            if ptype:
                vals['picking_type_id'] = ptype
            moves |= Move.create(vals)
        hace_20d = datetime.now() - relativedelta(days=20)
        self.env.cr.execute(
            "UPDATE stock_move SET state = 'done', date = %s "
            "WHERE id IN %s", (hace_20d, tuple(moves.ids)))
        self.env.cr.execute(
            "UPDATE stock_move_line SET state = 'done', date = %s "
            "WHERE move_id IN %s", (hace_20d, tuple(moves.ids)))
        self.env.invalidate_all()
        period = date.today().replace(day=1)
        mapa, planta = self.Costo._rendimiento_map(period)
        rend_g, fuente_g = mapa[grande.id]
        # 18,000 vendibles de 20,000 clasificados = 90% — ni el CVU de
        # 99,999 m ni el reingreso de 5,000 lo movieron
        self.assertAlmostEqual(rend_g, 0.90, places=4)
        self.assertEqual(fuente_g, 'producto')
        # La chica (500 m < umbral 1,000) hereda la tasa de planta
        rend_c, fuente_c = mapa[chica.id]
        self.assertEqual(fuente_c, 'planta')
        self.assertAlmostEqual(rend_c, planta, places=6)
        self.assertAlmostEqual(planta, 18300.0 / 20500.0, places=4)
        # A7: la vista de rentabilidad expone el margen real y el semáforo
        # se calcula sobre él sin reventar
        rows = self.env['qb.producto.rentabilidad'].search([], limit=20)
        rows.read(['margen_neto_real_12m', 'margen_neto_real_pct',
                   'rendimiento_pct'])
        for r in rows:
            self.assertIn(r.semaforo, ('rojo', 'ambar', 'verde'))

    def test_produccion_arriba_de_capacidad_normal_se_senala(self):
        """Caso Acabado: la producción real (952K m) superaba la capacidad
        capturada (915,733 — una rama nueva sin reflejar) y el modelo lo
        TAPABA: utilización al 100, ocioso en cero, sobre-absorción muda.
        Ahora: (1) el período usa la producción real como denominador
        (IAS 2, producción anormalmente alta), (2) el flag queda en los
        factores, (3) el panel lo marca como error de configuración y
        (4) la vista de ociosidad muestra la utilización real >100."""
        uom_m = self.env.ref('uom.product_uom_meter')
        centro = self.env['qb.costeo.centro'].create({
            'code': 'TB5U', 'name': 'CENTRO CAPACIDAD SUPERADA TEST',
            'driver_principal': 'largo', 'es_denominador_m': True,
            'capacidad_normal': 50.0, 'mo_name_pattern': 'TB5U%'})
        tela = self.env['product.product'].create({
            'name': 'TELA CAPACIDAD TEST', 'is_storable': True,
            'uom_id': uom_m.id})
        mo = self.env['mrp.production'].create({
            'name': 'TB5U/TEST1', 'product_id': tela.id,
            'product_qty': 900.0, 'product_uom_id': uom_m.id})
        hace_20d = datetime.now() - relativedelta(days=20)
        self.env.cr.execute(
            "UPDATE mrp_production SET state = 'done', date_finished = %s "
            "WHERE id = %s", (hace_20d, mo.id))
        self.env.invalidate_all()
        Config = self.env['qb.costeo.factor.config']
        ov = Config.search([('key', '=', 'denominador_m_override')], limit=1)
        if ov:
            ov.write({'value': 10.0, 'active': True})
        else:
            Config.create({'key': 'denominador_m_override', 'value': 10.0})
        period = date.today().replace(day=1)
        self.Costo.action_recompute_period(period)
        fact = self.env['qb.costo.factores'].search(
            [('period', '=', period)], limit=1)
        self.assertTrue(fact.capacidad_superada_m)
        # El denominador se ajustó a la producción real: sin sobre-absorción
        self.assertAlmostEqual(
            fact.m_denom_month, fact.m_produccion_month, places=2)
        self.assertLessEqual(fact.utilizacion_m_pct, 100.0001)
        # El panel lo marca como configuración por corregir, no lo esconde
        panel = self.env['qb.costeo.panel'].create({})
        estado = panel._build_estado()
        self.assertIn('Capacidad normal vs producción real', estado)
        self.assertIn('SUPERA', estado)
        # Y la vista de ociosidad enseña la utilización real, sin tope
        fila = self.env['qb.ociosidad'].search(
            [('centro_id', '=', centro.id)], limit=1)
        self.assertGreater(fila.utilization_pct, 100.0)
        self.assertEqual(fila.idle_cost_month, 0.0)

    def test_analisis_de_productos_trae_costo_unitario(self):
        """El análisis de productos mostraba precio y márgenes pero no el
        COSTO unitario — el número que faltaba para leer de un vistazo
        contra qué compite el precio. En rentabilidad (12m) y en el
        programa mensual, costo_unit = variable + fabricación + operación
        por unidad, y cuadra por construcción con el margen: precio −
        costo = margen neto unitario."""
        for modelo, qty_f, precio_f, margen_f in (
                ('qb.producto.rentabilidad', 'qty_12m', 'precio_prom',
                 'margen_neto_12m'),
                ('qb.producto.mensual', 'qty', 'precio_prom',
                 'margen_neto')):
            rows = self.env[modelo].search([])
            rows.read([qty_f, precio_f, 'costo_unit', margen_f])
            for r in rows:
                qty = r[qty_f]
                if qty <= 0:
                    continue
                self.assertAlmostEqual(
                    r['costo_unit'],
                    r[precio_f] - r[margen_f] / qty, places=2,
                    msg='%s: costo_unit no cuadra con precio - margen/qty'
                        % modelo)

    def test_pesos_derivados_de_ops_reales(self):
        """El kg/m de una tela en metros se puede MEDIR: la báscula pesa
        cada rollo de tejido y ese peso entra como consumo de las OPs.
        «Derivar de OPs» convierte consumo_kg ÷ metros_producidos en el
        maestro de pesos con fuente op_consumo (medida — apaga la alerta
        peso_estimado). Toma el componente kg DOMINANTE (la tela, no los
        químicos de baño), propaga cadenas m→m (el caso X140: su OP
        consume XJ140 en metros) y nunca pisa un peso manual/CVU."""
        uom_m = self.env.ref('uom.product_uom_meter')
        uom_kg = self.env.ref('uom.product_uom_kgm')
        Prod = self.env['product.product']
        tejido = Prod.create({
            'name': 'TEJIDO PESO OPS', 'is_storable': True,
            'uom_id': uom_kg.id})
        quimico = Prod.create({
            'name': 'QUIMICO BANO PESO OPS', 'is_storable': True,
            'uom_id': uom_kg.id})
        tela = Prod.create({
            'name': 'TELA PESO OPS', 'default_code': 'ZPT140TST165',
            'is_storable': True, 'uom_id': uom_m.id, 'sale_ok': True})
        tela2 = Prod.create({
            'name': 'TELA ACABADA PESO OPS', 'default_code': 'ZPT140TSF165',
            'is_storable': True, 'uom_id': uom_m.id, 'sale_ok': True})
        loc = self.env.ref('stock.stock_location_stock',
                           raise_if_not_found=False) \
            or self.env['stock.location'].search([], limit=1)
        mo1 = self.env['mrp.production'].create({
            'product_id': tela.id, 'product_qty': 60000.0,
            'product_uom_id': uom_m.id})
        moves = self.env['stock.move'].create([
            {'name': 'tejido real', 'product_id': tejido.id,
             'product_uom': uom_kg.id, 'quantity': 14400.0,
             'location_id': loc.id, 'location_dest_id': loc.id,
             'raw_material_production_id': mo1.id},
            {'name': 'quimico de bano', 'product_id': quimico.id,
             'product_uom': uom_kg.id, 'quantity': 3000.0,
             'location_id': loc.id, 'location_dest_id': loc.id,
             'raw_material_production_id': mo1.id}])
        mo2 = self.env['mrp.production'].create({
            'product_id': tela2.id, 'product_qty': 60000.0,
            'product_uom_id': uom_m.id})
        moves |= self.env['stock.move'].create([
            {'name': 'tela en metros', 'product_id': tela.id,
             'product_uom': uom_m.id, 'quantity': 60000.0,
             'location_id': loc.id, 'location_dest_id': loc.id,
             'raw_material_production_id': mo2.id},
            # El caso K40T/perfoquim: el ÚNICO kg directo es el hilo de
            # tramado (minúsculo) y la tela entra en metros — la cadena
            # debe ganarle al kg directo, no al revés.
            {'name': 'hilo de tramado', 'product_id': tejido.id,
             'product_uom': uom_kg.id, 'quantity': 600.0,
             'location_id': loc.id, 'location_dest_id': loc.id,
             'raw_material_production_id': mo2.id}])
        self.env.cr.execute(
            "UPDATE mrp_production SET state = 'done' WHERE id IN %s",
            (tuple((mo1 | mo2).ids),))
        self.env.cr.execute(
            "UPDATE stock_move SET state = 'done' WHERE id IN %s",
            (tuple(moves.ids),))
        self.env.invalidate_all()
        Peso = self.env['qb.producto.peso']
        Peso.action_derivar_de_ops()
        rec = Peso.search([('product_id', '=', tela.id)])
        self.assertEqual(rec.source, 'op_consumo')
        # Dominante: 14,400/60,000 = 0.24 — el químico (3,000 kg) NO suma
        self.assertAlmostEqual(rec.kg_per_unit, 0.24, places=4)
        # Cadena m→m: tela2 consume tela 1:1 en metros y hereda su kg/m.
        # Su kg DIRECTO es el hilo de tramado (600/60,000 = 0.01) y NO
        # debe ganar: la masa dominante viene de la tela en metros.
        rec2 = Peso.search([('product_id', '=', tela2.id)])
        self.assertEqual(rec2.source, 'op_consumo')
        self.assertAlmostEqual(rec2.kg_per_unit, 0.24, places=4)
        # Fuente MEDIDA: el motor no la marca peso_estimado
        self.assertNotIn('op_consumo', Peso.PESO_SOURCES_ESTIMADAS)
        self.assertEqual(Peso.resolve_kg_source(tela2), 'op_consumo')
        # No-pisado: un peso manual es autoritativo y sobrevive re-corridas
        rec.write({'source': 'manual', 'kg_per_unit': 0.5})
        Peso.action_derivar_de_ops()
        self.assertEqual(rec.kg_per_unit, 0.5)
        self.assertEqual(rec.source, 'manual')

    def test_auditoria_de_pesos_clasifica(self):
        """La auditoría separa ok / revisar / crítico / sin peso por la
        desviación motor vs teórico, y el generador corre sin error."""
        Aud = self.env['qb.peso.auditoria']
        self.assertEqual(Aud._estado_para(0.0, 0.038, 'sin_peso')[0],
                         'sin_peso')
        self.assertEqual(Aud._estado_para(0.038, 0.038, 'manual')[0], 'ok')
        estado, desv = Aud._estado_para(0.050, 0.038, 'ref_gramaje')
        self.assertEqual(estado, 'revisar')
        self.assertAlmostEqual(desv, 100.0 * (0.050 / 0.038 - 1), places=2)
        # El caso real de FXI: 0.1 capturado vs 0.038 teórico → crítico
        self.assertEqual(Aud._estado_para(0.1, 0.038, 'odoo_weight')[0],
                         'critico')
        # Estimado sin teórico contra qué comparar → revisar
        self.assertEqual(Aud._estado_para(0.08, 0.0, 'odoo_weight')[0],
                         'revisar')
        # Medido sin teórico → ok (no hay evidencia en contra)
        self.assertEqual(Aud._estado_para(0.08, 0.0, 'cvu')[0], 'ok')
        Aud.action_generar()

    def test_reporte_cliente_formato_completo(self):
        """El PDF para cliente trae TODO el formato F-P-A28-12: folio,
        atención a, condiciones de venta, checklist ✓/✗ de términos,
        muestra, firmas y clave de control — sin ningún dato interno de
        costo o margen."""
        partner = self.env['res.partner'].create(
            {'name': 'CLIENTE PDF TEST', 'phone': '55 1234 5678'})
        cot = self.env['qb.cotizacion'].create({
            'name': 'COT PDF TEST', 'partner_id': partner.id,
            'product_id': self.tela.id, 'volumen': 5000.0,
            'uom_name': 'm', 'costo_variable': 6.0,
            'costo_absorbido_sin_op': 12.0, 'op_pct': 15.0,
            'piso_ocioso': 6.0, 'piso_lleno': 14.0,
            'precio_objetivo': 16.0, 'atencion_a': 'Ing. Prueba Compras',
        })
        html = self.env['ir.actions.report']._render_qweb_html(
            'qb_capacidad_costeo.report_cotizacion_cliente', cot.ids)[0]
        html = html.decode('utf-8') if isinstance(html, bytes) else str(html)
        for pedazo in (
                'COTIZACIÓN DE PRODUCTO', cot.folio,
                'En atención a', 'Ing. Prueba Compras',
                'WJ045NT160',                       # la referencia interna
                '5,000 m',                          # lote mínimo default
                '500 m ± 100 m',                    # presentación default
                'EXWORKS',                          # lugar de entrega
                '4 semanas',                        # tiempo de entrega
                'CoA al 100%', 'PPAP', 'LTA', 'Cotizado / Quoted',
                '✓', '✗',
                'Muestra menor a 50 m',
                'VENTAS QUIMIBOND', 'APROBADA POR CLIENTE',
                'no será válida sin la firma de ambas partes',
                'F-P-A28-12'):
            self.assertIn(pedazo, html,
                          'falta «%s» en el PDF para cliente' % pedazo)
        # Y nada interno se cuela
        for prohibido in ('margen', 'Margen', 'costo variable',
                          'Costo variable', 'piso'):
            self.assertNotIn(prohibido, html,
                             '«%s» es dato interno: no va al cliente'
                             % prohibido)

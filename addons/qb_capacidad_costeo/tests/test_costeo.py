# -*- coding: utf-8 -*-
from datetime import date

from dateutil.relativedelta import relativedelta

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

    def test_mp_receta_ambigua_usa_avco(self):
        """Semiterminado con VARIAS BOMs activas (receta ambigua, como los
        genéricos 'MUESTRA PILOTO' con 26 recetas): _bom_find elegiría una al
        azar y el costo se colapsa. Con AVCO válido el modelo usa el
        standard_price de Odoo en vez de explotar una receta arbitraria
        (bug WD080: MP $0.13 en vez de ~$12)."""
        uom_m = self.env.ref('uom.product_uom_meter')
        uom_kg = self.env.ref('uom.product_uom_kgm')
        barato = self.env['product.product'].create({
            'name': 'INSUMO BARATO', 'is_storable': True,
            'uom_id': uom_kg.id, 'standard_price': 1.0,
        })
        semi = self.env['product.product'].create({
            'name': 'MUESTRA PILOTO TEST', 'is_storable': True,
            'uom_id': uom_kg.id, 'standard_price': 88.0,
        })
        # DOS recetas para el mismo semiterminado → receta ambigua
        for _i in range(2):
            self.env['mrp.bom'].create({
                'product_tmpl_id': semi.product_tmpl_id.id,
                'product_id': semi.id,
                'product_qty': 1.0, 'product_uom_id': uom_kg.id,
                'bom_line_ids': [(0, 0, {
                    'product_id': barato.id, 'product_qty': 0.01,
                    'product_uom_id': uom_kg.id})],
            })
        self.assertTrue(self.Costo._has_multiple_boms(semi))
        # Explotar daría ~$0.01/kg (colapsado); el AVCO da $88
        self.assertAlmostEqual(self.Costo._mp_cost_unit(semi), 88.0, places=4)
        # Un producto final que consume 0.1446 kg del semi → 0.1446 × 88
        tela2 = self.env['product.product'].create({
            'name': 'DESAGUJADO TEST', 'default_code': 'WD080Q46JNT161',
            'is_storable': True, 'uom_id': uom_m.id, 'sale_ok': True,
        })
        self.env['mrp.bom'].create({
            'product_tmpl_id': tela2.product_tmpl_id.id,
            'product_qty': 1.0, 'product_uom_id': uom_m.id,
            'bom_line_ids': [(0, 0, {
                'product_id': semi.id, 'product_qty': 0.1446,
                'product_uom_id': uom_kg.id})],
        })
        self.assertAlmostEqual(
            self.Costo._mp_cost_unit(tela2), 0.1446 * 88.0, places=3)
        # El camino batch (ctx precomputa multi_bom_ids) da lo mismo
        ctx = self.Costo._engine_ctx([tela2.id])
        self.assertIn(semi.id, ctx['multi_bom_ids'])
        self.assertAlmostEqual(
            self.Costo._mp_cost_unit(tela2, ctx=ctx), 0.1446 * 88.0, places=3)
        # Receta ÚNICA sigue explotando normal (no se toca ese camino)
        self.assertFalse(self.Costo._has_multiple_boms(self.tela))
        self.assertAlmostEqual(
            self.Costo._mp_cost_unit(self.tela), 0.072 * 50.0, places=4)

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
        """Importados (' I'): MP = landed (avg) y NO cargan fabricación."""
        bucket, _ = self.Ruteo.resolve(self.importado)
        self.assertEqual(bucket, 'importado')
        self.assertAlmostEqual(
            self.Costo._mp_cost_unit(self.importado), 7.51, places=2)
        factores = self.env['qb.costo.factores'].create({
            'period': date(2026, 1, 1), 'factor_fab_kg': 30.0,
            'factor_fab_m': 3.0, 'entretela_factor_m': 2.3,
        })
        fab = self.Costo._fab_unit('importado', False, 0.1, 10.0, factores)
        self.assertEqual(fab, 0.0)

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
            'brecha', 'brecha_pct', 'cobertura_pct'])
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

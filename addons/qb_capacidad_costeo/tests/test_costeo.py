# -*- coding: utf-8 -*-
from datetime import date

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
        sales = {self.tela.id: (-5.0, -100.0)}  # qty neta negativa
        vals, _ = self.Costo._compute_product_vals(
            self.tela, period, factores, sales, ctx, self.Ruteo, self.Peso)
        self.assertEqual(vals['precio_prom'], 0.0)
        self.assertNotEqual(vals['alerta'], 'bajo_variable')

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
        """costo_absorbido = MP + energía + fab + op, exacto por producto."""
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
            if rec.product_bucket in ('importado', 'subproducto'):
                self.assertEqual(rec.fab_unit, 0.0)
            self.assertTrue(rec.alerta, 'alerta debe poblarse siempre')
            self.assertAlmostEqual(
                rec.contrib_total,
                (rec.margen_contribucion * rec.qty_vendida
                 if rec.precio_prom else 0.0), places=2)

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
        qty, revenue, divisa = sales[self.tela.id]
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

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
        """Los resultados del wizard se computan en vivo (sin botón) y el
        precio sugerido cubre op% + margen meta sobre venta."""
        self.env['qb.costo.factores'].create({
            'period': date(2026, 3, 1), 'window_months': 12,
            'factor_fab_kg': 30.0, 'factor_fab_m': 3.0,
            'energia_por_kg': 4.0, 'op_pct': 0.18,
        })
        wiz = self.env['qb.cotizador.wizard'].create({
            'product_id': self.tela.id, 'volumen': 1000,
            'target_margin': 30.0,
        })
        # MP 3.6 + energía 4×0.072 = variable; fab por híbrida en metros
        self.assertAlmostEqual(wiz.mp_unit, 0.072 * 50.0, places=4)
        self.assertAlmostEqual(wiz.energia_unit, 4.0 * 0.072, places=4)
        self.assertAlmostEqual(
            wiz.costo_variable, wiz.mp_unit + wiz.energia_unit, places=4)
        self.assertAlmostEqual(wiz.fab_unit, 0.072 * 30.0 + 3.0, places=4)
        # precio sugerido = (variable+fab) / (1 − op − margen)
        esperado = (wiz.costo_variable + wiz.fab_unit) / (1 - 0.18 - 0.30)
        self.assertAlmostEqual(wiz.precio_sugerido, esperado, places=3)
        self.assertEqual(wiz.piso_ocioso, wiz.costo_variable)
        # Márgenes al precio sugerido: neto == margen meta; bruto = neto + op
        self.assertAlmostEqual(wiz.margen_neto_pct, 30.0, places=3)
        self.assertAlmostEqual(wiz.margen_bruto_pct, 30.0 + 18.0, places=3)
        # Guardar produce la cotización con los mismos números
        action = wiz.action_cotizar()
        cot = self.env['qb.cotizacion'].browse(action['res_id'])
        self.assertAlmostEqual(cot.costo_variable, wiz.costo_variable, places=4)
        self.assertAlmostEqual(cot.precio_sugerido, wiz.precio_sugerido, places=4)

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
        - Al precio SUGERIDO, el margen absorbido == margen meta.
        - Al piso LLENO, el margen absorbido == 0 (cubre todo, gana nada).
        - Al piso OCIOSO, la contribución == 0.
        - El semáforo cambia exactamente en los pisos."""
        factores = self.env['qb.costo.factores'].create({
            'period': date(2026, 5, 1), 'window_months': 12,
            'factor_fab_kg': 30.0, 'factor_fab_m': 3.0,
            'energia_por_kg': 4.0, 'op_pct': 0.18,
        })
        q = self.Costo.quote_product(self.tela, factores, target=0.30)
        v, f, op = q['variable'], q['fab'], q['op_pct']

        # margen absorbido en el sugerido = (p − v − f − op·p) / p = target
        p = q['precio_sugerido']
        self.assertAlmostEqual((p - v - f - op * p) / p, 0.30, places=6)
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
        # jerarquía de precios: ocioso < lleno < sugerido
        self.assertLess(q['piso_ocioso'], q['piso_lleno'])
        self.assertLess(q['piso_lleno'], q['precio_sugerido'])

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
        # el sugerido de la tela cubre op + margen meta sobre venta
        self.assertGreater(l_tela.precio_sugerido, l_tela.piso_lleno)
        # aplicar en lote: solo la marcada cambia
        wiz.action_aplicar_seleccionados()
        self.assertAlmostEqual(
            order.order_line[0].price_unit, l_tela.precio_sugerido, places=2)
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
        # El nuevo precio default es el sugerido CONVERTIDO a la divisa
        self.assertAlmostEqual(
            line.nuevo_precio, line.precio_sugerido / 20.0, places=2)
        # Aplicar escribe en EUR (moneda del pedido), no en MXN
        line.aplicar = True
        wiz.action_aplicar_seleccionados()
        self.assertAlmostEqual(
            order.order_line[0].price_unit, line.precio_sugerido / 20.0,
            places=2)

        # Wizard individual: el precio objetivo se captura EN EUR y el
        # modelo lo convierte; los espejos en divisa cuadran con el TC
        wiz_ind = self.env['qb.cotizador.wizard'].with_context(
            active_model='sale.order', active_id=order.id).create({})
        self.assertEqual(wiz_ind.currency_id, eur)
        self.assertAlmostEqual(wiz_ind.fx_rate, 20.0, places=2)
        wiz_ind.precio_objetivo = 3.0  # EUR (= 60 MXN)
        self.assertNotEqual(wiz_ind.semaforo, 'rojo')
        self.assertAlmostEqual(
            wiz_ind.precio_sugerido_divisa,
            wiz_ind.precio_sugerido / 20.0, places=3)
        self.assertAlmostEqual(
            wiz_ind.piso_ocioso_divisa, wiz_ind.piso_ocioso / 20.0, places=3)
        # Con precio 0.5 EUR (= 10 MXN < variable) sí es rojo
        wiz_ind.precio_objetivo = 0.5
        self.assertEqual(wiz_ind.semaforo, 'rojo')

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

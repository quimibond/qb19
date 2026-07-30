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

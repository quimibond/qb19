# -*- coding: utf-8 -*-
"""MA-03 «Calidad PQ». test_03 se mudó de quimibond_sgi/tests/test_kpi_fase4.py
en quimibond_sgi 57.10.0 (A-019, J-018), sin cambios: el cálculo debe medir
igual que en el núcleo."""
import datetime

from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestCalidadPq(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Indicator = cls.env['sgi.indicator']
        cls.stock = cls.env['stock.location'].search([('usage', '=', 'internal')], limit=1)
        cls.prodloc = cls.env['stock.location'].search([('usage', '=', 'production')], limit=1)

    def test_03_calidad_pq(self):
        tag = self.env['quality.tag'].create({'name': 'TEJIDO Agujero'})
        main = self.env['product.product'].create({
            'name': 'Tela revisado test', 'type': 'consu', 'tracking': 'lot'})
        mo = self.env['mrp.production'].create({'product_id': main.id, 'product_qty': 5.0})
        lot = self.env['stock.lot'].create({'name': 'ROLLO-PQ-1', 'product_id': main.id})
        Log = self.env['mrp.revision.log']
        today = datetime.date.today()
        base = self.Indicator.new({'calc_mode': 'calidad_pq'})._detail_calidad_pq(
            today - datetime.timedelta(days=1), today + datetime.timedelta(days=1))
        # 3 rollos sin defecto, 1 con defecto → 75% sin defecto.
        for _ in range(3):
            Log.create({'production_id': mo.id, 'lot_id': lot.id})
        Log.create({'production_id': mo.id, 'lot_id': lot.id, 'causa_id': tag.id})
        indicator = self.Indicator.new({'calc_mode': 'calidad_pq'})
        window = (today - datetime.timedelta(days=1), today + datetime.timedelta(days=1))
        # create_date no se puede fijar: la ventana incluye hoy, y en una copia
        # de producción hoy ya hay rollos revisados. Se compara contra la base.
        detail = indicator._detail_calidad_pq(*window)
        self.assertEqual(detail['denominator'] - base['denominator'], 4)
        self.assertEqual(detail['numerator'] - base['numerator'], 3,
                         "3 de 4 rollos sin defecto.")
        self.assertEqual(indicator._calc_calidad_pq(*window), detail['value'])

    def test_04_modo_registrado_desde_el_satelite(self):
        field = self.Indicator._fields['calc_mode']
        modes = [value for value, _label in field.selection]
        self.assertIn('calidad_pq', modes)
        self.assertEqual(modes.index('calidad_pq'), modes.index('reproceso') + 1,
                         "Mismo lugar en la lista que tenía en el núcleo.")
        indicator = self.Indicator.new({'calc_mode': 'calidad_pq'})
        self.assertEqual(indicator.source_type, 'auto')
        self.assertIn('revisado de telas', indicator.source_info)
        self.assertEqual(self.env['sgi.indicator.measure']._EVIDENCE['calidad_pq'][0], 'mrp.revision.log')

    def test_05_siembra_solo_si_sigue_en_manual(self):
        fresh = self.Indicator.create({'code': 'ZPQ-1', 'name': 'PQ nuevo', 'calc_mode': 'manual'})
        self.assertEqual(fresh._sgi_seed_calidad_pq(), fresh)
        self.assertEqual(fresh.calc_mode, 'calidad_pq')
        measured = self.Indicator.create({'code': 'ZPQ-2', 'name': 'PQ con mediciones', 'calc_mode': 'manual'})
        self.env['sgi.indicator.measure'].create({
            'indicator_id': measured.id, 'period_date': datetime.date(2040, 1, 1)})
        measured._sgi_seed_calidad_pq()
        self.assertEqual(measured.calc_mode, 'manual', "Lo que ya se mide a mano no se toca (B-001).")

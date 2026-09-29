# -*- coding: utf-8 -*-
import datetime

from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestKpiFase4(TransactionCase):

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

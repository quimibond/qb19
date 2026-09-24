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

    def _mkmove(self, mo, product, qty):
        move = self.env['stock.move'].create({
            'product_id': product.id,
            'product_uom_qty': qty,
            'product_uom': product.uom_id.id,
            'location_id': self.prodloc.id,
            'location_dest_id': self.stock.id,
            'production_id': mo.id,
            'state': 'done',
        })
        move.quantity = qty
        move.picked = True
        return move

    def test_01_desperdicio_subproducto(self):
        # Junio 2045: un mes sin producción real (la copia de producción sí
        # tiene órdenes terminadas en 2026 que entraban al cálculo).
        categ = self.env['product.category'].create({'name': 'SubProducto KPI prueba'})
        # La categoría sale del parámetro (en producción: «Subproducto»).
        self.env['ir.config_parameter'].sudo().set_param(
            'quimibond_sgi.waste_subproduct_category', categ.name)
        main = self.env['product.product'].create({'name': 'Tela KPI test', 'type': 'consu'})
        byp = self.env['product.product'].create({
            'name': 'SALDO TEJIDO D KPI', 'type': 'consu', 'categ_id': categ.id})
        mo = self.env['mrp.production'].create({'product_id': main.id, 'product_qty': 100.0})
        self._mkmove(mo, main, 90.0)
        self._mkmove(mo, byp, 10.0)
        mo.write({'state': 'done', 'date_finished': datetime.datetime(2045, 6, 15, 10, 0, 0)})
        indicator = self.Indicator.new({'calc_mode': 'desperdicio'})
        value = indicator._calc_desperdicio(datetime.date(2045, 6, 1), datetime.date(2045, 6, 30))
        self.assertEqual(value, 11.11, "10 kg de SALDO TEJIDO D sobre 90 kg producidos.")

    def test_02_desperdicio_none_sin_categoria(self):
        # Sin categoría SubProducto (parámetro apuntando a algo inexistente) → None.
        self.env['ir.config_parameter'].sudo().set_param(
            'quimibond_sgi.waste_subproduct_category', 'CategoriaInexistenteXYZ')
        main = self.env['product.product'].create({'name': 'Tela KPI test2', 'type': 'consu'})
        mo = self.env['mrp.production'].create({'product_id': main.id, 'product_qty': 100.0})
        self._mkmove(mo, main, 90.0)
        mo.write({'state': 'done', 'date_finished': datetime.datetime(2045, 6, 15, 10, 0, 0)})
        indicator = self.Indicator.new({'calc_mode': 'desperdicio'})
        self.assertIsNone(
            indicator._calc_desperdicio(datetime.date(2045, 6, 1), datetime.date(2045, 6, 30)))

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

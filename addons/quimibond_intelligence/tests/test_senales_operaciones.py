# -*- coding: utf-8 -*-
from odoo.tests import tagged

from .common import SenalesCommon


@tagged('post_install', '-at_install', 'quimibond_intelligence')
class TestSenalesOperaciones(SenalesCommon):

    def _op(self, hace):
        prod = self.env['product.product'].create({'name': 'Rollo', 'type': 'consu', 'is_storable': True})
        mo = self.env['mrp.production'].create({'product_id': prod.id, 'product_qty': 1, 'product_uom_id': prod.uom_id.id, 'date_start': self.hace(hace)})
        mo.action_confirm()
        mo.date_start = self.hace(hace)
        return mo

    def test_op_atrasada_y_zombie(self):
        vieja = self._op(100)
        media = self._op(10)
        self._op(-3)
        filas = self.filas('op_atrasada', {'umbrales': {'dias': 7}})
        fv = self.fila_de(filas, 'op_atrasada:mrp.production:%d' % vieja.id)
        fm = self.fila_de(filas, 'op_atrasada:mrp.production:%d' % media.id)
        self.assertEqual(fv['payload']['fecha_base'], str(self.hace(100)))  # senales_actualizar la hará zombie (> 90)
        self.assertEqual(fm['valor'], 10)
        self.assertEqual(len(filas), 2)

    def test_existencia_negativa_por_ubicacion(self):
        wh = self.env['stock.warehouse'].search([('company_id', '=', self.env.company.id)], limit=1)
        prod = self.env['product.product'].create({'name': 'Hilo', 'type': 'consu', 'is_storable': True})
        self.env['stock.quant'].sudo().create({'product_id': prod.id, 'location_id': wh.lot_stock_id.id, 'quantity': -4})
        f = self.fila_de(self.filas('existencia_negativa'), 'existencia_negativa:stock.location:%d' % wh.lot_stock_id.id)
        self.assertEqual(f['payload']['grupo'], wh.lot_stock_id.complete_name)
        self.assertEqual(f['valor'], 1)

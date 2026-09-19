# -*- coding: utf-8 -*-
from odoo.tests import tagged

from .common import SenalesCommon


@tagged('post_install', '-at_install', 'quimibond_intelligence')
class TestSenalesComercial(SenalesCommon):

    def _salida(self, partner, hace):
        wh = self.env['stock.warehouse'].search([('company_id', '=', self.env.company.id)], limit=1)
        prod = self.env['product.product'].create({'name': 'Tela', 'type': 'consu', 'is_storable': True})
        pick = self.env['stock.picking'].create({
            'partner_id': partner.id, 'picking_type_id': wh.out_type_id.id,
            'location_id': wh.lot_stock_id.id, 'location_dest_id': self.env.ref('stock.stock_location_customers').id,
            'scheduled_date': self.hace(hace),
            'move_ids': [(0, 0, {'name': 'Tela', 'product_id': prod.id, 'product_uom_qty': 5, 'product_uom': prod.uom_id.id,
                                 'location_id': wh.lot_stock_id.id, 'location_dest_id': self.env.ref('stock.stock_location_customers').id})],
        })
        pick.action_confirm()
        pick.move_ids.write({'date': self.hace(hace)})  # scheduled_date del picking se calcula de los movimientos
        return pick

    def test_entrega_vencida_por_cliente(self):
        a = self._salida(self.cliente, 3)
        self._salida(self.cliente, -2)  # futura
        f = self.fila_de(self.filas('entrega_vencida'), 'entrega_vencida:partner:%d' % self.cliente.id)
        self.assertEqual([d['id'] for d in f['documentos']], [a.id])
        self.assertEqual(f['valor'], 1)
        self.assertEqual(f['payload']['fecha_base'], str(self.hace(3)))

    def test_pedido_sin_fecha(self):
        prod = self.env['product.product'].create({'name': 'Entretela', 'type': 'consu'})
        so = self.env['sale.order'].create({'partner_id': self.cliente.id, 'order_line': [(0, 0, {'product_id': prod.id, 'product_uom_qty': 2})]})
        so.action_confirm()
        so.commitment_date = False
        f = self.fila_de(self.filas('pedido_sin_fecha'), 'pedido_sin_fecha:partner:%d' % self.cliente.id)
        self.assertEqual(f['documentos'][0]['id'], so.id)

    def test_costeo_no_instalado_devuelve_none(self):
        if 'qb.cotizacion' not in self.env:
            self.assertIsNone(self.filas('cotizacion_bajo_costo'))

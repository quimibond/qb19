# -*- coding: utf-8 -*-
from odoo import Command
from odoo.exceptions import UserError
from odoo.tests import tagged

from .common import PedimentoCommon


@tagged('post_install', '-at_install')
class TestDocumentosLigados(PedimentoCommon):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.tela = cls.env['product.product'].create({
            'name': 'TELA SIN TEJER IMPREGNADA', 'is_storable': True, 'standard_price': 50})

        def costo(nombre):
            return cls.env['product.product'].create({
                'name': nombre, 'type': 'service', 'landed_cost_ok': True,
                'property_account_expense_id': cls.company_data['default_account_expense'].id})
        cls.env.company.write({
            'qb_pedimento_product_igi_id': costo('IGI').id,
            'qb_pedimento_product_dta_id': costo('DTA').id,
            'qb_pedimento_product_prv_id': costo('PRV').id,
            'qb_pedimento_product_gastos_id': costo('Gastos de importación').id,
        })

    def _compra_recibida(self):
        po = self.env['purchase.order'].create({
            'partner_id': self.proveedor.id,
            'order_line': [Command.create({'product_id': self.tela.id, 'product_qty': 100, 'price_unit': 3.13})],
        })
        po.button_confirm()
        picking = po.picking_ids
        picking.move_ids.quantity = 100
        picking.move_ids.picked = True
        picking.button_validate()
        self.assertEqual(picking.state, 'done')
        return po, picking

    def test_desde_la_compra(self):
        po, picking = self._compra_recibida()
        ctx = po.action_view_qb_pedimentos()['context']
        ped = self.Pedimento.with_context(**ctx).create({'numero': '26 16 3774 6000206'})
        self.assertEqual(ped.purchase_ids, po)
        self.assertEqual(ped.picking_ids, picking)
        self.assertEqual(ped.partner_id, self.proveedor)
        self.assertEqual(po.qb_pedimento_count, 1)
        self.assertEqual(picking.qb_pedimento_ids, ped)
        if 'l10n_mx_edi_pedimento_number' in picking._fields:
            self.assertEqual(picking.l10n_mx_edi_pedimento_number, '26  16  3774  6000206')

    def test_costo_en_destino_en_borrador(self):
        po, picking = self._compra_recibida()
        ped = self._pedimento(numero='26 16 3774 6000206', purchase_ids=[Command.set(po.ids)],
                              picking_ids=[Command.set(picking.ids)],
                              igi=187454, dta=5999, prv=330, iva=150924, gastos_sin_iva=157216.16)
        action = ped.action_crear_costo_destino()
        landed = self.env['stock.landed.cost'].browse(action['res_id'])
        self.assertEqual(landed.state, 'draft')
        self.assertEqual(landed.picking_ids, picking)
        self.assertEqual(ped.landed_cost_ids, landed)
        # El IVA no entra al costo.
        self.assertAlmostEqual(sum(landed.cost_lines.mapped('price_unit')), 187454 + 5999 + 330 + 157216.16)
        self.assertEqual(len(landed.cost_lines), 4)

    def test_costo_en_destino_exige_recepcion(self):
        ped = self._pedimento(igi=100)
        with self.assertRaises(UserError):
            ped.action_crear_costo_destino()

    def test_facturas_sin_pedimento(self):
        """Auditoría Edomex: venta a cliente con pedimento virtual sin pedimento ligado."""
        factura = self.init_invoice('out_invoice', partner=self.cliente_virtual, amounts=[1000], post=True)
        otra = self.init_invoice('out_invoice', partner=self.proveedor, amounts=[1000], post=True)
        Move = self.env['account.move']
        sin = Move.search([('qb_pedimento_requerido', '=', True), ('qb_pedimento_ids', '=', False)])
        self.assertIn(factura, sin)
        self.assertNotIn(otra, sin)
        self.assertNotIn(factura, Move.search([('qb_pedimento_requerido', '=', False)]))
        self._pedimento(operacion='exportacion', clave_id=self.clave_v1.id, partner_id=self.cliente_virtual.id,
                        invoice_ids=[Command.set(factura.ids)])
        sin = Move.search([('qb_pedimento_requerido', '=', True), ('qb_pedimento_ids', '=', False)])
        self.assertNotIn(factura, sin)
        self.assertEqual(factura.qb_pedimento_count, 1)

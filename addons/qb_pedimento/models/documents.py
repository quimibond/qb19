# -*- coding: utf-8 -*-
"""Pedimentos desde los documentos que ampara: compra, entrega o recepción,
factura y costo en destino. Las relaciones viven en las tablas de qb.pedimento."""
from odoo import _, api, fields, models


class _PedimentoLinkMixin(models.AbstractModel):
    _name = 'qb.pedimento.link.mixin'
    _description = 'Botón de pedimentos en un documento'

    qb_pedimento_count = fields.Integer('N.º de pedimentos', compute='_compute_qb_pedimento_count')

    def _qb_pedimentos(self):
        return self.qb_pedimento_ids

    def _compute_qb_pedimento_count(self):
        for rec in self:
            rec.qb_pedimento_count = len(rec._qb_pedimentos())

    def _qb_pedimento_defaults(self):
        return {}

    def action_view_qb_pedimentos(self):
        self.ensure_one()
        pedimentos = self._qb_pedimentos()
        action = {
            'type': 'ir.actions.act_window', 'res_model': 'qb.pedimento', 'name': _('Pedimentos'),
            'domain': [('id', 'in', pedimentos.ids)], 'view_mode': 'list,form',
            'context': self._qb_pedimento_defaults(),
        }
        if len(pedimentos) == 1:
            action.update(res_id=pedimentos.id, view_mode='form')
        return action


class PurchaseOrder(models.Model):
    _name = 'purchase.order'
    _inherit = ['purchase.order', 'qb.pedimento.link.mixin']

    qb_pedimento_ids = fields.Many2many(
        'qb.pedimento', 'qb_pedimento_purchase_rel', 'purchase_id', 'pedimento_id', 'Pedimentos', copy=False)

    def _qb_pedimento_defaults(self):
        return {'default_operacion': 'importacion', 'default_purchase_ids': [self.id],
                'default_partner_id': self.partner_id.id, 'default_factura_currency_id': self.currency_id.id,
                'default_incoterm_id': self.incoterm_id.id,
                'default_picking_ids': self.picking_ids.filtered(lambda p: p.picking_type_code == 'incoming').ids}


class SaleOrder(models.Model):
    _name = 'sale.order'
    _inherit = ['sale.order', 'qb.pedimento.link.mixin']

    qb_pedimento_ids = fields.Many2many(
        'qb.pedimento', 'qb_pedimento_sale_rel', 'sale_id', 'pedimento_id', 'Pedimentos', copy=False)

    def _qb_pedimento_defaults(self):
        return {'default_operacion': 'exportacion', 'default_sale_ids': [self.id],
                'default_partner_id': self.partner_id.id, 'default_incoterm_id': self.incoterm.id,
                'default_picking_ids': self.picking_ids.filtered(lambda p: p.picking_type_code == 'outgoing').ids}


class StockPicking(models.Model):
    _name = 'stock.picking'
    _inherit = ['stock.picking', 'qb.pedimento.link.mixin']

    qb_pedimento_ids = fields.Many2many(
        'qb.pedimento', 'qb_pedimento_picking_rel', 'picking_id', 'pedimento_id', 'Pedimentos', copy=False)

    def _qb_pedimento_defaults(self):
        importacion = self.picking_type_code == 'incoming'
        ctx = {'default_operacion': 'importacion' if importacion else 'exportacion',
               'default_picking_ids': [self.id], 'default_partner_id': self.partner_id.commercial_partner_id.id}
        if importacion and self.purchase_id:
            ctx['default_purchase_ids'] = [self.purchase_id.id]
        if not importacion and self.sale_id:
            ctx['default_sale_ids'] = [self.sale_id.id]
        return ctx


class AccountMove(models.Model):
    _name = 'account.move'
    _inherit = ['account.move', 'qb.pedimento.link.mixin']

    qb_pedimento_ids = fields.Many2many(
        'qb.pedimento', 'qb_pedimento_move_rel', 'move_id', 'pedimento_id', 'Pedimentos', copy=False)
    qb_pedimento_gasto_ids = fields.Many2many(
        'qb.pedimento', 'qb_pedimento_gasto_move_rel', 'move_id', 'pedimento_id', 'Pedimentos (gastos)',
        copy=False)
    qb_pedimento_requerido = fields.Boolean(
        'Requiere pedimento', compute='_compute_qb_pedimento_requerido', search='_search_qb_pedimento_requerido',
        help='Factura de venta con complemento de comercio exterior, o a un cliente con pedimento virtual.')

    def _qb_pedimentos(self):
        return self.qb_pedimento_ids | self.qb_pedimento_gasto_ids

    def _qb_pedimento_defaults(self):
        venta = self.move_type in ('out_invoice', 'out_refund')
        return {'default_operacion': 'exportacion' if venta else 'importacion',
                'default_invoice_ids': [self.id], 'default_partner_id': self.commercial_partner_id.id}

    @api.model
    def _qb_comercio_exterior_activo(self, move):
        if 'l10n_mx_edi_external_trade' not in self._fields:
            return False
        return bool(move.l10n_mx_edi_external_trade)

    @api.depends('move_type', 'partner_id', 'commercial_partner_id.qb_pedimento_virtual')
    def _compute_qb_pedimento_requerido(self):
        for move in self:
            move.qb_pedimento_requerido = move.move_type in ('out_invoice', 'out_refund') and (
                move.commercial_partner_id.qb_pedimento_virtual or self._qb_comercio_exterior_activo(move))

    def _search_qb_pedimento_requerido(self, operator, value):
        if operator not in ('=', '!=') or not isinstance(value, bool):
            return NotImplemented
        domain = [('commercial_partner_id.qb_pedimento_virtual', '=', True)]
        if 'l10n_mx_edi_external_trade' in self._fields:
            domain = ['|', ('l10n_mx_edi_external_trade', '=', True)] + domain
        domain = [('move_type', 'in', ('out_invoice', 'out_refund'))] + domain
        if (operator == '=') == value:
            return domain
        return [('id', 'not in', self._search(domain))]


class StockLandedCost(models.Model):
    _inherit = 'stock.landed.cost'

    qb_pedimento_id = fields.Many2one('qb.pedimento', 'Pedimento', index=True, copy=False, ondelete='set null')

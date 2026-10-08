# -*- coding: utf-8 -*-
"""Ganada automática (spec §6): una cotización viva cuyo producto y cliente
aparecen en un pedido confirmado pasa a «Ganada» y se liga al pedido."""
from odoo import models


class SaleOrderCotizador(models.Model):
    _inherit = 'sale.order'

    def action_confirm(self):
        res = super().action_confirm()
        Cot = self.env['qb.cotizador.cotizacion'].sudo()
        for order in self:
            products = order.order_line.mapped('product_id')
            if not products or not order.partner_id:
                continue
            partner = order.partner_id.commercial_partner_id
            vivas = Cot.search([
                ('state', 'in', ('presentada', 'vencida')),
                ('company_id', '=', order.company_id.id),
                ('product_id', 'in', products.ids),
                ('partner_id.commercial_partner_id', '=', partner.id),
            ])
            for cot in vivas:
                cot._marcar_ganada(sale_order=order, automatico=True)
        return res

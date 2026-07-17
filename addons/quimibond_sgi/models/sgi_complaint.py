# -*- coding: utf-8 -*-
from odoo import models, fields


class HelpdeskTicket(models.Model):
    _inherit = 'helpdesk.ticket'

    sgi_sale_order_id = fields.Many2one('sale.order', string="Pedido de venta")
    sgi_product_id = fields.Many2one('product.product', string="Producto")
    sgi_lot_id = fields.Many2one('stock.lot', string="Lote")
    sgi_qty_affected = fields.Float(string="Metros afectados")
    sgi_disposition = fields.Selection([
        ('devolucion', "Devolución"),
        ('reposicion', "Reposición"),
        ('nota_credito', "Nota de crédito"),
        ('concesion', "Concesión"),
        ('na', "N/A"),
    ], string="Disposición")
    sgi_alert_id = fields.Many2one('quality.alert', string="No Conformidad", readonly=True)

    def action_sgi_generate_nc(self):
        self.ensure_one()
        team = self.env.ref('quimibond_sgi.sgi_quality_team_internal', raise_if_not_found=False)
        vals = {
            'title': "Reclamación: %s" % (self.name or ''),
            'sgi_origin_type': 'reclamacion',
            'partner_id': self.partner_id.id,
            'product_id': self.sgi_product_id.id,
            'product_tmpl_id': self.sgi_product_id.product_tmpl_id.id if self.sgi_product_id else False,
            'lot_ids': [(6, 0, self.sgi_lot_id.ids)] if self.sgi_lot_id else False,
            'sgi_deviation': "Reclamación de cliente %s.\n%s" % (
                self.partner_id.display_name or '', self.description or ''),
            'sgi_complaint_ticket_id': self.id,
        }
        if team:
            vals['team_id'] = team.id
        alert = self.env['quality.alert'].create(vals)
        self.sgi_alert_id = alert.id
        return {
            'type': 'ir.actions.act_window',
            'name': "No Conformidad",
            'res_model': 'quality.alert',
            'res_id': alert.id,
            'view_mode': 'form',
        }

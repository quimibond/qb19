# -*- coding: utf-8 -*-
from odoo import models


class ProductProduct(models.Model):
    _inherit = 'product.product'

    # Los campos ficha_tecnica_tejido_id/count y ficha_tecnica_acabado_id/count
    # ya están disponibles aquí automáticamente por la delegación _inherits
    # de product.product hacia product.template (product_tmpl_id). Lo único
    # que falta son los métodos de los botones, que _inherits NO delega
    # (solo delega campos, no métodos Python) — de ahí el error
    # "action_view_ficha_tecnica_tejido no es una acción válida en
    # product.product" al abrir la vista de Variante de producto.

    def action_view_ficha_tecnica_tejido(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': 'Ficha Técnica de Tejido',
            'res_model': 'ficha.tecnica.tejido',
            'view_mode': 'form',
            'res_id': self.ficha_tecnica_tejido_id.id,
        }

    def action_view_ficha_tecnica_acabado(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': 'Ficha Técnica de Acabado',
            'res_model': 'ficha.tecnica.acabado',
            'view_mode': 'form',
            'res_id': self.ficha_tecnica_acabado_id.id,
        }

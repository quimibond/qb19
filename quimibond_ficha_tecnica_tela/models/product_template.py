# -*- coding: utf-8 -*-
from odoo import fields, models


class ProductTemplate(models.Model):
    _inherit = 'product.template'

    ficha_tecnica_tejido_id = fields.Many2one(
        'ficha.tecnica.tejido', compute='_compute_ficha_tecnica_tejido',
        string='Ficha técnica de tejido')
    ficha_tecnica_tejido_count = fields.Integer(
        string='No. de fichas de tejido', compute='_compute_ficha_tecnica_tejido')

    ficha_tecnica_acabado_id = fields.Many2one(
        'ficha.tecnica.acabado', compute='_compute_ficha_tecnica_acabado',
        string='Ficha técnica de acabado')
    ficha_tecnica_acabado_count = fields.Integer(
        string='No. de fichas de acabado', compute='_compute_ficha_tecnica_acabado')

    def _compute_ficha_tecnica_tejido(self):
        FichaTejido = self.env['ficha.tecnica.tejido']
        for tmpl in self:
            product_ids = tmpl.product_variant_ids.ids
            ficha = FichaTejido.search([('product_proceso_id', 'in', product_ids)], limit=1)
            tmpl.ficha_tecnica_tejido_id = ficha
            tmpl.ficha_tecnica_tejido_count = 1 if ficha else 0

    def _compute_ficha_tecnica_acabado(self):
        FichaAcabado = self.env['ficha.tecnica.acabado']
        for tmpl in self:
            product_ids = tmpl.product_variant_ids.ids
            ficha = FichaAcabado.search([('product_acabado_id', 'in', product_ids)], limit=1)
            tmpl.ficha_tecnica_acabado_id = ficha
            tmpl.ficha_tecnica_acabado_count = 1 if ficha else 0

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

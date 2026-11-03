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
            ficha = FichaAcabado.search(['|', ('product_acabado_id', 'in', product_ids),
                                         ('product_proceso_id', 'in', product_ids)], limit=1)
            tmpl.ficha_tecnica_acabado_id = ficha
            tmpl.ficha_tecnica_acabado_count = 1 if ficha else 0

    def action_view_ficha_tecnica_tejido(self):
        """2.23.0: todas las fichas de tejido del crudo (una por circular), no solo la primera."""
        self.ensure_one()
        fichas = self.env['ficha.tecnica.tejido'].search(
            [('product_proceso_id', 'in', self.with_context(active_test=False).product_variant_ids.ids)])
        if len(fichas) == 1:
            return {'type': 'ir.actions.act_window', 'name': 'Ficha de tejido', 'res_model': 'ficha.tecnica.tejido',
                    'view_mode': 'form', 'res_id': fichas.id}
        return {'type': 'ir.actions.act_window', 'name': 'Fichas de tejido — %s' % self.display_name,
                'res_model': 'ficha.tecnica.tejido', 'view_mode': 'list,form', 'domain': [('id', 'in', fichas.ids)]}

    def action_view_ficha_tecnica_acabado(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': 'Ficha Técnica de Acabado',
            'res_model': 'ficha.tecnica.acabado',
            'view_mode': 'form',
            'res_id': self.ficha_tecnica_acabado_id.id,
        }

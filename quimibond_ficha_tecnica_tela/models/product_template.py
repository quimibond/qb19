# -*- coding: utf-8 -*-
from odoo import fields, models


class ProductTemplate(models.Model):
    _inherit = 'product.template'

    ficha_tecnica_ids = fields.One2many(
        'ficha.tecnica.tela', compute='_compute_ficha_tecnica_ids',
        string='Fichas técnicas')
    ficha_tecnica_count = fields.Integer(
        string='No. de fichas técnicas', compute='_compute_ficha_tecnica_ids')

    def _compute_ficha_tecnica_ids(self):
        Ficha = self.env['ficha.tecnica.tela']
        for tmpl in self:
            product_ids = tmpl.product_variant_ids.ids
            fichas = Ficha.search([
                '|',
                ('product_proceso_id', 'in', product_ids),
                ('product_acabado_id', 'in', product_ids),
            ])
            tmpl.ficha_tecnica_ids = fichas
            tmpl.ficha_tecnica_count = len(fichas)

    def action_view_fichas_tecnicas(self):
        self.ensure_one()
        fichas = self.ficha_tecnica_ids
        action = {
            'type': 'ir.actions.act_window',
            'name': 'Fichas Técnicas',
            'res_model': 'ficha.tecnica.tela',
        }
        if len(fichas) == 1:
            action.update({'view_mode': 'form', 'res_id': fichas.id})
        else:
            action.update({'view_mode': 'list,form', 'domain': [('id', 'in', fichas.ids)]})
        return action

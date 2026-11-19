# -*- coding: utf-8 -*-
from odoo import models, fields, api

class MrpWorkorder(models.Model):
    _inherit = 'mrp.workorder'

    product_id_category_name = fields.Char(
        related='production_id.product_id.categ_id.display_name',
        string='Categoria de Producto',
        readonly=True
    )
    pesaje_es_crudo = fields.Boolean(
        string="Crudo de tejido (pesaje)", compute='_compute_pesaje_es_crudo',
        help="El producto de la orden es crudo de tejido (categoría «Producto en Proceso / Tejido Circular» o debajo).")

    @api.depends('production_id.product_id.categ_id.complete_name')
    def _compute_pesaje_es_crudo(self):
        from .mrp_production import es_categ_crudo
        for wo in self:
            wo.pesaje_es_crudo = es_categ_crudo(wo.production_id.product_id.categ_id)

    def action_view_recorded_rolls(self):
        self.ensure_one()
        return self.production_id.action_view_recorded_rolls()
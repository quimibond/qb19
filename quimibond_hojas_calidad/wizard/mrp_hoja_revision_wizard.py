# -*- coding: utf-8 -*-
from odoo import api, fields, models


class MrpHojaRevisionWizard(models.TransientModel):
    """Calidad → Revisión tela acabada: pide la orden en metros para identificar el producto y abre su libro de Inspección."""
    _name = 'mrp.hoja.revision.wizard'
    _description = 'Revisión de tela acabada (libro de Inspección)'

    production_domain_ids = fields.Many2many('mrp.production', compute='_compute_production_domain_ids')
    production_id = fields.Many2one('mrp.production', 'Orden de fabricación en metros', required=True)
    producto_id = fields.Many2one(related='production_id.product_id')

    @api.depends_context('uid')
    def _compute_production_domain_ids(self):
        ordenes = self.env['mrp.production'].sudo()._ordenes_clasificacion()
        for wiz in self:
            wiz.production_domain_ids = ordenes

    def action_abrir(self):
        self.ensure_one()
        return self.env['mrp.hoja.calidad']._abrir(self.production_id.sudo().product_id, 'inspeccion')

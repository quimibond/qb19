# -*- coding: utf-8 -*-
from odoo import api, fields, models, _
from odoo.exceptions import UserError


class MrpHojaRamaWizard(models.TransientModel):
    """Calidad → Registro Rama: pide la orden en kg y abre el libro de Rama del artículo. Lo hace control de calidad, no
    el operador de Rama (Junta de Calidad, 2026-11-13)."""
    _name = 'mrp.hoja.rama.wizard'
    _description = 'Registro Rama (libro de Rama)'

    production_domain_ids = fields.Many2many('mrp.production', compute='_compute_production_domain_ids')
    production_id = fields.Many2one('mrp.production', 'Orden de fabricación en kg', required=True)
    producto_id = fields.Many2one(related='production_id.product_id')

    @api.depends_context('uid')
    def _compute_production_domain_ids(self):
        ordenes = self.env['mrp.production'].sudo()._ordenes_rama()
        for wiz in self:
            wiz.production_domain_ids = ordenes

    def action_abrir(self):
        self.ensure_one()
        orden = self.production_id.sudo()
        if orden not in self.env['mrp.production'].sudo()._ordenes_rama():
            raise UserError(_('La orden %s no está abierta o su operación de Rama aún no inicia: no se registran rollos.',
                              orden.name))
        return self.env['mrp.hoja.calidad']._abrir(orden.product_id, 'rama')

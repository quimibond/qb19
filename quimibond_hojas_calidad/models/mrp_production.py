# -*- coding: utf-8 -*-
from odoo import api, models


class MrpProduction(models.Model):
    _inherit = 'mrp.production'

    @api.model
    def _ordenes_rama(self):
        """Órdenes en kg abiertas cuya operación de Rama está en proceso o terminada (Junta de Calidad, 2026-11-13):
        control de calidad no registra rollos de una Rama que todavía no inicia."""
        operaciones = self.env['mrp.workorder'].search([
            ('state', 'in', ('progress', 'done')), ('production_id.state', 'not in', ('done', 'cancel'))])
        return operaciones.filtered('es_rama').production_id

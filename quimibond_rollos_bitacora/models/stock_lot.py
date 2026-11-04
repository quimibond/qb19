# -*- coding: utf-8 -*-
from odoo import models, _
from odoo.exceptions import UserError


class StockLot(models.Model):
    _inherit = 'stock.lot'

    def write(self, vals):
        # El lote-serie del rollo (Orden de Fabricación + No. de rollo) es inmutable (§12.4).
        if 'name' in vals:
            Bitacora = self.env['mrp.rollo.bitacora'].sudo()
            for lote in self:
                if lote.name != vals['name'] and Bitacora.search_count(
                        ['|', ('lot_id', '=', lote.id), ('lot_b_id', '=', lote.id)]):
                    raise UserError(_('El lote «%s» es el de un rollo de la bitácora y no se puede renombrar.',
                                      lote.name))
        return super().write(vals)

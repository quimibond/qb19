# -*- coding: utf-8 -*-
from odoo import models


class StockMove(models.Model):
    _inherit = 'stock.move'

    def _adjust_procure_method(self, picking_type_code=False):
        """3.3.1: la orden de saldos de Inspección consume rollos de Rama que ya están en inventario; su consumo siempre sale
        de existencias y no lanza abastecimiento (con la regla bajo pedido de la ruta del producto en kg se creaba una
        orden en kg nueva)."""
        if self.env.context.get('qb_insp_sin_abastecer'):
            self.procure_method = 'make_to_stock'
            return None
        return super()._adjust_procure_method(picking_type_code=picking_type_code)

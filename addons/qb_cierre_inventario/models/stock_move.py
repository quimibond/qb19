from odoo import api, models
from odoo.fields import Domain


class StockMove(models.Model):
    _inherit = 'stock.move'

    @api.model
    def _read_group(self, domain, *args, **kwargs):
        # res.company._get_location_valuation_vals agrupa los movimientos con _read_group y no
        # recibe un filtro de producto: el contexto lleva las categorías que no se valúan.
        excluir = self.env.context.get('qb_cierre_excluir_categ_ids')
        if excluir:
            domain = Domain(domain) & Domain('product_id.categ_id', 'not in', excluir)
        return super()._read_group(domain, *args, **kwargs)

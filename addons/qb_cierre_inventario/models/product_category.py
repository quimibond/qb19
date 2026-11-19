from odoo import api, fields, models


class ProductCategory(models.Model):
    _inherit = 'product.category'

    qb_excluir_cierre = fields.Boolean(
        string='No valuar en el cierre',
        help='El cierre de inventario y el reporte de valuación ignoran los productos de esta '
             'categoría y de sus subcategorías: ni su existencia ni sus consumos pasan a la '
             'contabilidad. Para refacciones que van a gasto al comprarse y para activo fijo.',
    )

    @api.model
    def _qb_categorias_excluidas(self):
        """Categorías marcadas y todas sus subcategorías."""
        marcadas = self.search([('qb_excluir_cierre', '=', True)])
        if not marcadas:
            return self.browse()
        return self.search([('id', 'child_of', marcadas.ids)])

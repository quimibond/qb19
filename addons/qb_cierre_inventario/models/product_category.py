from odoo import api, fields, models


class ProductCategory(models.Model):
    _inherit = 'product.category'

    qb_excluir_cierre = fields.Boolean(
        string='No valuar en el cierre',
        help='El cierre de inventario y el reporte de valuación ignoran los productos de esta '
             'categoría: ni su existencia ni sus consumos pasan a la contabilidad. Para refacciones '
             'que van a gasto al comprarse y para activo fijo. Cada subcategoría se marca por separado '
             '(Empaque / Bolsas y Tubos sí se valúan aunque cuelguen de Refacciones).',
    )

    @api.model
    def _qb_categorias_excluidas(self):
        """Solo las categorías marcadas: una subcategoría no hereda la casilla."""
        return self.search([('qb_excluir_cierre', '=', True)])

# -*- coding: utf-8 -*-
"""1.3.0: excepción al «precio de lista mínimo plausible» por producto y por
categoría (las tiras perforadas Perfoquim cuestan menos de $5/m de verdad).

El umbral general (Ajustes, ``quimibond_sgi.price_min_plausible``, $5) no
cambia: protege del placeholder de $1 al resto del catálogo. Un producto o una
categoría pueden llevar su propio mínimo; el del producto manda, luego el de la
categoría más cercana (la categoría o su padre, abuelo…) y al final el general.
"""
from odoo import fields, models

_MIN_PRICE_HELP = (
    "Presupuesto de ventas: precio de lista mínimo (moneda de la compañía) "
    "con que una regla de la lista cuenta como precio real para {target}. "
    "Vacío (0) = se usa el de {fallback}. Úselo para productos que de verdad "
    "cuestan menos que el umbral general de Ajustes (p. ej. tiras perforadas a "
    "$0.55/m): con 0.01 se acepta cualquier precio mayor que cero. Ojo: un "
    "placeholder de $1 por encima de este mínimo se tomará como precio real.")


class ProductTemplate(models.Model):
    _inherit = 'product.template'

    sgi_budget_min_price = fields.Float(
        string="Precio mínimo plausible propio",
        digits='Product Price',
        help=_MIN_PRICE_HELP.format(
            target="este producto",
            fallback="la categoría (o el general de Ajustes)"))

    def _sgi_budget_min_price(self):
        """Mínimo propio del producto o, si no tiene, el de su categoría más
        cercana. ``None`` si ninguno lo define (entonces aplica el general)."""
        self.ensure_one()
        if self.sgi_budget_min_price > 0:
            return self.sgi_budget_min_price, "producto"
        return self.categ_id._sgi_budget_min_price()


class ProductCategory(models.Model):
    _inherit = 'product.category'

    sgi_budget_min_price = fields.Float(
        string="Precio mínimo plausible propio",
        digits='Product Price',
        help=_MIN_PRICE_HELP.format(
            target="los productos de esta categoría y sus subcategorías",
            fallback="la categoría padre (o el general de Ajustes)"))

    def _sgi_budget_min_price(self):
        """(mínimo, origen) de la categoría más cercana que lo define, subiendo
        por los padres; ``None`` si ninguna."""
        categ = self
        while categ:
            if categ.sgi_budget_min_price > 0:
                return categ.sgi_budget_min_price, "categoría '%s'" % categ.display_name
            categ = categ.parent_id
        return None

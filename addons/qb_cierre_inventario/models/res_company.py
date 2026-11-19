from odoo import models


class ResCompany(models.Model):
    _inherit = 'res.company'

    def _qb_categorias_excluidas(self):
        return self.env['product.category'].sudo()._qb_categorias_excluidas()

    def _get_valuation_product_domain(self):
        domain = super()._get_valuation_product_domain()
        excluidas = self._qb_categorias_excluidas()
        if excluidas:
            domain = list(domain) + [('categ_id', 'not in', excluidas.ids)]
        return domain

    def _get_location_valuation_vals(self, at_date=None, location_domain=False):
        excluidas = self._qb_categorias_excluidas()
        if excluidas:
            self = self.with_context(qb_cierre_excluir_categ_ids=excluidas.ids)
        return super()._get_location_valuation_vals(at_date=at_date, location_domain=location_domain)

    def _qb_cuentas_excluidas(self):
        """Cuentas de inventario que solo usan categorías que no se valúan."""
        self.ensure_one()
        Categ = self.env['product.category'].with_company(self).sudo()
        excluidas = self._qb_categorias_excluidas().with_company(self)
        if not excluidas:
            return self.env['account.account']
        cuentas = excluidas.property_stock_valuation_account_id
        otras = Categ.search([('id', 'not in', excluidas.ids)]).property_stock_valuation_account_id
        return cuentas - otras - self.account_stock_valuation_id

    def stock_accounting_value(self, accounts_by_product=None, at_date=None):
        data = super().stock_accounting_value(accounts_by_product=accounts_by_product, at_date=at_date)
        for cuenta in self._qb_cuentas_excluidas():
            data.pop(cuenta, None)
        return data

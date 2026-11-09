# -*- coding: utf-8 -*-
"""VE-02 sobre la venta real (19.0.3.12.0, diseño v3 2.7).

Decisión de Jose (4-nov): los saldos y el desperdicio no cuentan en el cumplimiento del
presupuesto. Van a la misma cuenta de venta (401) que la tela, así que el VE-02 los sumaba y
el cierre del Excel no (27 mil en septiembre industrial; 0.19 % de la venta ene–oct 2026).

Con este módulo instalado, el VE-02 («Cumplimiento de presupuesto de ventas») y lo que el
presupuesto compara contra la venta (base del año anterior, cumplimiento al corte) miden la
**misma venta que «Venta real»** (`qb.sales.actual`): las líneas de venta del VE-02 sin los
productos de saldo o desperdicio (``sgi.sales.budget._qb_is_saldo``). Se calcula directo de
las facturas con la misma regla, no de la tabla, para no depender de que esté reconstruida.

El resto de los indicadores de venta (VE-01, cartera, DSO…) no cambian: el filtro se pide con
el contexto ``qb_sin_saldos`` solo desde el VE-02 y el presupuesto.
"""
from odoo import models


class SgiIndicatorVentaReal(models.Model):
    _inherit = 'sgi.indicator'

    def _qb_saldo_amount(self, date_from, date_to):
        """Venta (−balance) de los productos de saldo o desperdicio en el rango, con la regla
        de líneas de venta del VE-02."""
        Budget = self.env['sgi.sales.budget']
        groups = self.env['account.move.line']._read_group(
            self._sgi_sales_lines_domain(date_from, date_to), ['product_id'], ['balance:sum'])
        return -sum(balance or 0.0 for product, balance in groups
                    if product and Budget._qb_is_saldo(product))

    def _sgi_net_invoiced(self, date_from, date_to, taxed=False):
        total = super()._sgi_net_invoiced(date_from, date_to, taxed=taxed)
        if self.env.context.get('qb_sin_saldos') and not taxed:
            total -= self._qb_saldo_amount(date_from, date_to)
        return total

    def _calc_presupuesto_ventas(self, date_from, date_to):
        return super(SgiIndicatorVentaReal, self.with_context(qb_sin_saldos=True))._calc_presupuesto_ventas(
            date_from, date_to)

    def _note_presupuesto_ventas(self, date_from, date_to):
        note = super()._note_presupuesto_ventas(date_from, date_to)
        saldos = self._qb_saldo_amount(date_from, date_to)
        if saldos:
            extra = ("Venta sin saldos ni desperdicio (%s fuera del cumplimiento)."
                     % '{:,.2f}'.format(saldos))
            note = "%s %s" % (note, extra) if note else extra
        return note

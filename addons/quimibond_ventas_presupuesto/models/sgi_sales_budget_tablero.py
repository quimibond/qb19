# -*- coding: utf-8 -*-
"""Cifras de la ficha del presupuesto anual (3.22.0; 3.23.0 con vistas nativas).

Jose (19-nov): «lo necesito lo más nativo de Odoo posible». La ficha muestra campos normales
(montos, porcentajes con barra de avance) y los botones inteligentes abren el comparativo
(``qb.sales.compare``) en pivote, gráfica y lista, agrupado por mercado, mes o cliente, con bajada
al detalle. Aquí solo se calculan las cifras:

- ``amount_month_plan_fill``: lo que el presupuesto del mes pone en los meses y mercados sin renglones
  (``_qb_month_plan_fill``; la REV2 de 2026 no trae Industrial ni Proyectos en noviembre y diciembre).
- ``amount_year_full``: renglones más eso.
- ``amount_close_estimate``: lo vendido en los meses cerrados; en los que faltan, por mercado, el
  presupuesto del mes si trae ese mercado y si no el anual; si ya se vendió más, lo vendido.
"""
from collections import defaultdict
from datetime import date

from dateutil.relativedelta import relativedelta

from odoo import api, fields, models

from .sgi_sales_budget import MERCADO_BASE, team_mercado


class SgiSalesBudgetTablero(models.Model):
    _inherit = 'sgi.sales.budget'

    amount_month_plan_fill = fields.Monetary(
        string="Del presupuesto del mes", compute='_compute_qb_tablero',
        help="Lo que el presupuesto del mes pone en los meses y mercados que este presupuesto no trae.")
    amount_year_full = fields.Monetary(
        string="Presupuesto del año", compute='_compute_qb_tablero',
        help="Renglones del presupuesto más lo que el presupuesto del mes pone en los meses y mercados "
             "sin renglones.")
    amount_close_estimate = fields.Monetary(
        string="Cierre estimado", compute='_compute_qb_tablero',
        help="Lo vendido en los meses cerrados más, en los que faltan y por mercado, el presupuesto del mes "
             "(si trae ese mercado) o el anual; si ya se vendió más, lo vendido.")
    close_estimate_pct = fields.Float(
        string="Cierre estimado contra el año (%)", compute='_compute_qb_tablero', digits=(16, 1))

    def _qb_month_sales(self):
        """{(mes 1-12, mercado base): venta} del año con la regla del cumplimiento (sin saldos)."""
        self.ensure_one()
        out = defaultdict(float)
        if self.year > fields.Date.context_today(self).year:
            return out
        for aml in self._qb_sales_lines():
            if aml.product_id and self._qb_is_saldo(aml.product_id):
                continue
            out[(aml.move_id.invoice_date.month, team_mercado(self.env, aml.move_id.team_id))] += -aml.balance
        return out

    def _qb_month_plan_by_mercado(self):
        """{(mes 1-12, mercado base): importe} de los presupuestos del mes del año."""
        self.ensure_one()
        out = defaultdict(float)
        if 'qb.sales.month.plan.line' not in self.env:
            return out
        for line in self.env['qb.sales.month.plan.line'].sudo().search([
                ('plan_id.company_id', '=', self.company_id.id),
                ('plan_id.month', '>=', date(self.year, 1, 1)), ('plan_id.month', '<=', date(self.year, 12, 31))]):
            base = MERCADO_BASE.get(line.mercado)
            if base:
                out[(line.plan_id.month.month, base)] += line.amount
        return out

    @api.depends('line_ids.amount_budget', 'line_ids.date', 'line_ids.mercado', 'year', 'kind', 'state')
    def _compute_qb_tablero(self):
        for budget in self:
            if budget.kind != 'presupuesto' or budget.state == 'obsoleto' or not budget.year:
                budget.amount_month_plan_fill = budget.amount_year_full = 0.0
                budget.amount_close_estimate = budget.close_estimate_pct = 0.0
                continue
            fill = budget._qb_month_plan_fill()
            budget.amount_month_plan_fill = sum(fill.values())
            budget.amount_year_full = budget.amount_budget_total + budget.amount_month_plan_fill
            annual = defaultdict(float)
            for line in budget.line_ids.filtered('date'):
                annual[(line.date.month, MERCADO_BASE.get(line.mercado))] += line.amount_budget
            for (month_date, mercado), amount in fill.items():
                annual[(month_date.month, MERCADO_BASE[mercado])] += amount
            plan = budget._qb_month_plan_by_mercado()
            sold = budget._qb_month_sales()
            cutoff = budget.cutoff_date
            estimate = 0.0
            for month in range(1, 13):
                month_end = date(budget.year, month, 1) + relativedelta(months=1, days=-1)
                closed = bool(cutoff and month_end <= cutoff)
                for base in ('industrial', 'confeccion', 'especiales', None):
                    sold_m = sold.get((month, base), 0.0)
                    if closed or base is None:
                        estimate += sold_m
                        continue
                    expected = plan[(month, base)] if (month, base) in plan else annual.get((month, base), 0.0)
                    estimate += max(expected, sold_m)
            budget.amount_close_estimate = estimate
            budget.close_estimate_pct = (round(estimate / budget.amount_year_full * 100.0, 1)
                                         if budget.amount_year_full else 0.0)

    # --- Botones inteligentes ----------------------------------------------------
    def _qb_compare_action(self, name, rows, cols, view_order=('pivot', 'graph', 'list')):
        """Comparativo (``qb.sales.compare``) de este presupuesto: sus renglones, lo que pone el presupuesto
        del mes y la venta real del año, en la compañía."""
        self.ensure_one()
        refs = {'pivot': 'qb_sales_compare_view_pivot', 'graph': 'qb_sales_compare_view_graph',
                'list': 'qb_sales_compare_view_list'}
        return {
            'type': 'ir.actions.act_window',
            'name': "%s — %s" % (name, self.name),
            'res_model': 'qb.sales.compare',
            'view_mode': ','.join(view_order),
            'views': [(self.env.ref('quimibond_ventas_presupuesto.%s' % refs[v]).id, v) for v in view_order],
            'search_view_id': self.env.ref('quimibond_ventas_presupuesto.qb_sales_compare_view_search').id,
            'domain': [('company_id', '=', self.company_id.id), ('year', '=', self.year), '|',
                       ('budget_id', '=', self.id), ('fuente', '=', 'real')],
            'context': {'pivot_row_groupby': rows, 'pivot_column_groupby': cols,
                        'pivot_measures': ['amount_budget', 'amount_real', 'amount_diff'],
                        'graph_groupbys': rows[:1] + ['fuente'], 'graph_measure': 'amount',
                        'search_default_sin_pronostico': 1},
        }

    def action_qb_compare_mercado(self):
        return self._qb_compare_action("Por mercado", ['mercado'], [])

    def action_qb_compare_mes(self):
        return self._qb_compare_action("Por mes", ['month:month'], [], view_order=('graph', 'pivot', 'list'))

    def action_qb_compare_cliente(self):
        return self._qb_compare_action("Por cliente", ['partner_id', 'articulo_id'], [])

    def action_qb_open_lines(self):
        """Los renglones se abren aquí (lista agrupada por mercado y cliente, matriz, pivote)."""
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Renglones — %s" % self.name,
            'res_model': 'sgi.sales.budget.line',
            'view_mode': 'list,grid,pivot,form',
            'views': [
                (self.env.ref('quimibond_ventas_presupuesto.sgi_sales_budget_line_view_list').id, 'list'),
                (self.env.ref('quimibond_ventas_presupuesto.sgi_sales_budget_line_grid_qty').id, 'grid'),
                (self.env.ref('quimibond_ventas_presupuesto.sgi_sales_budget_line_view_pivot').id, 'pivot'),
                (self.env.ref('quimibond_ventas_presupuesto.sgi_sales_budget_line_view_form').id, 'form')],
            'search_view_id': self.env.ref('quimibond_ventas_presupuesto.sgi_sales_budget_line_view_search').id,
            'domain': [('budget_id', '=', self.id)],
            'context': {'default_budget_id': self.id, 'group_by': ['mercado', 'partner_id'],
                        'grid_anchor': fields.Date.to_string(date(self.year, 1, 1))},
        }

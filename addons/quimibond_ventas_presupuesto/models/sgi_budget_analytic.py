# -*- coding: utf-8 -*-
"""P-3: el presupuesto de gastos (budget.analytic, app Presupuestos) se liga
con el presupuesto de ventas del SGI del que parte. Con eso la actividad
E1.02 «Armar el presupuesto de gastos…» se mide sola: su entrada (el
presupuesto de ventas aprobado) se liga con su salida por
``sgi_sales_budget_id`` (``"match": "sgi_sales_budget_id"`` en el JSON)."""
from odoo import fields, models


class BudgetAnalytic(models.Model):
    _inherit = 'budget.analytic'

    sgi_sales_budget_id = fields.Many2one(
        'sgi.sales.budget', string="Presupuesto de ventas base", index=True,
        ondelete='set null', domain="[('kind', '=', 'presupuesto')]",
        help="Presupuesto de ventas del SGI del que parte este presupuesto de "
             "gastos e inversiones (E1.02).")

# -*- coding: utf-8 -*-
"""Supuestos por cliente del presupuesto anual (19.0.3.33.0).

Jose (23-nov-2026): «todo tiene que tener soporte». El crecimiento ya no es un porcentaje por mercado:
solo un supuesto por cliente, con su motivo y su evidencia (archivo o liga), cambia la venta base de
ese cliente desde un mes. Los renglones que tocó lo llevan en «Supuesto del cliente»; sin evidencia,
quedan sin soporte.
"""
from odoo import api, fields, models

from .sgi_sales_budget import MESES

MONTHS = [(str(m), MESES[m - 1].capitalize()) for m in range(1, 13)]


class QbSalesBudgetAssumption(models.Model):
    _name = 'qb.sales.budget.assumption'
    _description = "Supuesto por cliente del presupuesto de ventas"
    _order = 'budget_id, partner_id, month_from'

    budget_id = fields.Many2one('sgi.sales.budget', string="Presupuesto", required=True, index=True,
                                ondelete='cascade')
    company_id = fields.Many2one(related='budget_id.company_id', store=True)
    partner_id = fields.Many2one('res.partner', string="Cliente", required=True,
                                 domain="[('is_company', '=', True)]")
    growth_pct = fields.Float(
        string="Cambio sobre la venta base (%)", digits=(6, 1),
        help="Sobre lo vendido en el periodo base. −100 = deja de comprar.")
    month_from = fields.Selection(MONTHS, string="Desde", required=True, default='1')
    note = fields.Char(string="Motivo", required=True,
                       help="Programa nuevo, fin de un modelo, cliente que se va…")
    evidence_url = fields.Char(string="Liga de la evidencia",
                               help="Correo, carpeta o documento donde está el respaldo.")
    attachment_ids = fields.Many2many('ir.attachment', string="Evidencia",
                                      help="OC, nominación, correo del cliente.")
    evidence_ok = fields.Boolean(string="Con evidencia", compute='_compute_evidence_ok', store=True)
    line_count = fields.Integer(string="Renglones", compute='_compute_line_count')

    @api.depends('attachment_ids', 'evidence_url')
    def _compute_evidence_ok(self):
        for assumption in self:
            assumption.evidence_ok = bool(assumption.attachment_ids or (assumption.evidence_url or '').strip())

    def _compute_line_count(self):
        Line = self.env['sgi.sales.budget.line']
        for assumption in self:
            assumption.line_count = Line.search_count([('assumption_id', '=', assumption.id)])

# -*- coding: utf-8 -*-
"""Tipo de cambio por mes del presupuesto anual (diseño v3, 2.6; 19.0.3.5.0).

Un tipo de cambio USD/MXN (pesos por dólar) por mes, o el mismo en los 12 con
«Aplicar a los 12 meses». El importe de un renglón con precio en dólares usa el
del mes de su fecha; si el mes no tiene, el presupuestal de Ajustes, que se
guarda en el documento al aprobar. Aprobado o reemplazado, no se toca.
"""
from odoo import api, fields, models
from odoo.exceptions import UserError, ValidationError

MONTHS = [(str(m), name) for m, name in enumerate(
    ("Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre",
     "Octubre", "Noviembre", "Diciembre"), start=1)]
LOCKED = ('aprobado', 'reemplazado')


class QbSalesBudgetRate(models.Model):
    _name = 'qb.sales.budget.rate'
    _description = "Tipo de cambio por mes del presupuesto de ventas"
    _order = 'budget_id, month'

    budget_id = fields.Many2one('sgi.sales.budget', string="Presupuesto", required=True,
                                ondelete='cascade', index=True)
    company_id = fields.Many2one(related='budget_id.company_id', store=True)
    month = fields.Integer(string="Mes", required=True)
    month_name = fields.Char(string="Mes (nombre)", compute='_compute_month_name')
    rate = fields.Float(string="Pesos por dólar", digits=(12, 4), required=True)

    _budget_month_uniq = models.Constraint(
        'unique (budget_id, month)', "Ese mes ya tiene tipo de cambio en este presupuesto.")

    @api.depends('month')
    def _compute_month_name(self):
        names = dict(MONTHS)
        for rate in self:
            rate.month_name = names.get(str(rate.month), '')

    @api.constrains('month', 'rate')
    def _check_values(self):
        for rate in self:
            if not 1 <= rate.month <= 12:
                raise ValidationError("El mes va de 1 a 12.")
            if rate.rate < 0:
                raise ValidationError("El tipo de cambio no puede ser negativo.")

    def _qb_check_lock(self, budgets):
        if self.env.su or self.env.user.has_group('quimibond_sgi.group_sgi_manager'):
            return
        locked = budgets.filtered(lambda b: b.state in LOCKED)
        if locked:
            raise UserError("El tipo de cambio de un presupuesto aprobado no se cambia (%s): "
                            "haga una revisión nueva." % ", ".join(locked.mapped('name')))

    @api.model_create_multi
    def create(self, vals_list):
        self._qb_check_lock(self.env['sgi.sales.budget'].browse(
            [v.get('budget_id') for v in vals_list if v.get('budget_id')]))
        return super().create(vals_list)

    def write(self, vals):
        self._qb_check_lock(self.budget_id)
        return super().write(vals)

    def unlink(self):
        self._qb_check_lock(self.budget_id)
        return super().unlink()

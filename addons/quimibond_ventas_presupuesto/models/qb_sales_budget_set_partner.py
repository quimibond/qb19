# -*- coding: utf-8 -*-
"""Poner cliente a varios renglones a la vez (19.0.3.6.4).

El anti-doble-conteo no deja que un producto tenga renglones con cliente y sin
cliente en el mismo mercado. Cambiar el cliente renglón por renglón pasa por ese
estado a medias y el sistema lo rechaza; aquí se cambian todos en una sola
escritura, así que solo cuenta cómo quedan al final.
"""
from markupsafe import Markup

from odoo import api, fields, models
from odoo.exceptions import UserError

from .sgi_sales_budget import MERCADOS_CON_CLIENTE


class QbSalesBudgetSetPartner(models.TransientModel):
    _name = 'qb.sales.budget.set.partner'
    _description = "Poner cliente a renglones del presupuesto"

    budget_id = fields.Many2one('sgi.sales.budget', string="Presupuesto", required=True)
    line_ids = fields.Many2many(
        'sgi.sales.budget.line', string="Renglones",
        domain="[('budget_id', '=', budget_id)]",
        help="Renglones a los que se les pone el cliente. Para no mezclar, elija todos los "
             "renglones del producto en ese mercado.")
    partner_id = fields.Many2one(
        'res.partner', string="Cliente", domain="[('is_company', '=', True)]",
        help="Vacío quita el cliente (presupuesto global del producto, como en Confección).")

    @api.model
    def default_get(self, fields_list):
        res = super().default_get(fields_list)
        ctx = self.env.context
        lines = self.env['sgi.sales.budget.line']
        if ctx.get('active_model') == 'sgi.sales.budget.line' and ctx.get('active_ids'):
            lines = lines.browse(ctx['active_ids'])
            if len(lines.budget_id) > 1:
                raise UserError("Elija renglones de un solo presupuesto.")
            res['budget_id'] = lines.budget_id.id
        elif ctx.get('active_model') == 'sgi.sales.budget' and ctx.get('active_id'):
            budget = self.env['sgi.sales.budget'].browse(ctx['active_id'])
            res['budget_id'] = budget.id
            # De entrada, los que bloquean la aprobación: Industrial o Especiales sin cliente.
            lines = budget.line_ids.filtered(
                lambda l: l.mercado in MERCADOS_CON_CLIENTE and not l.partner_id)
        if 'line_ids' in fields_list:
            res['line_ids'] = [(6, 0, lines.ids)]
        return res

    def action_apply(self):
        self.ensure_one()
        lines = self.line_ids.filtered(lambda l: l.budget_id == self.budget_id)
        if not lines:
            raise UserError("Elija los renglones a los que se les pone el cliente.")
        if self.budget_id.state not in ('borrador', 'revisado'):
            raise UserError(
                "El cliente se cambia en un presupuesto en borrador o revisado; el %s está "
                "aprobado o cerrado. Haga una nueva revisión." % self.budget_id.name)
        lines.write({'partner_id': self.partner_id.id or False})
        self.budget_id.message_post(body=Markup("Cliente <b>%s</b> puesto a %d renglón(es).") % (
            self.partner_id.display_name or "(sin cliente)", len(lines)))
        return {'type': 'ir.actions.act_window_close'}


class SgiSalesBudgetSetPartner(models.Model):
    _inherit = 'sgi.sales.budget'

    def action_qb_open_set_partner(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'name': "Poner cliente",
                'res_model': 'qb.sales.budget.set.partner', 'view_mode': 'form', 'target': 'new',
                'context': {'active_model': 'sgi.sales.budget', 'active_id': self.id}}

    def action_qb_set_partner(self, line_ids, partner_id):
        """Pone ``partner_id`` (o nada) a los renglones ``line_ids`` de este presupuesto en
        una sola escritura; para MCP y el asistente."""
        self.ensure_one()
        wizard = self.env['qb.sales.budget.set.partner'].create({
            'budget_id': self.id, 'line_ids': [(6, 0, line_ids)], 'partner_id': partner_id or False})
        wizard.action_apply()
        return True

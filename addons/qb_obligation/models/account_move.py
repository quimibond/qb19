# -*- coding: utf-8 -*-
from odoo import api, fields, models


class AccountMove(models.Model):
    _inherit = 'account.move'

    obligation_count = fields.Integer(compute='_compute_obligation_count')

    def _compute_obligation_count(self):
        Obligation = self.env['qb.obligation']
        counts = {}
        if self.ids:
            groups = Obligation._read_group(
                [('res_model', '=', 'account.move'), ('res_id', 'in', self.ids),
                 ('state', 'in', ('candidate', 'confirmed'))],
                ['res_id'], ['__count'])
            counts = {res_id: count for res_id, count in groups}
        for move in self:
            move.obligation_count = counts.get(move.id, 0)

    def action_open_obligations(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window', 'res_model': 'qb.obligation',
            'name': 'Obligaciones', 'view_mode': 'list,form',
            'domain': [('res_model', '=', 'account.move'), ('res_id', '=', self.id)],
            'context': {'default_res_model': 'account.move', 'default_res_id': self.id,
                        'default_partner_id': self.partner_id.commercial_partner_id.id},
        }

    @api.model
    def _obligation_overdue_domain(self, company, today):
        grace = company.obligation_grace_days or 0
        limit = fields.Date.subtract(today, days=grace)
        return [
            ('company_id', '=', company.id), ('move_type', '=', 'out_invoice'), ('state', '=', 'posted'),
            ('payment_state', 'in', ('not_paid', 'partial')), ('invoice_date_due', '<', limit),
        ]

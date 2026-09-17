# -*- coding: utf-8 -*-
"""Estado del SAT visible en la factura: qué CFDI del SAT están ligados a
ella y un aviso cuando el SAT y Odoo no coinciden (cancelada en uno y no en
el otro, o totales distintos)."""
from odoo import _, api, fields, models


class AccountMove(models.Model):
    _inherit = 'account.move'

    sat_cfdi_ids = fields.One2many('sat.cfdi', 'move_id', string='CFDI en el SAT')
    sat_cfdi_count = fields.Integer(compute='_compute_sat', string='CFDI SAT')
    sat_estado = fields.Selection([
        ('vigente', 'Vigente en el SAT'),
        ('cancelado', 'Cancelado en el SAT'),
        ('sin_cfdi', 'Sin CFDI en el SAT'),
    ], string='Estado SAT', compute='_compute_sat')
    sat_alerta = fields.Char(string='Aviso SAT', compute='_compute_sat')

    @api.depends('sat_cfdi_ids.estado_sat', 'sat_cfdi_ids.issue', 'sat_cfdi_ids.total', 'state', 'amount_total')
    def _compute_sat(self):
        for move in self:
            cfdis = move.sat_cfdi_ids.filtered(lambda c: c.tipo in ('I', 'E'))
            move.sat_cfdi_count = len(move.sat_cfdi_ids)
            if not cfdis:
                move.sat_estado = 'sin_cfdi'
                move.sat_alerta = False
                continue
            vigente = cfdis.filtered(lambda c: c.estado_sat != 'cancelado')
            move.sat_estado = 'vigente' if vigente else 'cancelado'
            if move.state == 'posted' and not vigente:
                move.sat_alerta = _('Esta factura está CANCELADA en el SAT y sigue publicada en Odoo.')
            elif move.state == 'cancel' and vigente:
                move.sat_alerta = _('Esta factura está cancelada en Odoo pero su CFDI sigue VIGENTE en el SAT.')
            elif any(c.issue == 'moneda' for c in vigente):
                move.sat_alerta = _('El CFDI en el SAT está en otra moneda que esta factura.')
            elif any(c.issue == 'monto' for c in vigente):
                c = vigente.filtered(lambda c: c.issue == 'monto')[0]
                move.sat_alerta = _('El total del CFDI en el SAT (%(sat).2f) no coincide con la factura (%(odoo).2f).') % {
                    'sat': c.total, 'odoo': move.amount_total}
            else:
                move.sat_alerta = False

    def action_open_sat_cfdi(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window', 'name': _('CFDI en el SAT'),
            'res_model': 'sat.cfdi', 'view_mode': 'list,form',
            'domain': [('move_id', '=', self.id)],
        }

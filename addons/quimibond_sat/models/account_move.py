# -*- coding: utf-8 -*-
"""Estado del SAT visible en la factura: qué CFDI del SAT están ligados a
ella y un aviso cuando el SAT y Odoo no coinciden (cancelada en uno y no en
el otro, o totales distintos)."""
from datetime import timedelta

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
    sat_uuid = fields.Char(string='Folio fiscal (SAT)', compute='_compute_sat_uuid',
                           help='UUID del CFDI del SAT ligado a esta factura (vigente primero).')

    @api.depends('sat_cfdi_ids.uuid', 'sat_cfdi_ids.estado_sat', 'sat_cfdi_ids.tipo')
    def _compute_sat_uuid(self):
        for move in self:
            cfdis = move.sat_cfdi_ids.filtered(lambda c: c.tipo in ('I', 'E'))
            vigentes = cfdis.filtered(lambda c: c.estado_sat != 'cancelado') or cfdis
            move.sat_uuid = (vigentes[:1].uuid or '').upper() if vigentes else False

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

    def action_sat_reconcile(self):
        self.ensure_one()
        return self.env['sat.reconcile.wizard'].open_for(self)

    def _sat_reconcile_candidates(self, days=180, tol_pct=0.005):
        """CFDI del SAT sin factura (o ignorados) de la misma contraparte y
        tipo, con el mismo total (±tol_pct, mín. $1) y fecha a ±days."""
        self.ensure_one()
        Cfdi = self.env['sat.cfdi']
        rfc = self.commercial_partner_id.vat
        if not rfc or self.move_type not in ('out_invoice', 'out_refund', 'in_invoice', 'in_refund'):
            return Cfdi
        direction = 'issued' if self.move_type.startswith('out_') else 'received'
        tipo = 'E' if self.move_type.endswith('_refund') else 'I'
        tol = max(1.0, tol_pct * abs(self.amount_total)) if tol_pct else 0.01
        domain = [('company_id', '=', self.company_id.id), ('direction', '=', direction), ('tipo', '=', tipo),
                  ('estado_sat', '=', 'vigente'), ('match_status', 'in', ('solo_sat', 'ignorado')),
                  ('counterparty_rfc', '=ilike', rfc),
                  ('total', '>=', abs(self.amount_total) - tol), ('total', '<=', abs(self.amount_total) + tol)]
        if self.invoice_date and days:
            domain += [('fecha_emision', '>=', fields.Datetime.to_datetime(self.invoice_date) - timedelta(days=days)),
                       ('fecha_emision', '<', fields.Datetime.to_datetime(self.invoice_date) + timedelta(days=days + 1))]

        def key(cfdi):
            d = fields.Date.to_date(cfdi.fecha_emision) if cfdi.fecha_emision else None
            dd = abs((d - self.invoice_date).days) if (d and self.invoice_date) else 999
            return (round(abs(cfdi.total - abs(self.amount_total)), 2), dd, -cfdi.id)
        return Cfdi.search(domain).sorted(key=key)

    def action_open_sat_cfdi(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window', 'name': _('CFDI en el SAT'),
            'res_model': 'sat.cfdi', 'view_mode': 'list,form',
            'domain': [('move_id', '=', self.id)],
        }

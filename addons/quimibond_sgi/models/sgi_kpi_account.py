# -*- coding: utf-8 -*-
"""Campos que faltaban para que los indicadores capturados a mano pasen a
fórmula configurable (55.0.0, 2026-09-25); guardados en la base para poder
filtrarlos desde un término (``sgi.indicator.term``). A-022 (auditoría
2026-09): antes vivían todos en ``sgi_kpi_fields.py``; ahora un archivo por
tema (``sgi_kpi_sales``, ``sgi_kpi_quality``, ``sgi_kpi_account``,
``sgi_kpi_review``, ``sgi_kpi_hr``). Mover código entre archivos no toca la
base.

Contabilidad y compras:

- S1-04 ``account.move.sgi_payment_date``: fecha del último pago que dejó la
  factura pagada.
- S1-05 ``account.move.line.sgi_po_price_diff``: (precio facturado − precio de
  la orden de compra) × cantidad, en la moneda de la línea.
- S3-01 ``sgi.lock.date.log``: bitácora de cada movimiento de una fecha de
  bloqueo contable con el día hábil del mes en que se hizo.
- S3-04 ``sgi.inventory.value``: valor del inventario por mes con el mismo
  cálculo de AL-01 (existencias en ubicaciones internas, valuación), para
  compararlo contra las cuentas 115.
"""
from dateutil.relativedelta import relativedelta

from odoo import api, fields, models

from .sgi_calendar import sgi_nth_business_day, _working_dates

LOCK_FIELDS = {
    'fiscalyear_lock_date': "Cierre fiscal",
    'tax_lock_date': "Impuestos",
    'sale_lock_date': "Ventas",
    'purchase_lock_date': "Compras",
    'hard_lock_date': "Bloqueo duro",
}

# ---- S1-05 ----------------------------------------------------------------
class AccountMoveLinePoDiff(models.Model):
    _inherit = 'account.move.line'

    sgi_po_price_diff = fields.Monetary(
        string="Diferencia vs orden de compra", compute='_compute_sgi_po_price_diff', store=True,
        currency_field='currency_id',
        help="(precio facturado − precio de la orden de compra) × cantidad (S1-05).")

    @api.depends('price_unit', 'quantity', 'purchase_line_id.price_unit', 'move_id.move_type')
    def _compute_sgi_po_price_diff(self):
        for line in self:
            po_line = line.purchase_line_id
            if po_line and line.move_id.move_type in ('in_invoice', 'in_refund'):
                line.sgi_po_price_diff = (line.price_unit - po_line.price_unit) * line.quantity
            else:
                line.sgi_po_price_diff = 0.0


# ---- S1-04 ----------------------------------------------------------------
class AccountMovePaymentDate(models.Model):
    _inherit = 'account.move'

    sgi_payment_date = fields.Date(
        string="Fecha de pago", compute='_compute_sgi_payment_date', store=True,
        help="Fecha del último pago que dejó la factura pagada (S1-04: pagada en su "
             "vencimiento o antes).")

    @api.depends('payment_state', 'line_ids.matched_debit_ids.max_date',
                 'line_ids.matched_credit_ids.max_date')
    def _compute_sgi_payment_date(self):
        for move in self:
            if move.payment_state not in ('paid', 'in_payment'):
                move.sgi_payment_date = False
                continue
            lines = move.line_ids.filtered(lambda l: l.account_id.account_type in (
                'asset_receivable', 'liability_payable'))
            partials = lines.matched_debit_ids | lines.matched_credit_ids
            dates = [d for d in partials.mapped('max_date') if d]
            move.sgi_payment_date = max(dates) if dates else move.invoice_date or False


# ---- S3-01 ----------------------------------------------------------------
class SgiLockDateLog(models.Model):
    _name = 'sgi.lock.date.log'
    _description = "Bitácora de fechas de bloqueo contable"
    _order = 'moved_at desc, id desc'

    company_id = fields.Many2one('res.company', string="Compañía", required=True, index=True)
    lock_field = fields.Selection(list(LOCK_FIELDS.items()), string="Fecha de bloqueo", required=True)
    date_before = fields.Date(string="Antes")
    date_after = fields.Date(string="Después")
    moved_at = fields.Datetime(string="Movida el", required=True, default=fields.Datetime.now)
    user_id = fields.Many2one('res.users', string="Quién", default=lambda self: self.env.user)
    business_day = fields.Integer(
        string="Día hábil del mes", compute='_compute_business_day', store=True,
        help="Número de día hábil del mes (calendario del SGI) en que se movió (S3-01).")

    @api.depends('moved_at', 'company_id')
    def _compute_business_day(self):
        for log in self:
            if not log.moved_at:
                log.business_day = 0
                continue
            day = fields.Datetime.context_timestamp(log, log.moved_at).date()
            working = _working_dates(self.env, day.replace(day=1), day, log.company_id)
            log.business_day = len([d for d in working if d <= day])

    @api.model
    def sgi_business_day_of(self, day, nth, company=None):
        """El día hábil número ``nth`` del mes de ``day`` (para filtros)."""
        return sgi_nth_business_day(self.env, day.year, day.month, nth, company)


class ResCompanyLockLog(models.Model):
    _inherit = 'res.company'

    def write(self, vals):
        tracked = [f for f in LOCK_FIELDS if f in vals and f in self._fields]
        before = {c.id: {f: c[f] for f in tracked} for c in self} if tracked else {}
        res = super().write(vals)
        if tracked:
            Log = self.env['sgi.lock.date.log'].sudo()
            for company in self:
                for field_name in tracked:
                    old, new = before[company.id][field_name], company[field_name]
                    if old != new:
                        Log.create({'company_id': company.id, 'lock_field': field_name,
                                    'date_before': old or False, 'date_after': new or False})
        return res


# ---- S3-04 ----------------------------------------------------------------
class SgiInventoryValue(models.Model):
    _name = 'sgi.inventory.value'
    _description = "Valor del inventario al cierre de mes (cálculo de AL-01)"
    _order = 'date desc, company_id'

    company_id = fields.Many2one('res.company', string="Compañía", required=True, index=True)
    date = fields.Date(string="Cierre", required=True, index=True,
                       help="Último día del mes al que corresponde la foto.")
    value = fields.Monetary(string="Valor del inventario", currency_field='currency_id')
    currency_id = fields.Many2one(related='company_id.currency_id')
    quant_count = fields.Integer(string="# Existencias")
    taken_at = fields.Datetime(string="Tomada el", default=fields.Datetime.now)

    _company_date_uniq = models.Constraint(
        'unique(company_id, date)', "Ya hay una foto del inventario para ese mes.")

    @api.model
    def _sgi_snapshot(self, date=None, company=None):
        """F-015 (auditoría 2026-09): privado; antes cualquiera reescribía por RPC
        la foto de meses pasados con las existencias de hoy.

        Guarda (o actualiza) la foto del valor del inventario: existencias
        en ubicaciones internas con valuación, igual que AL-01. Por omisión el
        último día del mes anterior y la compañía de los KPI."""
        Indicator = self.env['sgi.indicator']
        company = company or Indicator._sgi_kpi_company()
        if not date:
            today = fields.Date.context_today(self)
            date = today.replace(day=1) - relativedelta(days=1)
        if 'value' not in self.env['stock.quant']._fields:
            return self.browse()
        quants = self.env['stock.quant'].sudo().search([
            ('company_id', '=', company.id), ('location_id.usage', '=', 'internal')])
        vals = {'company_id': company.id, 'date': date, 'value': sum(quants.mapped('value')),
                'quant_count': len(quants), 'taken_at': fields.Datetime.now()}
        snapshot = self.sudo().search([('company_id', '=', company.id), ('date', '=', date)], limit=1)
        if snapshot:
            snapshot.write(vals)
            return snapshot
        return self.sudo().create(vals)

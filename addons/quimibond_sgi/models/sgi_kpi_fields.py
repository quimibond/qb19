# -*- coding: utf-8 -*-
"""Campos que faltaban para que 28 indicadores capturados a mano pasen a
fórmula configurable (55.0.0, 2026-09-25). Todos guardados en la base para
poder filtrarlos desde un término (``sgi.indicator.term``):

- C1-04 ``product.template.sgi_first_sale_date``: fecha del primer pedido
  confirmado del producto (se fija al confirmar el pedido).
- C2-05 ``stock.picking.sgi_export_crossing_datetime`` y
  ``sgi_export_file_closed_date``: cruce y cierre de expediente de la entrega
  de exportación (los captura Logística).
- C5-01 ``quality.alert.sgi_claimed_meters``: metros reclamados; el denominador
  (metros embarcados) sale de los movimientos de la entrega.
- C5-03 ``stock.picking.sgi_quality_approved_at`` / ``sgi_release_hours``: hora
  de la última aprobación de Calidad del traslado y horas desde que Inspección
  lo entregó (creación del traslado).
- S1-04 ``account.move.sgi_payment_date``: fecha del último pago que dejó la
  factura pagada.
- E1-02 ``sgi.management.review.agreement.done_date``: cumplimiento del
  acuerdo (la de su acción, o a mano).
- S1-05 ``account.move.line.sgi_po_price_diff``: (precio facturado − precio de
  la orden de compra) × cantidad, en la moneda de la línea.
- S3-01 ``sgi.lock.date.log``: bitácora de cada movimiento de una fecha de
  bloqueo contable con el día hábil del mes en que se hizo.
- S3-04 ``sgi.inventory.value``: valor del inventario por mes con el mismo
  cálculo de AL-01 (existencias en ubicaciones internas, valuación), para
  compararlo contra las cuentas 115.
- S4-01 ``hr.version.sgi_departure_reason_id`` / ``sgi_departure_registered_at``:
  motivo de la baja (el del empleado si la versión no lo trae) y cuándo se
  registró.
- S4-02 ``hr.employee.sgi_trial_date_end``: fin del periodo de prueba del
  contrato vigente, guardado en el empleado para filtrar evaluaciones.
- S4-04 ``sgi.employer.obligation``: obligaciones patronales (IMSS, INFONAVIT,
  ISR, ISN…) con vencimiento y fecha de presentación.
- S6-02 ``res.users.sgi_deactivated_date``: fecha en que se desactivó el
  usuario, para compararla con la baja del empleado.
- S4-03 vive en ``quimibond_nomina`` (``hr.payslip.run``), porque este módulo
  no depende de nómina.
- MT-01 no necesita campo: ``mrp.workcenter.productivity`` con
  ``loss_id.name = 'Mantenimiento'`` y ``duration`` (minutos, factor 1/60).
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


# ---- C1-04 ----------------------------------------------------------------
class ProductTemplateFirstSale(models.Model):
    _inherit = 'product.template'

    sgi_first_sale_date = fields.Date(
        string="Primer pedido confirmado", readonly=True, copy=False, index=True,
        help="Fecha del primer pedido de venta confirmado con este producto (C1-04).")


class SaleOrderFirstSale(models.Model):
    _inherit = 'sale.order'

    def action_confirm(self):
        res = super().action_confirm()
        for order in self:
            day = fields.Date.context_today(order, order.date_order) if order.date_order \
                else fields.Date.context_today(order)
            templates = order.order_line.product_id.product_tmpl_id.sudo().filtered(
                lambda t: not t.sgi_first_sale_date or t.sgi_first_sale_date > day)
            templates.write({'sgi_first_sale_date': day})
        return res


# ---- C2-05 / C5-03 --------------------------------------------------------
class StockPickingKpi(models.Model):
    _inherit = 'stock.picking'

    sgi_export_crossing_datetime = fields.Datetime(
        string="Cruce (exportación)", copy=False,
        help="Fecha y hora en que la mercancía cruzó la frontera (C2-05).")
    sgi_export_file_closed_date = fields.Date(
        string="Expediente cerrado", copy=False,
        help="Fecha en que quedó completo el expediente de exportación (C2-05).")
    sgi_quality_approved_at = fields.Datetime(
        string="Aprobado por Calidad", compute='_compute_sgi_quality_approved', store=True,
        help="Última aprobación de un control de calidad de este traslado (C5-03).")
    sgi_release_hours = fields.Float(
        string="Horas hasta la aprobación", compute='_compute_sgi_quality_approved', store=True,
        digits=(16, 2),
        help="Horas entre la entrega de Inspección (creación del traslado) y la "
             "aprobación de Calidad (C5-03).")

    @api.depends('check_ids.quality_state', 'check_ids.control_date', 'create_date')
    def _compute_sgi_quality_approved(self):
        for picking in self:
            passed = picking.check_ids.filtered(lambda c: c.quality_state == 'pass' and c.control_date)
            approved = max(passed.mapped('control_date')) if passed else False
            picking.sgi_quality_approved_at = approved
            if approved and picking.create_date:
                picking.sgi_release_hours = round(
                    max((approved - picking.create_date).total_seconds(), 0.0) / 3600.0, 2)
            else:
                picking.sgi_release_hours = 0.0


# ---- C5-01 ----------------------------------------------------------------
class QualityAlertClaimedMeters(models.Model):
    _inherit = 'quality.alert'

    sgi_claimed_meters = fields.Float(
        string="Metros reclamados", digits=(16, 2),
        help="Metros que el cliente reclama en esta NC (C5-01).")


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


# ---- E1-02 ----------------------------------------------------------------
class SgiManagementReviewAgreementDone(models.Model):
    _inherit = 'sgi.management.review.agreement'

    done_date = fields.Date(
        string="Cumplido el", compute='_compute_done_date', store=True, readonly=False,
        help="Fecha de cumplimiento del acuerdo: la de su acción al terminarse, o "
             "capturada a mano si el acuerdo no tiene acción (E1-02: cerrado antes de su límite).")

    @api.depends('action_line_id.date_done')
    def _compute_done_date(self):
        for agreement in self:
            if agreement.action_line_id.date_done:
                agreement.done_date = agreement.action_line_id.date_done
            elif not agreement.done_date:
                agreement.done_date = False


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
    def sgi_snapshot(self, date=None, company=None):
        """Guarda (o actualiza) la foto del valor del inventario: existencias
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


# ---- S4-01 / S4-02 ----------------------------------------------------------
class HrVersionDeparture(models.Model):
    _inherit = 'hr.version'

    sgi_departure_reason_id = fields.Many2one(
        'hr.departure.reason', string="Motivo de baja (SGI)", compute='_compute_sgi_departure',
        store=True, help="El motivo de la versión o, si no lo trae, el del empleado (S4-01).")
    sgi_departure_registered_at = fields.Datetime(
        string="Baja registrada el", readonly=True, copy=False,
        help="Cuándo se capturó la fecha de baja (S4-01).")

    @api.depends('departure_reason_id', 'employee_id.departure_reason_id')
    def _compute_sgi_departure(self):
        for version in self:
            version.sgi_departure_reason_id = (version.departure_reason_id
                                               or version.employee_id.sudo().departure_reason_id)

    def write(self, vals):
        if vals.get('departure_date') and 'sgi_departure_registered_at' not in vals:
            vals = dict(vals, sgi_departure_registered_at=fields.Datetime.now())
        return super().write(vals)


class HrEmployeeTrial(models.Model):
    _inherit = 'hr.employee'

    sgi_trial_date_end = fields.Date(
        string="Fin del periodo de prueba", related='version_id.trial_date_end', store=True,
        help="Del contrato vigente; para medir las evaluaciones del periodo de prueba (S4-02).")


# ---- S4-04 ----------------------------------------------------------------
class SgiEmployerObligation(models.Model):
    _name = 'sgi.employer.obligation'
    _description = "Obligación patronal"
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _order = 'due_date desc, id desc'

    name = fields.Char(string="Obligación", required=True, tracking=True)
    kind = fields.Selection([
        ('imss', "IMSS"), ('infonavit', "INFONAVIT"), ('isr', "ISR retenido"),
        ('isn', "Impuesto sobre nómina"), ('sar', "SAR / AFORE"), ('stps', "STPS"), ('otro', "Otra"),
    ], string="Tipo", required=True, default='imss', tracking=True)
    period_date = fields.Date(string="Periodo", required=True,
                              help="Primer día del mes o bimestre al que corresponde.")
    due_date = fields.Date(string="Vence", required=True, tracking=True)
    filed_date = fields.Date(string="Presentada el", tracking=True, copy=False)
    responsible_id = fields.Many2one('res.users', string="Responsable", tracking=True,
                                     default=lambda self: self.env.user)
    company_id = fields.Many2one('res.company', string="Compañía", required=True,
                                 default=lambda self: self.env.company)
    amount = fields.Monetary(string="Importe", currency_field='currency_id')
    currency_id = fields.Many2one(related='company_id.currency_id')
    reference = fields.Char(string="Folio / referencia")
    notes = fields.Text(string="Notas")
    state = fields.Selection([
        ('pendiente', "Pendiente"), ('presentada', "Presentada a tiempo"),
        ('tarde', "Presentada tarde"), ('vencida', "Vencida sin presentar"),
    ], string="Estado", compute='_compute_state', store=True)
    on_time = fields.Boolean(string="A tiempo", compute='_compute_state', store=True)

    @api.depends('due_date', 'filed_date')
    def _compute_state(self):
        today = fields.Date.context_today(self)
        for obligation in self:
            if obligation.filed_date:
                on_time = bool(obligation.due_date) and obligation.filed_date <= obligation.due_date
                obligation.state = 'presentada' if on_time else 'tarde'
                obligation.on_time = on_time
            else:
                obligation.state = 'vencida' if obligation.due_date and obligation.due_date < today else 'pendiente'
                obligation.on_time = False

    def action_mark_filed(self):
        self.filtered(lambda o: not o.filed_date).write({'filed_date': fields.Date.context_today(self)})
        return True


# ---- S6-02 ----------------------------------------------------------------
class ResUsersDeactivated(models.Model):
    _inherit = 'res.users'

    sgi_deactivated_date = fields.Date(
        string="Desactivado el", readonly=True, copy=False,
        help="Fecha en que se desactivó el usuario (S6-02, contra la baja del empleado).")

    def write(self, vals):
        if 'active' in vals and 'sgi_deactivated_date' not in vals:
            vals = dict(vals, sgi_deactivated_date=False if vals['active'] else fields.Date.context_today(self))
        return super().write(vals)

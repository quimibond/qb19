# -*- coding: utf-8 -*-
"""Complementos de pago (CFDI tipo P) como los entrega Syntage: un
``InvoicePayment`` por documento relacionado (factura pagada, parcialidad,
importe pagado, saldo anterior e insoluto). Llegan por webhook
(``invoice_payment.*``), por API (``/invoices/payments``) o por importación
en lote (``action_import_payments``, p.ej. el CSV de Syntage).

Cada pago se liga a la factura del SAT por ``invoiceUuid`` y, a través de
ella, a la factura de Odoo. La comparación vive en ``sat.pago.compare``.

Desde 1.30.0 cada renglón sabe también de qué CFDI de pago (tipo P) viene
(``batchPayment.invoice.id`` de Syntage) y con qué pago de Odoo se cruza:

* emitidos: por folio fiscal (``account.payment.l10n_mx_edi_cfdi_uuid``),
  aunque el pago esté cancelado en Odoo (así sale el REP que Odoo canceló y
  el SAT sigue teniendo vigente: Proyecciones de la Moda, jun-2026);
* sin folio (recibidos): el pago de Odoo conciliado con la factura, del
  importe del lote y a ±15 días.

Y solo **cuenta** (comparación, IVA del mes) si está vigente, su CFDI de pago
no está cancelado y no es copia de otro renglón: el mismo documento
relacionado del mismo pago llegó dos veces de Syntage con distinto id, una
sin su CFDI de pago (Jacobo Mizrahi Penhos, FACTU/2025/12/025).
"""
import logging
from datetime import timedelta

from odoo import _, api, fields, models

_logger = logging.getLogger(__name__)


# Días entre la fecha del complemento y la del pago de Odoo para ligarlos
# sin folio fiscal.
PAGO_DIAS = 15


def _num(value):
    try:
        return float(value) if value not in (None, '') else 0.0
    except (TypeError, ValueError):
        return 0.0


class SatCfdiPago(models.Model):
    _name = 'sat.cfdi.pago'
    _description = 'Complemento de pago (SAT)'
    _order = 'fecha_pago desc, id desc'

    name = fields.Char(compute='_compute_name')
    syntage_id = fields.Char(string='ID en Syntage', required=True, index=True)
    company_id = fields.Many2one('res.company', string='Compañía', required=True, index=True,
                                 default=lambda self: self.env.company)
    invoice_uuid = fields.Char(string='UUID de la factura pagada', index=True)
    invoice_cfdi_id = fields.Many2one('sat.cfdi', string='Factura (CFDI)', compute='_compute_invoice_cfdi',
                                      store=True, index=True)
    move_id = fields.Many2one(related='invoice_cfdi_id.move_id', string='Factura en Odoo', store=True)
    kind = fields.Selection([
        ('cobro', 'Cobro (factura emitida)'),
        ('pago', 'Pago (factura recibida)'),
    ], string='Sentido', compute='_compute_invoice_cfdi', store=True)
    counterparty_name = fields.Char(related='invoice_cfdi_id.counterparty_name', string='Contraparte', store=True)
    fecha_pago = fields.Datetime(string='Fecha de pago', index=True)
    forma_pago = fields.Char(string='Forma de pago')
    moneda = fields.Char(default='MXN')
    tipo_cambio = fields.Float(digits=(16, 6), default=1.0)
    monto = fields.Float(string='Importe pagado', digits=(16, 2))
    parcialidad = fields.Integer(string='Parcialidad')
    saldo_anterior = fields.Float(string='Saldo anterior', digits=(16, 2))
    saldo_insoluto = fields.Float(string='Saldo insoluto', digits=(16, 2))
    num_operacion = fields.Char(string='Núm. de operación')
    estado_sat = fields.Selection([('vigente', 'Vigente'), ('cancelado', 'Cancelado')],
                                  default='vigente', index=True, string='Estado SAT')
    last_event_type = fields.Char(string='Último evento')
    raw_json = fields.Json(string='Objeto Syntage')

    # 1.30.0: CFDI de pago, pago de Odoo y duplicados
    payment_cfdi_syntage_id = fields.Char(string='ID del CFDI de pago en Syntage', index=True,
                                          compute='_compute_payment_cfdi_syntage_id', store=True)
    payment_cfdi_id = fields.Many2one('sat.cfdi', string='CFDI de pago (REP)', index=True,
                                      compute='_compute_payment_cfdi', store=True)
    payment_uuid = fields.Char(related='payment_cfdi_id.uuid', string='Folio fiscal del REP', store=True,
                               index=True)
    payment_cfdi_estado = fields.Selection(related='payment_cfdi_id.estado_sat', string='Estado del REP',
                                           store=True)
    batch_monto = fields.Float(string='Importe del pago (lote)', digits=(16, 2),
                               compute='_compute_payment_cfdi_syntage_id', store=True,
                               help='Importe total del pago del REP, que puede cubrir varias facturas.')
    account_payment_id = fields.Many2one('account.payment', string='Pago en Odoo', index=True, readonly=True,
                                         copy=False)
    account_payment_state = fields.Selection(related='account_payment_id.state', string='Estado del pago en Odoo',
                                             store=True)
    payment_match = fields.Selection([
        ('uuid', 'Folio fiscal'),
        ('factura', 'Pago de la factura'),
    ], string='Ligado al pago por', readonly=True, copy=False)
    duplicado_de_id = fields.Many2one('sat.cfdi.pago', string='Copia de', readonly=True, copy=False, index=True,
                                      help='El mismo documento relacionado del mismo pago ya está en otro renglón: '
                                           'este no cuenta.')
    cuenta = fields.Boolean(string='Cuenta', compute='_compute_cuenta', store=True, index=True,
                            help='Vigente, con su CFDI de pago no cancelado y sin ser copia de otro renglón. '
                                 'Solo estos entran a la comparación de pagos y al IVA del mes.')

    _syntage_id_uniq = models.Constraint('unique(syntage_id)', 'Ya existe ese pago de Syntage.')

    @api.depends('invoice_cfdi_id', 'monto', 'parcialidad')
    def _compute_name(self):
        for rec in self:
            base = rec.invoice_cfdi_id.name or (rec.invoice_uuid or '')[:8].upper()
            rec.name = _('Pago %(n)s de %(inv)s: %(amt).2f') % {
                'n': rec.parcialidad or 1, 'inv': base, 'amt': rec.monto or 0.0}

    @api.depends('invoice_uuid', 'raw_json')
    def _compute_invoice_cfdi(self):
        Cfdi = self.env['sat.cfdi'].sudo()
        for rec in self:
            cfdi = Cfdi.search([('uuid', '=ilike', rec.invoice_uuid)], limit=1) if rec.invoice_uuid else Cfdi
            rec.invoice_cfdi_id = cfdi
            if cfdi:
                rec.kind = 'cobro' if cfdi.direction == 'issued' else 'pago'
            else:
                # Syntage: importe positivo = nos pagaron, negativo = pagamos.
                rec.kind = 'pago' if _num((rec.raw_json or {}).get('amount')) < 0 else 'cobro'

    @api.depends('raw_json')
    def _compute_payment_cfdi_syntage_id(self):
        for rec in self:
            raw = rec.raw_json or {}
            bp = raw.get('batchPayment') if isinstance(raw.get('batchPayment'), dict) else {}
            inv = bp.get('invoice')
            sid = False
            if isinstance(inv, dict):
                sid = inv.get('id') or (inv.get('@id') or '').rsplit('/', 1)[-1] or False
            elif isinstance(inv, str) and inv:
                sid = inv.rsplit('/', 1)[-1]
            rec.payment_cfdi_syntage_id = sid
            rec.batch_monto = abs(_num(bp.get('amount'))) or abs(_num(raw.get('amount')))

    @api.depends('payment_cfdi_syntage_id')
    def _compute_payment_cfdi(self):
        Cfdi = self.env['sat.cfdi'].sudo()
        sids = [s for s in self.mapped('payment_cfdi_syntage_id') if s]
        by_sid = {c.syntage_id: c for c in Cfdi.search([('syntage_id', 'in', sids)])} if sids else {}
        for rec in self:
            rec.payment_cfdi_id = by_sid.get(rec.payment_cfdi_syntage_id, Cfdi)

    @api.depends('estado_sat', 'payment_cfdi_id.estado_sat', 'duplicado_de_id')
    def _compute_cuenta(self):
        for rec in self:
            rec.cuenta = (rec.estado_sat == 'vigente' and rec.payment_cfdi_id.estado_sat != 'cancelado'
                          and not rec.duplicado_de_id)

    # ── ingesta ────────────────────────────────────────────────────────

    @api.model
    def _vals_from_syntage(self, obj, company, deleted=False):
        sid = obj.get('id') or (obj.get('@id') or '').rsplit('/', 1)[-1]
        bp = obj.get('batchPayment') if isinstance(obj.get('batchPayment'), dict) else {}
        amount = _num(obj.get('amount'))
        cancelled = deleted or bool(obj.get('canceledAt'))
        return {
            'syntage_id': sid or False,
            'company_id': company.id,
            'invoice_uuid': (obj.get('invoiceUuid') or '').strip().lower() or False,
            'fecha_pago': obj.get('date') or bp.get('date') or False,
            'forma_pago': obj.get('paymentMethod') or bp.get('paymentMethod') or False,
            'moneda': obj.get('currency') or 'MXN',
            'tipo_cambio': _num(obj.get('exchangeRate')) or 1.0,
            'monto': abs(amount),
            'parcialidad': int(_num(obj.get('installment'))) or 1,
            'saldo_anterior': _num(obj.get('previousBalance')),
            'saldo_insoluto': _num(obj.get('outstandingBalance')),
            'num_operacion': bp.get('operationNumber') or False,
            'estado_sat': 'cancelado' if cancelled else 'vigente',
            'raw_json': obj,
        }

    @api.model
    def _upsert_from_syntage(self, obj, company, event_type=None):
        vals = self._vals_from_syntage(obj, company, deleted=(event_type == 'invoice_payment.deleted'))
        if not vals['syntage_id']:
            raise ValueError('Pago sin id')
        vals['last_event_type'] = event_type or 'pull'
        rec = self.sudo().search([('syntage_id', '=', vals['syntage_id'])], limit=1)
        if rec:
            rec.write(vals)
        else:
            rec = self.sudo().create(vals)
        rec._link_account_payment()
        rec._mark_duplicates()
        return rec

    @api.model
    def action_import_payments(self, rows, company_id=None):
        """Importación en lote de objetos ``InvoicePayment`` (CSV de Syntage o
        un espejo previo). Los números pueden venir como texto. Idempotente."""
        company = self.env['res.company'].browse(company_id) if company_id else self.env.company
        upserted = errored = 0
        errors = []
        for obj in rows or []:
            try:
                with self.env.cr.savepoint():
                    self._upsert_from_syntage(obj, company, event_type='import')
                upserted += 1
            except Exception as exc:
                errored += 1
                errors.append('%s: %s' % (obj.get('id'), str(exc)[:200]))
        return {'upserted': upserted, 'errored': errored, 'errors': errors[:50]}

    def _relink(self):
        """Vuelve a buscar la factura del SAT (p.ej. se cargó después que el pago)."""
        self._compute_invoice_cfdi()

    # ── pago de Odoo y duplicados (1.30.0) ────────────────────────────

    def _recompute_payment_cfdi(self):
        """Vuelve a buscar el CFDI de pago (llegó después que el renglón)."""
        if self:
            self.env.add_to_compute(self._fields['payment_cfdi_id'], self)
            self.flush_recordset(['payment_cfdi_id'])

    def _odoo_payments_by_uuid(self):
        """{folio fiscal en minúsculas: account.payment} de los REP de estos
        renglones. Vacío sin l10n_mx_edi (CI community)."""
        Payment = self.env['account.payment'].sudo()
        uuids = list({u.lower() for u in self.mapped('payment_uuid') if u})
        if not uuids or 'l10n_mx_edi_cfdi_uuid' not in Payment._fields:
            return {}
        found = {}
        variants = uuids + [u.upper() for u in uuids]
        # El activo gana sobre el cancelado si los dos traen el mismo folio.
        for pay in Payment.with_context(active_test=False).search(
                [('l10n_mx_edi_cfdi_uuid', 'in', variants)], order='id'):
            key = (pay.l10n_mx_edi_cfdi_uuid or '').lower()
            if key not in found or found[key].state == 'canceled':
                found[key] = pay
        return found

    def _payment_from_invoice(self):
        """El pago de Odoo conciliado con la factura de este renglón, del
        importe del lote (o del renglón) y el más cercano en fecha a ±PAGO_DIAS."""
        self.ensure_one()
        move = self.move_id
        if not move or not self.fecha_pago or not hasattr(move, '_get_reconciled_payments'):
            return self.env['account.payment']
        fecha = fields.Date.to_date(self.fecha_pago)
        best, best_key = self.env['account.payment'], None
        candidates = move.sudo()._get_reconciled_payments()
        if 'matched_payment_ids' in move._fields:
            # Odoo 18+: pagos ligados a la factura aunque no tengan asiento.
            candidates |= move.sudo().matched_payment_ids.filtered(lambda p: p.state != 'canceled')
        for pay in candidates:
            if not pay.date:
                continue
            dias = abs((pay.date - fecha).days)
            if dias > PAGO_DIAS:
                continue
            montos = [m for m in (self.batch_monto, self.monto) if m]
            importe_ok = any(abs(abs(pay.amount) - m) <= 1.0 for m in montos)
            key = (0 if importe_ok else 1, dias, pay.id)
            if best_key is None or key < best_key:
                best, best_key = pay, key
        return best

    def _link_account_payment(self):
        """Liga cada renglón con su pago de Odoo: por folio fiscal del REP y, si
        no hay, por el pago conciliado con la factura."""
        by_uuid = self._odoo_payments_by_uuid()
        for rec in self:
            pay = by_uuid.get((rec.payment_uuid or '').lower()) if rec.payment_uuid else None
            method = 'uuid' if pay else False
            if not pay:
                pay = rec._payment_from_invoice()
                method = 'factura' if pay else False
            if not pay and rec.account_payment_id.state == 'canceled':
                # Un pago que se canceló en Odoo ya no está conciliado con la
                # factura: la liga se queda para que el REP vigente salga.
                continue
            pay = pay or self.env['account.payment']
            if rec.account_payment_id != pay or rec.payment_match != method:
                rec.write({'account_payment_id': pay.id, 'payment_match': method})

    def _dup_key(self):
        fecha = fields.Date.to_date(self.fecha_pago) if self.fecha_pago else False
        return (self.invoice_cfdi_id.id or self.invoice_uuid, self.parcialidad, round(self.monto or 0.0, 2),
                round(self.saldo_anterior or 0.0, 2), fecha)

    def _mark_duplicates(self):
        """Dentro de cada factura, dos renglones vigentes con la misma
        parcialidad, importe, saldo anterior y fecha son el mismo documento
        relacionado si traen el mismo REP o a uno le falta el suyo: el que
        tiene REP (y si no, el más viejo) se queda y el otro es copia. Dos
        REP distintos y vigentes no son copia: son dos CFDI en el SAT
        (complementos que exceden el total), y se quedan los dos."""
        uuids = {u for u in self.mapped('invoice_uuid') if u}
        if not uuids:
            return
        rows = self.sudo().search([('invoice_uuid', 'in', list(uuids))], order='id')
        groups = {}
        for row in rows:
            if row.estado_sat == 'vigente':
                groups.setdefault(row._dup_key(), []).append(row)
        dup_of = {}
        for members in groups.values():
            members.sort(key=lambda r: (0 if r.payment_cfdi_id else 1, r.id))
            kept = []
            for row in members:
                original = next((k for k in kept if not row.payment_uuid or not k.payment_uuid
                                 or row.payment_uuid == k.payment_uuid), None)
                if original:
                    dup_of[row.id] = original.id
                else:
                    kept.append(row)
        for row in rows:
            target = dup_of.get(row.id, False)
            if row.duplicado_de_id.id != target:
                row.write({'duplicado_de_id': target})

    @api.model
    def _cron_link_payments(self, days=None):
        """Repasa los renglones de los últimos ``days`` días (todos si es
        None): CFDI de pago que llegó después, pago de Odoo y copias."""
        domain = []
        if days:
            domain = [('fecha_pago', '>=', fields.Datetime.now() - timedelta(days=days))]
        recs = self.sudo().search(domain)
        recs.filtered(lambda r: r.payment_cfdi_syntage_id and not r.payment_cfdi_id)._recompute_payment_cfdi()
        recs._link_account_payment()
        recs._mark_duplicates()
        _logger.info('sat.cfdi.pago: %s renglones repasados, %s con pago en Odoo, %s copias',
                     len(recs), len(recs.filtered('account_payment_id')), len(recs.filtered('duplicado_de_id')))
        return len(recs)

    def action_open_account_payment(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'res_model': 'account.payment',
                'res_id': self.account_payment_id.id, 'view_mode': 'form'}

    def action_open_payment_cfdi(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'res_model': 'sat.cfdi',
                'res_id': self.payment_cfdi_id.id, 'view_mode': 'form'}

    def action_open_invoice_cfdi(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'res_model': 'sat.cfdi',
                'res_id': self.invoice_cfdi_id.id, 'view_mode': 'form'}

# -*- coding: utf-8 -*-
"""Complementos de pago (CFDI tipo P) como los entrega Syntage: un
``InvoicePayment`` por documento relacionado (factura pagada, parcialidad,
importe pagado, saldo anterior e insoluto). Llegan por webhook
(``invoice_payment.*``), por API (``/invoices/payments``) o por importación
en lote (``action_import_payments``, p.ej. el CSV de Syntage).

Cada pago se liga a la factura del SAT por ``invoiceUuid`` y, a través de
ella, a la factura de Odoo. La comparación vive en ``sat.pago.compare``.
"""
import logging

from odoo import _, api, fields, models

_logger = logging.getLogger(__name__)


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

    def action_open_invoice_cfdi(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'res_model': 'sat.cfdi',
                'res_id': self.invoice_cfdi_id.id, 'view_mode': 'form'}

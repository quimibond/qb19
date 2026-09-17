# -*- coding: utf-8 -*-
import json
import logging

from odoo import api, fields, models

_logger = logging.getLogger(__name__)


class SatWebhookEvent(models.Model):
    _name = 'sat.webhook.event'
    _description = 'Evento de webhook de Syntage'
    _order = 'id desc'

    event_id = fields.Char(string='ID del evento', required=True, index=True)
    event_type = fields.Char(string='Tipo', index=True)
    taxpayer = fields.Char(string='Contribuyente (RFC)')
    company_id = fields.Many2one('res.company', string='Compañía', index=True)
    received_at = fields.Datetime(string='Recibido', default=fields.Datetime.now)
    state = fields.Selection([
        ('received', 'Recibido'), ('processed', 'Procesado'), ('skipped', 'Omitido'), ('error', 'Error'),
    ], default='received', required=True, index=True)
    error = fields.Text(string='Detalle')
    payload = fields.Json(string='Evento')
    cfdi_id = fields.Many2one('sat.cfdi', string='CFDI')
    payload_text = fields.Text(string='Evento (JSON)', compute='_compute_payload_text')

    _event_uniq = models.Constraint('unique(event_id)', 'Este evento ya se recibió.')

    @api.depends('payload')
    def _compute_payload_text(self):
        for rec in self:
            rec.payload_text = json.dumps(rec.payload or {}, indent=2, ensure_ascii=False)

    @api.model
    def _record(self, event):
        """Guarda el evento si es nuevo. Devuelve (registro, es_nuevo)."""
        event_id = str(event.get('id') or '')
        existing = self.sudo().search([('event_id', '=', event_id)], limit=1)
        if existing:
            return existing, False
        taxpayer = event.get('taxpayer') or {}
        rec = self.sudo().create({
            'event_id': event_id,
            'event_type': event.get('type') or '',
            'taxpayer': (taxpayer.get('rfc') or taxpayer.get('id') or '') if isinstance(taxpayer, dict) else str(taxpayer),
            'payload': event,
        })
        return rec, True

    def process(self):
        for rec in self:
            try:
                with self.env.cr.savepoint():
                    rec._process_one()
            except Exception as exc:
                _logger.exception('Evento Syntage %s falló', rec.event_id)
                rec.write({'state': 'error', 'error': str(exc)[:2000]})

    def _process_one(self):
        self.ensure_one()
        etype = self.event_type or ''
        rfc = (self.taxpayer or '').strip().upper()
        company = self.env['res.company'].sudo().search([('vat', '=ilike', rfc)], limit=1) if rfc else None
        if not company:
            self.write({'state': 'skipped', 'error': 'El contribuyente %s no es una compañía de Odoo' % rfc})
            return
        self.company_id = company
        payload = self.payload or {}
        obj = (payload.get('data') or {}).get('object') or {}
        if etype.startswith('invoice.'):
            cfdi = self.env['sat.cfdi']._upsert_from_syntage(obj, company, event_type=etype)
            self.write({'state': 'processed', 'cfdi_id': cfdi.id, 'error': False})
        elif etype.startswith('invoice_payment.'):
            pago = self.env['sat.cfdi.pago']._upsert_from_syntage(obj, company, event_type=etype)
            self.write({'state': 'processed', 'cfdi_id': pago.invoice_cfdi_id.id or False, 'error': False})
        else:
            self.write({'state': 'skipped', 'error': 'Tipo de evento no manejado: %s' % etype})

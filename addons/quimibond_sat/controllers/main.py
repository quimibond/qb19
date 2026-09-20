# -*- coding: utf-8 -*-
"""Receptor del webhook de Syntage.

    POST /quimibond_sat/webhook
    Header X-Satws-Signature: t=<ts>,s=<hmac>  (secreto: quimibond_sat.webhook_secret)

Guarda el evento (idempotente por id), lo procesa y responde JSON. Un
contribuyente que no sea compañía de Odoo se omite con 200 para que Syntage
no reintente.
"""
import json
import logging

from odoo import http
from odoo.http import request

from ..syntage_signature import verify_signature

_logger = logging.getLogger(__name__)


class SatWebhookController(http.Controller):

    @http.route('/quimibond_sat/webhook', type='http', auth='public', methods=['GET'], csrf=False,
                save_session=False)
    def webhook_info(self, **kw):
        return self._json({'ok': True, 'service': 'quimibond_sat', 'accepts': 'POST con X-Satws-Signature'})

    @http.route('/quimibond_sat/webhook', type='http', auth='public', methods=['POST'], csrf=False,
                save_session=False)
    def webhook(self, **kw):
        raw = request.httprequest.get_data()
        secret = (request.env['ir.config_parameter'].sudo().get_param('quimibond_sat.webhook_secret') or '').strip()
        if not secret:
            return self._json({'error': 'quimibond_sat.webhook_secret no configurado'}, 503)
        headers = request.httprequest.headers
        signature = headers.get('X-Satws-Signature') or headers.get('X-Syntage-Signature') or ''
        if not verify_signature(raw, signature, secret):
            _logger.warning('Webhook Syntage rechazado: firma inválida')
            return self._json({'error': 'firma inválida'}, 401)
        try:
            event = json.loads(raw.decode('utf-8'))
        except (ValueError, UnicodeDecodeError):
            return self._json({'error': 'JSON inválido'}, 400)
        if not isinstance(event, dict) or not event.get('id'):
            return self._json({'error': 'evento sin id'}, 400)

        Event = request.env['sat.webhook.event'].sudo()
        rec, created = Event._record(event)
        if not created:
            return self._json({'ok': True, 'duplicate': True, 'event_id': rec.event_id})
        rec.process()
        return self._json({'ok': True, 'event_id': rec.event_id, 'state': rec.state,
                           'cfdi_id': rec.cfdi_id.id or None})

    @staticmethod
    def _json(data, status=200):
        return request.make_json_response(data, status=status)

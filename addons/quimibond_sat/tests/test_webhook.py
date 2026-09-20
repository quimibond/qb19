# -*- coding: utf-8 -*-
import json
import time

from odoo.tests import HttpCase, tagged

from ..syntage_signature import compute_signature
from .common import RFC_QUIMIBOND, syntage_invoice

UUID_W = '06f6098a-fb7b-49aa-b6a0-bec894b754eb'


@tagged('post_install', '-at_install', 'quimibond_sat')
class TestSatWebhook(HttpCase):

    def setUp(self):
        super().setUp()
        self.env['ir.config_parameter'].sudo().set_param('quimibond_sat.webhook_secret', 'secreto-test')
        self.env.company.vat = RFC_QUIMIBOND

    def _post(self, event, secret='secreto-test', t=None):
        body = json.dumps(event).encode('utf-8')
        t = t or int(time.time())
        headers = {
            'Content-Type': 'application/json',
            'X-Satws-Signature': 't=%d,s=%s' % (t, compute_signature(body, secret, t)),
        }
        return self.url_open('/quimibond_sat/webhook', data=body, headers=headers)

    def _event(self, event_id='evt_1', etype='invoice.created', obj=None):
        return {
            'id': event_id, 'type': etype,
            'taxpayer': {'id': RFC_QUIMIBOND, 'name': 'QUIMIBOND'},
            'data': {'object': obj or syntage_invoice(UUID_W)},
            'createdAt': '2026-09-17 00:00:00',
        }

    def test_get_info(self):
        resp = self.url_open('/quimibond_sat/webhook')
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.json().get('ok'))

    def test_invalid_signature_is_rejected(self):
        resp = self._post(self._event(), secret='otro')
        self.assertEqual(resp.status_code, 401)
        self.assertFalse(self.env['sat.webhook.event'].search([('event_id', '=', 'evt_1')]))

    def test_event_creates_cfdi_and_is_idempotent(self):
        resp = self._post(self._event())
        self.assertEqual(resp.status_code, 200, resp.text)
        data = resp.json()
        self.assertEqual(data['state'], 'processed')
        cfdi = self.env['sat.cfdi'].search([('uuid', '=', UUID_W)])
        self.assertEqual(len(cfdi), 1)
        self.assertEqual(cfdi.last_event_type, 'invoice.created')
        event = self.env['sat.webhook.event'].search([('event_id', '=', 'evt_1')])
        self.assertEqual(event.state, 'processed')
        self.assertEqual(event.cfdi_id, cfdi)
        # Mismo evento otra vez: no se reprocesa
        resp = self._post(self._event())
        self.assertTrue(resp.json().get('duplicate'))
        self.assertEqual(self.env['sat.webhook.event'].search_count([('event_id', '=', 'evt_1')]), 1)

    def test_unknown_taxpayer_and_unhandled_type_are_skipped(self):
        ev = self._event(event_id='evt_2')
        ev['taxpayer'] = {'id': 'XAXX010101000'}
        self.assertEqual(self._post(ev).json()['state'], 'skipped')
        ev = self._event(event_id='evt_3', etype='tax_status.updated', obj={'foo': 'bar'})
        self.assertEqual(self._post(ev).json()['state'], 'skipped')
        self.assertFalse(self.env['sat.cfdi'].search([('uuid', '=', UUID_W)]))

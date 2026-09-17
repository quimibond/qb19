# -*- coding: utf-8 -*-
"""Descarga por API con la red simulada: una página con dos CFDI y sin
siguiente, y la entrada pública action_pull_period (la que usa MCP)."""
from unittest.mock import patch

from odoo.tests import tagged

from .common import RFC_CLIENTE, RFC_QUIMIBOND, SatCommon, syntage_invoice

UUID_1 = 'aaaaaaaa-1111-4222-8333-444444444444'
UUID_2 = 'bbbbbbbb-1111-4222-8333-444444444444'
ENTITY = 'a13aaec4-e56d-48c6-8f74-038d8ff6c1e5'


@tagged('post_install', '-at_install', 'quimibond_sat')
class TestSatPull(SatCommon):

    def _fake_request(self, calls):
        def _request(client, method, path, params=None, json=None, headers=None, timeout=60):
            calls.append((method, path, params, json))
            if path == '/entities':
                # Syntage devuelve TODAS las entidades de la organización: la
                # primera es de otro contribuyente y no debe elegirse.
                return {'hydra:member': [
                    {'id': 'otra-entidad', 'taxpayer': {'id': 'MIPJ691003QJ1'},
                     'credential': {'rfc': 'MIPJ691003QJ1', 'status': 'valid'}},
                    {'id': ENTITY, 'taxpayer': {'id': RFC_QUIMIBOND, '@id': '/taxpayers/%s' % RFC_QUIMIBOND},
                     'credential': {'rfc': RFC_QUIMIBOND, 'status': 'valid'}},
                ]}
            if '/invoices' in path:
                return {'hydra:member': [
                    syntage_invoice(UUID_1),
                    syntage_invoice(UUID_2, id='b', isIssuer=True, isReceiver=False, total=5000.0,
                                    issuer={'rfc': RFC_QUIMIBOND, 'name': 'QUIMIBOND'},
                                    receiver={'rfc': RFC_CLIENTE, 'name': 'EUROTECNICA TEXTIL'}),
                    {'id': 'sin-uuid'},   # fila mala: se reporta, no tira la página
                ], 'hydra:view': {}}
            if path == '/extractions':
                return {'id': 'extr-1'}
            raise AssertionError('ruta inesperada %s' % path)
        return _request

    def test_pull_invoices_upserts_and_logs(self):
        self.env['ir.config_parameter'].sudo().set_param('quimibond_sat.api_key', 'k')
        calls = []
        Client = type(self.env['sat.syntage.client'])
        with patch.object(Client, '_request', self._fake_request(calls)):
            result = self.env['sat.cfdi'].action_pull_period('2026-09-01', '2026-09-17')
        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['items_fetched'], 3)
        self.assertEqual(result['items_upserted'], 2)
        self.assertEqual(result['items_errored'], 1)
        # La entidad se resolvió por RFC y quedó cacheada en la compañía
        self.assertEqual(self.company.sat_syntage_entity_id, ENTITY)
        self.assertEqual(calls[0][1], '/entities')
        self.assertIn(ENTITY, calls[1][1])
        # Un día de holgura por lado (Syntage filtra en UTC con límite a las 00:00)
        self.assertIn('issuedAt%5Bafter%5D=2026-08-31', calls[1][1])
        self.assertIn('issuedAt%5Bbefore%5D=2026-09-19', calls[1][1])
        cfdis = self.env['sat.cfdi'].search([('uuid', 'in', [UUID_1, UUID_2])])
        self.assertEqual(len(cfdis), 2)
        self.assertEqual(set(cfdis.mapped('last_event_type')), {'pull'})
        self.assertEqual(cfdis.filtered(lambda c: c.uuid == UUID_2).direction, 'issued')
        log = self.env['sat.sync.log'].browse(result['log_id'])
        self.assertEqual(log.kind, 'pull')
        self.assertIn('sin-uuid', log.summary)
        # Segunda corrida: idempotente y ya no consulta /entities
        calls.clear()
        with patch.object(Client, '_request', self._fake_request(calls)):
            self.env['sat.cfdi'].action_pull_period('2026-09-01', '2026-09-17')
        self.assertEqual(self.env['sat.cfdi'].search_count([('uuid', 'in', [UUID_1, UUID_2])]), 2)
        self.assertNotIn('/entities', [c[1] for c in calls])

    def test_extraction_mode(self):
        self.env['ir.config_parameter'].sudo().set_param('quimibond_sat.api_key', 'k')
        calls = []
        Client = type(self.env['sat.syntage.client'])
        with patch.object(Client, '_request', self._fake_request(calls)):
            result = self.env['sat.cfdi'].action_pull_period('2026-09-01', '2026-09-17', mode='extraction')
        self.assertEqual(result['status'], 'success')
        self.assertEqual(calls[0][1], '/extractions')
        body = calls[0][3]
        self.assertEqual(body['taxpayer'], '/taxpayers/%s' % RFC_QUIMIBOND)
        self.assertEqual(body['options']['period'], {'from': '2026-09-01', 'to': '2026-09-17'})
        self.assertIn('extr-1', self.env['sat.sync.log'].browse(result['log_id']).summary)

    def test_entity_must_match_rfc(self):
        Client = self.env['sat.syntage.client']
        members = [{'id': 'x', 'taxpayer': {'id': 'MIPJ691003QJ1'}},
                   {'id': 'y', '@id': '/entities/y', 'taxpayer': {'@id': '/taxpayers/%s' % RFC_QUIMIBOND},
                    'credential': {'status': 'invalid'}},
                   {'id': 'z', 'credential': {'rfc': RFC_QUIMIBOND.lower(), 'status': 'valid'}}]
        self.assertEqual(Client._pick_entity(members, RFC_QUIMIBOND), 'z')   # con credencial válida gana
        self.assertEqual(Client._pick_entity(members[:2], RFC_QUIMIBOND), 'y')
        self.assertIsNone(Client._pick_entity(members[:1], RFC_QUIMIBOND))

    def test_background_pull_is_queued_and_run_by_cron(self):
        self.env['ir.config_parameter'].sudo().set_param('quimibond_sat.api_key', 'k')
        result = self.env['sat.cfdi'].action_pull_period('2026-09-01', '2026-09-17', background=True)
        log = self.env['sat.sync.log'].browse(result['log_id'])
        self.assertEqual(log.status, 'queued')
        self.assertEqual(log.mode, 'pull')
        self.assertFalse(self.env['sat.cfdi'].search([('uuid', '=', UUID_1)]))
        calls = []
        Client = type(self.env['sat.syntage.client'])
        with patch.object(Client, '_request', self._fake_request(calls)):
            self.env['sat.cfdi']._cron_run_queued()
        self.assertEqual(log.status, 'partial')
        self.assertEqual(log.items_upserted, 2)
        self.assertTrue(self.env['sat.cfdi'].search([('uuid', '=', UUID_1)]))
        # Ya no está en cola: una segunda corrida del cron no hace nada
        calls.clear()
        with patch.object(Client, '_request', self._fake_request(calls)):
            self.env['sat.cfdi']._cron_run_queued()
        self.assertFalse(calls)

    def test_background_extraction(self):
        self.env['ir.config_parameter'].sudo().set_param('quimibond_sat.api_key', 'k')
        result = self.env['sat.cfdi'].action_pull_period('2026-09-01', '2026-09-17', mode='extraction',
                                                        background=True)
        log = self.env['sat.sync.log'].browse(result['log_id'])
        self.assertEqual(log.mode, 'extraction')
        calls = []
        Client = type(self.env['sat.syntage.client'])
        with patch.object(Client, '_request', self._fake_request(calls)):
            self.env['sat.cfdi']._cron_run_queued()
        self.assertEqual(log.status, 'success')
        self.assertIn('extr-1', log.summary)
        self.assertEqual(calls[0][1], '/extractions')

    def test_pull_error_is_recorded_in_log(self):
        self.env['ir.config_parameter'].sudo().set_param('quimibond_sat.api_key', 'k')
        Client = type(self.env['sat.syntage.client'])

        def _boom(client, method, path, **kw):
            from odoo.exceptions import UserError
            raise UserError('Syntage respondió 403')
        # Sin assertRaises: Odoo lo envuelve en un savepoint y desharía la bitácora.
        raised = False
        with patch.object(Client, '_request', _boom):
            try:
                self.env['sat.cfdi'].action_pull_period('2026-09-01', '2026-09-17')
            except Exception:
                raised = True
        self.assertTrue(raised)
        log = self.env['sat.sync.log'].search([('kind', '=', 'pull')], order='id desc', limit=1)
        self.assertEqual(log.status, 'error')
        self.assertIn('403', log.summary)

    def test_missing_api_key_is_a_user_error(self):
        from odoo.exceptions import UserError
        self.env['ir.config_parameter'].sudo().set_param('quimibond_sat.api_key', '')
        with self.assertRaises(UserError):
            self.env['sat.cfdi'].action_pull_period('2026-09-01', '2026-09-17')

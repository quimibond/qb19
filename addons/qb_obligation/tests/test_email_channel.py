# -*- coding: utf-8 -*-
"""Canal del correo: los pendientes detectados en la memoria (Supabase) entran
como candidatas por área con el buzón como dueño, y se cierran o cancelan
cuando la memoria los da por resueltos o expirados. Áreas por tipo."""
from unittest.mock import patch

from odoo.exceptions import UserError
from odoo.tests import tagged

from .common import ObligationCommon

PENDING = [
    {'id': 101, 'thread_id': 5001, 'tipo': 'compromiso_entrega', 'descripcion': 'Confirmar fecha de entrega del M208908',
     'deadline': '2026-10-01', 'company_id': 6031, 'company_name': 'BELSUEÑO', 'account': 'ventas@test.local',
     'detected_at': '2026-09-03T22:40:31+00:00', 'status': 'open'},
    {'id': 102, 'thread_id': 5002, 'tipo': 'promesa_pago', 'descripcion': 'BELSUEÑO debe pagar $15,022.00',
     'deadline': None, 'company_id': 6031, 'company_name': 'BELSUEÑO', 'account': 'desconocido@test.local',
     'detected_at': '2026-09-04T10:00:00+00:00', 'status': 'open'},
    {'id': 103, 'thread_id': 5003, 'tipo': 'cotizacion', 'descripcion': 'Cotizar 500 m de entretela',
     'deadline': '2026-09-20', 'company_id': None, 'company_name': None, 'account': 'nadie@test.local',
     'detected_at': '2026-09-05T10:00:00+00:00', 'status': 'open'},
]


def fake_client(pending, companies, statuses=None):
    def get(_self, table, params):
        if table == 'email_pending_actions':
            if params.get('status') == 'eq.open':
                return [r for r in pending if r['status'] == 'open']
            ids = [int(x) for x in params['id'][4:-1].split(',')]
            return [{'id': i, 'status': s, 'resolved_at': '2026-09-10T12:00:00+00:00'}
                    for i, s in (statuses or {}).items() if i in ids]
        if table == 'companies':
            return companies
        return []
    return get


@tagged('post_install', '-at_install', 'qb_obligation')
class TestEmailChannel(ObligationCommon):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        Users = cls.env['res.users'].with_context(no_reset_password=True)
        cls.ventas = Users.create({'name': 'Ventas', 'login': 'ventas@test.local', 'email': 'ventas@test.local'})
        cls.comercial = Users.create({'name': 'Comercial default', 'login': 'com@test.local', 'email': 'com@test.local'})
        cls.Client = type(cls.env['qb.memoria.client'])
        cls.companies = [{'id': 6031, 'odoo_partner_id': cls.cliente.id, 'rfc': None, 'name': 'BELSUEÑO'}]

    def test_area_from_type(self):
        self._configure()
        a = self.Obligation.create_candidate({'obligation_type': 'collection.payment_promise', 'description': 'x',
                                              'partner_id': self.cliente.id, 'source_ref': 'r1'})
        self.assertEqual(a.area, 'finanzas')
        self.company.obligation_owner_comercial_id = self.comercial
        b = self.Obligation.create_candidate({'obligation_type': 'comercial.quote', 'description': 'y',
                                              'source_ref': 'r2'})
        self.assertEqual((b.area, b.user_id), ('comercial', self.comercial))
        self.assertFalse(self.Obligation.create_candidate({'obligation_type': 'sgi.record', 'description': 'z',
                                                           'source_ref': 'r3'}), 'sin dueño de área no se crea')

    def test_pending_become_candidates_and_close(self):
        self._configure()                                   # cobranza → cxc
        self.company.obligation_owner_comercial_id = self.comercial
        with patch.object(self.Client, 'get', fake_client(PENDING, self.companies)):
            created, closed, cancelled = self.Obligation._sync_email_pending()
        self.assertEqual(len(created), 3)
        by_ref = {r.source_ref: r for r in created}
        entrega = by_ref['supabase:email_pending_actions:101']
        self.assertEqual((entrega.obligation_type, entrega.area), ('comercial.delivery_commitment', 'comercial'))
        self.assertEqual(entrega.user_id, self.ventas, 'el buzón que recibió el correo es el dueño')
        self.assertEqual(entrega.partner_id, self.cliente)
        self.assertEqual((entrega.state, entrega.source, entrega.evidence_rule_key), ('candidate', 'email', 'email_resolved'))
        self.assertEqual(str(entrega.date_deadline), '2026-10-01')
        self.assertEqual(entrega.source_thread_key, '5001')
        promesa = by_ref['supabase:email_pending_actions:102']
        self.assertEqual(promesa.user_id, self.cxc, 'buzón desconocido: dueño de cobranza')
        self.assertTrue(promesa.weak_key)
        cot = by_ref['supabase:email_pending_actions:103']
        self.assertEqual((cot.user_id, cot.partner_id.id), (self.comercial, False))
        # Idempotente
        with patch.object(self.Client, 'get', fake_client(PENDING, self.companies)):
            created2, _c, _x = self.Obligation._sync_email_pending()
        self.assertFalse(created2)
        self.assertEqual(self.Obligation.search_count([('source', '=', 'email')]), 3)
        # La memoria resuelve uno y expira otro
        entrega.action_confirm()
        with patch.object(self.Client, 'get', fake_client(PENDING, self.companies, {101: 'resolved', 102: 'expired'})):
            created3, closed, cancelled = self.Obligation._sync_email_pending()
        self.assertEqual((closed, cancelled), (entrega, promesa))
        self.assertEqual((entrega.state, entrega.close_method), ('done', 'evidence'))
        self.assertIn('Resuelto en el correo', entrega.evidence_summary)
        self.assertEqual(promesa.state, 'cancelled')
        self.assertEqual(cot.state, 'candidate')

    def test_cron_survives_memory_outage(self):
        self._configure()
        with patch.object(self.Client, 'get', side_effect=UserError('Supabase 500')):
            self.Obligation._cron_collection()
        self.assertFalse(self.Obligation.search([('source', '=', 'email')]))

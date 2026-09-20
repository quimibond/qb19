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

    DUPS = [
        {'id': 201, 'thread_id': 7001, 'tipo': 'compromiso_entrega', 'descripcion': 'Quimibond debe entregar el pedido PO39264 en la fecha confirmada',
         'deadline': '2026-10-01', 'company_id': 6031, 'company_name': 'BELSUEÑO', 'account': 'otro@test.local',
         'detected_at': '2026-09-03T22:40:31+00:00', 'status': 'open'},
        {'id': 202, 'thread_id': 7002, 'tipo': 'compromiso_entrega', 'descripcion': 'Entregar PO39264 a BELSUEÑO en la fecha acordada',
         'deadline': '2026-10-01', 'company_id': 6031, 'company_name': 'BELSUEÑO', 'account': 'ventas@test.local',
         'detected_at': '2026-09-03T22:41:31+00:00', 'status': 'open'},
        {'id': 203, 'thread_id': 7003, 'tipo': 'compromiso_entrega', 'descripcion': 'Quimibond debe entregar el pedido PO39264 (fecha confirmada)',
         'deadline': '2026-10-01', 'company_id': 6031, 'company_name': 'BELSUEÑO', 'account': 'com@test.local',
         'detected_at': '2026-09-03T22:42:31+00:00', 'status': 'open'},
    ]

    def test_same_pending_in_several_mailboxes_is_one_obligation(self):
        """El mismo hilo llega a tres buzones: una sola obligación, del primer
        buzón que es usuario; el pendiente de cualquier buzón la cierra."""
        self._configure()
        with patch.object(self.Client, 'get', fake_client(self.DUPS, self.companies)):
            created, _c, _x = self.Obligation._sync_email_pending()
        self.assertEqual(len(created), 1)
        ob = created
        self.assertEqual(ob.user_id, self.ventas, 'otro@ no es usuario; ventas@ sí')
        self.assertEqual(ob.source_ref, 'supabase:email_pending_actions:201')
        self.assertEqual(ob.detection_payload['duplicates'], [202, 203])
        self.assertTrue(ob.dedupe_key)
        self.assertEqual(self.Obligation.search_count([('source', '=', 'email')]), 1)
        # Idempotente y cierre por el duplicado
        with patch.object(self.Client, 'get', fake_client(self.DUPS, self.companies, {203: 'resolved'})):
            created2, closed, _x = self.Obligation._sync_email_pending()
        self.assertFalse(created2)
        self.assertEqual(closed, ob)
        self.assertEqual(ob.state, 'done')

    def test_duplicates_prefer_learned_owner(self):
        self._configure()
        self.cliente.write({'memoria_owner_areas': {'comercial': {'user_id': self.comercial.id, 'mailbox': 'x'}}})
        with patch.object(self.Client, 'get', fake_client(self.DUPS, self.companies)):
            created, _c, _x = self.Obligation._sync_email_pending()
        self.assertEqual(created.user_id, self.comercial, 'con varios buzones manda el encargado aprendido')

    def test_dedupe_existing_candidates(self):
        """Candidatas creadas antes de 3.1.0 (sin clave) con el mismo pendiente:
        queda una y las demás se descartan."""
        self._configure()
        recs = self.Obligation
        for row in self.DUPS:
            recs |= self.Obligation.create_candidate({
                'obligation_type': 'comercial.delivery_commitment', 'description': row['descripcion'],
                'partner_id': self.cliente.id, 'user_id': self.ventas.id, 'source': 'email',
                'source_ref': 'supabase:email_pending_actions:%s' % row['id'], 'detection_payload': row,
                'date_deadline': row['deadline']})
        self.assertEqual(len(recs), 3)
        discarded = self.Obligation._email_dedupe_existing()
        self.assertEqual(len(discarded), 2)
        kept = recs - discarded
        self.assertEqual(kept.source_ref, 'supabase:email_pending_actions:201', 'queda la más antigua')
        self.assertTrue(all(r.state == 'discarded' and 'Duplicado' in r.discard_reason for r in discarded))
        self.assertEqual(len({r.dedupe_key for r in recs}), 1)

    def test_cron_survives_memory_outage(self):
        self._configure()
        with patch.object(self.Client, 'get', side_effect=UserError('Supabase 500')):
            self.Obligation._cron_collection()
        self.assertFalse(self.Obligation.search([('source', '=', 'email')]))

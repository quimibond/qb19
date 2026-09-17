# -*- coding: utf-8 -*-
"""Pestaña Memoria con la API simulada: resolución por partner y por RFC,
render de las secciones, caché y errores sin reventar la ficha."""
from unittest.mock import patch

from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged

COMPANY = {'id': 6031, 'name': 'SHAWMUT LLC', 'odoo_partner_id': 0, 'rfc': 'SHA010101AAA',
           'relationship_summary': 'Cliente clave de entretela.', 'strategic_notes': None,
           'risk_signals': ['paga tarde'], 'opportunity_signals': [], 'monthly_avg': 1000, 'trend_pct': 5,
           'payment_term': '30 días', 'total_receivable': 0, 'total_overdue_odoo': 0}
THREAD = {'id': 1, 'gmail_thread_id': 'abc123', 'subject': 'Pedido de agosto <urgente>', 'account': 'ventas@quimibond.com',
          'status': 'waiting_us', 'message_count': 4, 'last_sender_type': 'external',
          'hours_without_response': 30, 'last_activity': '2026-08-05T22:22:57+00:00'}
PENDING = {'id': 9, 'thread_id': 1, 'tipo': 'compromiso_entrega', 'descripcion': 'Confirmar fecha de entrega',
           'deadline': '2026-08-10', 'account': 'ventas@quimibond.com', 'status': 'open',
           'detected_at': '2026-08-06T10:00:00+00:00', 'resolved_at': None}


def fake_get(rows_by_table):
    calls = []

    def get(_self, table, params):
        calls.append((table, dict(params)))
        rows = rows_by_table.get(table, [])
        if callable(rows):
            return rows(params)
        return rows
    get.calls = calls
    return get


@tagged('post_install', '-at_install', 'qb_memoria')
class TestMemoria(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        icp = cls.env['ir.config_parameter'].sudo()
        icp.set_param('quimibond_intelligence.supabase_url', 'https://x.supabase.co')
        icp.set_param('quimibond_intelligence.supabase_service_key', 'k')
        cls.Client = type(cls.env['qb.memoria.client'])
        cls.partner = cls.env['res.partner'].create({'name': 'SHAWMUT LLC', 'is_company': True, 'vat': 'SHA010101AAA'})
        cls.person = cls.env['res.partner'].create({'name': 'Ana', 'parent_id': cls.partner.id})

    def test_render_by_partner_and_cache(self):
        company = dict(COMPANY, odoo_partner_id=self.partner.id)
        get = fake_get({
            'companies': lambda params: [company] if 'odoo_partner_id' in params else [],
            'threads': lambda params: [THREAD] if 'order' in params else [THREAD, dict(THREAD, id=2, status='ok')],
            'email_pending_actions': [PENDING],
            'customer_demand_signals': [{'product_ref': 'WM4032OW152', 'product_desc': 'Entretela', 'qty': 1200,
                                         'uom': 'm', 'period_label': 'sep', 'demand_date': None,
                                         'detected_at': '2026-08-06T10:00:00+00:00'}],
            'contacts': [{'name': 'Ana', 'email': 'ana@shawmut.com', 'role': 'Compras', 'department': None,
                          'last_activity': '2026-08-05T22:22:57+00:00', 'interaction_count': 12,
                          'avg_response_time_hours': 5.5, 'relationship_score': 80}],
        })
        with patch.object(self.Client, 'get', get):
            html = self.person.memoria_html   # el contacto persona resuelve por su empresa
        self.assertTrue(self.person.memoria_cached_at)
        self.assertIn('Pedido de agosto &lt;urgente&gt;', html)      # escapado, no inyectado
        self.assertIn('mail.google.com/mail/u/0/#all/abc123', html)
        self.assertIn('Confirmar fecha de entrega', html)
        self.assertIn('WM4032OW152', html)
        self.assertIn('ana@shawmut.com', html)
        self.assertIn('Cliente clave de entretela.', html)
        self.assertIn('paga tarde', html)
        # Contadores: 2 hilos en 90 días, 1 esperando respuesta nuestra
        self.assertIn('>2<', html)
        self.assertIn('>1<', html)
        # Con caché fresca no vuelve a llamar
        n = len(get.calls)
        with patch.object(self.Client, 'get', get):
            self.person.invalidate_recordset(['memoria_html'])
            self.person.memoria_html
        self.assertEqual(len(get.calls), n)
        # Actualizar fuerza la lectura
        with patch.object(self.Client, 'get', get):
            self.person.action_memoria_refresh()
        self.assertGreater(len(get.calls), n)

    def test_fallback_by_rfc_and_unknown(self):
        get = fake_get({'companies': lambda params: [COMPANY] if 'rfc' in params else []})
        with patch.object(self.Client, 'get', get):
            self.partner.action_memoria_refresh()
        self.assertEqual(self.partner.memoria_cache['company']['id'], 6031)
        self.assertEqual([t for t, _p in get.calls if t == 'companies'], ['companies', 'companies'])
        other = self.env['res.partner'].create({'name': 'Nadie', 'is_company': True})
        with patch.object(self.Client, 'get', fake_get({})):
            other.action_memoria_refresh()
        self.assertIn('no conoce', other.memoria_html)

    def test_errors_do_not_break_the_form(self):
        with patch.object(self.Client, 'get', side_effect=UserError('La memoria respondió 500')):
            self.partner.action_memoria_refresh()
        self.assertIn('No se pudo leer la memoria', self.partner.memoria_html)
        self.env['ir.config_parameter'].sudo().set_param('quimibond_intelligence.supabase_service_key', '')
        self.partner.action_memoria_refresh()
        self.assertIn('no está configurada', self.partner.memoria_html)

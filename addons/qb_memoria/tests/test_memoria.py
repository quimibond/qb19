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


BRIEF = {
    'empresa': {'id': 6031, 'name': 'SHAWMUT LLC'},
    'encargados': [{'buzon': 'innovacion@quimibond.com', 'area': None, 'n': 42, 'share': 71},
                   {'buzon': 'cxcobrar@quimibond.com', 'area': 'finanzas', 'n': 5, 'share': 100}],
    'contactos': [{'email': 'ana@shawmut.com', 'nombre': 'Ana', 'rol': 'Compras', 'interacciones': 12}],
    'hechos': [{'categoria': 'condiciones_pago', 'hecho': 'Paga a 60 días contra factura.', 'vigente_desde': '2026-08-01',
                'veces': 2, 'confianza': 0.9, 'sobre': 'empresa'},
               {'categoria': 'contacto_clave', 'hecho': 'Ana decide las compras de entretela.', 'vigente_desde': None,
                'veces': 1, 'confianza': 0.8, 'sobre': 'ana@shawmut.com'}],
    'hilos': [{'thread_id': 1, 'gmail_thread_id': 'abc123', 'asunto': 'Pedido de agosto', 'buzon': 'ventas@quimibond.com',
               'tema': 'Pedido de agosto de entretela WM4032', 'resumen': 'Pidieron 1,200 m; falta confirmar fecha.',
               'estado': 'abierto', 'esperando_a': 'nosotros', 'tono': 'neutral', 'acuerdos': [],
               'pendientes': [{'que': 'Confirmar fecha de entrega', 'quien': 'nosotros', 'vence': '2026-08-10'}],
               'ultimo': '2026-08-05T22:22:57+00:00', 'mensajes': 4}],
    'stats': {'hilos_90d': 2, 'esperan_respuesta_nuestra': 1, 'hilos_resumidos': 1, 'hilos_abiertos': 1},
}


def fake_rpc(result):
    calls = []

    def rpc(_self, name, params):
        calls.append((name, dict(params or {})))
        if isinstance(result, Exception):
            raise result
        return result
    rpc.calls = calls
    return rpc


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

    def setUp(self):
        super().setUp()
        # Las pruebas que no miran la ficha consolidada no deben salir a la red.
        patcher = patch.object(self.Client, 'rpc', fake_rpc({}))
        patcher.start()
        self.addCleanup(patcher.stop)

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

    def test_brief_sections(self):
        company = dict(COMPANY, odoo_partner_id=self.partner.id)
        get = fake_get({'companies': lambda params: [company] if 'odoo_partner_id' in params else []})
        rpc = fake_rpc(BRIEF)
        with patch.object(self.Client, 'get', get), patch.object(self.Client, 'rpc', rpc):
            self.partner.action_memoria_refresh()
        html = self.partner.memoria_html
        self.assertEqual(rpc.calls, [('memoria_brief', {'p_company_id': 6031})])
        self.assertIn('Quién la atiende', html)
        self.assertIn('innovacion@quimibond.com', html)
        self.assertIn('Lo que sabemos', html)
        self.assertIn('Condiciones de pago', html)
        self.assertIn('Paga a 60 días contra factura.', html)
        self.assertIn('Contactos clave', html)
        self.assertIn('(ana@shawmut.com)', html)
        self.assertIn('Conversaciones (resumen de la memoria)', html)
        self.assertIn('Pedido de agosto de entretela WM4032', html)
        self.assertIn('esperan respuesta nuestra', html)
        self.assertIn('Confirmar fecha de entrega', html)
        self.assertIn('1 resumidas, 1 abiertas', html)

    def test_brief_failure_is_soft(self):
        company = dict(COMPANY, odoo_partner_id=self.partner.id)
        get = fake_get({'companies': lambda params: [company] if 'odoo_partner_id' in params else [],
                        'threads': [THREAD]})
        with patch.object(self.Client, 'get', get), \
                patch.object(self.Client, 'rpc', fake_rpc(UserError('La memoria respondió 500 en memoria_brief'))):
            self.partner.action_memoria_refresh()
        html = self.partner.memoria_html
        self.assertIn('Hilos recientes', html)
        self.assertNotIn('Lo que sabemos', html)
        self.assertEqual(self.partner.memoria_cache.get('brief'), None)

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

import json
import logging

from odoo import api, fields, models

_logger = logging.getLogger(__name__)


class IntelligenceQuery(models.TransientModel):
    _name = 'intelligence.query'
    _description = 'Consulta al cerebro de inteligencia'

    question = fields.Text(string='Pregunta', required=True)
    answer = fields.Html(string='Respuesta', readonly=True)
    context_used = fields.Text(string='Contexto utilizado', readonly=True)

    def action_ask(self):
        self.ensure_one()
        get = lambda k, d='': (
            self.env['ir.config_parameter'].sudo()
            .get_param('quimibond_intelligence.%s' % k, d)
        )
        anthropic_key = get('anthropic_api_key')
        voyage_key = get('voyage_api_key')
        supa_url = get('supabase_url')
        supa_key = get('supabase_service_role_key') or get('supabase_key')

        if not all([anthropic_key, supa_url, supa_key]):
            self.answer = '<p><b>Error:</b> Faltan API keys en la configuracion.</p>'
            return self._return_form()

        from ..services.claude_service import ClaudeService, VoyageService
        from ..services.supabase_service import SupabaseService

        supa = SupabaseService(supa_url, supa_key)
        claude_model = get('claude_model')
        claude = ClaudeService(anthropic_key, model=claude_model)

        # Paso 1: Buscar contexto en Odoo
        odoo_context = self._search_odoo(self.question)

        # Paso 2: Buscar contexto semantico en Supabase (RAG)
        rag_context = ''
        if voyage_key:
            try:
                voyage = VoyageService(voyage_key)
                query_embedding = voyage.embed_query(self.question)
                results = supa._request(
                    '/rest/v1/rpc/search_similar_emails',
                    'POST', {
                        'query_embedding': query_embedding,
                        'match_threshold': 0.3,
                        'match_count': 15,
                    })
                if results:
                    rag_parts = []
                    for r in results[:15]:
                        rag_parts.append(
                            'De: %s | Asunto: %s | Fecha: %s\n%s' % (
                                r.get('from_email', ''),
                                r.get('subject', ''),
                                r.get('date', ''),
                                (r.get('body', '') or r.get('snippet', ''))[:300],
                            )
                        )
                    rag_context = '\n---\n'.join(rag_parts)
            except Exception as exc:
                _logger.warning('RAG search failed: %s', exc)

        # Paso 3: Buscar briefings recientes
        briefings = self.env['intelligence.briefing'].search(
            [], limit=3, order='date desc')
        briefing_context = ''
        if briefings:
            parts = []
            for b in briefings:
                from odoo.tools import html2plaintext
                plain = html2plaintext(b.html_content or '')[:1500]
                parts.append('Briefing %s:\n%s' % (b.date, plain))
            briefing_context = '\n---\n'.join(parts)

        # Paso 4: Alertas abiertas
        alerts = self.env['intelligence.alert'].search(
            [('state', '=', 'open')], limit=10, order='create_date desc')
        alert_context = ''
        if alerts:
            alert_context = '\n'.join([
                '[%s] %s: %s' % (a.severity, a.alert_type, a.name)
                for a in alerts
            ])

        # Construir prompt
        full_context = []
        if odoo_context:
            full_context.append('DATOS DE ODOO ERP:\n%s' % odoo_context)
        if briefing_context:
            full_context.append(
                'BRIEFINGS RECIENTES:\n%s' % briefing_context)
        if alert_context:
            full_context.append('ALERTAS ABIERTAS:\n%s' % alert_context)
        if rag_context:
            full_context.append(
                'EMAILS RELEVANTES (busqueda semantica):\n%s' % rag_context)

        context_str = '\n\n'.join(full_context)
        self.context_used = context_str[:3000]

        prompt = (
            'Eres el cerebro de inteligencia de Quimibond, empresa '
            'manufacturera de textiles no tejidos en Mexico.\n\n'
            'CONTEXTO DISPONIBLE:\n%s\n\n'
            'PREGUNTA DE JOSE (Director General):\n%s\n\n'
            'Responde de forma directa, ejecutiva y accionable. '
            'Si no tienes suficiente informacion, dilo claramente. '
            'Usa HTML para formatear la respuesta.'
        ) % (context_str, self.question)

        try:
            system = ('Eres el cerebro de inteligencia de Quimibond, empresa '
                      'manufacturera de textiles no tejidos en Mexico. '
                      'Responde de forma directa, ejecutiva y accionable en HTML.')
            response = claude._call(system, prompt, max_tokens=2000)
            self.answer = response
        except Exception as exc:
            self.answer = '<p><b>Error:</b> %s</p>' % str(exc)

        return self._return_form()

    def _search_odoo(self, question):
        parts = []
        q_lower = question.lower()

        # Buscar partners mencionados
        words = [w for w in q_lower.split() if len(w) > 3]
        Partner = self.env['res.partner'].sudo()
        for word in words:
            partners = Partner.search([
                '|', ('name', 'ilike', word), ('email', 'ilike', word),
            ], limit=5)
            for p in partners:
                info = 'Partner: %s (%s)' % (p.name, p.email or 'sin email')
                if p.customer_rank > 0:
                    info += ' [CLIENTE]'
                    invoices = self.env['account.move'].sudo().search([
                        ('partner_id', '=', p.id),
                        ('move_type', '=', 'out_invoice'),
                        ('payment_state', 'in', ['not_paid', 'partial']),
                    ], limit=5)
                    if invoices:
                        total = sum(i.amount_residual for i in invoices)
                        info += ' Facturas pendientes: $%s' % '{:,.0f}'.format(total)
                if p.supplier_rank > 0:
                    info += ' [PROVEEDOR]'
                if getattr(p, 'intelligence_score', 0):
                    info += ' Score: %d/100 Riesgo: %s' % (
                        p.intelligence_score, getattr(p, 'intelligence_risk', None) or 'N/A')
                parts.append(info)

        # Buscar ventas si la pregunta menciona ventas/pedidos
        if any(w in q_lower for w in ['venta', 'pedido', 'orden', 'cotizacion']):
            sales = self.env['sale.order'].sudo().search(
                [], limit=10, order='date_order desc')
            for s in sales:
                parts.append('Venta %s: %s - $%s %s (%s)' % (
                    s.name, s.partner_id.name,
                    '{:,.0f}'.format(s.amount_total),
                    s.currency_id.name, s.state))

        # Buscar facturas si menciona factura/cobro/pago
        if any(w in q_lower for w in ['factura', 'cobr', 'pago', 'deuda', 'debe']):
            invoices = self.env['account.move'].sudo().search([
                ('move_type', '=', 'out_invoice'),
                ('payment_state', 'in', ['not_paid', 'partial']),
            ], limit=10, order='amount_residual desc')
            for i in invoices:
                parts.append('Factura %s: %s - Pendiente $%s (%s)' % (
                    i.name, i.partner_id.name,
                    '{:,.0f}'.format(i.amount_residual),
                    i.invoice_date or ''))

        return '\n'.join(parts) if parts else ''

    def _return_form(self):
        return {
            'type': 'ir.actions.act_window',
            'res_model': 'intelligence.query',
            'res_id': self.id,
            'view_mode': 'form',
            'target': 'new',
        }

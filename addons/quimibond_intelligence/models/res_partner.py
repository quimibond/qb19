import json
import logging

from odoo import api, fields, models

_logger = logging.getLogger(__name__)


class ResPartner(models.Model):
    _inherit = 'res.partner'

    intelligence_score = fields.Integer(
        string='Score de relacion',
        compute='_compute_intelligence_score', store=True)
    intelligence_risk = fields.Selection([
        ('low', 'Bajo'),
        ('medium', 'Medio'),
        ('high', 'Alto'),
    ], string='Riesgo', compute='_compute_intelligence_score', store=True)
    intelligence_score_ids = fields.One2many(
        'intelligence.client.score', 'partner_id',
        string='Historial de scores')
    intelligence_alert_ids = fields.One2many(
        'intelligence.alert', 'partner_id',
        string='Alertas de inteligencia')
    intelligence_alert_count = fields.Integer(
        string='Alertas abiertas',
        compute='_compute_intelligence_alert_count')
    intelligence_panorama = fields.Html(
        string='Panorama 360',
        compute='_compute_intelligence_panorama',
        sanitize=True, sanitize_overridable=True)
    intelligence_action_ids = fields.One2many(
        'intelligence.action.item', 'partner_id',
        string='Acciones de inteligencia')
    intelligence_priority = fields.Selection([
        ('urgent', 'Urgente'),
        ('high', 'Alta'),
        ('normal', 'Normal'),
        ('low', 'Baja'),
    ], string='Prioridad inteligencia',
        compute='_compute_intelligence_priority', store=True)

    # ── Cache de datos de Supabase (para 360 view offline) ────────────────
    intelligence_profile_json = fields.Text(
        string='Perfil (cache)', copy=False)
    intelligence_facts_json = fields.Text(
        string='Hechos KG (cache)', copy=False)
    intelligence_company_json = fields.Text(
        string='Empresa (cache)', copy=False)
    intelligence_last_sync = fields.Datetime(
        string='Ultima sync inteligencia', copy=False)

    # ── Computed financial/operational fields (live from Odoo) ───────────
    intel_total_invoiced = fields.Monetary(
        string='Total facturado', compute='_compute_intel_financial',
        currency_field='currency_id')
    intel_total_overdue = fields.Monetary(
        string='Saldo vencido', compute='_compute_intel_financial',
        currency_field='currency_id')
    intel_overdue_count = fields.Integer(
        string='Facturas vencidas', compute='_compute_intel_financial')
    intel_pending_deliveries = fields.Integer(
        string='Entregas pendientes', compute='_compute_intel_operational')
    intel_late_deliveries = fields.Integer(
        string='Entregas atrasadas', compute='_compute_intel_operational')
    intel_crm_pipeline_value = fields.Monetary(
        string='Pipeline CRM', compute='_compute_intel_crm',
        currency_field='currency_id')
    intel_crm_opportunity_count = fields.Integer(
        string='Oportunidades', compute='_compute_intel_crm')
    intel_pending_activities = fields.Integer(
        string='Actividades pendientes',
        compute='_compute_intel_activities')
    intel_overdue_activities = fields.Integer(
        string='Actividades vencidas',
        compute='_compute_intel_activities')

    def _compute_intel_financial(self):
        Move = self.env['account.move'].sudo()
        for partner in self:
            cpid = partner.commercial_partner_id.id or partner.id
            domain = [
                ('partner_id.commercial_partner_id', '=', cpid),
                ('move_type', '=', 'out_invoice'),
                ('state', '=', 'posted'),
            ]
            invoices = Move.search(domain)
            partner.intel_total_invoiced = sum(invoices.mapped('amount_total'))
            overdue = invoices.filtered(
                lambda i: i.payment_state in ('not_paid', 'partial')
                and i.invoice_date_due and i.invoice_date_due < fields.Date.today()
            )
            partner.intel_total_overdue = sum(overdue.mapped('amount_residual'))
            partner.intel_overdue_count = len(overdue)

    def _compute_intel_operational(self):
        Picking = self.env['stock.picking'].sudo()
        for partner in self:
            cpid = partner.commercial_partner_id.id or partner.id
            pending = Picking.search_count([
                ('partner_id.commercial_partner_id', '=', cpid),
                ('state', 'not in', ['done', 'cancel']),
                ('picking_type_code', '=', 'outgoing'),
            ])
            late = Picking.search_count([
                ('partner_id.commercial_partner_id', '=', cpid),
                ('state', 'not in', ['done', 'cancel']),
                ('picking_type_code', '=', 'outgoing'),
                ('scheduled_date', '<', fields.Datetime.now()),
            ])
            partner.intel_pending_deliveries = pending
            partner.intel_late_deliveries = late

    def _compute_intel_crm(self):
        Lead = self.env['crm.lead'].sudo()
        for partner in self:
            cpid = partner.commercial_partner_id.id or partner.id
            opps = Lead.search([
                ('partner_id.commercial_partner_id', '=', cpid),
                ('type', '=', 'opportunity'),
                ('active', '=', True),
            ])
            partner.intel_crm_pipeline_value = sum(
                opps.mapped('expected_revenue'))
            partner.intel_crm_opportunity_count = len(opps)

    def _compute_intel_activities(self):
        Activity = self.env['mail.activity'].sudo()
        today = fields.Date.today()
        for partner in self:
            total = Activity.search_count([
                ('res_id', '=', partner.id),
                ('res_model', '=', 'res.partner'),
            ])
            overdue = Activity.search_count([
                ('res_id', '=', partner.id),
                ('res_model', '=', 'res.partner'),
                ('date_deadline', '<', today),
            ])
            partner.intel_pending_activities = total
            partner.intel_overdue_activities = overdue

    @api.depends('intelligence_score_ids', 'intelligence_score_ids.total_score')
    def _compute_intelligence_score(self):
        Score = self.env['intelligence.client.score']
        for partner in self:
            last = Score.search([
                ('partner_id', '=', partner.id),
            ], limit=1, order='date desc')
            if last:
                partner.intelligence_score = last.total_score
                partner.intelligence_risk = last.risk_level
            else:
                partner.intelligence_score = 0
                partner.intelligence_risk = False

    def _compute_intelligence_alert_count(self):
        for partner in self:
            partner.intelligence_alert_count = self.env[
                'intelligence.alert'].search_count([
                    ('partner_id', '=', partner.id),
                    ('state', '=', 'open'),
                ])

    @api.depends('intelligence_score', 'intelligence_risk',
                 'intelligence_alert_count')
    def _compute_intelligence_priority(self):
        """Computa prioridad basada en score + risk + alertas abiertas."""
        for partner in self:
            score = partner.intelligence_score or 0
            risk = partner.intelligence_risk or ''
            alerts = partner.intelligence_alert_count or 0

            if risk == 'high' or score < 25 or alerts >= 5:
                partner.intelligence_priority = 'urgent'
            elif risk == 'medium' or score < 45 or alerts >= 3:
                partner.intelligence_priority = 'high'
            elif score < 65 or alerts >= 1:
                partner.intelligence_priority = 'normal'
            else:
                partner.intelligence_priority = 'low'

    def _compute_intelligence_panorama(self):
        """Genera panorama 360 HTML. Usa cache Odoo primero, Supabase como fallback."""
        from datetime import timedelta
        cache_ttl = timedelta(hours=24)
        now = fields.Datetime.now()

        for partner in self:
            if not partner.email:
                partner.intelligence_panorama = (
                    '<p class="text-muted">Sin email configurado</p>')
                continue

            # Intentar usar cache si es reciente
            use_cache = (
                partner.intelligence_last_sync
                and (now - partner.intelligence_last_sync) < cache_ttl
                and partner.intelligence_profile_json
            )

            try:
                if use_cache:
                    partner.intelligence_panorama = (
                        self._build_panorama_from_cache(partner))
                else:
                    partner.intelligence_panorama = (
                        self._build_panorama(partner))
            except Exception as exc:
                _logger.debug('Panorama %s: %s', partner.email, exc)
                # Fallback a cache si Supabase falla
                if partner.intelligence_profile_json:
                    try:
                        partner.intelligence_panorama = (
                            self._build_panorama_from_cache(partner))
                    except Exception:
                        partner.intelligence_panorama = ''
                else:
                    partner.intelligence_panorama = ''

    def _build_panorama_from_cache(self, partner):
        """Construye panorama HTML desde campos cache de Odoo."""
        sections = []
        profile = json.loads(partner.intelligence_profile_json or '{}')
        facts = json.loads(partner.intelligence_facts_json or '[]')
        company = json.loads(partner.intelligence_company_json or '{}')

        if profile:
            parts = []
            for key, label in [
                ('role', 'Rol'), ('company', 'Empresa'),
                ('communication_style', 'Estilo'),
                ('decision_power', 'Poder decisión'),
            ]:
                val = profile.get(key)
                if val:
                    parts.append(f'<strong>{label}:</strong> {val}')
            interests = profile.get('key_interests', [])
            if interests:
                parts.append(
                    f'<strong>Intereses:</strong> {", ".join(interests[:5])}')
            if parts:
                sections.append(
                    '<div class="mb-3">'
                    '<h4>Perfil</h4>'
                    f'<p>{" | ".join(parts)}</p>'
                    '</div>'
                )

        if facts:
            items = ''.join(
                f'<li>{f.get("fact_text", "")}</li>' for f in facts[:10])
            sections.append(
                '<div class="mb-3">'
                '<h4>Hechos conocidos</h4>'
                f'<ul>{items}</ul>'
                '</div>'
            )

        if company:
            parts = []
            if company.get('name'):
                parts.append(f'<strong>{company["name"]}</strong>')
            if company.get('industry'):
                parts.append(f'Industria: {company["industry"]}')
            if company.get('relationship_summary'):
                parts.append(company['relationship_summary'])
            if parts:
                sections.append(
                    '<div class="mb-3">'
                    '<h4>Empresa</h4>'
                    f'<p>{" | ".join(parts)}</p>'
                    '</div>'
                )

        if not sections:
            return '<p class="text-muted">Sin datos de inteligencia en cache</p>'

        cache_age = ''
        if partner.intelligence_last_sync:
            cache_age = (
                f'<small class="text-muted">Cache: '
                f'{partner.intelligence_last_sync.strftime("%Y-%m-%d %H:%M")}'
                f'</small>'
            )
        return f'{"".join(sections)}{cache_age}'

    def _build_panorama(self, partner):
        """Construye HTML del panorama 360."""
        email = partner.email
        sections = []

        # ── Supabase intelligence ────────────────────────────────────────
        supa_data = self._get_supabase_intel(email)

        # ── Header: quién es ─────────────────────────────────────────────
        profile = supa_data.get('profile', {})
        if profile:
            role = profile.get('role') or ''
            company = profile.get('company') or partner.parent_id.name or ''
            style = profile.get('communication_style') or ''
            power = profile.get('decision_power') or ''
            interests = profile.get('key_interests') or []
            notes = profile.get('personality_notes') or ''
            neg_style = profile.get('negotiation_style') or ''

            parts = []
            if role:
                parts.append(f'<strong>Rol:</strong> {role}')
            if company:
                parts.append(f'<strong>Empresa:</strong> {company}')
            if power:
                badge = {'high': 'danger', 'medium': 'warning',
                         'low': 'secondary'}.get(power, 'secondary')
                parts.append(
                    f'<strong>Poder decisión:</strong> '
                    f'<span class="badge text-bg-{badge}">{power}</span>')
            if style:
                parts.append(f'<strong>Estilo:</strong> {style}')
            if neg_style:
                parts.append(
                    f'<strong>Negociación:</strong> {neg_style}')
            if interests:
                parts.append(
                    f'<strong>Intereses:</strong> {", ".join(interests[:5])}')
            if notes:
                parts.append(f'<em>{notes}</em>')

            if parts:
                sections.append(
                    '<h4>👤 Perfil de Personalidad</h4>'
                    '<div style="margin-left:12px">'
                    + '<br/>'.join(parts)
                    + '</div>')

        # ── Temas recientes (de account_summaries) ───────────────────────
        topics = supa_data.get('recent_topics', [])
        if topics:
            items = ''.join(
                f'<li><strong>{t.get("item", "")}</strong>'
                f' — {t.get("action_needed", "")}</li>'
                for t in topics[:8]
            )
            sections.append(
                f'<h4>📋 Temas Activos ({len(topics)})</h4>'
                f'<ul style="margin-left:12px">{items}</ul>')

        # ── Alertas abiertas ─────────────────────────────────────────────
        alerts = supa_data.get('open_alerts', [])
        if alerts:
            rows = ''
            for a in alerts[:5]:
                sev = a.get('severity', 'medium')
                sev_color = {'critical': '#dc3545', 'high': '#fd7e14',
                             'medium': '#ffc107', 'low': '#6c757d'
                             }.get(sev, '#6c757d')
                rows += (
                    f'<tr><td><span style="color:{sev_color}">●</span> '
                    f'{a.get("title", "")}</td>'
                    f'<td>{a.get("alert_type", "")}</td>'
                    f'<td>{a.get("created_at", "")[:10]}</td></tr>')
            sections.append(
                f'<h4>⚠️ Alertas Abiertas ({len(alerts)})</h4>'
                f'<table class="table table-sm"><thead><tr>'
                f'<th>Alerta</th><th>Tipo</th><th>Fecha</th>'
                f'</tr></thead><tbody>{rows}</tbody></table>')

        # ── Acciones pendientes ──────────────────────────────────────────
        actions = supa_data.get('pending_actions', [])
        if actions:
            items = ''
            for ac in actions[:5]:
                pri = ac.get('priority', 'medium')
                pri_icon = {'critical': '🔴', 'high': '🟠',
                            'medium': '🟡', 'low': '⚪'}.get(pri, '🟡')
                due = ac.get('due_date') or ''
                items += (
                    f'<li>{pri_icon} {ac.get("description", "")}'
                    f'{f" — vence {due}" if due else ""}</li>')
            sections.append(
                f'<h4>✅ Acciones Pendientes ({len(actions)})</h4>'
                f'<ul style="margin-left:12px">{items}</ul>')

        # ── Hechos del Knowledge Graph ───────────────────────────────────
        facts = supa_data.get('facts', [])
        if facts:
            items = ''.join(
                f'<li>{f.get("fact", "")} '
                f'<span class="text-muted">({f.get("date", "")[:10]})</span>'
                f'</li>'
                for f in facts[:8]
            )
            sections.append(
                f'<h4>🧠 Lo que Sabemos ({len(facts)} hechos)</h4>'
                f'<ul style="margin-left:12px">{items}</ul>')

        # ── Emails recientes ─────────────────────────────────────────────
        emails = supa_data.get('recent_emails', [])
        if emails:
            items = ''
            for em in emails[:5]:
                items += (
                    f'<li><strong>{em.get("subject", "")}</strong>'
                    f' — {em.get("snippet", "")[:100]}'
                    f' <span class="text-muted">'
                    f'({em.get("date", "")[:10]})</span></li>')
            sections.append(
                f'<h4>📧 Emails Recientes ({len(emails)})</h4>'
                f'<ul style="margin-left:12px">{items}</ul>')

        # ── Empresa y contactos relacionados ────────────────────────────
        company_info = supa_data.get('company')
        siblings = supa_data.get('sibling_contacts') or []
        if company_info:
            co_parts = []
            co_name = company_info.get('name', '')
            if co_name:
                co_parts.append(
                    f'<strong>{co_name}</strong>')
            if company_info.get('industry'):
                co_parts.append(
                    f'Industria: {company_info["industry"]}')
            if company_info.get('lifetime_value'):
                co_parts.append(
                    f'Facturado empresa: '
                    f'<strong>${company_info["lifetime_value"]:,.0f}</strong>')
            if company_info.get('credit_limit'):
                co_parts.append(
                    f'Limite credito: ${company_info["credit_limit"]:,.0f}')
            if company_info.get('delivery_otd_rate') is not None:
                co_parts.append(
                    f'OTD empresa: {company_info["delivery_otd_rate"]}%')

            if siblings:
                sib_items = ', '.join(
                    f'{s.get("name", "")} ({s.get("role") or s.get("decision_power", "")})'
                    for s in siblings[:8]
                )
                co_parts.append(
                    f'<strong>Otros contactos ({len(siblings)}):</strong> '
                    f'{sib_items}')

            if co_parts:
                sections.append(
                    '<h4>🏢 Empresa</h4>'
                    '<div style="margin-left:12px">'
                    + '<br/>'.join(co_parts) + '</div>')

        # ── Odoo context (financial) ─────────────────────────────────────
        contact = supa_data.get('contact', {})
        odoo_ctx = contact.get('odoo_context') or {}
        if odoo_ctx:
            fin_parts = []
            if odoo_ctx.get('total_invoiced'):
                fin_parts.append(
                    f'Facturado total: <strong>'
                    f'${odoo_ctx["total_invoiced"]:,.0f}</strong>')
            if odoo_ctx.get('monthly_avg'):
                fin_parts.append(
                    f'Promedio mensual: ${odoo_ctx["monthly_avg"]:,.0f}')
            if odoo_ctx.get('trend_pct'):
                t = odoo_ctx['trend_pct']
                icon = '📈' if t > 0 else '📉'
                fin_parts.append(f'Tendencia: {icon} {t:+d}%')
            aging = odoo_ctx.get('aging', {})
            if aging:
                aging_parts = []
                for k, v in aging.items():
                    if v and k != 'current':
                        aging_parts.append(f'{k}: ${v:,.0f}')
                if aging_parts:
                    fin_parts.append(
                        f'Cartera vencida: {" | ".join(aging_parts)}')
            if odoo_ctx.get('otd_rate') is not None:
                fin_parts.append(
                    f'Entrega OTD: {odoo_ctx["otd_rate"]}%')
            if fin_parts:
                sections.append(
                    '<h4>💰 Contexto Financiero</h4>'
                    '<div style="margin-left:12px">'
                    + '<br/>'.join(fin_parts) + '</div>')

        if not sections:
            return ('<p class="text-muted">'
                    'Sin datos de inteligencia aún. '
                    'Ejecuta el pipeline o "Solo enriquecer datos".</p>')

        return '<div>' + ''.join(sections) + '</div>'

    def _get_supabase_intel(self, email):
        """Obtiene inteligencia de Supabase para un contacto.

        Usa get_contact_360 RPC que ahora incluye:
        - contact (con profile fields integrados)
        - company (datos de la empresa)
        - sibling_contacts (otros contactos de la misma empresa)
        - recent_alerts, recent_actions, entity_facts, health_history
        """
        get = lambda k, d='': (
            self.env['ir.config_parameter'].sudo()
            .get_param(f'quimibond_intelligence.{k}', d)
        )
        url = get('supabase_url')
        key = get('supabase_service_role_key') or get('supabase_key')
        if not url or not key:
            return {}

        try:
            from ..services.supabase_service import SupabaseService
            with SupabaseService(url, key) as supa:
                # get_contact_360 now returns contact + company + siblings
                result = supa._request(
                    '/rest/v1/rpc/get_contact_360', 'POST',
                    {'p_email': email},
                )
                if result and isinstance(result, dict) and result.get('contact'):
                    data = result
                else:
                    data = {}

                # Profile is now embedded in the contact record
                contact = data.get('contact', {})
                if contact:
                    data['profile'] = {
                        'role': contact.get('role'),
                        'company': contact.get('company_name')
                        or contact.get('company'),
                        'decision_power': contact.get('decision_power'),
                        'communication_style': contact.get('communication_style'),
                        'key_interests': contact.get('key_interests'),
                        'personality_notes': contact.get('personality_notes'),
                        'negotiation_style': contact.get('negotiation_style'),
                    }

                # Alerts and actions come from get_contact_360
                data['open_alerts'] = data.get('recent_alerts') or []
                data['pending_actions'] = data.get('recent_actions') or []
                data['facts'] = data.get('entity_facts') or []

                # Also get recent emails for this person
                from urllib.parse import quote as _q
                enc = _q(email, safe='')
                recent = supa._request(
                    f'/rest/v1/emails?sender=eq.{enc}'
                    '&order=email_date.desc&limit=5'
                    '&select=subject,snippet,email_date',
                )
                if recent and isinstance(recent, list):
                    data['recent_emails'] = [
                        {'subject': e.get('subject', ''),
                         'snippet': e.get('snippet', ''),
                         'date': e.get('email_date', '')}
                        for e in recent
                    ]

                # Get recent topics/key_items from account_summaries
                summaries = supa._request(
                    '/rest/v1/briefings?scope=eq.account'
                    '&order=briefing_date.desc&limit=3'
                    '&select=waiting_response,external_contacts',
                )
                if summaries and isinstance(summaries, list):
                    topics = []
                    name_lower = contact.get('name', '').lower()
                    for s in summaries:
                        ext = s.get('external_contacts') or []
                        if isinstance(ext, list):
                            for c in ext:
                                c_email = (c.get('email') or '').lower()
                                c_name = (c.get('name') or '').lower()
                                if c_email == email.lower() or (
                                    name_lower and c_name == name_lower
                                ):
                                    for ki in (s.get('key_items') or []):
                                        topics.append(ki)
                                    break
                    if topics:
                        data['recent_topics'] = topics[:10]

                return data
        except Exception as exc:
            _logger.debug('Supabase intel for %s: %s', email, exc)
            return {}

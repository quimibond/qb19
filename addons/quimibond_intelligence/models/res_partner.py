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

    def _compute_intelligence_panorama(self):
        """Genera panorama 360 HTML combinando Odoo + Supabase."""
        for partner in self:
            if not partner.email:
                partner.intelligence_panorama = (
                    '<p class="text-muted">Sin email configurado</p>')
                continue
            try:
                partner.intelligence_panorama = self._build_panorama(partner)
            except Exception as exc:
                _logger.debug('Panorama %s: %s', partner.email, exc)
                partner.intelligence_panorama = ''

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
        """Obtiene inteligencia de Supabase para un contacto."""
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
            supa = SupabaseService(url, key)

            # Try contact_360 RPC first
            result = supa._request(
                '/rest/v1/rpc/get_contact_360', 'POST',
                {'p_email': email},
            )
            if result and isinstance(result, dict) and result.get('contact'):
                data = result
            else:
                data = {}

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
            # Search for account summaries where this email appears
            summaries = supa._request(
                '/rest/v1/account_summaries'
                '?order=summary_date.desc&limit=3'
                '&select=key_items,external_contacts',
            )
            if summaries and isinstance(summaries, list):
                topics = []
                name_lower = (
                    data.get('contact', {}).get('name') or ''
                ).lower()
                for s in summaries:
                    # Check if this contact appears in external_contacts
                    ext = s.get('external_contacts') or []
                    if isinstance(ext, list):
                        for c in ext:
                            c_email = (c.get('email') or '').lower()
                            c_name = (c.get('name') or '').lower()
                            if c_email == email.lower() or (
                                name_lower and c_name == name_lower
                            ):
                                # This summary mentions our contact
                                for ki in (s.get('key_items') or []):
                                    topics.append(ki)
                                break
                if topics:
                    data['recent_topics'] = topics[:10]

            # Get person profile
            pp = supa._request(
                f'/rest/v1/person_profiles?email=eq.{enc}'
                '&limit=1&select=*',
            )
            if pp and isinstance(pp, list) and pp:
                data['profile'] = pp[0]

            # Open alerts
            contact = data.get('contact', {})
            name = contact.get('name') or ''
            if name:
                from urllib.parse import quote as _q2
                enc_name = _q2(name, safe='')
                alerts = supa._request(
                    f'/rest/v1/alerts?contact_name=eq.{enc_name}'
                    '&is_resolved=eq.false&order=created_at.desc'
                    '&limit=5&select=title,alert_type,severity,created_at',
                )
                if alerts and isinstance(alerts, list):
                    data['open_alerts'] = alerts

            # Pending actions
            if name:
                actions = supa._request(
                    f'/rest/v1/action_items?contact_name=eq.{enc_name}'
                    '&status=in.(open,pending)&order=due_date.asc'
                    '&limit=5&select=description,priority,due_date',
                )
                if actions and isinstance(actions, list):
                    data['pending_actions'] = actions

            # Facts from KG
            facts_data = data.get('entity_facts') or []
            if isinstance(facts_data, list):
                data['facts'] = facts_data

            return data
        except Exception as exc:
            _logger.debug('Supabase intel for %s: %s', email, exc)
            return {}

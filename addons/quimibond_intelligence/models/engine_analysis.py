"""
Quimibond Intelligence — Analysis (Claude, KG, embeddings, accountability)
Métodos de análisis con Claude, Knowledge Graph, embeddings y alertas de accountability.
"""
import json
import logging
import time
from collections import defaultdict
from datetime import datetime

from odoo import api, models

from .intelligence_config import (
    TZ_CDMX,
    acquire_lock,
    get_account_departments,
    release_lock,
)

_logger = logging.getLogger(__name__)


class IntelligenceEngine(models.Model):
    _inherit = 'intelligence.engine'

    # ══════════════════════════════════════════════════════════════════════════
    #   MICRO-PIPELINE: ANALYZE EMAILS
    # ══════════════════════════════════════════════════════════════════════════

    @api.model
    def run_analyze_emails(self):
        """Analiza emails no procesados con Claude. Corre cada 1-2h."""
        lock = 'quimibond_intelligence.analyze_running'
        if not acquire_lock(self.env, lock):
            return
        start = time.time()

        try:
            cfg = self._load_config()
            if not cfg:
                return

            account_departments = get_account_departments(self.env)

            from ..services.analysis_service import (
                AnalysisService,
                normalize_supabase_emails,
            )
            from ..services.claude_service import ClaudeService, VoyageService
            from ..services.odoo_enrichment import OdooEnrichmentService
            from ..services.supabase_service import SupabaseService

            claude = ClaudeService(cfg['anthropic_api_key'])
            voyage = (VoyageService(cfg['voyage_api_key'])
                      if cfg.get('voyage_api_key') else None)
            today = datetime.now(TZ_CDMX).strftime('%Y-%m-%d')
            analysis = AnalysisService()

            # Odoo enrichment primero (fuente de verdad)
            odoo_svc = OdooEnrichmentService(self.env)
            contacts = odoo_svc.extract_contacts()
            odoo_context = odoo_svc.enrich(contacts)

            with SupabaseService(cfg['supabase_url'], cfg['supabase_key']) as supa:
                # Buscar emails de las últimas 36h para cubrir timezone gaps
                # (CDMX es UTC-6, emails del día anterior en horario vespertino
                # tienen email_date en UTC que puede ser "hoy")
                from datetime import timedelta
                cutoff = (
                    datetime.now(TZ_CDMX) - timedelta(hours=36)
                ).strftime('%Y-%m-%dT%H:%M:%SZ')
                try:
                    recent_emails = supa._request(
                        '/rest/v1/emails?order=email_date.desc'
                        '&limit=500'
                        '&select=*'
                        f'&email_date=gte.{cutoff}',
                    ) or []
                except Exception:
                    recent_emails = []

                if not recent_emails:
                    _logger.info('Analyze: sin emails recientes')
                    return

                emails = normalize_supabase_emails(
                    recent_emails, account_departments)

                account_summaries, kg_by_account = (
                    self._analyze_accounts(
                        emails, claude, odoo_context,
                        account_departments, supa=supa,
                    )
                )
                supa.save_account_summaries(account_summaries, today)

                threads = self._build_threads(emails, cfg)
                metrics = analysis.compute_metrics(emails, threads, cfg)
                supa.save_metrics(metrics, today)

                alerts = analysis.generate_alerts(
                    threads, metrics, cfg,
                    account_summaries=account_summaries,
                    odoo_ctx=odoo_context,
                )
                supa.save_alerts(alerts, today)

                # Build team list for Claude assignee resolution
                team_members = []
                for u in self.env['res.users'].sudo().search([
                    ('active', '=', True),
                    ('share', '=', False),
                ], limit=50):
                    member = {
                        'name': u.name,
                        'email': u.email or u.login,
                    }
                    if hasattr(u, 'department_id') and u.department_id:
                        member['department'] = u.department_id.name
                    elif hasattr(u, 'job_title') and u.job_title:
                        member['department'] = u.job_title
                    team_members.append(member)

                self._feed_knowledge_graph(
                    emails, claude, supa, today,
                    kg_by_account=kg_by_account,
                    team_members=team_members,
                )

                if voyage:
                    self._generate_embeddings(emails, voyage, supa)

                supa.log_event('emails_analyzed', 'cron_analyze_emails',
                               payload={
                                   'emails': len(emails),
                                   'summaries': len(account_summaries),
                                   'alerts': len(alerts),
                                   'odoo_partners': len(odoo_context.get('partners', {})),
                                   'elapsed_s': round(time.time() - start, 1),
                               })

                _logger.info(
                    '✓ Analyze: %d emails, %d alerts (%.1fs)',
                    len(emails), len(alerts), time.time() - start,
                )
        except Exception as exc:
            _logger.error('run_analyze_emails: %s', exc, exc_info=True)
        finally:
            release_lock(self.env, lock)

    # ── Análisis por cuenta ───────────────────────────────────────────────────

    def _analyze_accounts(self, emails: list, claude, odoo_context: dict,
                          account_departments: dict = None,
                          supa=None) -> tuple:
        """Análisis unificado: resumen + KG en una sola llamada Claude por cuenta.

        Retorna (summaries, kg_by_account) donde kg_by_account es un dict
        de account → knowledge_graph data para alimentar el KG sin otra
        llamada a Claude.
        """
        from ..services.analysis_service import AnalysisService
        analysis = AnalysisService()
        account_departments = account_departments or {}

        person_profiles = {}

        by_account = defaultdict(list)
        for e in emails:
            by_account[e['account']].append(e)

        summaries = []
        kg_by_account = {}
        accounts_ok = 0
        accounts_failed = 0

        # Sort accounts by email count (most first) and limit to top 25
        # to avoid overwhelming Claude with too many sequential requests
        sorted_accounts = sorted(
            by_account.items(), key=lambda x: len(x[1]), reverse=True,
        )[:25]

        for account, acct_emails in sorted_accounts:
            dept = account_departments.get(account, 'Otro')
            ext_count = sum(
                1 for e in acct_emails if e['sender_type'] == 'external'
            )
            int_count = len(acct_emails) - ext_count

            # Skip accounts with very few emails (not worth a Claude call)
            if len(acct_emails) < 2:
                continue

            email_text = analysis.format_emails_for_claude(
                acct_emails, odoo_context, person_profiles,
            )

            try:
                _logger.info('  Analyzing %s (%d emails)...', account, len(acct_emails))
                full_result = claude.analyze_account_full(
                    dept, account, email_text, ext_count, int_count,
                )
                result = full_result['summary']
                result['account'] = account
                result['department'] = dept
                result['total_emails'] = len(acct_emails)
                summaries.append(result)

                # Guardar KG data para procesamiento posterior
                kg_data = full_result.get('knowledge_graph', {})
                if kg_data:
                    kg_by_account[account] = kg_data

                if supa and result.get('person_insights'):
                    for pi in result['person_insights']:
                        try:
                            supa.upsert_person_profile({
                                'name': pi.get('name', ''),
                                'email': pi.get('email'),
                                'company': pi.get('company'),
                                'role': pi.get('role_detected'),
                                'communication_style': pi.get(
                                    'communication_style', 'formal',
                                ),
                                'key_interests': pi.get('key_interests', []),
                                'personality_traits': pi.get(
                                    'personality_traits', [],
                                ),
                                'decision_factors': pi.get(
                                    'decision_factors', [],
                                ),
                                'decision_power': pi.get(
                                    'decision_power', 'medium',
                                ),
                                'personality_notes': pi.get('notes', ''),
                                'source_account': account,
                                'last_seen_date': (
                                    datetime.now(TZ_CDMX)
                                    .strftime('%Y-%m-%d')
                                ),
                            })
                        except Exception as exc:
                            _logger.debug('person_insight upsert: %s', exc)

                accounts_ok += 1
                _logger.info('  ✓ %s (%s): %d emails analizados',
                             dept, account, len(acct_emails))
            except Exception as exc:
                accounts_failed += 1
                _logger.error('  ✗ %s: %s', account, exc)
                # Continue with next account — don't let one failure kill all

        _logger.info('Account analysis: %d ok, %d failed (of %d)',
                     accounts_ok, accounts_failed, len(sorted_accounts))
        return summaries, kg_by_account

    # ── Knowledge Graph ──────────────────────────────────────────────────────

    def _feed_knowledge_graph(self, emails, claude, supa, today,
                              kg_by_account=None, team_members=None):
        """Alimenta el Knowledge Graph con datos extraídos.

        Si kg_by_account se proporciona (pre-extraído por analyze_account_full),
        lo usa directamente sin llamar a Claude de nuevo. Si no, hace fallback
        al comportamiento original con extract_knowledge().
        """
        if not emails:
            return

        from ..services.analysis_service import AnalysisService
        analysis = AnalysisService()

        gids = [e['gmail_message_id'] for e in emails if e.get('gmail_message_id')]
        try:
            kg_done = supa.get_gmail_message_ids_kg_processed(gids)
        except Exception as exc:
            _logger.warning(
                'KG: no se pudo leer kg_processed (%s); se procesan todos los emails',
                exc,
            )
            kg_done = set()

        pending = [
            e for e in emails
            if e.get('gmail_message_id') and e['gmail_message_id'] not in kg_done
        ]
        if not pending:
            _logger.info('Knowledge graph: todos los emails ya estaban procesados')
            return

        kg_by_account = kg_by_account or {}

        by_account = defaultdict(list)
        for e in pending:
            by_account[e['account']].append(e)

        ActionItem = self.env['intelligence.action.item'].sudo()
        Partner = self.env['res.partner'].sudo()
        User = self.env['res.users'].sudo()

        for account, acct_emails in by_account.items():
            if not acct_emails:
                continue

            # Usar KG pre-extraído si disponible, sino llamar a Claude
            kg = kg_by_account.get(account)
            if not kg:
                email_text = analysis.format_emails_for_claude(acct_emails, {})
                try:
                    kg = claude.extract_knowledge(
                        email_text, account,
                        team_members=team_members,
                    )
                except Exception as exc:
                    _logger.warning('KG extraction failed for %s: %s', account, exc)
                    continue

            # Guardar entidades
            entity_map = {}
            ent_ok, ent_fail = 0, 0
            for ent in kg.get('entities', []):
                try:
                    result = supa.upsert_entity(ent)
                    if result and isinstance(result, list) and result:
                        entity_map[ent['name']] = result[0].get('id')
                        ent_ok += 1
                    else:
                        ent_fail += 1
                except Exception as exc:
                    ent_fail += 1
                    _logger.warning('KG entity save failed (%s): %s', ent.get('name', '?'), exc)
            _logger.info('  KG entities: %d ok, %d failed', ent_ok, ent_fail)

            # Guardar hechos
            fact_ok, fact_fail, fact_skip = 0, 0, 0
            for fact in kg.get('facts', []):
                ent_name = fact.get('entity_name', '')
                ent_id = entity_map.get(ent_name)
                if not ent_id:
                    existing = supa.get_entity_by_name(ent_name)
                    ent_id = existing.get('id') if existing else None
                if ent_id:
                    try:
                        supa.save_fact({
                            'entity_id': ent_id,
                            'fact_type': fact.get('type', 'information'),
                            'fact_text': fact.get('text', ''),
                            'fact_date': fact.get('date'),
                            'is_future': fact.get('is_future', False),
                            'confidence': fact.get('confidence', 0.5),
                            'source_account': account,
                        })
                        fact_ok += 1
                    except Exception as exc:
                        fact_fail += 1
                        _logger.debug('KG fact save failed: %s', exc)
                else:
                    fact_skip += 1
            _logger.info('  KG facts: %d ok, %d failed, %d skipped (no entity)', fact_ok, fact_fail, fact_skip)

            # Guardar action items en Supabase + Odoo
            for item in kg.get('action_items', []):
                try:
                    # Resolve assignee: Claude gives a name, we find the user
                    assignee_name = item.get('assignee', '')
                    assignee_user = False
                    assignee_email = None
                    if assignee_name:
                        assignee_user = User.search([
                            '|', ('name', 'ilike', assignee_name),
                            ('login', 'ilike', assignee_name),
                        ], limit=1)
                        if assignee_user:
                            assignee_email = (
                                assignee_user.email or assignee_user.login)
                            assignee_name = assignee_user.name

                    # Resolve related contact
                    related = item.get('related_to', '')
                    partner = False
                    if related:
                        partner = Partner.search([
                            '|', ('name', 'ilike', related),
                            ('email', 'ilike', related),
                        ], limit=1)

                    # Save to Supabase
                    result = supa.save_action_item({
                        'assignee_name': assignee_name,
                        'assignee_email': assignee_email,
                        'description': item.get('description', ''),
                        'reason': item.get('reason'),
                        'action_type': item.get('type', 'other'),
                        'priority': item.get('priority', 'medium'),
                        'due_date': item.get('due_date'),
                        'contact_name': item.get('related_to', ''),
                    })
                    supa_id = (
                        result[0].get('id')
                        if result and isinstance(result, list)
                        else False
                    )

                    # Save to Odoo
                    ActionItem.create({
                        'name': item.get('description', '')[:200],
                        'action_type': item.get('type', 'other'),
                        'priority': item.get('priority', 'medium'),
                        'due_date': item.get('due_date') or False,
                        'partner_id': partner.id if partner else False,
                        'assignee_id': (
                            assignee_user.id if assignee_user else False),
                        'source_date': today,
                        'source_account': account,
                        'supabase_id': supa_id,
                    })
                except Exception as exc:
                    _logger.debug('Action item save error: %s', exc)

            # Guardar relaciones
            rel_ok, rel_fail = 0, 0
            for rel in kg.get('relationships', []):
                a_id = entity_map.get(rel.get('entity_a'))
                b_id = entity_map.get(rel.get('entity_b'))
                if a_id and b_id:
                    try:
                        supa.save_relationship({
                            'entity_a_id': a_id,
                            'entity_b_id': b_id,
                            'relationship_type': rel.get('type', 'mentioned_with'),
                            'context': rel.get('context', ''),
                        })
                        rel_ok += 1
                    except Exception as exc:
                        rel_fail += 1
                        _logger.debug('KG relationship save failed: %s', exc)
            _logger.info('  KG relationships: %d ok, %d failed', rel_ok, rel_fail)

            # Guardar perfiles de personas
            for profile in kg.get('person_profiles', []):
                try:
                    supa.upsert_person_profile({
                        'name': profile.get('name', ''),
                        'email': profile.get('email'),
                        'company': profile.get('company'),
                        'role': profile.get('role'),
                        'department': profile.get('department'),
                        'decision_power': profile.get('decision_power', 'medium'),
                        'communication_style': profile.get(
                            'communication_style', 'formal',
                        ),
                        'language_preference': profile.get(
                            'language_preference', 'es',
                        ),
                        'key_interests': profile.get('key_interests', []),
                        'personality_notes': profile.get('personality_notes', ''),
                        'negotiation_style': profile.get('negotiation_style'),
                        'response_pattern': profile.get('response_pattern'),
                        'influence_on_deals': profile.get('influence_on_deals'),
                        'source_account': account,
                        'last_seen_date': today,
                    })
                except Exception as exc:
                    _logger.debug('Person profile save error: %s', exc)

            batch_ids = [
                e['gmail_message_id'] for e in acct_emails
                if e.get('gmail_message_id')
            ]
            try:
                supa.mark_emails_kg_processed(batch_ids)
            except Exception as exc:
                _logger.warning('KG mark_processed %s: %s', account, exc)

        _logger.info('Knowledge graph alimentado (con perfiles de personas)')

    # ── Embeddings ────────────────────────────────────────────────────────────

    @staticmethod
    def _generate_embeddings(emails: list, voyage, supa):
        """Genera y guarda embeddings para emails con contenido sustancial."""
        if not voyage:
            return

        to_embed = [
            e for e in emails
            if len(e.get('body', '') or e.get('snippet', '')) > 50
            and e.get('gmail_message_id')
        ]
        if not to_embed:
            return

        gids = [e['gmail_message_id'] for e in to_embed]
        try:
            already = supa.get_gmail_message_ids_with_embedding(gids)
        except Exception as exc:
            _logger.warning('Consulta embeddings existentes falló (%s); se generan todos',
                            exc)
            already = set()
        if already:
            to_embed = [e for e in to_embed
                        if e['gmail_message_id'] not in already]
            _logger.info('Embeddings: omitiendo %d ya presentes en Supabase',
                         len(already))
        if not to_embed:
            _logger.info('Embeddings: nada pendiente')
            return

        batch_size = 64
        total = 0
        for i in range(0, len(to_embed), batch_size):
            batch = to_embed[i:i + batch_size]
            texts = [
                f"De: {e['from']} | Asunto: {e['subject']} | "
                f"{(e.get('body') or e.get('snippet', ''))[:500]}"
                for e in batch
            ]
            try:
                embeddings = voyage.embed(texts)
                for e, emb in zip(batch, embeddings):
                    supa.update_email_embedding(e['gmail_message_id'], emb)
                total += len(batch)
            except Exception as exc:
                _logger.warning('Embedding batch error: %s', exc)

        _logger.info('✓ %d embeddings generados', total)

    # ── Accountability alerts ────────────────────────────────────────────────

    def _generate_accountability_alerts(self, odoo_ctx, alerts, supa, today):
        """Genera alertas de accountability cuando hay acciones sin cumplir."""
        followup = odoo_ctx.get('action_followup', {})
        if not followup.get('items'):
            return

        ActionItem = self.env['intelligence.action.item'].sudo()

        for item in followup['items']:
            if item.get('evidence_of_action') and len(
                item['evidence_of_action']
            ) >= 2:
                try:
                    action = ActionItem.browse(item['id'])
                    if action.exists() and action.state in (
                        'open', 'in_progress',
                    ):
                        action.write({'state': 'done'})
                        _logger.info(
                            'Auto-completada acción #%d: %s (evidencia encontrada)',
                            item['id'], item['description'][:60],
                        )
                except Exception as exc:
                    _logger.debug('Auto-complete action #%d: %s',
                                  item['id'], exc)
                continue

            if item['is_overdue'] and not item.get('evidence_of_action'):
                alerts.append({
                    'alert_type': 'accountability',
                    'severity': 'high' if item['days_open'] > 5 else 'medium',
                    'title': (
                        f"Acción sin cumplir ({item['days_open']}d): "
                        f"{item['description'][:80]}"
                    ),
                    'description': (
                        f"Asignada a: {item.get('assigned_to', 'Sin asignar')}"
                        f" | Contacto: {item.get('partner', 'N/A')}"
                        f" | Tipo: {item.get('type', 'otro')}"
                        f" | Sin evidencia de acción en Odoo"
                    ),
                    'account': '',
                    'related_contact': item.get('partner', ''),
                })

        acct_alerts = [
            a for a in alerts if a.get('alert_type') == 'accountability'
        ]
        if acct_alerts:
            try:
                supa.save_alerts(acct_alerts, today)
            except Exception as exc:
                _logger.debug('Accountability alerts save: %s', exc)
            _logger.info(
                '✓ %d alertas de accountability generadas', len(acct_alerts),
            )

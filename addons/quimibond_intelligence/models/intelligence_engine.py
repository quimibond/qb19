"""
Quimibond Intelligence — Motor Principal (Orquestador)
Delega a servicios especializados:
- OdooEnrichmentService: enriquecimiento profundo con Odoo ORM
- AnalysisService: métricas, alertas, scoring, briefing
- SyncService: sincronización Odoo→Supabase (pendiente de extraer)
- KnowledgeGraphService: extracción de entidades y hechos (pendiente de extraer)
"""
import json
import logging
import re
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

from odoo import api, fields, models

from .intelligence_config import (
    INTERNAL_DOMAIN,
    get_account_departments,
    get_email_accounts,
)

_logger = logging.getLogger(__name__)

# ── Zona horaria CDMX ─────────────────────────────────────────────────────────
try:
    from zoneinfo import ZoneInfo
    TZ_CDMX = ZoneInfo('America/Mexico_City')
except ImportError:
    import pytz
    TZ_CDMX = pytz.timezone('America/Mexico_City')


class IntelligenceEngine(models.Model):
    """Motor que ejecuta el pipeline de inteligencia.

    Se invoca vía ir.cron o manualmente desde la vista de configuración.
    Usa Model (no AbstractModel) para que ir.cron pueda referenciar model_id.
    """
    _name = 'intelligence.engine'
    _description = 'Intelligence Engine (orquestador)'
    _log_access = False

    # ══════════════════════════════════════════════════════════════════════════
    #   PUNTO DE ENTRADA: CRON DIARIO
    # ══════════════════════════════════════════════════════════════════════════

    @api.model
    def run_daily_intelligence(self):
        """Método invocado por ir.cron cada día a las 19:00 CDMX."""
        # Concurrency guard — prevent duplicate runs
        lock_param = 'quimibond_intelligence.pipeline_running'
        ICP = self.env['ir.config_parameter'].sudo()
        if ICP.get_param(lock_param, 'false') == 'true':
            _logger.warning('Pipeline ya está corriendo — abortando')
            return
        ICP.set_param(lock_param, 'true')

        start = time.time()
        today = datetime.now(TZ_CDMX).strftime('%Y-%m-%d')
        _logger.info('═══ QUIMIBOND INTELLIGENCE — %s ═══', today)

        try:
            self._run_pipeline(today, start)
        except Exception as exc:
            _logger.error('═══ PIPELINE FALLÓ: %s ═══', exc, exc_info=True)
        finally:
            ICP.set_param(lock_param, 'false')
            elapsed = time.time() - start
            _logger.info('═══ PIPELINE FINALIZADO en %.1f segundos ═══', elapsed)

    # ══════════════════════════════════════════════════════════════════════════
    #   MICRO-PIPELINES (frecuencia independiente)
    # ══════════════════════════════════════════════════════════════════════════

    @api.model
    def run_sync_emails(self):
        """Sync incremental de emails desde Gmail → Supabase. Corre cada 30 min."""
        lock = 'quimibond_intelligence.sync_emails_running'
        ICP = self.env['ir.config_parameter'].sudo()
        if ICP.get_param(lock, 'false') == 'true':
            return
        ICP.set_param(lock, 'true')
        start = time.time()

        try:
            cfg = self._load_config()
            if not cfg:
                return

            email_accounts = get_email_accounts(self.env)
            account_departments = get_account_departments(self.env)

            from ..services.gmail_service import GmailService
            from ..services.supabase_service import SupabaseService

            sa_info = json.loads(cfg['service_account_json'])
            gmail = GmailService(sa_info)

            with SupabaseService(cfg['supabase_url'], cfg['supabase_key']) as supa:
                gmail_history = self._load_gmail_history_state()
                result = gmail.read_all_accounts(
                    email_accounts, history_state=gmail_history, max_workers=5,
                )
                self._save_gmail_history_state(result['gmail_history_state'])
                all_emails = result['emails']

                if not all_emails:
                    _logger.info('Sync: sin emails nuevos')
                    return

                emails = self._deduplicate(all_emails)
                for e in emails:
                    e['department'] = account_departments.get(
                        e['account'], 'Otro')

                supa.save_emails(emails)
                threads = self._build_threads(emails, cfg)
                supa.save_threads(threads)

                for acct, hid in result['gmail_history_state'].items():
                    supa.save_sync_state(acct, str(hid))

                supa._request('/rest/v1/events', 'POST', {
                    'event_type': 'emails_synced',
                    'source': 'cron_sync_emails',
                    'payload': {
                        'total': len(emails),
                        'accounts_ok': result['success_count'],
                        'accounts_failed': result['failed_count'],
                        'threads': len(threads),
                    },
                })

                _logger.info(
                    '✓ Sync: %d emails, %d threads (%.1fs)',
                    len(emails), len(threads),
                    time.time() - start,
                )
        except Exception as exc:
            _logger.error('run_sync_emails: %s', exc, exc_info=True)
        finally:
            ICP.set_param(lock, 'false')

    @api.model
    def run_analyze_emails(self):
        """Analiza emails no procesados con Claude. Corre cada 1-2h."""
        lock = 'quimibond_intelligence.analyze_running'
        ICP = self.env['ir.config_parameter'].sudo()
        if ICP.get_param(lock, 'false') == 'true':
            return
        ICP.set_param(lock, 'true')
        start = time.time()

        try:
            cfg = self._load_config()
            if not cfg:
                return

            account_departments = get_account_departments(self.env)

            from ..services.analysis_service import AnalysisService
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
                try:
                    recent_emails = supa._request(
                        '/rest/v1/emails?order=email_date.desc'
                        '&limit=500'
                        '&select=*'
                        f'&email_date=gte.{today}T00:00:00Z',
                    ) or []
                except Exception:
                    recent_emails = []

                if not recent_emails:
                    _logger.info('Analyze: sin emails recientes')
                    return

                emails = []
                for e in recent_emails:
                    emails.append({
                        'account': e.get('account', ''),
                        'from': e.get('sender', ''),
                        'from_email': e.get('sender', ''),
                        'to': e.get('recipient', ''),
                        'subject': e.get('subject', ''),
                        'subject_normalized': (e.get('subject') or '').lower(),
                        'body': e.get('body', ''),
                        'snippet': e.get('snippet', ''),
                        'date': e.get('email_date', ''),
                        'gmail_message_id': e.get('gmail_message_id', ''),
                        'gmail_thread_id': e.get('gmail_thread_id', ''),
                        'attachments': e.get('attachments'),
                        'is_reply': e.get('is_reply', False),
                        'sender_type': e.get('sender_type', 'external'),
                        'has_attachments': e.get('has_attachments', False),
                        'department': account_departments.get(
                            e.get('account', ''), 'Otro'),
                    })

                account_summaries = self._analyze_accounts(
                    emails, claude, odoo_context, account_departments,
                    supa=supa,
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

                self._feed_knowledge_graph(emails, claude, supa, today)

                if voyage:
                    self._generate_embeddings(emails, voyage, supa)

                supa._request('/rest/v1/events', 'POST', {
                    'event_type': 'emails_analyzed',
                    'source': 'cron_analyze_emails',
                    'payload': {
                        'emails': len(emails),
                        'summaries': len(account_summaries),
                        'alerts': len(alerts),
                        'odoo_partners': len(odoo_context.get('partners', {})),
                        'elapsed_s': round(time.time() - start, 1),
                    },
                })

                _logger.info(
                    '✓ Analyze: %d emails, %d alerts (%.1fs)',
                    len(emails), len(alerts), time.time() - start,
                )
        except Exception as exc:
            _logger.error('run_analyze_emails: %s', exc, exc_info=True)
        finally:
            ICP.set_param(lock, 'false')

    @api.model
    def run_update_scores(self):
        """Recalcula health scores y sync Odoo→Supabase. Corre cada 2h."""
        lock = 'quimibond_intelligence.scores_running'
        ICP = self.env['ir.config_parameter'].sudo()
        if ICP.get_param(lock, 'false') == 'true':
            return
        ICP.set_param(lock, 'true')
        start = time.time()

        try:
            cfg = self._load_config()
            if not cfg:
                return

            from ..services.odoo_enrichment import OdooEnrichmentService
            from ..services.supabase_service import SupabaseService
            today = datetime.now(TZ_CDMX).strftime('%Y-%m-%d')

            odoo_svc = OdooEnrichmentService(self.env)
            contacts = odoo_svc.extract_contacts()
            if not contacts:
                return
            odoo_ctx = odoo_svc.enrich(contacts)

            with SupabaseService(cfg['supabase_url'], cfg['supabase_key']) as supa:
                if odoo_ctx.get('partners'):
                    self._sync_contacts_to_supabase(odoo_ctx, supa, today)

                self._link_odoo_ids(supa)

                sb_contacts = [
                    {'email': e, 'contact_type': 'external'}
                    for e in odoo_ctx.get('partners', {})
                ]
                account_summaries = supa._request(
                    '/rest/v1/account_summaries?order=summary_date.desc'
                    '&limit=50',
                ) or []
                supa.compute_and_save_health_scores(
                    sb_contacts, account_summaries, today,
                )

                supa._request(
                    '/rest/v1/rpc/refresh_contact_360', 'POST', {},
                )

                supa._request('/rest/v1/events', 'POST', {
                    'event_type': 'scores_updated',
                    'source': 'cron_update_scores',
                    'payload': {
                        'contacts': len(contacts),
                        'partners_synced': len(odoo_ctx.get('partners', {})),
                        'elapsed_s': round(time.time() - start, 1),
                    },
                })

                _logger.info(
                    '✓ Scores: %d contacts updated (%.1fs)',
                    len(contacts), time.time() - start,
                )
        except Exception as exc:
            _logger.error('run_update_scores: %s', exc, exc_info=True)
        finally:
            ICP.set_param(lock, 'false')

    # ══════════════════════════════════════════════════════════════════════════
    #   PIPELINE COMPLETO (diario)
    # ══════════════════════════════════════════════════════════════════════════

    def _run_pipeline(self, today: str, start: float):
        """Ejecuta el pipeline completo. Separado para manejo de errores."""
        cfg = self._load_config()
        if not cfg:
            return

        email_accounts = get_email_accounts(self.env)
        account_departments = get_account_departments(self.env)

        gmail, claude, voyage, supa = self._init_services(cfg)

        try:
            self._run_pipeline_with_services(
                cfg, gmail, claude, voyage, supa,
                email_accounts, account_departments, today, start,
            )
        finally:
            supa.close()
            stats = supa.sync_stats
            _logger.info(
                'Supabase sync stats: %d success, %d failed, %d skipped',
                stats['success'], stats['failed'], stats['skipped'],
            )

    def _run_pipeline_with_services(self, cfg, gmail, claude, voyage, supa,
                                     email_accounts, account_departments,
                                     today, start):
        """Pipeline body — separated so supa.close() always runs."""
        from ..services.analysis_service import AnalysisService
        from ..services.odoo_enrichment import OdooEnrichmentService

        analysis = AnalysisService()
        odoo_svc = OdooEnrichmentService(self.env)

        # ══ FASE 1: Odoo → Supabase ══
        _logger.info('── FASE 1: Odoo → Supabase (fuente de verdad) ──')
        contacts = odoo_svc.extract_contacts()
        odoo_context = odoo_svc.enrich(contacts)

        if odoo_context.get('partners'):
            self._sync_contacts_to_supabase(odoo_context, supa, today)
            self._link_odoo_ids(supa)
            _logger.info('✓ %d partners Odoo sincronizados',
                         len(odoo_context['partners']))
        else:
            _logger.warning('Sin partners Odoo — continuando con emails')

        # ══ FASE 2: Leer emails ══
        _logger.info('── FASE 2: Lectura de emails ──')
        gmail_history_state = self._load_gmail_history_state()
        result = gmail.read_all_accounts(
            email_accounts, history_state=gmail_history_state, max_workers=5,
        )
        self._save_gmail_history_state(result['gmail_history_state'])
        all_emails = result['emails']
        _logger.info('Total bruto: %d emails (%d cuentas OK, %d fallidas)',
                      len(all_emails), result['success_count'], result['failed_count'])

        if not all_emails:
            _logger.warning('Sin emails — solo se sincronizó Odoo')
            return

        # ══ FASE 3: Dedup + persistencia ══
        _logger.info('── FASE 3: Dedup + persistencia ──')
        emails = self._deduplicate(all_emails)
        _logger.info('Después de dedup: %d emails únicos', len(emails))

        for e in emails:
            e['department'] = account_departments.get(e['account'], 'Otro')

        try:
            supa.save_emails(emails)
        except Exception as exc:
            _logger.error('Error guardando emails: %s', exc)

        threads = self._build_threads(emails, cfg)
        try:
            supa.save_threads(threads)
        except Exception as exc:
            _logger.error('Error guardando threads: %s', exc)

        # ══ FASE 4: Análisis con Claude ══
        _logger.info('── FASE 4: Análisis Claude por cuenta ──')
        account_summaries = self._analyze_accounts(
            emails, claude, odoo_context, account_departments, supa=supa,
        )

        try:
            supa.save_account_summaries(account_summaries, today)
        except Exception as exc:
            _logger.error('Error guardando summaries: %s', exc)

        # ══ FASE 5: Métricas y scoring ══
        _logger.info('── FASE 5: Métricas y scoring ──')
        metrics = analysis.compute_metrics(emails, threads, cfg)
        try:
            supa.save_metrics(metrics, today)
        except Exception as exc:
            _logger.error('Error guardando métricas: %s', exc)

        alerts = analysis.generate_alerts(
            threads, metrics, cfg,
            account_summaries=account_summaries,
            odoo_ctx=odoo_context,
        )
        try:
            supa.save_alerts(alerts, today)
        except Exception as exc:
            _logger.error('Error guardando alertas: %s', exc)

        client_scores = analysis.compute_client_scores(
            contacts, emails, threads, cfg,
            account_summaries=account_summaries,
            odoo_ctx=odoo_context,
        )
        contact_sentiments = {}
        for s in account_summaries:
            for ec in s.get('external_contacts', []):
                email_addr = (ec.get('email') or '').lower()
                if email_addr and ec.get('sentiment_score') is not None:
                    try:
                        contact_sentiments[email_addr] = float(
                            ec['sentiment_score'],
                        )
                    except (ValueError, TypeError):
                        pass

        try:
            supa.save_client_scores(
                client_scores, today,
                contact_sentiments=contact_sentiments,
            )
        except Exception as exc:
            _logger.error('Error guardando client scores: %s', exc)

        # ══ FASE 6: Síntesis ejecutiva ══
        _logger.info('── FASE 6: Síntesis ejecutiva ──')
        historical = {}
        try:
            historical = supa.get_historical_context()
        except Exception as exc:
            _logger.warning('Sin contexto histórico: %s', exc)

        data_package = analysis.build_data_package(
            today, account_summaries, metrics, alerts, threads,
            client_scores, odoo_context, historical,
        )
        try:
            briefing_html = claude.synthesize_briefing(data_package)
        except Exception as exc:
            _logger.error('Error generando briefing con Claude: %s', exc)
            briefing_html = (
                '<h2>Briefing no disponible</h2>'
                '<p>Error al generar el briefing: %s</p>'
                '<p>Emails procesados: %d | Cuentas OK: %d | Fallidas: %d</p>'
            ) % (exc, len(emails), result['success_count'], result['failed_count'])

        topics = []
        try:
            topics = claude.extract_topics(briefing_html)
        except Exception as exc:
            _logger.error('Error extrayendo temas: %s', exc)
        _logger.info('%d temas extraídos', len(topics))

        if topics:
            try:
                supa.save_topics(topics, today)
            except Exception as exc:
                _logger.error('Error guardando topics: %s', exc)

        key_events = analysis.build_key_events(alerts, account_summaries)

        try:
            supa.save_daily_summary(
                today, briefing_html, len(emails),
                result['success_count'], result['failed_count'], len(topics),
                key_events=key_events,
            )
        except Exception as exc:
            _logger.error('Error guardando daily summary: %s', exc)

        try:
            signals = supa._request(
                '/rest/v1/rpc/detect_cross_department_topics', 'POST', {},
            )
            if signals and isinstance(signals, list) and signals:
                _logger.info('✓ %d cross-department signals detected',
                             len(signals))
        except Exception as exc:
            _logger.debug('cross_department_signals: %s', exc)

        # ══ FASE 7: Knowledge Graph ══
        _logger.info('── FASE 7: Knowledge Graph ──')
        self._feed_knowledge_graph(emails, claude, supa, today)

        # ══ FASE 8: Embeddings ══
        if cfg.get('voyage_api_key'):
            _logger.info('── FASE 8: Embeddings ──')
            self._generate_embeddings(emails, voyage, supa)

        # ══ FASE 8.5: Scoring y aprendizaje ══
        _logger.info('── FASE 8.5: Scoring y aprendizaje ──')

        self._generate_accountability_alerts(
            odoo_context, alerts, supa, today,
        )

        try:
            sb_contacts = [
                {'email': e, 'contact_type': 'external'}
                for e in odoo_context.get('partners', {})
            ]
            supa.compute_and_save_health_scores(
                sb_contacts, account_summaries, today,
            )
        except Exception as exc:
            _logger.debug('Health scores: %s', exc)

        for acct, hid in result['gmail_history_state'].items():
            supa.save_sync_state(acct, str(hid))

        # ══ FASE 9: Envío del briefing ══
        _logger.info('── FASE 9: Envío del briefing ──')
        sender = (cfg.get('sender_email') or '').strip()
        recipient = (cfg.get('recipient_email') or '').strip()
        if sender and '@' in sender and recipient and '@' in recipient:
            try:
                subject = f'Intelligence Briefing — {today}'
                gmail.send_email(sender, recipient,
                                 subject, analysis.wrap_briefing_html(briefing_html, today))
                _logger.info('Briefing enviado a %s', recipient)
            except Exception as exc:
                _logger.error('Error enviando briefing: %s', exc)
        else:
            _logger.warning('Briefing no enviado: falta sender_email o recipient_email en config')

        try:
            self._save_to_odoo(
                today, briefing_html, emails, alerts,
                client_scores, contacts, time.time() - start,
                topics=topics,
                accounts_failed=result['failed_count'],
            )
        except Exception as exc:
            _logger.error('Error guardando en Odoo: %s', exc)

        # ══ FASE 10: Company enrichment ══
        _logger.info('── FASE 10: Company enrichment ──')
        try:
            self._enrich_companies(supa, claude, today)
        except Exception as exc:
            _logger.warning('Company enrichment error: %s', exc, exc_info=True)

        try:
            supa._request(
                '/rest/v1/rpc/refresh_contact_360', 'POST', {},
            )
            _logger.info('✓ contact_360 view refreshed')
        except Exception as exc:
            _logger.debug('refresh_contact_360: %s', exc)

        _logger.info('Pipeline completado exitosamente')

    # ══════════════════════════════════════════════════════════════════════════
    #   ENRICH ONLY — Odoo→Supabase sin pipeline completo
    # ══════════════════════════════════════════════════════════════════════════

    @api.model
    def run_enrich_only(self):
        """Sincroniza datos de Odoo → Supabase. Odoo es la fuente de verdad."""
        _logger.info('═══ ODOO → SUPABASE SYNC — %s ═══',
                      datetime.now(TZ_CDMX).strftime('%Y-%m-%d %H:%M'))
        cfg = self._load_config()
        if not cfg:
            return

        from ..services.odoo_enrichment import OdooEnrichmentService
        from ..services.supabase_service import SupabaseService
        today = datetime.now(TZ_CDMX).strftime('%Y-%m-%d')

        odoo_svc = OdooEnrichmentService(self.env)

        # ── FASE 1: Cargar partners de Odoo ──
        odoo_models = odoo_svc.load_models()
        if 'partner' not in odoo_models:
            _logger.error('res.partner no disponible')
            return
        Partner = odoo_models['partner']

        odoo_partners = Partner.search([
            ('email', '!=', False),
            ('email', '!=', ''),
            ('active', '=', True),
            '|',
            ('customer_rank', '>', 0),
            ('supplier_rank', '>', 0),
        ], order='customer_rank desc, supplier_rank desc')

        if not odoo_partners:
            _logger.warning('No se encontraron partners activos en Odoo')
            return

        _logger.info('FASE 1: %d partners activos en Odoo', len(odoo_partners))

        # ── FASE 2: Enriquecer cada partner (17 dimensiones) ──
        _logger.info('FASE 2: Enriquecimiento profundo')
        date_90d = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
        date_30d = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        date_7d = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        today_date = datetime.now().date()

        odoo_ctx = {
            'partners': {},
            'business_summary': {},
            'action_followup': {},
            'global_pipeline': {},
            'team_activities': {},
        }

        enriched = 0
        for partner in odoo_partners:
            try:
                email_addr = partner.email.strip().lower()
                if not email_addr or '@' not in email_addr:
                    continue
                ctx = odoo_svc.enrich_partner(
                    partner, odoo_models, date_90d, date_30d,
                    date_7d, today_date,
                )
                odoo_ctx['partners'][email_addr] = ctx
                odoo_ctx['business_summary'][email_addr] = (
                    ctx.get('_summary', '')
                )
                enriched += 1
            except Exception as exc:
                _logger.debug('Enrich skip %s: %s',
                              partner.email, exc)

        _logger.info('✓ %d/%d partners enriquecidos',
                     enriched, len(odoo_partners))

        odoo_ctx['action_followup'] = odoo_svc.verify_pending_actions(
            today_date)
        if odoo_models.get('crm_lead'):
            odoo_ctx['global_pipeline'] = odoo_svc.get_global_pipeline(
                odoo_models['crm_lead'])
        if odoo_models.get('mail_activity'):
            odoo_ctx['team_activities'] = odoo_svc.get_team_activities(
                odoo_models['mail_activity'], today_date)

        # ── FASE 3: Sync a Supabase ──
        _logger.info('FASE 3: Sync a Supabase (%d partners)',
                     len(odoo_ctx['partners']))

        with SupabaseService(cfg['supabase_url'], cfg['supabase_key']) as supa:
            contacts_to_save = []
            for email_addr, p in odoo_ctx['partners'].items():
                company_name = p.get('company_name', '')
                contacts_to_save.append({
                    'email': email_addr,
                    'name': p.get('name', ''),
                    'contact_type': 'external',
                    'company': company_name,
                })
            if contacts_to_save:
                supa.save_contacts(contacts_to_save)

            self._sync_contacts_to_supabase(odoo_ctx, supa, today)
            self._link_odoo_ids(supa)

            # ── FASE 4: Health scores ──
            _logger.info('FASE 4: Health scores')
            try:
                sb_contacts = [
                    {'email': e, 'contact_type': 'external'}
                    for e in odoo_ctx['partners']
                ]
                account_summaries = supa._request(
                    '/rest/v1/account_summaries?order=summary_date.desc'
                    '&limit=50',
                ) or []
                supa.compute_and_save_health_scores(
                    sb_contacts, account_summaries, today,
                )
            except Exception as exc:
                _logger.debug('Health scores: %s', exc)

            # ── FASE 5: Company enrichment con Claude ──
            _logger.info('FASE 5: Company enrichment')
            try:
                from ..services.claude_service import ClaudeService
                claude_key = cfg.get('anthropic_api_key')
                if claude_key:
                    claude = ClaudeService(claude_key)
                    self._enrich_companies(supa, claude, today)
            except Exception as exc:
                _logger.warning('Company enrichment: %s', exc,
                                exc_info=True)

            # ── FASE 6: Identity resolution + refresh ──
            _logger.info('FASE 6: Identity resolution')
            try:
                supa._request(
                    '/rest/v1/rpc/resolve_all_identities', 'POST', {},
                )
            except Exception as exc:
                _logger.debug('resolve_all_identities: %s', exc)

            try:
                supa._request(
                    '/rest/v1/rpc/refresh_contact_360', 'POST', {},
                )
                _logger.info('✓ contact_360 refreshed')
            except Exception as exc:
                _logger.debug('refresh_contact_360: %s', exc)

            stats = supa.sync_stats
            _logger.info(
                'Sync stats: %d success, %d failed, %d skipped',
                stats['success'], stats['failed'], stats['skipped'],
            )

        _logger.info('═══ ODOO → SUPABASE SYNC DONE (%d partners) ═══',
                     enriched)

    # ══════════════════════════════════════════════════════════════════════════
    #   DATA RETENTION
    # ══════════════════════════════════════════════════════════════════════════

    @api.model
    def run_data_retention(self):
        """Limpia datos antiguos de Odoo y Supabase."""
        _logger.info('═══ DATA RETENTION ═══')
        today = fields.Date.today()

        old_briefings = self.env['intelligence.briefing'].sudo().search([
            ('date', '<', today - timedelta(days=90)),
            ('state', '!=', 'archived'),
        ])
        if old_briefings:
            old_briefings.write({'state': 'archived'})
            _logger.info('Archived %d old briefings', len(old_briefings))

        old_scores = self.env['intelligence.client.score'].sudo().search([
            ('date', '<', today - timedelta(days=180)),
        ])
        if old_scores:
            count = len(old_scores)
            old_scores.unlink()
            _logger.info('Deleted %d old client scores', count)

        stale_actions = self.env['intelligence.action.item'].sudo().search([
            ('state', 'in', ['open', 'in_progress']),
            ('due_date', '<', today - timedelta(days=30)),
        ])
        if stale_actions:
            stale_actions.write({'state': 'cancelled'})
            _logger.info('Auto-cancelled %d stale actions', len(stale_actions))

        cfg = self._load_config()
        if not cfg:
            _logger.info('═══ DATA RETENTION DONE (Odoo only) ═══')
            return
        try:
            from ..services.supabase_service import SupabaseService
            with SupabaseService(cfg['supabase_url'], cfg['supabase_key']) as supa:
                cutoff_90d = (
                    datetime.now() - timedelta(days=90)
                ).strftime('%Y-%m-%d')
                cutoff_180d = (
                    datetime.now() - timedelta(days=180)
                ).strftime('%Y-%m-%d')

                supa._request(
                    '/rest/v1/alerts?is_resolved=eq.true'
                    f'&created_at=lt.{cutoff_90d}T00:00:00Z',
                    'DELETE',
                )
                supa._request(
                    '/rest/v1/facts?verified=eq.false'
                    f'&fact_date=lt.{cutoff_180d}',
                    'PATCH', {'expired': True},
                )

                decay_result = supa.decay_fact_confidence()
                _logger.info(
                    'Fact decay: %s decayed, %s expired',
                    decay_result.get('decayed', 0),
                    decay_result.get('expired', 0),
                )

                dedup_result = supa.auto_deduplicate_entities()
                if dedup_result.get('merged', 0):
                    _logger.info(
                        'Entity dedup: %d merged of %d candidates',
                        dedup_result['merged'],
                        dedup_result['candidates'],
                    )

                try:
                    supa._request(
                        '/rest/v1/rpc/cleanup_old_snapshots', 'POST',
                        {'p_days': 365},
                    )
                except Exception:
                    cutoff_365d = (
                        datetime.now() - timedelta(days=365)
                    ).strftime('%Y-%m-%d')
                    supa._request(
                        '/rest/v1/company_odoo_snapshots'
                        f'?snapshot_date=lt.{cutoff_365d}',
                        'DELETE',
                    )

                _logger.info('✓ Supabase data retention completed')
        except Exception as exc:
            _logger.warning('Supabase retention: %s', exc)

        _logger.info('═══ DATA RETENTION DONE ═══')

    # ══════════════════════════════════════════════════════════════════════════
    #   REPORTE SEMANAL
    # ══════════════════════════════════════════════════════════════════════════

    @api.model
    def run_weekly_analysis(self):
        """Reporte semanal con tendencias y comparativas."""
        _logger.info('═══ WEEKLY ANALYSIS ═══')
        cfg = self._load_config()
        if not cfg:
            return

        from ..services.analysis_service import AnalysisService
        from ..services.claude_service import ClaudeService
        from ..services.supabase_service import SupabaseService

        claude = ClaudeService(cfg['anthropic_api_key'])
        analysis = AnalysisService()

        with SupabaseService(cfg['supabase_url'], cfg['supabase_key']) as supa:
            week_start = (datetime.now(TZ_CDMX) - timedelta(days=7)).strftime('%Y-%m-%d')

            try:
                weekly_metrics = supa._request(
                    '/rest/v1/response_metrics?order=metric_date.desc&limit=70'
                    '&select=*&metric_date=gte.' + week_start,
                ) or []
            except Exception:
                weekly_metrics = []

            try:
                weekly_alerts = supa._request(
                    '/rest/v1/alerts?order=created_at.desc&limit=100'
                    '&select=*&created_at=gte.' + week_start,
                ) or []
            except Exception:
                weekly_alerts = []

            try:
                weekly_scores = supa._request(
                    '/rest/v1/customer_health_scores?order=score_date.desc'
                    '&limit=100&select=*&score_date=gte.' + week_start,
                ) or []
            except Exception:
                weekly_scores = []

            try:
                weekly_summaries = supa._request(
                    '/rest/v1/daily_summaries?order=summary_date.desc&limit=7'
                    '&select=summary_date,summary_text,total_emails'
                    '&summary_date=gte.' + week_start,
                ) or []
            except Exception:
                weekly_summaries = []

        if not weekly_metrics and not weekly_alerts:
            _logger.warning('Sin datos semanales')
            return

        alert_by_type = defaultdict(int)
        for a in weekly_alerts:
            alert_by_type[a.get('alert_type', 'unknown')] += 1

        prompt = (
            f'Genera un REPORTE SEMANAL de Quimibond (semana del {week_start}).\n\n'
            f'MÉTRICAS DIARIAS (últimos 7 días):\n'
            f'{json.dumps(weekly_metrics, default=str)}\n\n'
            f'ALERTAS DE LA SEMANA ({len(weekly_alerts)} total):\n'
            f'Por tipo: {json.dumps(dict(alert_by_type), default=str)}\n'
            f'Detalle: {json.dumps(weekly_alerts[:30], default=str)}\n\n'
            f'CLIENT SCORES:\n{json.dumps(weekly_scores[:30], default=str)}\n\n'
            f'RESÚMENES DIARIOS:\n{json.dumps(weekly_summaries, default=str)}\n\n'
            'ESTRUCTURA DEL REPORTE:\n'
            '<h2>📊 RESUMEN EJECUTIVO SEMANAL</h2>\n'
            '(3-5 bullets con lo más importante de la semana)\n\n'
            '<h2>📈 TENDENCIAS</h2>\n'
            '(Comparativa día a día: volumen, tiempos de respuesta, sentimiento. '
            '¿Están mejorando o empeorando? Usar datos concretos.)\n\n'
            '<h2>🏆 TOP 5 CONTACTOS MÁS ACTIVOS</h2>\n'
            '(Quiénes se comunicaron más y por qué)\n\n'
            '<h2>⚠️ TOP 3 RIESGOS</h2>\n'
            '(Los 3 riesgos principales basados en alertas, scores, y patrones)\n\n'
            '<h2>💡 OPORTUNIDADES DETECTADAS</h2>\n'
            '(Prospectos nuevos, señales de crecimiento, cross-sell)\n\n'
            '<h2>✅ ACCIONES: COMPLETADAS vs PENDIENTES</h2>\n'
            '(Tasa de cumplimiento del equipo, quién cumplió y quién no)\n\n'
            '<h2>🏭 COMPETENCIA</h2>\n'
            '(Competidores mencionados durante la semana, contexto, amenaza)\n\n'
            '<h2>📉 COMPARATIVA vs SEMANA ANTERIOR</h2>\n'
            '(Si hay datos suficientes, comparar KPIs clave)\n\n'
            '<h2>🎯 PRIORIDADES PARA LA PRÓXIMA SEMANA</h2>\n'
            '(Acciones concretas y específicas)\n\n'
            'Sé directo y honesto. Usa datos concretos, no generalidades.'
        )

        try:
            weekly_html = claude.synthesize_briefing(prompt)
            today = datetime.now(TZ_CDMX).strftime('%Y-%m-%d')
            sender = cfg.get('sender_email')
            recipient = cfg.get('recipient_email')

            if sender and recipient:
                from ..services.gmail_service import GmailService
                sa_info = json.loads(cfg['service_account_json'])
                gmail = GmailService(sa_info)
                gmail.send_email(
                    sender, recipient,
                    f'Weekly Intelligence Report — {today}',
                    analysis.wrap_briefing_html(weekly_html, today, weekly=True),
                )
                _logger.info('Reporte semanal enviado a %s', recipient)
            else:
                _logger.warning('Reporte semanal no enviado: falta sender/recipient en config')
        except Exception as exc:
            _logger.error('Error en reporte semanal: %s', exc)

    # ══════════════════════════════════════════════════════════════════════════
    #   HELPERS INTERNOS
    # ══════════════════════════════════════════════════════════════════════════

    def _load_gmail_history_state(self) -> dict:
        raw = (
            self.env['ir.config_parameter'].sudo()
            .get_param('quimibond_intelligence.gmail_history_state', '{}')
        )
        try:
            data = json.loads(raw) if raw else {}
            return data if isinstance(data, dict) else {}
        except json.JSONDecodeError:
            return {}

    def _save_gmail_history_state(self, state: dict):
        if not state:
            return
        self.env['ir.config_parameter'].sudo().set_param(
            'quimibond_intelligence.gmail_history_state',
            json.dumps(state),
        )

    def _load_config(self):
        """Carga toda la configuración desde ir.config_parameter."""
        get = lambda k, d='': (
            self.env['ir.config_parameter'].sudo()
            .get_param(f'quimibond_intelligence.{k}', d)
        )
        sa_json = get('service_account_json')
        anthropic_key = get('anthropic_api_key')
        supa_url = get('supabase_url')
        supa_key = get('supabase_key')
        supa_service_key = get('supabase_service_role_key')

        missing = []
        if not sa_json:
            missing.append('service_account_json')
        if not anthropic_key:
            missing.append('anthropic_api_key')
        if not supa_url:
            missing.append('supabase_url')
        if not supa_key and not supa_service_key:
            missing.append('supabase_key')
        if missing:
            _logger.error('Faltan API keys en ir.config_parameter: %s. '
                          'Configura desde Ajustes > Intelligence System.',
                          ', '.join(missing))
            return None

        return {
            'service_account_json': sa_json,
            'anthropic_api_key': anthropic_key,
            'supabase_url': supa_url,
            'supabase_key': supa_service_key or supa_key,
            'voyage_api_key': get('voyage_api_key'),
            'recipient_email': get('recipient_email'),
            'sender_email': get('sender_email'),
            'target_response_hours': int(get('target_response_hours', '4')),
            'slow_response_hours': int(get('slow_response_hours', '8')),
            'no_response_hours': int(get('no_response_hours', '24')),
            'stalled_thread_hours': int(get('stalled_thread_hours', '48')),
            'high_volume_threshold': int(get('high_volume_threshold', '50')),
            'client_score_decay_days': int(get('client_score_decay_days', '30')),
            'cold_client_days': int(get('cold_client_days', '14')),
        }

    def _init_services(self, cfg: dict):
        """Instancia los cuatro servicios externos."""
        from ..services.claude_service import ClaudeService, VoyageService
        from ..services.gmail_service import GmailService
        from ..services.supabase_service import SupabaseService

        sa_info = json.loads(cfg['service_account_json'])
        gmail = GmailService(sa_info)
        claude = ClaudeService(cfg['anthropic_api_key'])
        voyage = (VoyageService(cfg['voyage_api_key'])
                  if cfg.get('voyage_api_key') else None)
        supa = SupabaseService(cfg['supabase_url'], cfg['supabase_key'])

        return gmail, claude, voyage, supa

    # ── Deduplicación ─────────────────────────────────────────────────────────

    @staticmethod
    def _deduplicate(emails: list) -> list:
        """Elimina duplicados por fingerprint."""
        seen = set()
        unique = []
        for e in emails:
            try:
                date_str = e.get('date', '')
                date_minute = re.sub(r':\d{2}\s', ' ', date_str)[:16]
            except Exception:
                date_minute = ''

            fp = f"{e.get('from_email', '')}|{e.get('subject_normalized', '')}|{date_minute}"
            if fp not in seen:
                seen.add(fp)
                unique.append(e)
        return unique

    # ── Construcción de threads ───────────────────────────────────────────────

    @staticmethod
    def _build_threads(emails: list, cfg: dict) -> list:
        """Agrupa emails en threads por gmail_thread_id."""
        thread_map = defaultdict(list)
        for e in emails:
            tid = e.get('gmail_thread_id', e.get('subject_normalized', ''))
            thread_map[tid].append(e)

        now = datetime.now(timezone.utc)
        threads = []
        for tid, msgs in thread_map.items():
            msgs.sort(key=lambda m: m.get('date', ''))
            first = msgs[0]
            last = msgs[-1]

            participant_emails = list({
                m.get('from_email', '') for m in msgs if m.get('from_email')
            })
            has_internal = any(m['sender_type'] == 'internal' for m in msgs)
            has_external = any(m['sender_type'] == 'external' for m in msgs)

            def _parse_date(raw: str) -> str:
                if not raw:
                    return datetime.now(timezone.utc).isoformat()
                try:
                    return parsedate_to_datetime(raw).isoformat()
                except Exception:
                    try:
                        return datetime.fromisoformat(
                            raw.replace('Z', '+00:00')
                        ).isoformat()
                    except Exception:
                        return datetime.now(timezone.utc).isoformat()

            started_at_iso = _parse_date(first.get('date', ''))
            last_activity_iso = _parse_date(last.get('date', ''))

            hours_no_response = 0
            if last['sender_type'] == 'external':
                try:
                    last_date = datetime.fromisoformat(last_activity_iso)
                    hours_no_response = (now - last_date).total_seconds() / 3600
                except Exception:
                    pass

            no_resp_hours = cfg.get('no_response_hours', 24)
            stalled_hours = cfg.get('stalled_thread_hours', 48)
            if hours_no_response > stalled_hours:
                status = 'stalled'
            elif hours_no_response > no_resp_hours:
                status = 'needs_response'
            elif len(msgs) == 1:
                status = 'new'
            else:
                status = 'active'

            threads.append({
                'gmail_thread_id': tid,
                'subject': first.get('subject', ''),
                'subject_normalized': first.get('subject_normalized', ''),
                'started_by': first.get('from_email', ''),
                'started_by_type': first.get('sender_type', ''),
                'started_at': started_at_iso,
                'last_activity': last_activity_iso,
                'status': status,
                'message_count': len(msgs),
                'participant_emails': participant_emails,
                'has_internal_reply': has_internal,
                'has_external_reply': has_external,
                'last_sender': last.get('from_email', ''),
                'last_sender_type': last.get('sender_type', ''),
                'hours_without_response': round(hours_no_response, 1),
                'account': first.get('account', ''),
            })
        return threads

    # ── Análisis por cuenta ───────────────────────────────────────────────────

    def _analyze_accounts(self, emails: list, claude, odoo_context: dict,
                          account_departments: dict = None,
                          supa=None) -> list:
        """Fase 1 de Claude: análisis por cuenta con perfiles de personas."""
        from ..services.analysis_service import AnalysisService
        analysis = AnalysisService()
        account_departments = account_departments or {}

        person_profiles = {}

        by_account = defaultdict(list)
        for e in emails:
            by_account[e['account']].append(e)

        summaries = []
        for account, acct_emails in by_account.items():
            dept = account_departments.get(account, 'Otro')
            ext_count = sum(
                1 for e in acct_emails if e['sender_type'] == 'external'
            )
            int_count = len(acct_emails) - ext_count

            if not acct_emails:
                continue

            email_text = analysis.format_emails_for_claude(
                acct_emails, odoo_context, person_profiles,
            )

            try:
                result = claude.summarize_account(
                    dept, account, email_text, ext_count, int_count,
                )
                result['account'] = account
                result['department'] = dept
                result['total_emails'] = len(acct_emails)
                summaries.append(result)

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

                _logger.info('  ✓ %s (%s): %d emails analizados',
                             dept, account, len(acct_emails))
            except Exception as exc:
                _logger.error('  ✗ %s: %s', account, exc)

        return summaries

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

    # ── Build rich odoo_context for Supabase ────────────────────────────────

    @staticmethod
    def _build_contact_odoo_context(p: dict, lifetime: dict, aging: dict,
                                     deliv: dict, today: str) -> dict:
        """Construye odoo_context completo con todo el detalle operacional."""
        return {
            'name': p.get('name', ''),
            'odoo_partner_id': p.get('id'),
            'is_company': p.get('is_company', False),
            'company_name': p.get('company_name', ''),
            'synced_at': today,
            'total_invoiced': p.get('total_invoiced', 0),
            'credit_limit': p.get('credit_limit', 0),
            'lifetime': lifetime or {},
            'aging': aging or {},
            'recent_sales': p.get('recent_sales', []),
            'pending_invoices': p.get('pending_invoices', []),
            'recent_purchases': p.get('recent_purchases', []),
            'recent_payments': p.get('recent_payments', []),
            'products': p.get('products', []),
            'purchase_patterns': p.get('purchase_patterns', {}),
            'inventory_intelligence': p.get('inventory_intelligence', {}),
            'payment_behavior': p.get('payment_behavior', {}),
            'crm_leads': p.get('crm_leads', []),
            'pending_deliveries': p.get('pending_deliveries', []),
            'pending_activities': p.get('pending_activities', []),
            'manufacturing': p.get('manufacturing', []),
            'upcoming_meetings': p.get('upcoming_meetings', []),
            'credit_notes': p.get('credit_notes', []),
            'delivery_performance': deliv or {},
            'related_contacts': p.get('related_contacts', []),
            'recent_chatter': p.get('recent_chatter', []),
            'related_chatter': p.get('related_chatter', []),
            'summary': p.get('_summary', ''),
        }

    @staticmethod
    def _build_company_odoo_context(company_name: str, partners: dict,
                                     today: str) -> dict:
        """Agrega datos operacionales a nivel empresa desde todos sus contactos."""
        all_sales = []
        all_invoices = []
        all_products = {}
        all_deliveries = []
        all_leads = []
        all_manufacturing = []
        all_payments = []
        all_volume_drops = []
        all_discount_anomalies = []
        all_cross_sell = []
        all_inventory_at_risk = []
        contact_emails = []
        total_invoiced = 0
        total_pending = 0
        total_revenue_12m = 0

        for email_addr, p in partners.items():
            if p.get('company_name', '') != company_name:
                continue
            contact_emails.append(email_addr)
            total_invoiced += p.get('total_invoiced', 0)

            for s in p.get('recent_sales', []):
                all_sales.append({**s, '_contact': email_addr})
            for inv in p.get('pending_invoices', []):
                all_invoices.append({**inv, '_contact': email_addr})
                total_pending += inv.get('amount_residual', 0)
            for prod in p.get('products', []):
                pname = prod.get('name', '')
                if pname and pname not in all_products:
                    all_products[pname] = prod
            for d in p.get('pending_deliveries', []):
                all_deliveries.append({**d, '_contact': email_addr})
            for lead in p.get('crm_leads', []):
                all_leads.append(lead)
            for mo in p.get('manufacturing', []):
                all_manufacturing.append(mo)
            for pay in p.get('recent_payments', []):
                all_payments.append({**pay, '_contact': email_addr})

            # Aggregate purchase patterns
            patterns = p.get('purchase_patterns', {})
            total_revenue_12m += patterns.get('total_revenue_12m', 0)
            for vd in patterns.get('volume_drops', []):
                all_volume_drops.append({**vd, '_contact': email_addr})
            for da in patterns.get('discount_anomalies', []):
                all_discount_anomalies.append({**da, '_contact': email_addr})
            for cs in patterns.get('cross_sell', []):
                if cs.get('product') not in [
                    x.get('product') for x in all_cross_sell
                ]:
                    all_cross_sell.append(cs)

            # Aggregate inventory at-risk items (dedup by product)
            inv_intel = p.get('inventory_intelligence', {})
            for ar in inv_intel.get('at_risk', []):
                if ar.get('product') not in [
                    x.get('product') for x in all_inventory_at_risk
                ]:
                    all_inventory_at_risk.append(ar)

        all_sales.sort(key=lambda x: x.get('date', ''), reverse=True)
        all_invoices.sort(
            key=lambda x: x.get('days_overdue', 0), reverse=True)

        return {
            'synced_at': today,
            'contact_count': len(contact_emails),
            'contact_emails': contact_emails[:20],
            'total_invoiced': total_invoiced,
            'total_pending': total_pending,
            'recent_sales': all_sales[:20],
            'pending_invoices': all_invoices[:20],
            'products': list(all_products.values())[:15],
            'pending_deliveries': all_deliveries[:15],
            'crm_leads': all_leads[:10],
            'manufacturing': all_manufacturing[:10],
            'recent_payments': all_payments[:15],
            'sales_count_90d': len(all_sales),
            'pending_invoices_count': len(all_invoices),
            'pending_deliveries_count': len(all_deliveries),
            'crm_pipeline_value': sum(
                l.get('expected_revenue', 0) for l in all_leads),
            'crm_leads_count': len(all_leads),
            'manufacturing_count': len(all_manufacturing),
            'late_deliveries_count': sum(
                1 for d in all_deliveries if d.get('is_late')),
            'overdue_invoices_count': sum(
                1 for inv in all_invoices
                if inv.get('days_overdue', 0) > 0),
            'purchase_patterns': {
                'total_revenue_12m': round(total_revenue_12m, 2),
                'volume_drops': all_volume_drops[:10],
                'discount_anomalies': all_discount_anomalies[:10],
                'cross_sell': all_cross_sell[:5],
            },
            'inventory_at_risk': all_inventory_at_risk[:10],
        }

    # ── Sync Odoo → Supabase contacts ──────────────────────────────────────

    @staticmethod
    def _sync_contacts_to_supabase(odoo_ctx: dict, supa, today: str = None):
        """Sincroniza datos de Odoo partners a Supabase contacts + companies + revenue."""
        partners = odoo_ctx.get('partners', {})
        if not partners:
            return

        all_company_names = list({
            p.get('company_name', '') for p in partners.values()
            if p.get('company_name')
        })
        company_cache = supa.batch_resolve_companies(all_company_names)

        # Phase 1: Aggregate company-level data
        company_data = {}
        for email_addr, p in partners.items():
            company_name = p.get('company_name', '')
            if not company_name:
                continue
            if company_name not in company_data:
                company_data[company_name] = {
                    'lifetime_value': 0,
                    'total_credit_notes': 0,
                    'is_customer': False,
                    'is_supplier': False,
                    'odoo_partner_id': None,
                    'credit_limit': 0,
                    'total_pending': 0,
                    'monthly_avg': 0,
                    'trend_pct': None,
                    'delivery_otd_rate': None,
                }
            cd = company_data[company_name]
            lifetime = p.get('lifetime', {})
            deliv = p.get('delivery_performance', {})
            cd['lifetime_value'] += lifetime.get(
                'total_invoiced', p.get('total_invoiced', 0))
            cd['total_credit_notes'] += sum(
                cn.get('amount', 0) for cn in p.get('credit_notes', []))
            cd['is_customer'] = cd['is_customer'] or p.get('is_customer', False)
            cd['is_supplier'] = cd['is_supplier'] or p.get('is_supplier', False)
            cd['total_pending'] += sum(
                inv.get('amount_residual', 0)
                for inv in p.get('pending_invoices', []))
            cd['monthly_avg'] += lifetime.get('monthly_avg', 0)
            if p.get('is_company'):
                cd['odoo_partner_id'] = p.get('id')
                cd['credit_limit'] = p.get('credit_limit', 0)
            if lifetime.get('trend_pct') is not None:
                cd['trend_pct'] = lifetime['trend_pct']
            if deliv.get('on_time_rate') is not None:
                cd['delivery_otd_rate'] = deliv['on_time_rate']

        snapshot_batch = []
        for company_name, cd in company_data.items():
            try:
                company_id = supa._resolve_or_create_company(
                    company_name, cd, _cache=company_cache,
                )
                if not company_id:
                    continue

                co_ctx = IntelligenceEngine._build_company_odoo_context(
                    company_name, partners, today or '',
                )

                patch_data = {
                    'lifetime_value': cd['lifetime_value'],
                    'total_credit_notes': cd['total_credit_notes'],
                    'is_customer': cd['is_customer'],
                    'is_supplier': cd['is_supplier'],
                    'credit_limit': cd['credit_limit'],
                    'total_pending': cd['total_pending'],
                    'monthly_avg': cd['monthly_avg'],
                    'trend_pct': cd['trend_pct'],
                    'delivery_otd_rate': cd['delivery_otd_rate'],
                    'odoo_context': co_ctx,
                }
                if cd.get('odoo_partner_id'):
                    patch_data['odoo_partner_id'] = cd['odoo_partner_id']

                supa.sync_company_odoo_data(company_id, patch_data)

                if today:
                    snapshot_batch.append({
                        'company_id': company_id,
                        'snapshot_date': today,
                        'total_invoiced': cd['lifetime_value'],
                        'pending_amount': cd['total_pending'],
                        'overdue_amount': sum(
                            inv.get('amount_residual', 0)
                            for inv in co_ctx.get('pending_invoices', [])
                            if inv.get('days_overdue', 0) > 0
                        ),
                        'monthly_avg': cd['monthly_avg'],
                        'open_orders_count': co_ctx.get(
                            'sales_count_90d', 0),
                        'pending_deliveries_count': co_ctx.get(
                            'pending_deliveries_count', 0),
                        'late_deliveries_count': co_ctx.get(
                            'late_deliveries_count', 0),
                        'crm_pipeline_value': co_ctx.get(
                            'crm_pipeline_value', 0),
                        'crm_leads_count': co_ctx.get(
                            'crm_leads_count', 0),
                        'manufacturing_count': co_ctx.get(
                            'manufacturing_count', 0),
                        'credit_notes_total': cd['total_credit_notes'],
                    })
            except Exception as exc:
                _logger.debug('Company sync %s: %s', company_name, exc)

        if snapshot_batch:
            try:
                supa.save_company_snapshots(snapshot_batch)
            except Exception as exc:
                _logger.warning('Company snapshots: %s', exc)

        # Phase 2: Sync individual contacts
        synced = 0
        revenue_batch = []
        for email_addr, p in partners.items():
            try:
                lifetime = p.get('lifetime', {})
                aging = p.get('aging', {})
                deliv = p.get('delivery_performance', {})
                cn_total = sum(
                    cn.get('amount', 0)
                    for cn in p.get('credit_notes', [])
                )
                # NOTE: self is not available in @staticmethod, use class ref
                contact_ctx = IntelligenceEngine._build_contact_odoo_context(
                    p, lifetime, aging, deliv, today,
                )
                supa.sync_contact_odoo_data(email_addr, {
                    'odoo_partner_id': p.get('id'),
                    'is_customer': p.get('is_customer', False),
                    'is_supplier': p.get('is_supplier', False),
                    'company': p.get('company_name', ''),
                    'lifetime_value': lifetime.get(
                        'total_invoiced', p.get('total_invoiced', 0)),
                    'total_credit_notes': cn_total,
                    'delivery_otd_rate': deliv.get('on_time_rate'),
                    'odoo_context': contact_ctx,
                }, _company_cache=company_cache)
                synced += 1

                if p.get('is_customer') and today:
                    try:
                        recent_sales = p.get('recent_sales', [])
                        pending_invoices = p.get('pending_invoices', [])
                        overdue = [
                            inv for inv in pending_invoices
                            if inv.get('days_overdue', 0) > 0
                        ]
                        # Sum inbound payments as total_collected
                        total_collected = sum(
                            pay.get('amount', 0)
                            for pay in p.get('recent_payments', [])
                            if pay.get('payment_type') == 'inbound'
                        )
                        revenue_batch.append({
                            'contact_email': email_addr,
                            'period_start': today[:8] + '01',
                            'period_end': today,
                            'period_type': 'monthly',
                            'total_invoiced': p.get('total_invoiced', 0),
                            'pending_amount': sum(
                                inv.get('amount_residual', 0)
                                for inv in pending_invoices
                            ),
                            'overdue_amount': sum(
                                inv.get('amount_residual', 0)
                                for inv in overdue
                            ),
                            'overdue_days_max': max(
                                (inv.get('days_overdue', 0) for inv in overdue),
                                default=0,
                            ),
                            'num_orders': len(recent_sales),
                            'avg_order_value': (
                                sum(s.get('amount', 0) for s in recent_sales)
                                / len(recent_sales)
                                if recent_sales else 0
                            ),
                            'odoo_partner_id': p.get('id'),
                            'total_collected': total_collected,
                        })
                    except Exception as exc:
                        _logger.debug('revenue_metrics skip %s: %s',
                                      email_addr, exc)
            except Exception as exc:
                _logger.warning('Contact sync error %s: %s', email_addr, exc)
        if synced:
            _logger.info('✓ %d contactos sincronizados Odoo → Supabase', synced)

        if revenue_batch:
            try:
                supa.save_revenue_metrics_batch(revenue_batch)
            except Exception as exc:
                _logger.warning('Batch revenue metrics: %s', exc)

    # ── Company Enrichment with Claude ──────────────────────────────────────

    @staticmethod
    def _enrich_companies(supa, claude, today: str):
        """Enriquece empresas sin perfil usando Claude."""
        companies = supa.get_companies_needing_enrichment(limit=10)
        if not companies:
            _logger.info('No hay empresas pendientes de enriquecimiento')
            return

        company_ids = [co['id'] for co in companies]
        all_contacts_map = {}
        try:
            cid_list = ','.join(str(cid) for cid in company_ids)
            all_contacts = supa._request(
                f'/rest/v1/contacts?company_id=in.({cid_list})'
                '&select=id,email,name,role,decision_power,'
                'relationship_score,risk_level,company_id'
                '&order=name',
            ) or []
            for ct in all_contacts:
                cid = ct.get('company_id')
                if cid:
                    all_contacts_map.setdefault(cid, []).append(ct)
        except Exception as exc:
            _logger.debug('batch contacts for enrichment: %s', exc)

        all_contact_emails = []
        for cts in all_contacts_map.values():
            for ct in cts[:5]:
                if ct.get('email'):
                    all_contact_emails.append(ct['email'])
        emails_by_sender = {}
        if all_contact_emails:
            try:
                from urllib.parse import quote as _q
                enc = ','.join(
                    f'"{_q(e, safe="")}"' for e in all_contact_emails[:50]
                )
                if enc:
                    sample_emails = supa._request(
                        f'/rest/v1/emails?sender=in.({enc})'
                        '&order=email_date.desc&limit=100'
                        '&select=subject,snippet,sender',
                    ) or []
                    for e in sample_emails:
                        sender = (e.get('sender') or '').lower()
                        if sender:
                            emails_by_sender.setdefault(sender, []).append(e)
            except Exception as exc:
                _logger.debug('batch emails for enrichment: %s', exc)

        enriched = 0
        for co in companies:
            company_id = co['id']
            company_name = co['name']

            try:
                context_parts = [f'EMPRESA: {company_name}']

                if co.get('domain'):
                    context_parts.append(f'DOMINIO: {co["domain"]}')
                if co.get('is_customer'):
                    context_parts.append('ES CLIENTE de Quimibond')
                if co.get('is_supplier'):
                    context_parts.append('ES PROVEEDOR de Quimibond')

                odoo_ctx = co.get('odoo_context') or {}
                if odoo_ctx and any(odoo_ctx.values()):
                    ctx_items = []
                    if odoo_ctx.get('total_invoiced'):
                        ctx_items.append(
                            f'Facturado total: ${odoo_ctx["total_invoiced"]:,.0f}')
                    if odoo_ctx.get('monthly_avg'):
                        ctx_items.append(
                            f'Promedio mensual: ${odoo_ctx["monthly_avg"]:,.0f}')
                    if ctx_items:
                        context_parts.append(
                            'DATOS ODOO:\n' + '\n'.join(ctx_items))

                if co.get('entity_id'):
                    try:
                        entity_data = supa.get_entity_intelligence(
                            name=company_name,
                            entity_type='company',
                        )
                        if entity_data and entity_data.get('found'):
                            attrs = entity_data.get('entity', {}).get(
                                'attributes', {})
                            if attrs:
                                context_parts.append(
                                    f'KNOWLEDGE GRAPH:\n{json.dumps(attrs, ensure_ascii=False)}')
                            facts = entity_data.get('facts', [])
                            if facts:
                                fact_texts = [
                                    f.get('text') or f.get('fact_text', '')
                                    for f in facts[:10]
                                ]
                                context_parts.append(
                                    'HECHOS CONOCIDOS:\n'
                                    + '\n'.join(f'- {ft}' for ft in fact_texts if ft))
                    except Exception:
                        pass

                contacts = all_contacts_map.get(company_id, [])
                if contacts:
                    contact_info = []
                    for ct in contacts[:10]:
                        role = ct.get('role') or ''
                        dp = ct.get('decision_power') or ''
                        contact_info.append(
                            f'  - {ct.get("name", "")} ({role}, {dp}): '
                            f'{ct.get("email", "")}')
                    context_parts.append(
                        f'CONTACTOS ({len(contacts)}):\n'
                        + '\n'.join(contact_info))

                    email_lines = []
                    for ct in contacts[:5]:
                        ct_email = (ct.get('email') or '').lower()
                        for e in emails_by_sender.get(ct_email, [])[:3]:
                            email_lines.append(
                                f'  - {e.get("subject", "")}: '
                                f'{(e.get("snippet") or "")[:80]}')
                    if email_lines:
                        context_parts.append(
                            'EMAILS RECIENTES:\n'
                            + '\n'.join(email_lines[:10]))

                context = '\n\n'.join(context_parts)

                profile = claude.profile_company(company_name, context)
                if profile:
                    supa.save_company_profile(company_id, profile)
                    enriched += 1
                    _logger.info('  ✓ Empresa enriquecida: %s', company_name)
                else:
                    _logger.warning('  ✗ Claude no generó perfil para: %s',
                                    company_name)

            except Exception as exc:
                _logger.warning('Enrich company %s: %s', company_name, exc,
                                exc_info=True)

        if enriched:
            _logger.info('✓ %d empresas enriquecidas con Claude', enriched)

    # ── Link Odoo IDs to contacts + entities ────────────────────────────────

    def _link_odoo_ids(self, supa):
        """Vincula contactos y entities del Knowledge Graph con IDs de Odoo."""
        Partner = self.env['res.partner'].sudo()
        try:
            Users = self.env['res.users'].sudo()
        except KeyError:
            Users = None

        # 1. Internal contacts
        try:
            internals = supa._request(
                '/rest/v1/contacts?contact_type=eq.internal'
                '&odoo_partner_id=is.null'
                '&select=email,name',
            ) or []
        except Exception as exc:
            _logger.warning('Load internal contacts: %s', exc)
            internals = []

        linked_contacts = 0
        for c in internals:
            email = c.get('email', '')
            if not email:
                continue
            try:
                partner = Partner.search(
                    [('email', '=ilike', email)], limit=1,
                )
                if not partner:
                    continue
                update = {'odoo_partner_id': partner.id}
                if Users:
                    user = Users.search(
                        [('partner_id', '=', partner.id)], limit=1,
                    )
                    if user:
                        update['odoo_context'] = {
                            'odoo_user_id': user.id,
                            'name': partner.name,
                            'login': user.login,
                        }
                supa.sync_contact_odoo_data(email, update)
                linked_contacts += 1
            except Exception as exc:
                _logger.debug('Link internal %s: %s', email, exc)

        if linked_contacts:
            _logger.info('✓ %d contactos internos vinculados a Odoo',
                         linked_contacts)

        # 2. External contacts
        try:
            externals = supa._request(
                '/rest/v1/contacts?contact_type=eq.external'
                '&odoo_partner_id=is.null'
                '&select=email,name',
            ) or []
        except Exception as exc:
            _logger.warning('Load external contacts: %s', exc)
            externals = []

        linked_external = 0
        for c in externals:
            email = c.get('email', '')
            if not email:
                continue
            try:
                partner = Partner.search(
                    [('email', '=ilike', email)], limit=1,
                )
                if not partner and c.get('name'):
                    partner = Partner.search(
                        [('name', '=ilike', c['name']),
                         ('is_company', '=', False)],
                        limit=1,
                    )
                if partner:
                    supa.sync_contact_odoo_data(email, {
                        'odoo_partner_id': partner.id,
                    })
                    linked_external += 1
            except Exception as exc:
                _logger.debug('Link external %s: %s', email, exc)

        if linked_external:
            _logger.info('✓ %d contactos externos vinculados a Odoo',
                         linked_external)

        # 3. Entities
        try:
            entities = supa._request(
                '/rest/v1/entities?odoo_id=is.null'
                '&entity_type=in.(person,company)'
                '&select=id,name,entity_type,email',
            ) or []
        except Exception as exc:
            _logger.warning('Load entities: %s', exc)
            entities = []

        linked_entities = 0
        for ent in entities:
            try:
                partner = None
                ent_email = ent.get('email', '')
                ent_name = ent.get('name', '')
                ent_type = ent.get('entity_type', '')

                if ent_email:
                    partner = Partner.search(
                        [('email', '=ilike', ent_email)], limit=1,
                    )

                if not partner and ent_type == 'company' and ent_name:
                    partner = Partner.search(
                        [('name', '=ilike', ent_name),
                         ('is_company', '=', True)],
                        limit=1,
                    )
                    if not partner:
                        partner = Partner.search(
                            [('name', '=ilike', f'%{ent_name}%'),
                             ('is_company', '=', True)],
                            limit=1,
                        )

                if not partner and ent_type == 'person' and ent_name:
                    partner = Partner.search(
                        [('name', '=ilike', ent_name),
                         ('is_company', '=', False)],
                        limit=1,
                    )

                if partner:
                    supa._request(
                        f'/rest/v1/entities?id=eq.{ent["id"]}',
                        'PATCH',
                        {
                            'odoo_model': 'res.partner',
                            'odoo_id': partner.id,
                        },
                        extra_headers={'Prefer': 'return=minimal'},
                    )
                    linked_entities += 1
            except Exception as exc:
                _logger.debug('Link entity %s: %s', ent.get('name'), exc)

        if linked_entities:
            _logger.info('✓ %d entities vinculadas a Odoo', linked_entities)

        # 4. Resolve all identities
        try:
            result = supa._request(
                '/rest/v1/rpc/resolve_all_identities', 'POST', {},
            )
            if result:
                _logger.info('✓ Identity resolution: %s', result)
        except Exception as exc:
            _logger.warning('resolve_all_identities: %s', exc)

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

    # ── Knowledge Graph ──────────────────────────────────────────────────────

    def _feed_knowledge_graph(self, emails, claude, supa, today):
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

        by_account = defaultdict(list)
        for e in pending:
            by_account[e['account']].append(e)

        ActionItem = self.env['intelligence.action.item'].sudo()
        Partner = self.env['res.partner'].sudo()

        for account, acct_emails in by_account.items():
            if not acct_emails:
                continue
            email_text = analysis.format_emails_for_claude(acct_emails, {})
            try:
                kg = claude.extract_knowledge(email_text, account)
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
                    assignee_ent = supa.get_entity_by_name(item.get('assignee', ''))
                    related_ent = supa.get_entity_by_name(item.get('related_to', ''))
                    result = supa.save_action_item({
                        'assignee_entity_id': assignee_ent.get('id') if assignee_ent else None,
                        'assignee_name': item.get('assignee', ''),
                        'related_entity_id': related_ent.get('id') if related_ent else None,
                        'description': item.get('description', ''),
                        'action_type': item.get('type', 'other'),
                        'priority': item.get('priority', 'medium'),
                        'due_date': item.get('due_date'),
                        'source_briefing_date': today,
                        'contact_name': item.get('related_to', ''),
                    })
                    supa_id = (
                        result[0].get('id')
                        if result and isinstance(result, list)
                        else False
                    )
                    partner = False
                    related = item.get('related_to', '')
                    if related:
                        partner = Partner.search([
                            '|', ('name', 'ilike', related),
                            ('email', 'ilike', related),
                        ], limit=1)
                    ActionItem.create({
                        'name': item.get('description', '')[:200],
                        'action_type': item.get('type', 'other'),
                        'priority': item.get('priority', 'medium'),
                        'due_date': item.get('due_date') or False,
                        'partner_id': partner.id if partner else False,
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

    # ── Save to Odoo ─────────────────────────────────────────────────────────

    def _save_to_odoo(self, today, briefing_html, emails, alerts,
                      client_scores, contacts, execution_secs,
                      topics=None, accounts_failed=0):
        Briefing = self.env["intelligence.briefing"].sudo()
        Alert = self.env["intelligence.alert"].sudo()
        Score = self.env["intelligence.client.score"].sudo()
        Partner = self.env["res.partner"].sudo()

        briefing = Briefing.create({
            'date': today,
            'briefing_type': 'daily',
            'html_content': briefing_html,
            'total_emails': len(emails),
            'accounts_ok': len(set(e['account'] for e in emails)),
            'accounts_failed': accounts_failed,
            'topics_count': len(topics) if topics else 0,
            'topics_json': json.dumps(topics, default=str, ensure_ascii=False) if topics else False,
            'execution_seconds': execution_secs,
        })
        _logger.info('Briefing guardado en Odoo: %s', briefing.id)

        for a in alerts:
            partner = False
            contact_name = a.get('contact_name', '')
            if contact_name:
                partner = Partner.search([
                    '|', ('name', 'ilike', contact_name),
                    ('email', 'ilike', contact_name),
                ], limit=1)
            Alert.create({
                'name': a.get('title', 'Alerta')[:200],
                'alert_type': a.get('alert_type', 'anomaly'),
                'severity': a.get('severity', 'medium'),
                'state': 'open',
                'description': a.get('description', ''),
                'account': a.get('account', ''),
                'partner_id': partner.id if partner else False,
                'briefing_id': briefing.id,
                'gmail_thread_id': a.get('related_thread_id', ''),
                'supabase_id': a.get('supabase_id', False),
            })
        _logger.info('%d alertas guardadas en Odoo', len(alerts))

        for s in client_scores:
            email_addr = s.get('email', '')
            partner = Partner.search([
                ('email', '=ilike', email_addr)
            ], limit=1)
            if partner:
                Score.create({
                    'partner_id': partner.id,
                    'date': today,
                    'email': email_addr,
                    'total_score': s.get('total_score', 0),
                    'frequency_score': s.get('frequency_score', 0),
                    'responsiveness_score': s.get('responsiveness_score', 0),
                    'reciprocity_score': s.get('reciprocity_score', 0),
                    'sentiment_score': s.get('sentiment_score', 0),
                    'payment_compliance_score': s.get(
                        'payment_compliance_score', 0),
                    'risk_level': s.get('risk_level', 'medium'),
                })
        _logger.info('%d client scores en Odoo', len(client_scores))

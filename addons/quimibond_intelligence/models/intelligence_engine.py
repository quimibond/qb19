"""
Quimibond Intelligence — Motor Principal (Orquestador)
Solo contiene el pipeline diario y helpers de configuración.
Los micro-pipelines se encuentran en:
- engine_email_sync.py: run_sync_emails
- engine_analysis.py: run_analyze_emails
- engine_enrichment.py: run_enrich_only, run_update_scores
- engine_reporting.py: run_data_retention, run_weekly_analysis
"""
import json
import logging
import time
from datetime import datetime

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
    #   HELPERS INTERNOS
    # ══════════════════════════════════════════════════════════════════════════

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

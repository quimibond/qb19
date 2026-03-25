"""
Quimibond Intelligence — Motor Principal (Orquestador)

Orquestador liviano que delega a micro-pipelines independientes.
Cada micro-pipeline corre por su propio cron Y es llamado por el daily.
El daily solo agrega: síntesis de briefing + envío de email + save to Odoo.

Micro-pipelines:
- engine_email_sync.py: run_sync_emails
- engine_analysis.py: run_analyze_emails
- engine_enrichment.py: run_enrich_only, run_update_scores
- engine_reporting.py: run_data_retention, run_weekly_analysis
- engine_supabase_sync.py: run_supabase_sync
"""
import json
import logging
import time
from datetime import datetime

from odoo import api, fields, models

from .intelligence_config import TZ_CDMX, acquire_lock, release_lock

_logger = logging.getLogger(__name__)


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
        """Método invocado por ir.cron cada día a las 19:00 CDMX.

        Orquestador liviano: llama cada micro-pipeline en secuencia.
        Si uno falla, continúa con el siguiente. Al final genera y envía
        el briefing diario (la única pieza exclusiva del daily).
        """
        lock_param = 'quimibond_intelligence.pipeline_running'
        if not acquire_lock(self.env, lock_param):
            _logger.warning('Pipeline ya está corriendo — abortando')
            return

        start = time.time()
        today = datetime.now(TZ_CDMX).strftime('%Y-%m-%d')
        _logger.info('═══ QUIMIBOND INTELLIGENCE — %s ═══', today)

        try:
            # ── Micro-pipelines (cada uno maneja sus propios errores) ──
            pipelines = [
                ('Sync emails', self.run_sync_emails),
                ('Analyze emails', self.run_analyze_emails),
                ('Enrich contacts', self.run_enrich_only),
                ('Update scores', self.run_update_scores),
            ]
            for name, fn in pipelines:
                try:
                    _logger.info('── %s ──', name)
                    fn()
                except Exception as exc:
                    _logger.error('%s falló: %s', name, exc, exc_info=True)

            # ── Briefing diario (exclusivo del daily) ──
            try:
                self._run_daily_briefing(today, start)
            except Exception as exc:
                _logger.error('Briefing falló: %s', exc, exc_info=True)

            # ── Sync cambios a Supabase ──
            try:
                self.run_supabase_sync()
            except Exception as exc:
                _logger.error('Supabase sync falló: %s', exc, exc_info=True)

        finally:
            release_lock(self.env, lock_param)
            elapsed = time.time() - start
            _logger.info('═══ PIPELINE FINALIZADO en %.1f segundos ═══', elapsed)

    # ══════════════════════════════════════════════════════════════════════════
    #   BRIEFING DIARIO (única pieza exclusiva del daily)
    # ══════════════════════════════════════════════════════════════════════════

    def _run_daily_briefing(self, today: str, pipeline_start: float):
        """Genera briefing ejecutivo, lo envía por email, y guarda en Odoo.

        Lee los datos de análisis ya generados por run_analyze_emails()
        y run_update_scores(), los sintetiza con Claude, y produce el
        briefing diario.
        """
        cfg = self._load_config()
        if not cfg:
            return

        from ..services.analysis_service import AnalysisService
        from ..services.claude_service import ClaudeService
        from ..services.supabase_service import SupabaseService

        claude = ClaudeService(cfg['anthropic_api_key'])
        analysis = AnalysisService()

        with SupabaseService(cfg['supabase_url'], cfg['supabase_key']) as supa:
            # Leer datos del día generados por los micro-pipelines
            account_summaries = self._read_today_summaries(supa, today)
            if not account_summaries:
                _logger.warning('Sin summaries para briefing — omitiendo')
                return

            metrics = self._read_today_metrics(supa, today)
            alerts_data = self._read_today_alerts(supa, today)
            scores_data = self._read_today_scores(supa, today)

            historical = {}
            try:
                historical = supa.get_historical_context()
            except Exception:
                pass

            # Construir paquete de datos para Claude
            data_package = analysis.build_data_package(
                today, account_summaries, metrics, alerts_data, [],
                scores_data, {}, historical,
            )

            # Generar briefing con Claude
            try:
                briefing_html = claude.synthesize_briefing(data_package)
            except Exception as exc:
                _logger.error('Error generando briefing: %s', exc)
                briefing_html = (
                    '<h2>Briefing no disponible</h2>'
                    f'<p>Error: {exc}</p>'
                )

            # Extraer topics
            topics = []
            try:
                topics = claude.extract_topics(briefing_html)
            except Exception as exc:
                _logger.error('Error extrayendo temas: %s', exc)

            if topics:
                try:
                    supa.save_topics(topics, today)
                except Exception as exc:
                    _logger.debug('save_topics: %s', exc)

            # Guardar daily summary en Supabase
            key_events = analysis.build_key_events(
                alerts_data, account_summaries)
            try:
                total_emails = sum(
                    s.get('total_emails', 0) for s in account_summaries)
                accounts_ok = len(account_summaries)
                supa.save_daily_summary(
                    today, briefing_html, total_emails,
                    accounts_ok, 0, len(topics),
                    key_events=key_events,
                )
            except Exception as exc:
                _logger.error('Error guardando daily summary: %s', exc)

            # Cross-department signals
            try:
                supa._request(
                    '/rest/v1/rpc/detect_cross_department_topics',
                    'POST', {},
                )
            except Exception:
                pass

            # Refresh contact_360
            try:
                supa._request(
                    '/rest/v1/rpc/refresh_contact_360', 'POST', {},
                )
            except Exception:
                pass

        # Enviar briefing por email
        self._send_briefing_email(cfg, briefing_html, today, analysis)

        # Guardar en Odoo
        total_emails = sum(
            s.get('total_emails', 0) for s in account_summaries)
        self._save_to_odoo(
            today, briefing_html, total_emails, alerts_data,
            scores_data, time.time() - pipeline_start,
            topics=topics,
        )

        _logger.info('✓ Briefing diario completado')

    # ── Lecturas de datos del día ─────────────────────────────────────────────

    @staticmethod
    def _read_today_summaries(supa, today: str) -> list:
        try:
            return supa._request(
                '/rest/v1/account_summaries?order=created_at.desc'
                '&select=*'
                f'&summary_date=eq.{today}',
            ) or []
        except Exception:
            return []

    @staticmethod
    def _read_today_metrics(supa, today: str) -> list:
        try:
            return supa._request(
                '/rest/v1/response_metrics?select=*'
                f'&metric_date=eq.{today}',
            ) or []
        except Exception:
            return []

    @staticmethod
    def _read_today_alerts(supa, today: str) -> list:
        try:
            return supa._request(
                '/rest/v1/alerts?order=created_at.desc'
                '&limit=200&select=*'
                f'&created_at=gte.{today}T00:00:00Z',
            ) or []
        except Exception:
            return []

    @staticmethod
    def _read_today_scores(supa, today: str) -> list:
        try:
            return supa._request(
                '/rest/v1/customer_health_scores?select=*'
                f'&score_date=eq.{today}',
            ) or []
        except Exception:
            return []

    # ── Envío de briefing ─────────────────────────────────────────────────────

    def _send_briefing_email(self, cfg, briefing_html, today, analysis):
        sender = (cfg.get('sender_email') or '').strip()
        recipient = (cfg.get('recipient_email') or '').strip()
        if not (sender and '@' in sender and recipient and '@' in recipient):
            _logger.warning('Briefing no enviado: falta sender/recipient')
            return
        try:
            from ..services.gmail_service import GmailService
            sa_info = json.loads(cfg['service_account_json'])
            gmail = GmailService(sa_info)
            subject = f'Intelligence Briefing — {today}'
            gmail.send_email(
                sender, recipient, subject,
                analysis.wrap_briefing_html(briefing_html, today),
            )
            _logger.info('Briefing enviado a %s', recipient)
        except Exception as exc:
            _logger.error('Error enviando briefing: %s', exc)

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

    def _save_to_odoo(self, today, briefing_html, total_emails, alerts,
                      client_scores, execution_secs,
                      topics=None, accounts_failed=0):
        """Persiste briefing, alertas y scores en modelos Odoo."""
        Briefing = self.env["intelligence.briefing"].sudo()
        Alert = self.env["intelligence.alert"].sudo()
        Score = self.env["intelligence.client.score"].sudo()
        Partner = self.env["res.partner"].sudo()

        accounts_ok = 0
        if isinstance(total_emails, list):
            # Backward compat: if raw emails list passed
            accounts_ok = len(set(e.get('account', '') for e in total_emails))
            total_emails = len(total_emails)
        else:
            accounts_ok = total_emails  # approximate

        briefing = Briefing.create({
            'date': today,
            'briefing_type': 'daily',
            'html_content': briefing_html,
            'total_emails': total_emails if isinstance(total_emails, int) else 0,
            'accounts_ok': accounts_ok,
            'accounts_failed': accounts_failed,
            'topics_count': len(topics) if topics else 0,
            'topics_json': json.dumps(
                topics, default=str, ensure_ascii=False,
            ) if topics else False,
            'execution_seconds': execution_secs,
        })
        _logger.info('Briefing guardado en Odoo: %s', briefing.id)

        for a in alerts:
            try:
                partner = False
                contact_name = (
                    a.get('contact_name', '') or a.get('title', ''))
                if contact_name:
                    partner = Partner.search([
                        '|', ('name', 'ilike', contact_name),
                        ('email', 'ilike', contact_name),
                    ], limit=1)
                Alert.create({
                    'name': (
                        a.get('title', '') or a.get('name', 'Alerta')
                    )[:200],
                    'alert_type': a.get('alert_type', 'anomaly'),
                    'severity': a.get('severity', 'medium'),
                    'state': 'open',
                    'description': a.get('description', ''),
                    'account': a.get('account', ''),
                    'partner_id': partner.id if partner else False,
                    'briefing_id': briefing.id,
                    'gmail_thread_id': a.get('related_thread_id', ''),
                    'supabase_id': a.get('id', False) or a.get(
                        'supabase_id', False),
                })
            except Exception as exc:
                _logger.debug('save alert to Odoo: %s', exc)
        _logger.info('%d alertas guardadas en Odoo', len(alerts))

        for s in client_scores:
            try:
                email_addr = (
                    s.get('email', '') or s.get('contact_email', ''))
                if not email_addr:
                    continue
                partner = Partner.search([
                    ('email', '=ilike', email_addr)
                ], limit=1)
                if partner:
                    Score.create({
                        'partner_id': partner.id,
                        'date': today,
                        'email': email_addr,
                        'total_score': s.get('total_score', 0) or s.get(
                            'overall_score', 0),
                        'frequency_score': s.get('frequency_score', 0),
                        'responsiveness_score': s.get(
                            'responsiveness_score', 0),
                        'reciprocity_score': s.get('reciprocity_score', 0),
                        'sentiment_score': s.get('sentiment_score', 0),
                        'payment_compliance_score': s.get(
                            'payment_compliance_score', 0),
                        'risk_level': s.get('risk_level', 'medium'),
                    })
            except Exception as exc:
                _logger.debug('save score to Odoo: %s', exc)
        _logger.info('%d client scores en Odoo', len(client_scores))

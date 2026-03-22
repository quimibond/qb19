"""
Quimibond Intelligence — Motor Principal
Orquesta el pipeline completo: Gmail → Dedup → Análisis → Odoo → Claude → Scoring → Supabase → Briefing.
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

    def _run_pipeline(self, today: str, start: float):
        """Ejecuta el pipeline completo. Separado para manejo de errores."""
        # ── Cargar configuración ──────────────────────────────────────────────
        cfg = self._load_config()
        if not cfg:
            return

        # ── Cargar cuentas de email desde configuración ───────────────────────
        email_accounts = get_email_accounts(self.env)
        account_departments = get_account_departments(self.env)

        # ── Instanciar servicios ──────────────────────────────────────────────
        gmail, claude, voyage, supa = self._init_services(cfg)

        # ══════════════════════════════════════════════════════════════════════
        #  FASE 1: Leer emails de las cuentas configuradas (incremental)
        # ══════════════════════════════════════════════════════════════════════
        _logger.info('── FASE 1: Lectura de emails ──')
        gmail_history_state = self._load_gmail_history_state()
        result = gmail.read_all_accounts(
            email_accounts, history_state=gmail_history_state, max_workers=5,
        )
        self._save_gmail_history_state(result['gmail_history_state'])
        all_emails = result['emails']
        _logger.info('Total bruto: %d emails (%d cuentas OK, %d fallidas)',
                      len(all_emails), result['success_count'], result['failed_count'])

        if not all_emails:
            _logger.warning('Sin emails — abortando pipeline')
            return

        # ══════════════════════════════════════════════════════════════════════
        #  FASE 2: Deduplicación
        # ══════════════════════════════════════════════════════════════════════
        _logger.info('── FASE 2: Deduplicación ──')
        emails = self._deduplicate(all_emails)
        _logger.info('Después de dedup: %d emails únicos', len(emails))

        # Asignar departamento
        for e in emails:
            e['department'] = account_departments.get(e['account'], 'Otro')

        # ══════════════════════════════════════════════════════════════════════
        #  FASE 3: Guardar en Supabase
        # ══════════════════════════════════════════════════════════════════════
        _logger.info('── FASE 3: Persistencia en Supabase ──')
        try:
            supa.save_emails(emails)
        except Exception as exc:
            _logger.error('Error guardando emails: %s', exc)

        # ── Construir threads y contactos ─────────────────────────────────────
        threads = self._build_threads(emails, cfg)
        contacts = self._extract_contacts(emails)

        try:
            supa.save_threads(threads)
            supa.save_contacts(contacts)
        except Exception as exc:
            _logger.error('Error guardando threads/contactos: %s', exc)

        # ══════════════════════════════════════════════════════════════════════
        #  FASE 4: Enriquecimiento con Odoo ORM
        # ══════════════════════════════════════════════════════════════════════
        _logger.info('── FASE 4: Enriquecimiento Odoo ORM ──')
        odoo_context = self._enrich_with_odoo(contacts, emails)

        # ══════════════════════════════════════════════════════════════════════
        #  FASE 5: Análisis con Claude (por cuenta)
        # ══════════════════════════════════════════════════════════════════════
        _logger.info('── FASE 5: Análisis Claude por cuenta ──')
        account_summaries = self._analyze_accounts(
            emails, claude, odoo_context, account_departments, supa=supa,
        )

        try:
            supa.save_account_summaries(account_summaries, today)
        except Exception as exc:
            _logger.error('Error guardando summaries: %s', exc)

        # ══════════════════════════════════════════════════════════════════════
        #  FASE 6: Métricas y scoring
        # ══════════════════════════════════════════════════════════════════════
        _logger.info('── FASE 6: Métricas y scoring ──')
        metrics = self._compute_metrics(emails, threads, cfg)
        try:
            supa.save_metrics(metrics, today)
        except Exception as exc:
            _logger.error('Error guardando métricas: %s', exc)

        alerts = self._generate_alerts(
            threads, metrics, cfg,
            account_summaries=account_summaries,
            odoo_ctx=odoo_context,
        )
        try:
            supa.save_alerts(alerts, today)
        except Exception as exc:
            _logger.error('Error guardando alertas: %s', exc)

        client_scores = self._compute_client_scores(
            contacts, emails, threads, cfg,
            account_summaries=account_summaries,
        )
        # Build sentiment map from Claude analysis for contacts.sentiment_score
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

        # ══════════════════════════════════════════════════════════════════════
        #  FASE 7: Contexto histórico + Síntesis ejecutiva
        # ══════════════════════════════════════════════════════════════════════
        _logger.info('── FASE 7: Síntesis ejecutiva ──')
        historical = {}
        try:
            historical = supa.get_historical_context()
        except Exception as exc:
            _logger.warning('Sin contexto histórico: %s', exc)

        data_package = self._build_data_package(
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

        # Extraer temas
        topics = []
        try:
            topics = claude.extract_topics(briefing_html)
        except Exception as exc:
            _logger.error('Error extrayendo temas: %s', exc)
        _logger.info('%d temas extraídos', len(topics))

        # Guardar temas en Supabase
        if topics:
            try:
                supa.save_topics(topics, today)
            except Exception as exc:
                _logger.error('Error guardando topics: %s', exc)

        # Build key_events for daily_summaries (frontend urgency panel)
        key_events = self._build_key_events(alerts, account_summaries)

        # Guardar briefing
        try:
            supa.save_daily_summary(
                today, briefing_html, len(emails),
                result['success_count'], result['failed_count'], len(topics),
                key_events=key_events,
            )
        except Exception as exc:
            _logger.error('Error guardando daily summary: %s', exc)

        # ══════════════════════════════════════════════════════════════════════
        #  FASE 7.5: Knowledge Graph — Extracción de entidades y hechos
        # ══════════════════════════════════════════════════════════════════════
        _logger.info('── FASE 7.5: Knowledge Graph ──')
        self._feed_knowledge_graph(emails, claude, supa, today)

        # ══════════════════════════════════════════════════════════════════════
        #  FASE 8: Embeddings (Voyage AI)
        # ══════════════════════════════════════════════════════════════════════
        if cfg.get('voyage_api_key'):
            _logger.info('── FASE 8: Embeddings ──')
            self._generate_embeddings(emails, voyage, supa)

        # ══════════════════════════════════════════════════════════════════════
        #  FASE 8.5: Sync Odoo → Supabase contacts + Learning + Patterns
        # ══════════════════════════════════════════════════════════════════════
        _logger.info('── FASE 8.5: Sync y aprendizaje continuo ──')

        # Sync Odoo partner data to Supabase contacts
        self._sync_contacts_to_supabase(odoo_context, supa)

        # Generate accountability alerts from action verification
        self._generate_accountability_alerts(
            odoo_context, alerts, supa, today,
        )

        # Save communication patterns
        try:
            patterns = self._compute_communication_patterns(
                emails, threads, today,
            )
            if patterns:
                supa.save_communication_patterns(patterns)
        except Exception as exc:
            _logger.debug('Communication patterns: %s', exc)

        # Detect and save system learnings
        self._detect_learnings(
            metrics, alerts, client_scores, odoo_context, supa,
        )

        # Sync Gmail history state to Supabase sync_state table
        for acct, hid in result['gmail_history_state'].items():
            supa.save_sync_state(acct, str(hid))

        # ══════════════════════════════════════════════════════════════════════
        #  FASE 9: Enviar briefing por email
        # ══════════════════════════════════════════════════════════════════════
        _logger.info('── FASE 9: Envío del briefing ──')
        sender = cfg.get('sender_email')
        recipient = cfg.get('recipient_email')
        if sender and recipient:
            try:
                subject = f'Intelligence Briefing — {today}'
                gmail.send_email(sender, recipient,
                                 subject, self._wrap_briefing_html(briefing_html, today))
                _logger.info('Briefing enviado a %s', recipient)
            except Exception as exc:
                _logger.error('Error enviando briefing: %s', exc)
        else:
            _logger.warning('Briefing no enviado: falta sender_email o recipient_email en config')

        # -- Guardar en Odoo (Capa 2) --
        try:
            self._save_to_odoo(
                today, briefing_html, emails, alerts,
                client_scores, contacts, time.time() - start,
                topics=topics,
                accounts_failed=result['failed_count'],
            )
        except Exception as exc:
            _logger.error('Error guardando en Odoo: %s', exc)

        # ══════════════════════════════════════════════════════════════════════
        #  FASE 10: Feedback Processing (Phase 2 — Auto-mejora)
        # ══════════════════════════════════════════════════════════════════════
        _logger.info('── FASE 10: Feedback processing ──')
        try:
            from ..services.feedback_service import FeedbackService
            feedback_svc = FeedbackService(cfg['supabase_url'], cfg['supabase_key'])
            processed, total_reward = feedback_svc.process_feedback_rewards()
            _logger.info('Feedback: %d señales procesadas, reward total: %.2f',
                         processed, total_reward)
            action_priorities = feedback_svc.get_action_priorities()
            if action_priorities:
                _logger.info('Action priorities: %s', action_priorities)
        except Exception as exc:
            _logger.warning('Feedback processing (non-critical): %s', exc)

        _logger.info('Pipeline completado exitosamente')

    # ══════════════════════════════════════════════════════════════════════════
    #   PUNTO DE ENTRADA: CALIBRACIÓN SEMANAL (Phase 2 — Auto-mejora)
    # ══════════════════════════════════════════════════════════════════════════

    @api.model
    def run_weekly_calibration(self):
        """Weekly feedback calibration — adjusts alert thresholds and action priorities."""
        _logger.info('═══ WEEKLY CALIBRATION ═══')
        cfg = self._load_config()
        if not cfg:
            return
        from ..services.feedback_service import FeedbackService
        feedback_svc = FeedbackService(cfg['supabase_url'], cfg['supabase_key'])
        try:
            calibrations = feedback_svc.calibrate_alerts()
            _logger.info('Calibraciones aplicadas: %s', calibrations)
            priorities = feedback_svc.get_action_priorities()
            if priorities:
                for cat, modifier in priorities.items():
                    if abs(modifier) > 0.2:
                        feedback_svc.save_learning(
                            learning_type='action_priority',
                            description=f'Ajuste de prioridad para {cat}: {modifier:+.2f}',
                            metric_name='priority_modifier',
                            metric_before=0.0,
                            metric_after=modifier,
                        )
            _logger.info('Action priorities: %s', priorities)
        except Exception as exc:
            _logger.error('Error en calibración semanal: %s', exc, exc_info=True)
        _logger.info('═══ WEEKLY CALIBRATION DONE ═══')

    # ══════════════════════════════════════════════════════════════════════════
    #   PUNTO DE ENTRADA: REPORTE SEMANAL (lunes 8am)
    # ══════════════════════════════════════════════════════════════════════════

    @api.model
    def run_weekly_analysis(self):
        """Reporte semanal con tendencias y comparativas."""
        _logger.info('═══ WEEKLY ANALYSIS ═══')
        cfg = self._load_config()
        if not cfg:
            return

        from ..services.claude_service import ClaudeService
        from ..services.supabase_service import SupabaseService

        claude = ClaudeService(cfg['anthropic_api_key'], model=cfg.get('claude_model', ''))
        supa = SupabaseService(cfg['supabase_url'], cfg['supabase_key'])

        week_start = (datetime.now(TZ_CDMX) - timedelta(days=7)).strftime('%Y-%m-%d')

        # Obtener métricas de últimos 7 días
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

        # Client scores de la semana
        try:
            weekly_scores = supa._request(
                '/rest/v1/client_scores?order=score_date.desc&limit=100'
                '&select=*&score_date=gte.' + week_start,
            ) or []
        except Exception:
            weekly_scores = []

        # Daily summaries de la semana
        try:
            weekly_summaries = supa._request(
                '/rest/v1/daily_summaries?order=summary_date.desc&limit=7'
                '&select=summary_date,summary,email_count'
                '&summary_date=gte.' + week_start,
            ) or []
        except Exception:
            weekly_summaries = []

        if not weekly_metrics and not weekly_alerts:
            _logger.warning('Sin datos semanales')
            return

        # Agrupar alertas por tipo para análisis
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
                    self._wrap_briefing_html(weekly_html, today, weekly=True),
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
        """Cuenta Gmail → último historyId sincronizado (JSON en ir.config_parameter)."""
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
            'claude_model': get('claude_model', ''),
        }

    def _init_services(self, cfg: dict):
        """Instancia los cuatro servicios externos."""
        from ..services.claude_service import ClaudeService, VoyageService
        from ..services.gmail_service import GmailService
        from ..services.supabase_service import SupabaseService

        sa_info = json.loads(cfg['service_account_json'])
        gmail = GmailService(sa_info)
        claude = ClaudeService(cfg['anthropic_api_key'], model=cfg.get('claude_model', ''))
        voyage = (VoyageService(cfg['voyage_api_key'])
                  if cfg.get('voyage_api_key') else None)
        supa = SupabaseService(cfg['supabase_url'], cfg['supabase_key'])

        return gmail, claude, voyage, supa

    # ── Deduplicación ─────────────────────────────────────────────────────────

    @staticmethod
    def _deduplicate(emails: list) -> list:
        """Elimina duplicados por fingerprint (from_email|subject_norm|date_minute)."""
        seen = set()
        unique = []
        for e in emails:
            try:
                date_str = e.get('date', '')
                # Normalizar fecha a minuto
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

            # Parse RFC 2822 dates to ISO 8601 for Supabase
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

            # Calcular horas sin respuesta
            hours_no_response = 0
            if last['sender_type'] == 'external':
                try:
                    last_date = datetime.fromisoformat(last_activity_iso)
                    hours_no_response = (now - last_date).total_seconds() / 3600
                except Exception:
                    pass

            # Determinar status
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

    # ── Extracción de contactos ───────────────────────────────────────────────

    @staticmethod
    def _extract_contacts(emails: list) -> list:
        """Extrae contactos únicos de los emails."""
        contact_map = {}
        for e in emails:
            email_addr = e.get('from_email', '').lower()
            if not email_addr:
                continue
            if email_addr not in contact_map:
                contact_map[email_addr] = {
                    'email': email_addr,
                    'name': e.get('from_name', ''),
                    'contact_type': e.get('sender_type', 'external'),
                    'department': e.get('department'),
                }
        return list(contact_map.values())

    # ── Enriquecimiento PROFUNDO con Odoo ORM ────────────────────────────────

    def _enrich_with_odoo(self, contacts: list, emails: list) -> dict:
        """Enriquecimiento profundo: 10 modelos de Odoo + verificación de acciones.

        Modelos consultados:
        1. res.partner — datos básicos, ranks, crédito
        2. sale.order — pedidos de venta (90 días)
        3. account.move — facturas pendientes
        4. purchase.order — órdenes de compra
        5. mail.message — comunicación interna (chatter, notas, emails desde Odoo)
        6. mail.activity — actividades pendientes/completadas
        7. crm.lead — pipeline comercial, oportunidades
        8. stock.picking — entregas y recepciones
        9. account.payment — pagos recibidos/emitidos
        10. calendar.event — reuniones agendadas
        + Verificación de action items previos (accountability)
        """
        odoo_ctx = {
            'partners': {},
            'business_summary': {},
            'action_followup': {},
            'global_pipeline': {},
            'team_activities': {},
        }
        external_emails = [
            c['email'] for c in contacts
            if c['contact_type'] == 'external' and c['email']
        ]

        if not external_emails:
            return odoo_ctx

        # ── Cargar modelos disponibles (graceful si no están instalados) ────
        models = self._load_odoo_models()
        if not models.get('partner'):
            return odoo_ctx

        Partner = models['partner']
        date_90d = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
        date_30d = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        date_7d = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        today = datetime.now().date()

        # ── Enriquecer por contacto externo ─────────────────────────────────
        for email_addr in external_emails:
            try:
                partner = Partner.search(
                    [('email', '=ilike', email_addr)], limit=1,
                )
                if not partner:
                    continue

                ctx = self._enrich_partner(
                    partner, models, date_90d, date_30d, date_7d, today,
                )
                odoo_ctx['partners'][email_addr] = ctx
                odoo_ctx['business_summary'][email_addr] = (
                    ctx.get('_summary', '')
                )

            except Exception as exc:
                _logger.warning('Odoo enrichment skip %s: %s', email_addr, exc)

        # ── Verificación de acciones sugeridas previamente ──────────────────
        odoo_ctx['action_followup'] = self._verify_pending_actions(today)

        # ── Contexto global del pipeline comercial ──────────────────────────
        if models.get('crm_lead'):
            odoo_ctx['global_pipeline'] = self._get_global_pipeline(
                models['crm_lead'],
            )

        # ── Actividades del equipo (quién tiene qué pendiente) ──────────────
        if models.get('mail_activity'):
            odoo_ctx['team_activities'] = self._get_team_activities(
                models['mail_activity'], today,
            )

        _logger.info(
            '✓ %d contactos enriquecidos (deep) | %d acciones verificadas',
            len(odoo_ctx['partners']),
            len(odoo_ctx.get('action_followup', {}).get('items', [])),
        )
        return odoo_ctx

    # ── Helpers de enriquecimiento profundo ─────────────────────────────────

    def _load_odoo_models(self) -> dict:
        """Carga los modelos ORM disponibles. Graceful si alguno no existe."""
        models = {}
        model_map = {
            'partner': 'res.partner',
            'sale_order': 'sale.order',
            'account_move': 'account.move',
            'purchase_order': 'purchase.order',
            'mail_message': 'mail.message',
            'mail_activity': 'mail.activity',
            'crm_lead': 'crm.lead',
            'stock_picking': 'stock.picking',
            'account_payment': 'account.payment',
            'calendar_event': 'calendar.event',
            'mrp_production': 'mrp.production',
            'product_product': 'product.product',
        }
        for key, model_name in model_map.items():
            try:
                models[key] = self.env[model_name].sudo()
            except KeyError:
                _logger.debug('Modelo %s no disponible (módulo no instalado)',
                              model_name)
        return models

    def _enrich_partner(self, partner, models, date_90d, date_30d,
                        date_7d, today) -> dict:
        """Construye el perfil completo de un partner con todos los modelos."""
        pid = partner.id
        summary_parts = []

        # ── 1. Datos básicos ────────────────────────────────────────────────
        ctx = {
            'id': pid,
            'name': partner.name,
            'email': partner.email,
            'phone': partner.phone or '',
            'is_company': partner.is_company,
            'company_name': (partner.parent_id.name
                             if partner.parent_id else
                             (partner.name if partner.is_company else '')),
            'customer_rank': partner.customer_rank,
            'supplier_rank': partner.supplier_rank,
            'is_customer': partner.customer_rank > 0,
            'is_supplier': partner.supplier_rank > 0,
            'credit_limit': getattr(partner, 'credit_limit', 0),
            'total_invoiced': partner.total_invoiced or 0,
        }

        # ── 2. Ventas recientes (sale.order) ────────────────────────────────
        if models.get('sale_order'):
            sales = models['sale_order'].search([
                ('partner_id', '=', pid),
                ('date_order', '>=', date_90d),
            ], order='date_order desc', limit=10)

            ctx['recent_sales'] = [{
                'name': s.name,
                'date': s.date_order.strftime('%Y-%m-%d') if s.date_order else '',
                'amount': s.amount_total,
                'state': s.state,
                'currency': s.currency_id.name,
            } for s in sales]

            if ctx['is_customer'] and ctx['recent_sales']:
                total_sales = sum(s['amount'] for s in ctx['recent_sales'])
                summary_parts.append(
                    f"VENTAS: {len(ctx['recent_sales'])} pedidos "
                    f"(${total_sales:,.0f}) en 90d"
                )

        # ── 3. Facturas pendientes (account.move) ──────────────────────────
        if models.get('account_move'):
            invoices = models['account_move'].search([
                ('partner_id', '=', pid),
                ('move_type', 'in', ['out_invoice', 'out_refund']),
                ('payment_state', 'in', ['not_paid', 'partial']),
            ], order='invoice_date desc', limit=10)

            ctx['pending_invoices'] = [{
                'name': inv.name,
                'date': (inv.invoice_date.strftime('%Y-%m-%d')
                         if inv.invoice_date else ''),
                'amount': inv.amount_total,
                'amount_residual': inv.amount_residual,
                'state': inv.state,
                'currency': inv.currency_id.name,
                'days_overdue': (
                    (today - inv.invoice_date_due).days
                    if inv.invoice_date_due and inv.invoice_date_due < today
                    else 0
                ),
            } for inv in invoices]

            if ctx['pending_invoices']:
                total_pend = sum(
                    i['amount_residual'] for i in ctx['pending_invoices']
                )
                overdue = [
                    i for i in ctx['pending_invoices'] if i['days_overdue'] > 0
                ]
                if overdue:
                    max_overdue = max(i['days_overdue'] for i in overdue)
                    summary_parts.append(
                        f"FACTURAS: ${total_pend:,.0f} pendiente "
                        f"({len(overdue)} vencidas, máx {max_overdue}d)"
                    )
                else:
                    summary_parts.append(
                        f"FACTURAS: ${total_pend:,.0f} pendiente (al corriente)"
                    )

        # ── 4. Compras (purchase.order) ─────────────────────────────────────
        if models.get('purchase_order') and partner.supplier_rank > 0:
            purchases = models['purchase_order'].search([
                ('partner_id', '=', pid),
                ('date_order', '>=', date_90d),
            ], order='date_order desc', limit=10)

            ctx['recent_purchases'] = [{
                'name': p.name,
                'date': (p.date_order.strftime('%Y-%m-%d')
                         if p.date_order else ''),
                'amount': p.amount_total,
                'state': p.state,
                'currency': p.currency_id.name,
            } for p in purchases]

            if ctx['recent_purchases']:
                total_purch = sum(
                    p['amount'] for p in ctx['recent_purchases']
                )
                summary_parts.append(
                    f"COMPRAS: {len(ctx['recent_purchases'])} OC "
                    f"(${total_purch:,.0f}) en 90d"
                )

        # ── 5. Pagos recibidos/emitidos (account.payment) ──────────────────
        if models.get('account_payment'):
            payments = models['account_payment'].search([
                ('partner_id', '=', pid),
                ('state', '=', 'posted'),
                ('date', '>=', date_30d),
            ], order='date desc', limit=10)

            ctx['recent_payments'] = [{
                'name': pay.name,
                'date': pay.date.strftime('%Y-%m-%d') if pay.date else '',
                'amount': pay.amount,
                'payment_type': pay.payment_type,
                'currency': pay.currency_id.name,
            } for pay in payments]

            if ctx['recent_payments']:
                inbound = [
                    p for p in ctx['recent_payments']
                    if p['payment_type'] == 'inbound'
                ]
                outbound = [
                    p for p in ctx['recent_payments']
                    if p['payment_type'] == 'outbound'
                ]
                if inbound:
                    total_in = sum(p['amount'] for p in inbound)
                    summary_parts.append(
                        f"COBROS: ${total_in:,.0f} recibido (30d)"
                    )
                if outbound:
                    total_out = sum(p['amount'] for p in outbound)
                    summary_parts.append(
                        f"PAGOS: ${total_out:,.0f} pagado (30d)"
                    )

        # ── 6. Comunicación interna - Chatter (mail.message) ───────────────
        if models.get('mail_message'):
            messages = models['mail_message'].search([
                ('res_id', '=', pid),
                ('model', '=', 'res.partner'),
                ('message_type', 'in', ['comment', 'email']),
                ('date', '>=', date_7d),
            ], order='date desc', limit=10)

            ctx['recent_chatter'] = [{
                'date': msg.date.strftime('%Y-%m-%d %H:%M') if msg.date else '',
                'author': msg.author_id.name if msg.author_id else 'Sistema',
                'type': msg.message_type,
                'preview': (msg.body or '')[:200].replace('<br>', ' ')
                           .replace('<p>', '').replace('</p>', ''),
                'subtype': (msg.subtype_id.name
                            if msg.subtype_id else ''),
            } for msg in messages]

            # Buscar también mensajes en modelos relacionados (SO, PO, etc.)
            related_msgs = models['mail_message'].search([
                ('partner_ids', 'in', pid),
                ('message_type', 'in', ['comment', 'email']),
                ('date', '>=', date_7d),
                ('model', '!=', 'res.partner'),
            ], order='date desc', limit=10)

            ctx['related_chatter'] = [{
                'date': (msg.date.strftime('%Y-%m-%d %H:%M')
                         if msg.date else ''),
                'author': (msg.author_id.name
                           if msg.author_id else 'Sistema'),
                'model': msg.model or '',
                'res_id': msg.res_id,
                'preview': (msg.body or '')[:200].replace('<br>', ' ')
                           .replace('<p>', '').replace('</p>', ''),
            } for msg in related_msgs]

            total_msgs = len(ctx['recent_chatter']) + len(ctx['related_chatter'])
            if total_msgs > 0:
                summary_parts.append(
                    f"COMUNICACION ODOO: {total_msgs} mensajes en 7d"
                )

        # ── 7. Actividades pendientes (mail.activity) ──────────────────────
        if models.get('mail_activity'):
            activities = models['mail_activity'].search([
                ('res_id', '=', pid),
                ('res_model', '=', 'res.partner'),
            ], order='date_deadline asc', limit=10)

            # Buscar en todos los modelos relacionados al partner
            partner_activities = list(activities)
            for model_name in ('sale.order', 'account.move', 'purchase.order',
                               'crm.lead'):
                try:
                    related = models['mail_activity'].search([
                        ('res_model', '=', model_name),
                        ('res_id', 'in', self._get_partner_record_ids(
                            partner, model_name, models,
                        )),
                    ], limit=10)
                    partner_activities.extend(related)
                except Exception:
                    pass

            ctx['pending_activities'] = [{
                'type': act.activity_type_id.name if act.activity_type_id else 'Tarea',
                'summary': act.summary or act.note or '',
                'deadline': (act.date_deadline.strftime('%Y-%m-%d')
                             if act.date_deadline else ''),
                'assigned_to': act.user_id.name if act.user_id else '',
                'is_overdue': (
                    act.date_deadline < today if act.date_deadline else False
                ),
                'model': act.res_model or '',
            } for act in partner_activities]

            overdue_acts = [
                a for a in ctx['pending_activities'] if a['is_overdue']
            ]
            pending_acts = [
                a for a in ctx['pending_activities'] if not a['is_overdue']
            ]
            if overdue_acts or pending_acts:
                parts = []
                if overdue_acts:
                    parts.append(f"{len(overdue_acts)} VENCIDAS")
                if pending_acts:
                    parts.append(f"{len(pending_acts)} pendientes")
                summary_parts.append(
                    f"ACTIVIDADES: {', '.join(parts)}"
                )

        # ── 8. Pipeline CRM (crm.lead) ─────────────────────────────────────
        if models.get('crm_lead'):
            leads = models['crm_lead'].search([
                ('partner_id', '=', pid),
                ('active', '=', True),
            ], order='create_date desc', limit=5)

            ctx['crm_leads'] = [{
                'name': lead.name,
                'stage': lead.stage_id.name if lead.stage_id else '',
                'expected_revenue': lead.expected_revenue or 0,
                'probability': lead.probability or 0,
                'date_deadline': (lead.date_deadline.strftime('%Y-%m-%d')
                                  if lead.date_deadline else ''),
                'user': lead.user_id.name if lead.user_id else '',
                'type': 'opportunity' if lead.type == 'opportunity' else 'lead',
                'days_open': (
                    (today - lead.create_date.date()).days
                    if lead.create_date else 0
                ),
            } for lead in leads]

            opps = [l for l in ctx['crm_leads'] if l['type'] == 'opportunity']
            if opps:
                total_rev = sum(l['expected_revenue'] for l in opps)
                summary_parts.append(
                    f"CRM: {len(opps)} oportunidades "
                    f"(${total_rev:,.0f} esperado)"
                )

        # ── 9. Entregas y recepciones (stock.picking) ──────────────────────
        if models.get('stock_picking'):
            pickings = models['stock_picking'].search([
                ('partner_id', '=', pid),
                ('state', 'not in', ['done', 'cancel']),
            ], order='scheduled_date asc', limit=10)

            ctx['pending_deliveries'] = [{
                'name': pick.name,
                'type': pick.picking_type_id.name if pick.picking_type_id else '',
                'scheduled': (pick.scheduled_date.strftime('%Y-%m-%d')
                              if pick.scheduled_date else ''),
                'state': pick.state,
                'is_late': (
                    pick.scheduled_date.date() < today
                    if pick.scheduled_date else False
                ),
                'origin': pick.origin or '',
            } for pick in pickings]

            if ctx['pending_deliveries']:
                late = [d for d in ctx['pending_deliveries'] if d['is_late']]
                if late:
                    summary_parts.append(
                        f"ENTREGAS: {len(ctx['pending_deliveries'])} "
                        f"pendientes ({len(late)} RETRASADAS)"
                    )
                else:
                    summary_parts.append(
                        f"ENTREGAS: {len(ctx['pending_deliveries'])} pendientes"
                    )

        # ── 10. Reuniones agendadas (calendar.event) ───────────────────────
        if models.get('calendar_event'):
            events = models['calendar_event'].search([
                ('partner_ids', 'in', pid),
                ('start', '>=', fields.Datetime.now()),
            ], order='start asc', limit=5)

            ctx['upcoming_meetings'] = [{
                'name': ev.name,
                'start': ev.start.strftime('%Y-%m-%d %H:%M') if ev.start else '',
                'attendees': [
                    att.display_name for att in (ev.attendee_ids or [])
                ][:5],
                'description': (ev.description or '')[:200],
            } for ev in events]

            if ctx['upcoming_meetings']:
                next_meeting = ctx['upcoming_meetings'][0]
                summary_parts.append(
                    f"REUNION: {next_meeting['name']} ({next_meeting['start']})"
                )

        # ── 11. Manufactura (mrp.production) ────────────────────────────────
        if models.get('mrp_production'):
            try:
                # Buscar producciones ligadas a pedidos de este partner
                sale_orders = (models.get('sale_order') or self.env['sale.order'].sudo())
                so_ids = sale_orders.search([
                    ('partner_id', '=', pid),
                    ('state', 'in', ['sale', 'done']),
                    ('date_order', '>=', date_90d),
                ]).ids
                if so_ids:
                    productions = models['mrp_production'].search([
                        ('origin', 'like', 'SO'),
                        ('state', 'not in', ['done', 'cancel']),
                    ], limit=20)
                    # Filtrar por origen que coincida con SOs del partner
                    so_names = set(
                        sale_orders.browse(so_ids).mapped('name')
                    )
                    partner_prods = [
                        p for p in productions
                        if p.origin and any(
                            sn in (p.origin or '') for sn in so_names
                        )
                    ]

                    ctx['manufacturing'] = [{
                        'name': mo.name,
                        'product': mo.product_id.name if mo.product_id else '',
                        'qty': mo.product_qty,
                        'state': mo.state,
                        'date_start': (mo.date_start.strftime('%Y-%m-%d')
                                       if mo.date_start else ''),
                        'origin': mo.origin or '',
                    } for mo in partner_prods[:5]]

                    if ctx.get('manufacturing'):
                        summary_parts.append(
                            f"PRODUCCION: {len(ctx['manufacturing'])} "
                            f"OMs en proceso"
                        )
            except Exception as exc:
                _logger.debug('MRP enrichment skip: %s', exc)

        # ── Resumen consolidado ─────────────────────────────────────────────
        ctx['_summary'] = ' | '.join(summary_parts) if summary_parts else ''
        return ctx

    def _get_partner_record_ids(self, partner, model_name, models) -> list:
        """IDs de registros de un modelo asociados a un partner."""
        try:
            if model_name == 'sale.order' and models.get('sale_order'):
                return models['sale_order'].search([
                    ('partner_id', '=', partner.id),
                ]).ids[:20]
            if model_name == 'account.move' and models.get('account_move'):
                return models['account_move'].search([
                    ('partner_id', '=', partner.id),
                ]).ids[:20]
            if model_name == 'purchase.order' and models.get('purchase_order'):
                return models['purchase_order'].search([
                    ('partner_id', '=', partner.id),
                ]).ids[:20]
            if model_name == 'crm.lead' and models.get('crm_lead'):
                return models['crm_lead'].search([
                    ('partner_id', '=', partner.id),
                ]).ids[:20]
        except Exception:
            pass
        return []

    def _verify_pending_actions(self, today) -> dict:
        """Verifica si las acciones sugeridas previamente se ejecutaron.

        Cruza intelligence.action.item con mail.activity y mail.message
        para detectar si el equipo actuó sobre las recomendaciones.
        """
        result = {
            'items': [],
            'completion_rate': 0,
            'overdue_count': 0,
            'completed_today': 0,
        }
        try:
            ActionItem = self.env['intelligence.action.item'].sudo()
            pending = ActionItem.search([
                ('state', 'in', ['open', 'in_progress']),
            ], order='priority_seq asc, due_date asc', limit=30)

            if not pending:
                return result

            total = len(pending)
            completed = ActionItem.search_count([
                ('state', '=', 'done'),
                ('write_date', '>=', today.strftime('%Y-%m-%d')),
            ])
            result['completed_today'] = completed

            for action in pending:
                item = {
                    'id': action.id,
                    'description': action.name,
                    'type': action.action_type,
                    'priority': action.priority,
                    'due_date': (action.due_date.strftime('%Y-%m-%d')
                                 if action.due_date else ''),
                    'assigned_to': (action.assignee_id.name
                                    if action.assignee_id else ''),
                    'partner': (action.partner_id.name
                                if action.partner_id else ''),
                    'days_open': (
                        (today - action.create_date.date()).days
                        if action.create_date else 0
                    ),
                    'is_overdue': action.is_overdue,
                    'evidence_of_action': [],
                }

                # Buscar evidencia de que alguien actuó
                if action.partner_id:
                    try:
                        MailMsg = self.env['mail.message'].sudo()
                        recent_msgs = MailMsg.search([
                            ('res_id', '=', action.partner_id.id),
                            ('model', '=', 'res.partner'),
                            ('date', '>=', action.create_date),
                            ('message_type', 'in', ['comment', 'email']),
                        ], limit=3, order='date desc')

                        for msg in recent_msgs:
                            item['evidence_of_action'].append({
                                'type': 'chatter_message',
                                'date': msg.date.strftime('%Y-%m-%d %H:%M'),
                                'author': (msg.author_id.name
                                           if msg.author_id else ''),
                                'preview': (msg.body or '')[:100],
                            })

                        # Buscar actividades completadas
                        MailActivity = self.env['mail.activity'].sudo()
                        # Actividades tipo del action_type
                        activity_type_map = {
                            'call': 'Llamada',
                            'email': 'Correo',
                            'meeting': 'Reunión',
                        }
                        act_type_name = activity_type_map.get(
                            action.action_type, '',
                        )
                        if act_type_name:
                            scheduled = MailActivity.search([
                                ('res_id', '=', action.partner_id.id),
                                ('res_model', '=', 'res.partner'),
                            ], limit=3)
                            for act in scheduled:
                                item['evidence_of_action'].append({
                                    'type': 'scheduled_activity',
                                    'activity': (act.activity_type_id.name
                                                 if act.activity_type_id
                                                 else ''),
                                    'deadline': (
                                        act.date_deadline.strftime('%Y-%m-%d')
                                        if act.date_deadline else ''
                                    ),
                                    'assigned_to': (act.user_id.name
                                                    if act.user_id else ''),
                                })
                    except Exception:
                        pass

                if action.is_overdue:
                    result['overdue_count'] += 1

                result['items'].append(item)

            # Tasa de completado de los últimos 7 días
            week_ago = (
                today - timedelta(days=7)
            ).strftime('%Y-%m-%d')
            total_week = ActionItem.search_count([
                ('create_date', '>=', week_ago),
            ])
            done_week = ActionItem.search_count([
                ('create_date', '>=', week_ago),
                ('state', '=', 'done'),
            ])
            result['completion_rate'] = (
                round(done_week / total_week * 100)
                if total_week > 0 else 0
            )

        except Exception as exc:
            _logger.warning('Action verification error: %s', exc)

        return result

    def _get_global_pipeline(self, CrmLead) -> dict:
        """Resumen global del pipeline comercial."""
        try:
            all_opps = CrmLead.search([
                ('type', '=', 'opportunity'),
                ('active', '=', True),
            ])
            if not all_opps:
                return {}

            by_stage = defaultdict(lambda: {'count': 0, 'revenue': 0})
            for opp in all_opps:
                stage_name = opp.stage_id.name if opp.stage_id else 'Sin etapa'
                by_stage[stage_name]['count'] += 1
                by_stage[stage_name]['revenue'] += opp.expected_revenue or 0

            total_revenue = sum(s['revenue'] for s in by_stage.values())
            return {
                'total_opportunities': len(all_opps),
                'total_expected_revenue': total_revenue,
                'by_stage': dict(by_stage),
            }
        except Exception as exc:
            _logger.debug('Pipeline error: %s', exc)
            return {}

    def _get_team_activities(self, MailActivity, today) -> dict:
        """Actividades pendientes del equipo agrupadas por usuario."""
        try:
            all_activities = MailActivity.search([
                ('date_deadline', '<=',
                 (today + timedelta(days=3)).strftime('%Y-%m-%d')),
            ], order='date_deadline asc', limit=50)

            by_user = defaultdict(lambda: {
                'pending': 0, 'overdue': 0, 'items': [],
            })
            for act in all_activities:
                user_name = act.user_id.name if act.user_id else 'Sin asignar'
                is_overdue = (
                    act.date_deadline < today if act.date_deadline else False
                )
                by_user[user_name]['pending'] += 1
                if is_overdue:
                    by_user[user_name]['overdue'] += 1
                if len(by_user[user_name]['items']) < 5:
                    by_user[user_name]['items'].append({
                        'type': (act.activity_type_id.name
                                 if act.activity_type_id else 'Tarea'),
                        'summary': act.summary or '',
                        'deadline': (act.date_deadline.strftime('%Y-%m-%d')
                                     if act.date_deadline else ''),
                        'model': act.res_model or '',
                        'overdue': is_overdue,
                    })
            return dict(by_user)
        except Exception as exc:
            _logger.debug('Team activities error: %s', exc)
            return {}

    # ── Análisis por cuenta ───────────────────────────────────────────────────

    def _analyze_accounts(self, emails: list, claude, odoo_context: dict,
                          account_departments: dict = None,
                          supa=None) -> list:
        """Fase 1 de Claude: análisis por cuenta con perfiles de personas."""
        account_departments = account_departments or {}

        # Cargar perfiles conocidos de personas desde Supabase
        person_profiles = {}
        if supa:
            all_sender_emails = list({
                e.get('from_email', '').lower()
                for e in emails if e.get('from_email')
            })
            try:
                person_profiles = supa.get_person_profiles_for_contacts(
                    all_sender_emails,
                )
                if person_profiles:
                    _logger.info(
                        '✓ %d perfiles de personas cargados de Supabase',
                        len(person_profiles),
                    )
            except Exception as exc:
                _logger.debug('Person profiles load: %s', exc)

        # Agrupar por cuenta
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

            # Construir texto con contexto Odoo + perfiles de personas
            email_text = self._format_emails_for_claude(
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

                # Save person_insights to Supabase (accumulative learning)
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
                        except Exception:
                            pass

                _logger.info('  ✓ %s (%s): %d emails analizados',
                             dept, account, len(acct_emails))
                time.sleep(3)  # Rate limit courtesy
            except Exception as exc:
                _logger.error('  ✗ %s: %s', account, exc)

        return summaries

    @staticmethod
    def _format_emails_for_claude(emails: list, odoo_ctx: dict,
                                  person_profiles: dict = None) -> str:
        """Formatea emails con contexto profundo de Odoo + perfiles conocidos."""
        person_profiles = person_profiles or {}
        lines = []
        for i, e in enumerate(emails, 1):
            lines.append(f'--- EMAIL {i} ---')
            lines.append(f'De: {e.get("from", "")}')
            lines.append(f'Para: {e.get("to", "")}')
            if e.get('cc'):
                lines.append(f'CC: {e["cc"]}')
            lines.append(f'Asunto: {e["subject"]}')
            lines.append(f'Fecha: {e["date"]}')
            lines.append(f'Tipo: {e["sender_type"]}')
            if e['is_reply']:
                lines.append('(Es respuesta)')
            if e['has_attachments']:
                att_names = ', '.join(
                    a['filename'] for a in e.get('attachments', [])
                )
                lines.append(f'Adjuntos: {att_names}')

            # Contexto de negocio de Odoo (resumen consolidado)
            sender_email = e.get('from_email', '')
            biz = odoo_ctx.get('business_summary', {}).get(sender_email)
            if biz:
                lines.append(f'[ODOO: {biz}]')

            # Perfil conocido de la persona (memoria acumulativa)
            profile = person_profiles.get(sender_email.lower())
            if profile:
                profile_parts = []
                if profile.get('role'):
                    profile_parts.append(f"Rol: {profile['role']}")
                if profile.get('company'):
                    profile_parts.append(f"Empresa: {profile['company']}")
                if profile.get('decision_power'):
                    profile_parts.append(
                        f"Poder decisión: {profile['decision_power']}"
                    )
                if profile.get('communication_style'):
                    profile_parts.append(
                        f"Estilo: {profile['communication_style']}"
                    )
                if profile.get('key_interests'):
                    interests = profile['key_interests']
                    if isinstance(interests, list):
                        interests = ', '.join(interests[:5])
                    profile_parts.append(f"Intereses: {interests}")
                if profile.get('personality_traits'):
                    traits = profile['personality_traits']
                    if isinstance(traits, list):
                        traits = ', '.join(traits[:5])
                    profile_parts.append(f"Rasgos: {traits}")
                if profile.get('decision_factors'):
                    factors = profile['decision_factors']
                    if isinstance(factors, list):
                        factors = ', '.join(factors[:5])
                    profile_parts.append(f"Decide por: {factors}")
                if profile.get('negotiation_style'):
                    profile_parts.append(
                        f"Negociación: {profile['negotiation_style']}"
                    )
                if profile.get('personality_notes'):
                    profile_parts.append(
                        f"Notas: {profile['personality_notes'][:100]}"
                    )
                if profile_parts:
                    lines.append(
                        f'[PERSONA CONOCIDA: {" | ".join(profile_parts)}]'
                    )

            body = (e.get('body') or e.get('snippet', ''))[:1500]
            lines.append(f'Cuerpo:\n{body}')
            lines.append('')
        return '\n'.join(lines)

    # ── Métricas ──────────────────────────────────────────────────────────────

    @staticmethod
    def _compute_metrics(emails: list, threads: list, cfg: dict) -> list:
        """Calcula métricas de respuesta por cuenta."""
        by_account = defaultdict(lambda: {
            'received': 0, 'sent': 0, 'ext_received': 0, 'int_received': 0,
        })

        for e in emails:
            acct = e['account']
            if e.get('from_email', '').endswith(f'@{INTERNAL_DOMAIN}'):
                by_account[acct]['sent'] += 1
            else:
                by_account[acct]['received'] += 1
                if e['sender_type'] == 'external':
                    by_account[acct]['ext_received'] += 1
                else:
                    by_account[acct]['int_received'] += 1

        # Threads por cuenta
        acct_threads = defaultdict(list)
        for t in threads:
            acct_threads[t['account']].append(t)

        metrics = []
        for acct, counts in by_account.items():
            acct_t = acct_threads.get(acct, [])
            replied = [t for t in acct_t if t['has_internal_reply']]
            unanswered = [t for t in acct_t
                          if t['status'] in ('needs_response', 'stalled')]

            # Tiempos de respuesta
            response_hours = [
                t['hours_without_response'] for t in acct_t
                if t['hours_without_response'] > 0
            ]

            metrics.append({
                'account': acct,
                'emails_received': counts['received'],
                'emails_sent': counts['sent'],
                'internal_received': counts['int_received'],
                'external_received': counts['ext_received'],
                'threads_started': len([t for t in acct_t if t['started_by_type'] == 'external']),
                'threads_replied': len(replied),
                'threads_unanswered': len(unanswered),
                'avg_response_hours': (
                    round(sum(response_hours) / len(response_hours), 1)
                    if response_hours else None
                ),
                'fastest_response_hours': (
                    round(min(response_hours), 1) if response_hours else None
                ),
                'slowest_response_hours': (
                    round(max(response_hours), 1) if response_hours else None
                ),
            })
        return metrics

    # ── Alertas ───────────────────────────────────────────────────────────────

    def _generate_alerts(self, threads: list, metrics: list, cfg: dict,
                         account_summaries: list = None,
                         odoo_ctx: dict = None) -> list:
        """Genera alertas basadas en umbrales configurables.

        Tipos de alerta:
        - stalled_thread: Thread sin respuesta > 48h
        - no_response: Thread sin respuesta > 24h
        - high_volume: Volumen de emails superior al umbral
        - competitor: Competidor mencionado en emails
        - negative_sentiment: Sentimiento negativo fuerte
        - invoice_silence: Factura vencida + sin respuesta a emails
        - churn_risk: Cliente que dejó de escribir (>14 días sin contacto)
        """
        alerts = []
        account_summaries = account_summaries or []
        odoo_ctx = odoo_ctx or {}
        no_resp_hours = cfg.get('no_response_hours', 24)
        stalled_hours = cfg.get('stalled_thread_hours', 48)
        high_vol = cfg.get('high_volume_threshold', 50)

        # Alertas por threads sin respuesta
        for t in threads:
            if t['hours_without_response'] > stalled_hours and t['started_by_type'] == 'external':
                alerts.append({
                    'alert_type': 'stalled_thread',
                    'severity': 'high',
                    'title': f"Thread estancado: {t['subject'][:80]}",
                    'description': (
                        f"{t['hours_without_response']:.0f}h sin respuesta de "
                        f"{t['last_sender']} en {t['account']}"
                    ),
                    'contact_name': t.get('last_sender'),
                    'account': t['account'],
                    'related_thread_id': t['gmail_thread_id'],
                })
            elif t['hours_without_response'] > no_resp_hours and t['started_by_type'] == 'external':
                alerts.append({
                    'alert_type': 'no_response',
                    'severity': 'medium',
                    'title': f"Sin respuesta: {t['subject'][:80]}",
                    'description': (
                        f"{t['hours_without_response']:.0f}h esperando en {t['account']}"
                    ),
                    'contact_name': t.get('last_sender'),
                    'account': t['account'],
                    'related_thread_id': t['gmail_thread_id'],
                })

        # Alerta por volumen alto
        for m in metrics:
            total = m['emails_received'] + m['emails_sent']
            if total > high_vol:
                alerts.append({
                    'alert_type': 'high_volume',
                    'severity': 'low',
                    'title': f"Alto volumen: {m['account']}",
                    'description': f'{total} emails hoy (umbral: {high_vol})',
                    'account': m['account'],
                })

        # ── Alertas inteligentes desde análisis de Claude ────────────────────

        for s in account_summaries:
            account = s.get('account', '')

            # Alerta por competidores mencionados
            for comp in s.get('competitors_mentioned', []):
                threat = comp.get('threat_level', 'medium')
                severity = 'high' if threat == 'high' else 'medium'
                alerts.append({
                    'alert_type': 'competitor',
                    'severity': severity,
                    'title': (
                        f"Competidor: {comp.get('name', '?')} "
                        f"mencionado por {comp.get('mentioned_by', '?')}"
                    )[:120],
                    'description': comp.get('detail', comp.get('context', '')),
                    'contact_name': comp.get('mentioned_by'),
                    'account': account,
                })

            # Alerta por sentimiento negativo fuerte
            score = s.get('sentiment_score')
            if isinstance(score, (int, float)) and score < -0.3:
                severity = 'critical' if score < -0.6 else 'high'
                alerts.append({
                    'alert_type': 'negative_sentiment',
                    'severity': severity,
                    'title': (
                        f"Sentimiento negativo ({score:.1f}) en {account}"
                    ),
                    'description': s.get('sentiment_detail', ''),
                    'account': account,
                })

            # Alerta por contactos con señal de riesgo
            for contact in s.get('external_contacts', []):
                c_score = contact.get('sentiment_score')
                signal = contact.get('relationship_signal', '')
                if signal == 'at_risk' or (
                    isinstance(c_score, (int, float)) and c_score < -0.4
                ):
                    alerts.append({
                        'alert_type': 'churn_risk',
                        'severity': 'high',
                        'title': (
                            f"Relación en riesgo: "
                            f"{contact.get('name', '?')} "
                            f"({contact.get('company', '?')})"
                        )[:120],
                        'description': (
                            f"Señal: {signal}. "
                            f"Sentimiento: {c_score}. "
                            f"Tema: {contact.get('topic', '?')}"
                        ),
                        'contact_name': contact.get('name'),
                        'account': account,
                    })

        # ── Alerta: factura vencida + sin respuesta a emails ─────────────────

        partners = odoo_ctx.get('partners', {})
        # Build set of emails with stalled/no_response threads
        stalled_emails = set()
        for t in threads:
            if t['status'] in ('stalled', 'needs_response'):
                stalled_emails.update(t.get('participant_emails', []))

        for email_addr, p in partners.items():
            overdue_invoices = [
                inv for inv in p.get('pending_invoices', [])
                if inv.get('days_overdue', 0) > 0
            ]
            if overdue_invoices and email_addr in stalled_emails:
                total_overdue = sum(
                    inv.get('amount_residual', 0) for inv in overdue_invoices
                )
                max_days = max(
                    inv.get('days_overdue', 0) for inv in overdue_invoices
                )
                alerts.append({
                    'alert_type': 'invoice_silence',
                    'severity': 'critical',
                    'title': (
                        f"Factura vencida + sin respuesta: "
                        f"{p.get('name', email_addr)}"
                    )[:120],
                    'description': (
                        f"${total_overdue:,.0f} en facturas vencidas "
                        f"(máx {max_days}d) Y tiene emails sin responder. "
                        f"Riesgo de cobranza. Requiere acción inmediata."
                    ),
                    'contact_name': p.get('name', email_addr),
                    'account': '',
                })

        return alerts

    # ── Client Scoring ────────────────────────────────────────────────────────

    @staticmethod
    def _compute_client_scores(contacts: list, emails: list, threads: list,
                               cfg: dict,
                               account_summaries: list = None) -> list:
        """Calcula score de relación 0-100 para contactos externos.

        Usa sentiment_score numérico de Claude si está disponible.
        """
        external = [c for c in contacts if c['contact_type'] == 'external']
        if not external:
            return []

        # Pre-computar datos por email
        email_counts = defaultdict(int)
        for e in emails:
            email_counts[e.get('from_email', '')] += 1

        thread_participation = defaultdict(int)
        for t in threads:
            for p in t.get('participant_emails', []):
                thread_participation[p] += 1

        # Build contact sentiment map from Claude analysis
        contact_sentiments = {}
        for s in (account_summaries or []):
            for ec in s.get('external_contacts', []):
                email_addr = (ec.get('email') or '').lower()
                if email_addr and ec.get('sentiment_score') is not None:
                    try:
                        contact_sentiments[email_addr] = float(
                            ec['sentiment_score'],
                        )
                    except (ValueError, TypeError):
                        pass

        scores = []
        for c in external:
            addr = c['email']
            msg_count = email_counts.get(addr, 0)
            thread_count = thread_participation.get(addr, 0)

            # Frequency score (0-25): más emails = mejor relación
            freq_score = min(25, 5 + msg_count * 4)

            # Responsiveness score (0-25): participación en threads
            resp_score = min(25, 5 + thread_count * 4)

            # Reciprocity score (0-25): ¿reciben respuesta?
            related_threads = [
                t for t in threads
                if addr in t.get('participant_emails', [])
            ]
            replied_count = sum(1 for t in related_threads if t['has_internal_reply'])
            recip_score = (
                round(replied_count / len(related_threads) * 25)
                if related_threads else 12
            )

            # Sentiment score (0-25): use Claude's numeric score if available
            # Convert from [-1, 1] range to [0, 25] range
            claude_sentiment = contact_sentiments.get(addr.lower())
            if claude_sentiment is not None:
                # -1.0 → 0, 0.0 → 12.5, 1.0 → 25
                sent_score = round((claude_sentiment + 1) * 12.5)
                sent_score = max(0, min(25, sent_score))
            else:
                sent_score = 15  # neutral baseline

            total = freq_score + resp_score + recip_score + sent_score

            # Risk level
            if total >= 60:
                risk = 'low'
            elif total >= 35:
                risk = 'medium'
            else:
                risk = 'high'

            scores.append({
                'email': addr,
                'total_score': total,
                'frequency_score': freq_score,
                'responsiveness_score': resp_score,
                'reciprocity_score': recip_score,
                'sentiment_score': sent_score,
                'risk_level': risk,
            })

        return scores

    # ── Key Events para daily_summaries ──────────────────────────────────────

    @staticmethod
    def _build_key_events(alerts: list, account_summaries: list) -> list:
        """Construye key_events JSON para daily_summaries.

        El frontend usa esto para el panel de urgencias del dashboard.
        Formato: [{"type": str, "description": str, "urgency": str}]
        """
        events = []

        # Critical and high alerts → key events
        severity_urgency = {
            'critical': 'critical', 'high': 'high',
            'medium': 'medium', 'low': 'low',
        }
        for a in (alerts or []):
            sev = a.get('severity', 'low')
            if sev in ('critical', 'high'):
                events.append({
                    'type': a.get('alert_type', 'alert'),
                    'description': a.get('title', ''),
                    'urgency': severity_urgency.get(sev, 'medium'),
                })

        # Urgent items from account summaries
        for s in (account_summaries or []):
            for item in s.get('urgent_items', []):
                events.append({
                    'type': 'urgent_item',
                    'description': item.get('item', ''),
                    'urgency': 'high',
                })

            # Competitors mentioned → key events
            for comp in s.get('competitors_mentioned', []):
                threat = comp.get('threat_level', 'medium')
                events.append({
                    'type': 'competitor',
                    'description': (
                        f"Competidor {comp.get('name', '?')} mencionado "
                        f"por {comp.get('mentioned_by', '?')}"
                    ),
                    'urgency': 'high' if threat == 'high' else 'medium',
                })

            # At-risk contacts → key events
            for contact in s.get('external_contacts', []):
                signal = contact.get('relationship_signal', '')
                if signal == 'at_risk':
                    events.append({
                        'type': 'churn_risk',
                        'description': (
                            f"Relación en riesgo: "
                            f"{contact.get('name', '?')} "
                            f"({contact.get('company', '?')})"
                        ),
                        'urgency': 'high',
                    })

        # Dedupe and limit
        seen = set()
        unique = []
        for e in events:
            key = e['description'][:80]
            if key not in seen:
                seen.add(key)
                unique.append(e)
        return unique[:20]

    # ── Data Package para síntesis ────────────────────────────────────────────

    @staticmethod
    def _build_data_package(today: str, summaries: list, metrics: list,
                            alerts: list, threads: list, client_scores: list,
                            odoo_ctx: dict, historical: dict) -> str:
        """Construye el paquete de datos completo para Claude fase 2.

        Incluye: análisis por cuenta, métricas, alertas, contexto profundo
        de Odoo (10 modelos), verificación de acciones, pipeline CRM,
        actividades del equipo, y perfiles detallados de contactos.
        """
        sections = [
            f'FECHA: {today}',
            f'TOTAL CUENTAS ANALIZADAS: {len(summaries)}',
        ]

        # ── Contexto histórico ──────────────────────────────────────────────
        if historical.get('previousSummary'):
            sections.append(
                f"\nRESUMEN DEL DÍA ANTERIOR:\n"
                f"{historical['previousSummary'][:1000]}"
            )
        if historical.get('openAlerts'):
            sections.append(
                f"\nALERTAS ABIERTAS PREVIAS:\n"
                + json.dumps(historical['openAlerts'][:10], default=str)
            )

        # ── Resúmenes por cuenta ────────────────────────────────────────────
        sections.append('\n═══ ANÁLISIS POR CUENTA ═══')
        for s in summaries:
            sections.append(
                f"\n── {s['department']} ({s['account']}) ──\n"
                f"Emails: {s.get('total_emails', 0)} "
                f"(ext:{s.get('external_emails', 0)}, "
                f"int:{s.get('internal_emails', 0)})\n"
                f"Resumen: {s.get('summary_text', '')}\n"
                f"Sentimiento: {s.get('overall_sentiment', 'N/A')}\n"
                f"Items clave: {json.dumps(s.get('key_items', []), default=str, ensure_ascii=False)}\n"
                f"Esperando respuesta: {json.dumps(s.get('waiting_response', []), default=str, ensure_ascii=False)}\n"
                f"Urgentes: {json.dumps(s.get('urgent_items', []), default=str, ensure_ascii=False)}\n"
                f"Contactos: {json.dumps(s.get('external_contacts', []), default=str, ensure_ascii=False)}\n"
                f"Temas: {json.dumps(s.get('topics_detected', []), default=str, ensure_ascii=False)}\n"
                f"Riesgos: {json.dumps(s.get('risks_detected', []), default=str, ensure_ascii=False)}\n"
                f"Sentimiento numérico: {s.get('sentiment_score', 'N/A')}\n"
                f"Competidores: {json.dumps(s.get('competitors_mentioned', []), default=str, ensure_ascii=False)}"
            )

        # ── Métricas ────────────────────────────────────────────────────────
        sections.append('\n═══ MÉTRICAS DE RESPUESTA ═══')
        for m in metrics:
            sections.append(
                f"{m['account']}: recv={m['emails_received']} "
                f"sent={m['emails_sent']} "
                f"replied={m['threads_replied']} "
                f"unanswered={m['threads_unanswered']} "
                f"avg_hrs={m.get('avg_response_hours', 'N/A')}"
            )

        # ── Alertas ─────────────────────────────────────────────────────────
        if alerts:
            sections.append(f'\n═══ ALERTAS ({len(alerts)}) ═══')
            for a in alerts[:20]:
                sections.append(
                    f"[{a['severity'].upper()}] {a['alert_type']}: "
                    f"{a['title']}"
                )

        # ══════════════════════════════════════════════════════════════════
        #  CONTEXTO PROFUNDO DE ODOO (10 modelos)
        # ══════════════════════════════════════════════════════════════════

        # ── Perfiles detallados de contactos ────────────────────────────────
        partners = odoo_ctx.get('partners', {})
        if partners:
            sections.append('\n═══ PERFILES DE CONTACTOS (Odoo ERP — datos en vivo) ═══')
            for email_addr, p in partners.items():
                summary = p.get('_summary', '')
                if not summary:
                    continue
                parts = [f"\n── {p.get('name', email_addr)} ({email_addr}) ──"]
                parts.append(f"RESUMEN: {summary}")

                # CRM Pipeline
                leads = p.get('crm_leads', [])
                if leads:
                    for l in leads[:3]:
                        parts.append(
                            f"  CRM: {l['name']} | Etapa: {l['stage']} | "
                            f"Revenue: ${l['expected_revenue']:,.0f} | "
                            f"Prob: {l['probability']}% | "
                            f"Responsable: {l['user']} | "
                            f"{l['days_open']}d abierto"
                        )

                # Actividades pendientes
                acts = p.get('pending_activities', [])
                if acts:
                    overdue = [a for a in acts if a['is_overdue']]
                    if overdue:
                        parts.append(
                            f"  ⚠ ACTIVIDADES VENCIDAS ({len(overdue)}):"
                        )
                        for a in overdue[:3]:
                            parts.append(
                                f"    - {a['type']}: {a['summary'][:80]} "
                                f"(vencida {a['deadline']}, "
                                f"asignada a {a['assigned_to']})"
                            )
                    pending = [a for a in acts if not a['is_overdue']]
                    if pending:
                        parts.append(
                            f"  Actividades programadas ({len(pending)}):"
                        )
                        for a in pending[:3]:
                            parts.append(
                                f"    - {a['type']}: {a['summary'][:80]} "
                                f"(para {a['deadline']}, {a['assigned_to']})"
                            )

                # Entregas pendientes
                deliveries = p.get('pending_deliveries', [])
                if deliveries:
                    late = [d for d in deliveries if d['is_late']]
                    if late:
                        parts.append(
                            f"  ⚠ ENTREGAS RETRASADAS ({len(late)}):"
                        )
                        for d in late[:3]:
                            parts.append(
                                f"    - {d['name']}: programada {d['scheduled']}"
                                f" ({d['type']}) origen: {d['origin']}"
                            )
                    on_time = [d for d in deliveries if not d['is_late']]
                    if on_time:
                        for d in on_time[:3]:
                            parts.append(
                                f"  Entrega: {d['name']} programada "
                                f"{d['scheduled']} ({d['type']})"
                            )

                # Manufactura
                mfg = p.get('manufacturing', [])
                if mfg:
                    parts.append(f"  Producción en proceso ({len(mfg)}):")
                    for m in mfg[:3]:
                        parts.append(
                            f"    - {m['name']}: {m['product']} "
                            f"x{m['qty']} ({m['state']}) "
                            f"origen: {m['origin']}"
                        )

                # Reuniones próximas
                meetings = p.get('upcoming_meetings', [])
                if meetings:
                    parts.append(f"  Reuniones próximas ({len(meetings)}):")
                    for ev in meetings[:3]:
                        parts.append(
                            f"    - {ev['name']} ({ev['start']}) "
                            f"con: {', '.join(ev['attendees'][:3])}"
                        )

                # Pagos recientes
                payments = p.get('recent_payments', [])
                if payments:
                    for pay in payments[:3]:
                        direction = (
                            'Cobro recibido' if pay['payment_type'] == 'inbound'
                            else 'Pago emitido'
                        )
                        parts.append(
                            f"  {direction}: {pay['name']} ${pay['amount']:,.0f}"
                            f" {pay['currency']} ({pay['date']})"
                        )

                # Comunicación reciente en Odoo (chatter)
                chatter = p.get('recent_chatter', [])
                related = p.get('related_chatter', [])
                all_msgs = chatter + related
                if all_msgs:
                    parts.append(
                        f"  Comunicación interna Odoo ({len(all_msgs)} msgs "
                        f"en 7d):"
                    )
                    for msg in all_msgs[:5]:
                        parts.append(
                            f"    - [{msg.get('date', '')}] "
                            f"{msg.get('author', '')}: "
                            f"{msg.get('preview', '')[:100]}"
                        )

                sections.append('\n'.join(parts))

        # ── Verificación de acciones (accountability) ───────────────────────
        followup = odoo_ctx.get('action_followup', {})
        if followup.get('items'):
            sections.append(
                f"\n═══ VERIFICACIÓN DE ACCIONES SUGERIDAS ═══\n"
                f"Tasa de completado (7 días): {followup.get('completion_rate', 0)}%\n"
                f"Completadas hoy: {followup.get('completed_today', 0)}\n"
                f"Vencidas sin hacer: {followup.get('overdue_count', 0)}"
            )
            for item in followup['items'][:15]:
                status = '⚠ VENCIDA' if item['is_overdue'] else 'pendiente'
                line = (
                    f"\n  [{item['priority'].upper()}] {item['description'][:100]}"
                    f" ({status})"
                )
                if item.get('assigned_to'):
                    line += f" → {item['assigned_to']}"
                if item.get('partner'):
                    line += f" | Contacto: {item['partner']}"
                if item.get('due_date'):
                    line += f" | Vence: {item['due_date']}"
                line += f" | {item['days_open']}d abierto"

                # Evidencia de acción
                evidence = item.get('evidence_of_action', [])
                if evidence:
                    line += '\n    EVIDENCIA ENCONTRADA:'
                    for ev in evidence[:3]:
                        if ev['type'] == 'chatter_message':
                            line += (
                                f"\n      ✓ Mensaje en Odoo de "
                                f"{ev['author']} ({ev['date']}): "
                                f"{ev['preview'][:80]}"
                            )
                        elif ev['type'] == 'scheduled_activity':
                            line += (
                                f"\n      ✓ Actividad programada: "
                                f"{ev['activity']} para {ev['deadline']} "
                                f"({ev['assigned_to']})"
                            )
                else:
                    line += '\n    ✗ SIN EVIDENCIA DE ACCIÓN'

                sections.append(line)

        # ── Pipeline comercial global ───────────────────────────────────────
        pipeline = odoo_ctx.get('global_pipeline', {})
        if pipeline:
            sections.append(
                f"\n═══ PIPELINE COMERCIAL (CRM) ═══\n"
                f"Total oportunidades: {pipeline.get('total_opportunities', 0)}\n"
                f"Revenue esperado total: "
                f"${pipeline.get('total_expected_revenue', 0):,.0f}"
            )
            for stage, data in pipeline.get('by_stage', {}).items():
                sections.append(
                    f"  {stage}: {data['count']} opps "
                    f"(${data['revenue']:,.0f})"
                )

        # ── Actividades del equipo ──────────────────────────────────────────
        team = odoo_ctx.get('team_activities', {})
        if team:
            sections.append('\n═══ ACTIVIDADES DEL EQUIPO (próximos 3 días) ═══')
            for user_name, data in team.items():
                overdue_str = (
                    f' ⚠ {data["overdue"]} VENCIDAS'
                    if data['overdue'] else ''
                )
                sections.append(
                    f"\n  {user_name}: {data['pending']} pendientes"
                    f"{overdue_str}"
                )
                for act in data['items'][:3]:
                    marker = '⚠' if act['overdue'] else '·'
                    sections.append(
                        f"    {marker} {act['type']}: {act['summary'][:60]} "
                        f"(vence {act['deadline']})"
                    )

        # ── Client scores ───────────────────────────────────────────────────
        if client_scores:
            at_risk = [s for s in client_scores if s['risk_level'] == 'high']
            if at_risk:
                sections.append('\n═══ CLIENTES EN RIESGO ═══')
                for s in at_risk:
                    sections.append(
                        f"{s['email']}: score={s['total_score']}/100 "
                        f"(freq={s['frequency_score']}, "
                        f"resp={s['responsiveness_score']}, "
                        f"recip={s['reciprocity_score']}, "
                        f"sent={s['sentiment_score']})"
                    )

        return '\n'.join(sections)

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

        # Procesar en lotes de 64
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

    # ── Sync Odoo → Supabase contacts ──────────────────────────────────────

    @staticmethod
    def _sync_contacts_to_supabase(odoo_ctx: dict, supa):
        """Sincroniza datos de Odoo partners a Supabase contacts."""
        partners = odoo_ctx.get('partners', {})
        if not partners:
            return
        synced = 0
        for email_addr, p in partners.items():
            try:
                supa.sync_contact_odoo_data(email_addr, {
                    'odoo_partner_id': p.get('id'),
                    'is_customer': p.get('is_customer', False),
                    'is_supplier': p.get('is_supplier', False),
                    'odoo_context': {
                        'name': p.get('name', ''),
                        'total_invoiced': p.get('total_invoiced', 0),
                        'credit_limit': p.get('credit_limit', 0),
                        'recent_sales_count': len(
                            p.get('recent_sales', []),
                        ),
                        'pending_invoices_count': len(
                            p.get('pending_invoices', []),
                        ),
                        'crm_leads_count': len(p.get('crm_leads', [])),
                        'pending_deliveries': len(
                            p.get('pending_deliveries', []),
                        ),
                    },
                })
                synced += 1
            except Exception:
                pass
        if synced:
            _logger.info('✓ %d contactos sincronizados Odoo → Supabase', synced)

    # ── Accountability alerts ────────────────────────────────────────────────

    def _generate_accountability_alerts(self, odoo_ctx, alerts, supa, today):
        """Genera alertas de accountability cuando hay acciones sin cumplir."""
        followup = odoo_ctx.get('action_followup', {})
        if not followup.get('items'):
            return

        ActionItem = self.env['intelligence.action.item'].sudo()

        for item in followup['items']:
            # Auto-complete actions with strong evidence
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
                except Exception:
                    pass
                continue

            # Generate alert for overdue actions WITHOUT evidence
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

        # Save accountability alerts to Supabase
        acct_alerts = [
            a for a in alerts if a.get('alert_type') == 'accountability'
        ]
        if acct_alerts:
            try:
                supa.save_alerts(acct_alerts, today)
            except Exception:
                pass
            _logger.info(
                '✓ %d alertas de accountability generadas', len(acct_alerts),
            )

    # ── Communication Patterns ───────────────────────────────────────────────

    @staticmethod
    def _compute_communication_patterns(
        emails: list, threads: list, today: str,
    ) -> list:
        """Calcula patrones de comunicación POR CONTACTO externo.

        Genera registros compatibles con el schema del frontend:
        communication_patterns(contact_id, pattern_type, description,
                               frequency, confidence)
        """
        from email.utils import parsedate_to_datetime as _pdt

        # Aggregate data per external contact email
        by_contact = defaultdict(lambda: {
            'total': 0, 'hours': defaultdict(int),
            'subjects': defaultdict(int), 'dates': [],
            'replied_threads': 0, 'total_threads': 0,
            'response_times_hrs': [],
        })

        for e in emails:
            sender = e.get('from_email', '')
            if not sender or e.get('sender_type') != 'external':
                continue
            data = by_contact[sender]
            data['total'] += 1
            try:
                dt = _pdt(e.get('date', ''))
                data['hours'][dt.hour] += 1
                data['dates'].append(dt)
            except Exception:
                pass
            subj = e.get('subject_normalized', e.get('subject', ''))
            if subj:
                data['subjects'][subj] += 1

        # Thread participation per contact
        for t in threads:
            for p in t.get('participant_emails', []):
                if p in by_contact:
                    by_contact[p]['total_threads'] += 1
                    if t.get('has_internal_reply'):
                        by_contact[p]['replied_threads'] += 1

        patterns = []
        for email_addr, data in by_contact.items():
            if data['total'] < 1:
                continue

            # Pattern: communication frequency
            total = data['total']
            if total >= 5:
                freq = 'daily'
                desc = f'Contacto muy activo: {total} emails en el periodo'
            elif total >= 3:
                freq = 'weekly'
                desc = f'Contacto frecuente: {total} emails en el periodo'
            else:
                freq = 'monthly'
                desc = f'Contacto ocasional: {total} emails en el periodo'
            patterns.append({
                'contact_email': email_addr,
                'pattern_type': 'communication_frequency',
                'description': desc,
                'frequency': freq,
                'confidence': min(1.0, 0.5 + total * 0.1),
            })

            # Pattern: preferred time
            if data['hours']:
                busiest_hour = max(
                    data['hours'].items(), key=lambda x: x[1],
                )[0]
                patterns.append({
                    'contact_email': email_addr,
                    'pattern_type': 'preferred_time',
                    'description': (
                        f'Suele escribir alrededor de las {busiest_hour}:00 hrs'
                    ),
                    'frequency': 'event_triggered',
                    'confidence': min(
                        1.0, data['hours'][busiest_hour] / max(total, 1),
                    ),
                })

            # Pattern: response rate (how often we reply)
            if data['total_threads'] >= 2:
                rate = data['replied_threads'] / data['total_threads']
                if rate < 0.5:
                    desc = (
                        f'Solo respondemos {rate:.0%} de sus hilos '
                        f'({data["replied_threads"]}/{data["total_threads"]})'
                    )
                    patterns.append({
                        'contact_email': email_addr,
                        'pattern_type': 'response_time',
                        'description': desc,
                        'frequency': 'event_triggered',
                        'confidence': 0.8,
                    })

            # Pattern: recurring topics
            if data['subjects']:
                top_subj = sorted(
                    data['subjects'].items(), key=lambda x: -x[1],
                )[:3]
                repeated = [s for s, c in top_subj if c >= 2]
                if repeated:
                    patterns.append({
                        'contact_email': email_addr,
                        'pattern_type': 'topic_preference',
                        'description': (
                            f'Temas recurrentes: {", ".join(repeated[:3])}'
                        ),
                        'frequency': 'weekly',
                        'confidence': 0.7,
                    })

        return patterns

    # ── System Learning Detection ────────────────────────────────────────────

    @staticmethod
    def _detect_learnings(metrics, alerts, client_scores, odoo_ctx, supa):
        """Detecta patrones y los registra como aprendizajes del sistema."""
        # 1. Detect accounts with degraded response times
        for m in metrics:
            if m.get('avg_response_hours') and m['avg_response_hours'] > 24:
                supa.save_learning(
                    'response_degradation',
                    f"Cuenta {m['account']}: tiempo promedio de respuesta "
                    f"{m['avg_response_hours']}h (>24h)",
                    {'account': m['account'],
                     'avg_hours': m['avg_response_hours'],
                     'unanswered': m.get('threads_unanswered', 0)},
                    account=m['account'],
                )

        # 2. Detect at-risk clients
        at_risk = [
            s for s in client_scores if s.get('risk_level') == 'high'
        ]
        if at_risk:
            supa.save_learning(
                'trend_identified',
                f"{len(at_risk)} clientes en riesgo alto detectados",
                {'clients': [s['email'] for s in at_risk[:10]]},
            )

        # 3. Detect high alert volume (pattern)
        if len(alerts) > 15:
            supa.save_learning(
                'pattern_detected',
                f"Alto volumen de alertas: {len(alerts)} alertas en un día",
                {'alert_count': len(alerts),
                 'types': list({a.get('alert_type') for a in alerts})},
            )

        # 4. Action completion rate insight
        followup = odoo_ctx.get('action_followup', {})
        rate = followup.get('completion_rate', 0)
        if rate < 30 and followup.get('items'):
            supa.save_learning(
                'trend_identified',
                f"Tasa de completado de acciones muy baja: {rate}%",
                {'completion_rate': rate,
                 'overdue': followup.get('overdue_count', 0),
                 'total_pending': len(followup.get('items', []))},
            )
        elif rate > 80 and followup.get('items'):
            supa.save_learning(
                'response_improvement',
                f"Excelente tasa de completado de acciones: {rate}%",
                {'completion_rate': rate},
            )

    # ── Knowledge Graph ──────────────────────────────────────────────────────

    def _feed_knowledge_graph(self, emails, claude, supa, today):
        if not emails:
            return

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
            email_text = self._format_emails_for_claude(acct_emails, {})
            try:
                kg = claude.extract_knowledge(email_text, account)
            except Exception as exc:
                _logger.warning('KG extraction failed for %s: %s', account, exc)
                continue

            # Guardar entidades
            entity_map = {}
            for ent in kg.get('entities', []):
                try:
                    result = supa.upsert_entity(ent)
                    if result and isinstance(result, list) and result:
                        entity_map[ent['name']] = result[0].get('id')
                except Exception:
                    pass

            # Guardar hechos
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
                    except Exception:
                        pass

            # Guardar action items en Supabase + Odoo
            for item in kg.get('action_items', []):
                try:
                    # Supabase
                    assignee_ent = supa.get_entity_by_name(item.get('assignee', ''))
                    related_ent = supa.get_entity_by_name(item.get('related_to', ''))
                    supa.save_action_item({
                        'assignee_entity_id': assignee_ent.get('id') if assignee_ent else None,
                        'assignee_name': item.get('assignee', ''),
                        'related_entity_id': related_ent.get('id') if related_ent else None,
                        'description': item.get('description', ''),
                        'action_type': item.get('type', 'other'),
                        'priority': item.get('priority', 'medium'),
                        'due_date': item.get('due_date'),
                        'source_briefing_date': today,
                        'source_account': account,
                    })
                    # Odoo
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
                    })
                except Exception as exc:
                    _logger.debug('Action item save error: %s', exc)

            # Guardar relaciones
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
                    except Exception:
                        pass

            # Guardar perfiles de personas (aprendizaje acumulativo)
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
            time.sleep(3)

        _logger.info('Knowledge graph alimentado (con perfiles de personas)')

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
            if a.get('account'):
                partner = Partner.search([
                    ('email', 'ilike', a.get('account', ''))
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
                    'risk_level': s.get('risk_level', 'medium'),
                })
        _logger.info('%d client scores en Odoo', len(client_scores))

    @staticmethod
    def _wrap_briefing_html(body_html: str, today: str, weekly: bool = False) -> str:
        """Envuelve el briefing en un template HTML completo para email."""
        title = 'Weekly Intelligence Report' if weekly else 'Daily Intelligence Briefing'
        return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8">
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         max-width: 800px; margin: 0 auto; padding: 20px; color: #1a1a1a;
         line-height: 1.6; }}
  h1 {{ color: #1e3a5f; border-bottom: 3px solid #2563eb; padding-bottom: 10px; }}
  h2 {{ color: #1e3a5f; margin-top: 25px; }}
  h3 {{ color: #374151; }}
  table {{ border-collapse: collapse; width: 100%; margin: 10px 0; }}
  th, td {{ border: 1px solid #d1d5db; padding: 8px 12px; text-align: left; }}
  th {{ background: #f3f4f6; font-weight: 600; }}
  .header {{ background: linear-gradient(135deg, #1e3a5f, #2563eb);
             color: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; }}
  .header h1 {{ color: white; border: none; margin: 0; }}
  .footer {{ margin-top: 30px; padding-top: 15px; border-top: 1px solid #e5e7eb;
             font-size: 0.85em; color: #6b7280; }}
  strong {{ color: #1e3a5f; }}
  ul {{ padding-left: 20px; }}
  li {{ margin-bottom: 5px; }}
</style>
</head>
<body>
<div class="header">
  <h1>Quimibond {title}</h1>
  <p style="margin:5px 0 0;opacity:0.9">{today} — Generado por Intelligence System v19</p>
</div>
{body_html}
<div class="footer">
  <p>Generado automáticamente por <strong>Quimibond Intelligence System</strong> (Odoo 19).<br>
  Powered by Claude AI + Voyage AI + Supabase + Google Workspace.</p>
</div>
</body>
</html>"""

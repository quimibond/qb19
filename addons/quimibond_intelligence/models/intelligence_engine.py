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
        start = time.time()
        today = datetime.now(TZ_CDMX).strftime('%Y-%m-%d')
        _logger.info('═══ QUIMIBOND INTELLIGENCE — %s ═══', today)

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
            emails, claude, odoo_context, account_departments,
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

        alerts = self._generate_alerts(threads, metrics, cfg)
        try:
            supa.save_alerts(alerts, today)
        except Exception as exc:
            _logger.error('Error guardando alertas: %s', exc)

        client_scores = self._compute_client_scores(contacts, emails, threads, cfg)
        try:
            supa.save_client_scores(client_scores, today)
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
        briefing_html = claude.synthesize_briefing(data_package)

        # Extraer temas
        topics = claude.extract_topics(briefing_html)
        _logger.info('%d temas extraídos', len(topics))

        # Guardar briefing
        try:
            supa.save_daily_summary(
                today, briefing_html, len(emails),
                result['success_count'], result['failed_count'], len(topics),
            )
        except Exception as exc:
            _logger.error('Error guardando daily summary: %s', exc)


        # ======================================================================
        #  FASE 7.5: Knowledge Graph — Extraccion de entidades y hechos
        # ======================================================================
        _logger.info('-- FASE 7.5: Knowledge Graph --')
        self._feed_knowledge_graph(emails, claude, supa, today)

        # ══════════════════════════════════════════════════════════════════════
        #  FASE 8: Embeddings (Voyage AI)
        # ══════════════════════════════════════════════════════════════════════
        if cfg.get('voyage_api_key'):
            _logger.info('── FASE 8: Embeddings ──')
            self._generate_embeddings(emails, voyage, supa)

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

        elapsed = time.time() - start
        _logger.info('═══ PIPELINE COMPLETADO en %.1f segundos ═══', elapsed)

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

        claude = ClaudeService(cfg['anthropic_api_key'])
        supa = SupabaseService(cfg['supabase_url'], cfg['supabase_key'])

        # Obtener métricas de últimos 7 días
        try:
            weekly_metrics = supa._request(
                '/rest/v1/response_metrics?order=metric_date.desc&limit=7'
                '&select=*',
            ) or []
        except Exception:
            weekly_metrics = []

        try:
            weekly_alerts = supa._request(
                '/rest/v1/alerts?order=created_at.desc&limit=50'
                '&select=*&created_at=gte.'
                + (datetime.now(TZ_CDMX) - timedelta(days=7)).strftime('%Y-%m-%d'),
            ) or []
        except Exception:
            weekly_alerts = []

        if not weekly_metrics:
            _logger.warning('Sin métricas semanales')
            return

        prompt = (
            f'Genera un REPORTE SEMANAL de Quimibond.\n\n'
            f'MÉTRICAS (últimos 7 días):\n{json.dumps(weekly_metrics, default=str)}\n\n'
            f'ALERTAS DE LA SEMANA:\n{json.dumps(weekly_alerts, default=str)}\n\n'
            'Incluye: tendencias, comparativa día a día, cuentas con mejores/peores '
            'tiempos de respuesta, temas recurrentes, recomendaciones.'
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

        if not all([sa_json, anthropic_key, supa_url, supa_key]):
            _logger.error('Faltan API keys en ir.config_parameter. '
                          'Configura desde Ajustes > Intelligence System.')
            return None

        return {
            'service_account_json': sa_json,
            'anthropic_api_key': anthropic_key,
            'supabase_url': supa_url,
            'supabase_key': supa_key,
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

            # Calcular horas sin respuesta
            hours_no_response = 0
            if last['sender_type'] == 'external':
                try:
                    last_date = datetime.fromisoformat(
                        last['date'].replace('Z', '+00:00')
                    )
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
                'started_at': first.get('date', ''),
                'last_activity': last.get('date', ''),
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
                _logger.debug('Odoo enrichment skip %s: %s', email_addr, exc)

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
                ('start', '>=', datetime.now().strftime('%Y-%m-%d')),
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
                          account_departments: dict = None) -> list:
        """Fase 1 de Claude: análisis por cada cuenta."""
        account_departments = account_departments or {}
        # Agrupar por cuenta
        by_account = defaultdict(list)
        for e in emails:
            by_account[e['account']].append(e)

        summaries = []
        for account, acct_emails in by_account.items():
            dept = account_departments.get(account, 'Otro')
            ext_count = sum(1 for e in acct_emails if e['sender_type'] == 'external')
            int_count = len(acct_emails) - ext_count

            if not acct_emails:
                continue

            # Construir texto de emails con contexto Odoo
            email_text = self._format_emails_for_claude(
                acct_emails, odoo_context,
            )

            try:
                result = claude.summarize_account(
                    dept, account, email_text, ext_count, int_count,
                )
                result['account'] = account
                result['department'] = dept
                result['total_emails'] = len(acct_emails)
                summaries.append(result)
                _logger.info('  ✓ %s (%s): %d emails analizados',
                             dept, account, len(acct_emails))
                time.sleep(3)  # Rate limit courtesy
            except Exception as exc:
                _logger.error('  ✗ %s: %s', account, exc)

        return summaries

    @staticmethod
    def _format_emails_for_claude(emails: list, odoo_ctx: dict) -> str:
        """Formatea emails en texto para el prompt de Claude, con contexto Odoo."""
        lines = []
        for i, e in enumerate(emails, 1):
            lines.append(f'--- EMAIL {i} ---')
            lines.append(f'De: {e["from"]}')
            lines.append(f'Para: {e["to"]}')
            if e.get('cc'):
                lines.append(f'CC: {e["cc"]}')
            lines.append(f'Asunto: {e["subject"]}')
            lines.append(f'Fecha: {e["date"]}')
            lines.append(f'Tipo: {e["sender_type"]}')
            if e['is_reply']:
                lines.append('(Es respuesta)')
            if e['has_attachments']:
                att_names = ', '.join(a['filename'] for a in e.get('attachments', []))
                lines.append(f'Adjuntos: {att_names}')

            # Contexto de negocio de Odoo
            sender_email = e.get('from_email', '')
            biz = odoo_ctx.get('business_summary', {}).get(sender_email)
            if biz:
                lines.append(f'[ODOO: {biz}]')

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

    @staticmethod
    def _generate_alerts(threads: list, metrics: list, cfg: dict) -> list:
        """Genera alertas basadas en umbrales configurables."""
        alerts = []
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

        return alerts

    # ── Client Scoring ────────────────────────────────────────────────────────

    @staticmethod
    def _compute_client_scores(contacts: list, emails: list, threads: list,
                               cfg: dict) -> list:
        """Calcula score de relación 0-100 para contactos externos."""
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

            # Sentiment score (0-25): base neutral, ajustado por alertas
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

    # ── Data Package para síntesis ────────────────────────────────────────────

    @staticmethod
    def _build_data_package(today: str, summaries: list, metrics: list,
                            alerts: list, threads: list, client_scores: list,
                            odoo_ctx: dict, historical: dict) -> str:
        """Construye el paquete de datos completo para Claude fase 2."""
        sections = [
            f'FECHA: {today}',
            f'TOTAL CUENTAS ANALIZADAS: {len(summaries)}',
        ]

        # Contexto histórico
        if historical.get('previousSummary'):
            sections.append(
                f"\nRESUMEN DEL DÍA ANTERIOR:\n{historical['previousSummary'][:1000]}"
            )
        if historical.get('openAlerts'):
            sections.append(
                f"\nALERTAS ABIERTAS PREVIAS:\n"
                + json.dumps(historical['openAlerts'][:10], default=str)
            )

        # Resúmenes por cuenta
        sections.append('\n═══ ANÁLISIS POR CUENTA ═══')
        for s in summaries:
            sections.append(
                f"\n── {s['department']} ({s['account']}) ──\n"
                f"Emails: {s.get('total_emails', 0)} "
                f"(ext:{s.get('external_emails', 0)}, int:{s.get('internal_emails', 0)})\n"
                f"Resumen: {s.get('summary_text', '')}\n"
                f"Sentimiento: {s.get('overall_sentiment', 'N/A')}\n"
                f"Items clave: {json.dumps(s.get('key_items', []), default=str, ensure_ascii=False)}\n"
                f"Esperando respuesta: {json.dumps(s.get('waiting_response', []), default=str, ensure_ascii=False)}\n"
                f"Urgentes: {json.dumps(s.get('urgent_items', []), default=str, ensure_ascii=False)}\n"
                f"Contactos: {json.dumps(s.get('external_contacts', []), default=str, ensure_ascii=False)}\n"
                f"Temas: {json.dumps(s.get('topics_detected', []), default=str, ensure_ascii=False)}\n"
                f"Riesgos: {json.dumps(s.get('risks_detected', []), default=str, ensure_ascii=False)}"
            )

        # Métricas
        sections.append('\n═══ MÉTRICAS DE RESPUESTA ═══')
        for m in metrics:
            sections.append(
                f"{m['account']}: recv={m['emails_received']} sent={m['emails_sent']} "
                f"replied={m['threads_replied']} unanswered={m['threads_unanswered']} "
                f"avg_hrs={m.get('avg_response_hours', 'N/A')}"
            )

        # Alertas
        if alerts:
            sections.append(f'\n═══ ALERTAS ({len(alerts)}) ═══')
            for a in alerts[:20]:
                sections.append(
                    f"[{a['severity'].upper()}] {a['alert_type']}: {a['title']}"
                )

        # Contexto Odoo
        biz = odoo_ctx.get('business_summary', {})
        if biz:
            sections.append('\n═══ CONTEXTO DE NEGOCIO (Odoo ERP) ═══')
            for email_addr, summary in biz.items():
                sections.append(f'{email_addr}: {summary}')

        # Client scores
        if client_scores:
            at_risk = [s for s in client_scores if s['risk_level'] == 'high']
            if at_risk:
                sections.append('\n═══ CLIENTES EN RIESGO ═══')
                for s in at_risk:
                    sections.append(
                        f"{s['email']}: score={s['total_score']}/100 "
                        f"(freq={s['frequency_score']}, resp={s['responsiveness_score']}, "
                        f"recip={s['reciprocity_score']}, sent={s['sentiment_score']})"
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

    # ── HTML wrapper ──────────────────────────────────────────────────────────

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

            batch_ids = [
                e['gmail_message_id'] for e in acct_emails
                if e.get('gmail_message_id')
            ]
            try:
                supa.mark_emails_kg_processed(batch_ids)
            except Exception as exc:
                _logger.warning('KG mark_processed %s: %s', account, exc)
            time.sleep(3)

        _logger.info('Knowledge graph alimentado')

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
  <h1>{'📊' if weekly else '🧠'} Quimibond {title}</h1>
  <p style="margin:5px 0 0;opacity:0.9">{today} — Generado por Intelligence System v19</p>
</div>
{body_html}
<div class="footer">
  <p>Generado automáticamente por <strong>Quimibond Intelligence System</strong> (Odoo 19).<br>
  Powered by Claude AI + Voyage AI + Supabase + Google Workspace.</p>
</div>
</body>
</html>"""

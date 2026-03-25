"""
Engine — Reporting (data retention, weekly analysis)
"""
import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta

from odoo import api, fields, models

_logger = logging.getLogger(__name__)

# ── Zona horaria CDMX ─────────────────────────────────────────────────────────
try:
    from zoneinfo import ZoneInfo
    TZ_CDMX = ZoneInfo('America/Mexico_City')
except ImportError:
    import pytz
    TZ_CDMX = pytz.timezone('America/Mexico_City')


class IntelligenceEngine(models.Model):
    _inherit = 'intelligence.engine'

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

                # ── Verificación cruzada de hechos con Claude ──
                self._verify_low_confidence_facts(supa, cfg)

        except Exception as exc:
            _logger.warning('Supabase retention: %s', exc)

        _logger.info('═══ DATA RETENTION DONE ═══')

    def _verify_low_confidence_facts(self, supa, cfg):
        """Verifica hechos de baja confianza contra emails recientes."""
        try:
            # Leer hechos con confianza baja pero no expirados
            low_facts = supa._request(
                '/rest/v1/facts?confidence=lt.0.4'
                '&expired=eq.false'
                '&verified=eq.false'
                '&order=confidence.asc'
                '&limit=20'
                '&select=id,entity_id,fact_text,fact_type,confidence',
            ) or []

            if not low_facts:
                return

            # Leer emails recientes como contexto
            week_ago = (
                datetime.now() - timedelta(days=7)
            ).strftime('%Y-%m-%d')
            recent_emails = supa._request(
                '/rest/v1/emails?order=email_date.desc'
                '&limit=50'
                '&select=sender,subject,snippet'
                f'&email_date=gte.{week_ago}T00:00:00Z',
            ) or []

            if not recent_emails:
                return

            # Formatear para Claude
            facts_text = '\n'.join(
                f'[ID={f["id"]}] ({f["fact_type"]}, '
                f'confianza={f["confidence"]}): {f["fact_text"]}'
                for f in low_facts
            )
            context = '\n'.join(
                f'De: {e.get("sender", "")} | '
                f'Asunto: {e.get("subject", "")} | '
                f'{(e.get("snippet", "") or "")[:200]}'
                for e in recent_emails[:30]
            )

            from ..services.claude_service import ClaudeService
            claude = ClaudeService(cfg['anthropic_api_key'])
            verifications = claude.verify_facts(facts_text, context)

            confirmed, contradicted = 0, 0
            for v in verifications:
                fact_id = v.get('fact_id')
                verdict = v.get('verdict', 'uncertain')
                if not fact_id:
                    continue

                if verdict == 'confirmed':
                    supa.verify_fact(fact_id, 'claude_cross_reference')
                    confirmed += 1
                elif verdict == 'contradicted':
                    supa._request(
                        f'/rest/v1/facts?id=eq.{fact_id}',
                        'PATCH', {
                            'expired': True,
                            'confidence': 0,
                        },
                        extra_headers={'Prefer': 'return=minimal'},
                    )
                    contradicted += 1

            if confirmed or contradicted:
                _logger.info(
                    'Fact verification: %d confirmed, %d contradicted '
                    'of %d checked',
                    confirmed, contradicted, len(low_facts),
                )
        except Exception as exc:
            _logger.debug('Fact verification: %s', exc)

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

"""
Engine — Predictions (anticipar churn, caída de volumen, oportunidades)

Analiza tendencias históricas de scores y genera alertas predictivas.
Corre semanalmente.
"""
import logging
import time
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

# Umbrales de predicción
CHURN_SCORE_DROP = 15       # Puntos de caída en 30 días para predecir churn
CHURN_WINDOW_DAYS = 30
VOLUME_DROP_THRESHOLD = 0.4  # Caída de 40%+ en facturación → alerta


class IntelligenceEngine(models.Model):
    _inherit = 'intelligence.engine'

    # ══════════════════════════════════════════════════════════════════════════
    #   MICRO-PIPELINE: PREDICTIONS
    # ══════════════════════════════════════════════════════════════════════════

    @api.model
    def run_predictions(self):
        """Analiza tendencias y genera alertas predictivas. Corre semanal."""
        lock = 'quimibond_intelligence.predictions_running'
        ICP = self.env['ir.config_parameter'].sudo()
        if ICP.get_param(lock, 'false') == 'true':
            return
        ICP.set_param(lock, 'true')
        start = time.time()

        try:
            today = fields.Date.today()
            _logger.info('═══ PREDICTIONS ═══')

            churn_count = self._predict_churn(today)
            volume_count = self._predict_volume_drop(today)
            opportunity_count = self._predict_opportunities(today)

            _logger.info(
                '✓ Predictions: %d churn, %d volume_drop, '
                '%d opportunities (%.1fs)',
                churn_count, volume_count, opportunity_count,
                time.time() - start,
            )
        except Exception as exc:
            _logger.error('run_predictions: %s', exc, exc_info=True)
        finally:
            ICP.set_param(lock, 'false')

    # ── Predicción de churn ───────────────────────────────────────────────────

    def _predict_churn(self, today) -> int:
        """Detecta clientes cuyo score baja >15pts en 30 días."""
        Score = self.env['intelligence.client.score'].sudo()
        Alert = self.env['intelligence.alert'].sudo()
        Partner = self.env['res.partner'].sudo()

        cutoff = today - timedelta(days=CHURN_WINDOW_DAYS)
        cutoff_recent = today - timedelta(days=7)

        # Buscar partners con scores recientes Y históricos
        recent_scores = Score.search([
            ('date', '>=', cutoff_recent),
        ], order='partner_id, date desc')

        # Agrupar por partner: score más reciente
        partner_recent = {}
        for s in recent_scores:
            if s.partner_id.id not in partner_recent:
                partner_recent[s.partner_id.id] = s

        alerts_created = 0
        for pid, recent in partner_recent.items():
            # Buscar score más antiguo en la ventana
            old_score = Score.search([
                ('partner_id', '=', pid),
                ('date', '>=', cutoff),
                ('date', '<', cutoff_recent),
            ], limit=1, order='date asc')

            if not old_score:
                continue

            drop = old_score.total_score - recent.total_score
            if drop < CHURN_SCORE_DROP:
                continue

            # Verificar que no exista ya una alerta predictiva reciente
            existing = Alert.search([
                ('partner_id', '=', pid),
                ('alert_type', '=', 'churn_risk'),
                ('create_date', '>=', datetime.combine(
                    cutoff_recent, datetime.min.time())),
            ], limit=1)
            if existing:
                continue

            partner = Partner.browse(pid)
            Alert.create({
                'name': (
                    f'Predicción churn: {partner.name} '
                    f'(score {old_score.total_score}→{recent.total_score}, '
                    f'-{drop}pts en {CHURN_WINDOW_DAYS}d)'
                ),
                'alert_type': 'churn_risk',
                'severity': 'high' if drop > 25 else 'medium',
                'description': (
                    f'El score de relación de {partner.name} ha caído '
                    f'{drop} puntos en los últimos {CHURN_WINDOW_DAYS} días '
                    f'(de {old_score.total_score} a {recent.total_score}). '
                    f'Riesgo: {recent.risk_level or "medium"}. '
                    f'Acción recomendada: contactar proactivamente.'
                ),
                'partner_id': pid,
                'state': 'open',
            })
            alerts_created += 1
            _logger.info(
                '  Churn prediction: %s (-%d pts)',
                partner.name, drop,
            )

        return alerts_created

    # ── Predicción de caída de volumen ────────────────────────────────────────

    def _predict_volume_drop(self, today) -> int:
        """Detecta clientes cuya facturación cae >40% vs período anterior."""
        Partner = self.env['res.partner'].sudo()
        Alert = self.env['intelligence.alert'].sudo()

        try:
            AccountMove = self.env['account.move'].sudo()
        except KeyError:
            return 0

        period_current_start = today - timedelta(days=90)
        period_previous_start = today - timedelta(days=180)

        # Buscar partners con facturación en ambos períodos
        partners = Partner.search([
            ('customer_rank', '>', 0),
            ('email', '!=', False),
        ], limit=500)

        alerts_created = 0
        for partner in partners:
            try:
                # Facturación período actual (últimos 90 días)
                current_invoices = AccountMove.search([
                    ('partner_id', '=', partner.id),
                    ('move_type', '=', 'out_invoice'),
                    ('state', '=', 'posted'),
                    ('invoice_date', '>=', period_current_start),
                ])
                current_total = sum(
                    inv.amount_total for inv in current_invoices)

                # Facturación período anterior (90-180 días)
                previous_invoices = AccountMove.search([
                    ('partner_id', '=', partner.id),
                    ('move_type', '=', 'out_invoice'),
                    ('state', '=', 'posted'),
                    ('invoice_date', '>=', period_previous_start),
                    ('invoice_date', '<', period_current_start),
                ])
                previous_total = sum(
                    inv.amount_total for inv in previous_invoices)

                if previous_total <= 0:
                    continue

                drop_pct = (previous_total - current_total) / previous_total
                if drop_pct < VOLUME_DROP_THRESHOLD:
                    continue

                # No duplicar
                existing = Alert.search([
                    ('partner_id', '=', partner.id),
                    ('alert_type', '=', 'volume_drop'),
                    ('create_date', '>=', datetime.combine(
                        today - timedelta(days=7),
                        datetime.min.time())),
                ], limit=1)
                if existing:
                    continue

                Alert.create({
                    'name': (
                        f'Caída de volumen: {partner.name} '
                        f'(-{drop_pct:.0%} vs trimestre anterior)'
                    ),
                    'alert_type': 'volume_drop',
                    'severity': 'high' if drop_pct > 0.6 else 'medium',
                    'description': (
                        f'La facturación de {partner.name} cayó '
                        f'{drop_pct:.0%} en los últimos 90 días '
                        f'(${current_total:,.0f} vs ${previous_total:,.0f} '
                        f'del trimestre anterior). '
                        f'Investigar causa y contactar.'
                    ),
                    'partner_id': partner.id,
                    'state': 'open',
                })
                alerts_created += 1
            except Exception as exc:
                _logger.debug('volume prediction %s: %s', partner.name, exc)

        return alerts_created

    # ── Detección de oportunidades ────────────────────────────────────────────

    def _predict_opportunities(self, today) -> int:
        """Detecta clientes con score subiendo → oportunidad de crecimiento."""
        Score = self.env['intelligence.client.score'].sudo()
        Alert = self.env['intelligence.alert'].sudo()
        Partner = self.env['res.partner'].sudo()

        cutoff = today - timedelta(days=30)
        cutoff_recent = today - timedelta(days=7)

        recent_scores = Score.search([
            ('date', '>=', cutoff_recent),
        ], order='partner_id, date desc')

        partner_recent = {}
        for s in recent_scores:
            if s.partner_id.id not in partner_recent:
                partner_recent[s.partner_id.id] = s

        alerts_created = 0
        for pid, recent in partner_recent.items():
            if recent.total_score < 70:
                continue

            old_score = Score.search([
                ('partner_id', '=', pid),
                ('date', '>=', cutoff),
                ('date', '<', cutoff_recent),
            ], limit=1, order='date asc')

            if not old_score:
                continue

            increase = recent.total_score - old_score.total_score
            if increase < 10:
                continue

            existing = Alert.search([
                ('partner_id', '=', pid),
                ('alert_type', '=', 'opportunity'),
                ('create_date', '>=', datetime.combine(
                    cutoff_recent, datetime.min.time())),
            ], limit=1)
            if existing:
                continue

            partner = Partner.browse(pid)
            Alert.create({
                'name': (
                    f'Oportunidad: {partner.name} '
                    f'(score {old_score.total_score}→{recent.total_score}, '
                    f'+{increase}pts)'
                ),
                'alert_type': 'opportunity',
                'severity': 'medium',
                'description': (
                    f'El score de {partner.name} subió {increase} puntos '
                    f'en 30 días (de {old_score.total_score} a '
                    f'{recent.total_score}). Señal de engagement positivo. '
                    f'Oportunidad para cross-sell o upsell.'
                ),
                'partner_id': pid,
                'state': 'open',
            })
            alerts_created += 1

        return alerts_created

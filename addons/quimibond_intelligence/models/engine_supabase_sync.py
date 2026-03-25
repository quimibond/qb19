"""
Engine — Supabase Batch Sync (Odoo → Supabase replica)

Odoo es la fuente de verdad para alertas, acciones y scores.
Este cron empuja cambios pendientes a Supabase cada 5 minutos
para que el frontend Next.js los refleje.
"""
import logging
import time
from datetime import datetime

from odoo import api, models

_logger = logging.getLogger(__name__)


class IntelligenceEngine(models.Model):
    _inherit = 'intelligence.engine'

    # ══════════════════════════════════════════════════════════════════════════
    #   MICRO-PIPELINE: SUPABASE BATCH SYNC
    # ══════════════════════════════════════════════════════════════════════════

    @api.model
    def run_supabase_sync(self):
        """Empuja cambios pendientes de Odoo → Supabase. Corre cada 5 min."""
        lock = 'quimibond_intelligence.supabase_sync_running'
        ICP = self.env['ir.config_parameter'].sudo()
        if ICP.get_param(lock, 'false') == 'true':
            return
        ICP.set_param(lock, 'true')
        start = time.time()

        try:
            cfg = self._load_config()
            if not cfg:
                return

            from ..services.supabase_service import SupabaseService

            with SupabaseService(cfg['supabase_url'], cfg['supabase_key']) as supa:
                alerts_synced = self._sync_alerts_to_supabase(supa)
                actions_synced = self._sync_actions_to_supabase(supa)

                if alerts_synced or actions_synced:
                    _logger.info(
                        '✓ Supabase sync: %d alerts, %d actions (%.1fs)',
                        alerts_synced, actions_synced,
                        time.time() - start,
                    )
        except Exception as exc:
            _logger.error('run_supabase_sync: %s', exc, exc_info=True)
        finally:
            ICP.set_param(lock, 'false')

    # ── Alert sync ────────────────────────────────────────────────────────────

    def _sync_alerts_to_supabase(self, supa) -> int:
        """Sync unsynced alerts to Supabase. Returns count synced."""
        alerts = self.env['intelligence.alert'].sudo().search([
            ('supabase_synced', '=', False),
        ], limit=200)

        if not alerts:
            return 0

        synced = 0
        for alert in alerts:
            try:
                if alert.supabase_id and alert.supabase_id > 0:
                    patch = {
                        'state': alert.state,
                        'is_resolved': alert.state in (
                            'resolved', 'dismissed'),
                    }
                    if alert.state == 'resolved' and alert.resolved_date:
                        patch['resolved_at'] = alert.resolved_date.isoformat()
                    if alert.resolution_notes:
                        patch['resolution_notes'] = alert.resolution_notes
                    supa._request(
                        f'/rest/v1/alerts?id=eq.{alert.supabase_id}',
                        'PATCH', patch,
                        extra_headers={'Prefer': 'return=minimal'},
                    )
                    synced += 1
                elif alert.name:
                    # Fallback: match by title
                    from urllib.parse import quote as url_quote
                    encoded = url_quote(alert.name[:200], safe='')
                    patch = {
                        'state': alert.state,
                        'is_resolved': alert.state in (
                            'resolved', 'dismissed'),
                    }
                    if alert.state == 'resolved' and alert.resolved_date:
                        patch['resolved_at'] = alert.resolved_date.isoformat()
                    if alert.resolution_notes:
                        patch['resolution_notes'] = alert.resolution_notes
                    supa._request(
                        f'/rest/v1/alerts?title=eq.{encoded}',
                        'PATCH', patch,
                        extra_headers={'Prefer': 'return=minimal'},
                    )
                    synced += 1

                alert.write({'supabase_synced': True})
            except Exception as exc:
                _logger.debug('sync alert %s: %s', alert.id, exc)

        return synced

    # ── Action sync ───────────────────────────────────────────────────────────

    def _sync_actions_to_supabase(self, supa) -> int:
        """Sync unsynced action items to Supabase. Returns count synced."""
        actions = self.env['intelligence.action.item'].sudo().search([
            ('supabase_synced', '=', False),
        ], limit=200)

        if not actions:
            return 0

        now = datetime.now()
        synced = 0
        for action in actions:
            try:
                if not action.supabase_id or action.supabase_id <= 0:
                    action.write({'supabase_synced': True})
                    continue

                patch = {'state': action.state}
                if action.state == 'done':
                    patch['status'] = 'completed'
                    patch['completed_date'] = now.strftime('%Y-%m-%d')
                    patch['completed_at'] = now.isoformat()
                elif action.state == 'cancelled':
                    patch['status'] = 'cancelled'
                elif action.state == 'in_progress':
                    patch['status'] = 'in_progress'
                elif action.state == 'open':
                    patch['status'] = 'pending'

                supa._request(
                    f'/rest/v1/action_items?id=eq.{action.supabase_id}',
                    'PATCH', patch,
                    extra_headers={'Prefer': 'return=minimal'},
                )
                synced += 1
                action.write({'supabase_synced': True})
            except Exception as exc:
                _logger.debug('sync action %s: %s', action.id, exc)

        return synced

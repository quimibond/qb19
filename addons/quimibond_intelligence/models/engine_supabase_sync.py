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

from .intelligence_config import acquire_lock, release_lock

_logger = logging.getLogger(__name__)

# Mapeo de estados Odoo → Frontend (Supabase)
# Odoo usa 'open', frontend usa 'new' como estado inicial
ALERT_STATE_MAP = {
    'open': 'new',
    'acknowledged': 'acknowledged',
    'resolved': 'resolved',
    'dismissed': 'resolved',  # Frontend no tiene 'dismissed'
}


class IntelligenceEngine(models.Model):
    _inherit = 'intelligence.engine'

    # ══════════════════════════════════════════════════════════════════════════
    #   MICRO-PIPELINE: SUPABASE BATCH SYNC
    # ══════════════════════════════════════════════════════════════════════════

    @api.model
    def run_supabase_sync(self):
        """Empuja cambios pendientes de Odoo → Supabase. Corre cada 5 min."""
        lock = 'quimibond_intelligence.supabase_sync_running'
        if not acquire_lock(self.env, lock):
            return
        start = time.time()

        try:
            cfg = self._load_config()
            if not cfg:
                return

            from ..services.supabase_service import SupabaseService

            with SupabaseService(cfg['supabase_url'], cfg['supabase_key']) as supa:
                alerts_synced = self._sync_alerts_to_supabase(supa)
                actions_synced = self._sync_actions_to_supabase(supa)
                scores_synced = self._sync_scores_to_supabase(supa)

                if alerts_synced or actions_synced or scores_synced:
                    _logger.info(
                        '✓ Supabase sync: %d alerts, %d actions, '
                        '%d scores (%.1fs)',
                        alerts_synced, actions_synced, scores_synced,
                        time.time() - start,
                    )
        except Exception as exc:
            _logger.error('run_supabase_sync: %s', exc, exc_info=True)
        finally:
            release_lock(self.env, lock)

    # ── Alert sync ────────────────────────────────────────────────────────────

    def _sync_alerts_to_supabase(self, supa) -> int:
        """Sync unsynced alerts to Supabase. Returns count synced.

        Maps Odoo states to frontend states:
        open→new, acknowledged→acknowledged, resolved→resolved, dismissed→resolved
        """
        alerts = self.env['intelligence.alert'].sudo().search([
            ('supabase_synced', '=', False),
        ], limit=200)

        if not alerts:
            return 0

        synced = 0
        for alert in alerts:
            try:
                supa_state = ALERT_STATE_MAP.get(alert.state, 'new')
                is_resolved = alert.state in ('resolved', 'dismissed')
                patch = {
                    'state': supa_state,
                }
                if is_resolved and alert.resolved_date:
                    patch['resolved_at'] = alert.resolved_date.isoformat()
                if alert.resolution_notes:
                    patch['resolution_notes'] = alert.resolution_notes

                if alert.supabase_id and alert.supabase_id > 0:
                    supa._request(
                        f'/rest/v1/alerts?id=eq.{alert.supabase_id}',
                        'PATCH', patch,
                        extra_headers={'Prefer': 'return=minimal'},
                    )
                    synced += 1
                elif alert.name:
                    from urllib.parse import quote as url_quote
                    encoded = url_quote(alert.name[:200], safe='')
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

                # Map Odoo states to Supabase states
                state_map = {
                    'done': 'completed',
                    'cancelled': 'dismissed',
                    'in_progress': 'in_progress',
                    'open': 'pending',
                }
                patch = {
                    'state': state_map.get(action.state, 'pending'),
                }
                if action.state == 'done':
                    patch['completed_at'] = now.isoformat()
                # Sync assignee info if available
                if action.assignee_id:
                    patch['assignee_name'] = action.assignee_id.name
                    patch['assignee_email'] = (
                        action.assignee_id.email
                        or action.assignee_id.login
                    )

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

    # ── Score sync ────────────────────────────────────────────────────────────

    def _sync_scores_to_supabase(self, supa) -> int:
        """Sync unsynced client scores to Supabase contacts table.

        Scores se crean en Odoo (fuente de verdad) y se replican como
        relationship_score + risk_level en Supabase contacts para que
        el frontend Next.js los muestre.
        """
        scores = self.env['intelligence.client.score'].sudo().search([
            ('supabase_synced', '=', False),
            ('email', '!=', False),
        ], limit=500, order='date desc')

        if not scores:
            return 0

        from urllib.parse import quote as url_quote

        synced = 0
        seen_emails = set()
        for score in scores:
            raw_email = (score.email or '').lower().strip()
            # Odoo partners can have multiple emails separated by ; or ,
            email = raw_email.split(';')[0].split(',')[0].strip()
            if not email or '@' not in email or email in seen_emails:
                score.write({'supabase_synced': True})
                continue
            seen_emails.add(email)

            try:
                encoded = url_quote(email, safe='')
                patch = {
                    'relationship_score': score.total_score or 0,
                    'risk_level': score.risk_level or 'medium',
                    'sentiment_score': score.sentiment_score or 0,
                    'payment_compliance_score': (
                        score.payment_compliance_score or 0),
                }
                supa._request(
                    f'/rest/v1/contacts?email=eq.{encoded}',
                    'PATCH', patch,
                    extra_headers={'Prefer': 'return=minimal'},
                )
                synced += 1
            except Exception as exc:
                _logger.debug('sync score %s: %s', email, exc)

            score.write({'supabase_synced': True})

        # Mark remaining duplicates as synced
        remaining = scores.filtered(lambda s: not s.supabase_synced)
        if remaining:
            remaining.write({'supabase_synced': True})

        return synced

    # ══════════════════════════════════════════════════════════════════════════
    #   REVERSE SYNC: SUPABASE → ODOO (frontend state changes)
    # ══════════════════════════════════════════════════════════════════════════

    # Mapeo de estados Supabase → Odoo
    REVERSE_ALERT_STATE = {
        'acknowledged': 'acknowledged',
        'resolved': 'resolved',
    }
    REVERSE_ACTION_STATE = {
        'completed': 'done',
        'dismissed': 'cancelled',
        'in_progress': 'in_progress',
    }

    @api.model
    def run_reverse_sync(self):
        """Pull state changes from Supabase → Odoo. Corre cada 5 min.

        When users update alert/action state in the frontend,
        this cron picks up those changes and applies them to Odoo.
        """
        lock = 'quimibond_intelligence.reverse_sync_running'
        if not acquire_lock(self.env, lock):
            return
        start = time.time()

        try:
            cfg = self._load_config()
            if not cfg:
                return

            from ..services.supabase_service import SupabaseService

            with SupabaseService(cfg['supabase_url'], cfg['supabase_key']) as supa:
                alerts_updated = self._reverse_sync_alerts(supa)
                actions_updated = self._reverse_sync_actions(supa)
                commands_run = self._process_sync_commands(supa)

                if alerts_updated or actions_updated or commands_run:
                    _logger.info(
                        '✓ Reverse sync: %d alerts, %d actions, '
                        '%d commands (%.1fs)',
                        alerts_updated, actions_updated, commands_run,
                        time.time() - start,
                    )
        except Exception as exc:
            _logger.error('run_reverse_sync: %s', exc, exc_info=True)
        finally:
            release_lock(self.env, lock)

    def _reverse_sync_alerts(self, supa) -> int:
        """Pull alert state changes from Supabase to Odoo."""
        # Get Odoo alerts that have a supabase_id and are still open
        alerts = self.env['intelligence.alert'].sudo().search([
            ('supabase_id', '>', 0),
            ('state', '=', 'open'),
        ], limit=500)

        if not alerts:
            return 0

        # Batch-fetch current states from Supabase
        supa_ids = [a.supabase_id for a in alerts]
        updated = 0

        # Query in chunks of 50
        for i in range(0, len(supa_ids), 50):
            chunk_ids = supa_ids[i:i + 50]
            ids_str = ','.join(str(x) for x in chunk_ids)
            try:
                rows = supa._request(
                    f'/rest/v1/alerts?id=in.({ids_str})'
                    '&state=neq.new'
                    '&select=id,state,resolved_at,resolution_notes',
                ) or []
            except Exception as exc:
                _logger.debug('reverse_sync_alerts fetch: %s', exc)
                continue

            for row in rows:
                supa_state = row.get('state', '')
                odoo_state = self.REVERSE_ALERT_STATE.get(supa_state)
                if not odoo_state:
                    continue

                odoo_alert = alerts.filtered(
                    lambda a, sid=row['id']: a.supabase_id == sid
                )
                if not odoo_alert:
                    continue

                vals = {
                    'state': odoo_state,
                    'supabase_synced': True,  # Already in sync
                }
                if odoo_state == 'resolved':
                    vals['resolved_date'] = (
                        row.get('resolved_at') or
                        datetime.now().isoformat()
                    )
                if row.get('resolution_notes'):
                    vals['resolution_notes'] = row['resolution_notes']

                try:
                    odoo_alert.write(vals)
                    updated += 1
                except Exception as exc:
                    _logger.debug('reverse alert %s: %s', row['id'], exc)

        return updated

    def _reverse_sync_actions(self, supa) -> int:
        """Pull action state changes from Supabase to Odoo."""
        actions = self.env['intelligence.action.item'].sudo().search([
            ('supabase_id', '>', 0),
            ('state', 'in', ('open', 'in_progress')),
        ], limit=500)

        if not actions:
            return 0

        supa_ids = [a.supabase_id for a in actions]
        updated = 0

        for i in range(0, len(supa_ids), 50):
            chunk_ids = supa_ids[i:i + 50]
            ids_str = ','.join(str(x) for x in chunk_ids)
            try:
                rows = supa._request(
                    f'/rest/v1/action_items?id=in.({ids_str})'
                    '&state=neq.pending'
                    '&select=id,state,completed_at',
                ) or []
            except Exception as exc:
                _logger.debug('reverse_sync_actions fetch: %s', exc)
                continue

            for row in rows:
                supa_state = row.get('state', '')
                odoo_state = self.REVERSE_ACTION_STATE.get(supa_state)
                if not odoo_state:
                    continue

                odoo_action = actions.filtered(
                    lambda a, sid=row['id']: a.supabase_id == sid
                )
                if not odoo_action:
                    continue

                vals = {
                    'state': odoo_state,
                    'supabase_synced': True,
                }

                try:
                    odoo_action.write(vals)
                    updated += 1
                except Exception as exc:
                    _logger.debug('reverse action %s: %s', row['id'], exc)

        return updated

    # ── Sync Commands (frontend → Odoo dispatch) ──────────────────────────

    # Commands the frontend can request
    ALLOWED_COMMANDS = {
        'run_sync_emails',
        'run_analyze_emails',
        'run_enrich_only',
        'run_update_scores',
        'run_supabase_sync',
        'run_sync_odoo_tables',
        'run_daily_intelligence',
        'run_predictions',
    }

    def _process_sync_commands(self, supa) -> int:
        """Check sync_commands table for pending commands from frontend."""
        try:
            rows = supa._request(
                '/rest/v1/sync_commands?status=eq.pending'
                '&order=created_at.asc&limit=5'
                '&select=id,command',
            ) or []
        except Exception as exc:
            _logger.debug('process_sync_commands fetch: %s', exc)
            return 0

        if not rows:
            return 0

        executed = 0
        engine = self.env['intelligence.engine']
        for row in rows:
            cmd = row.get('command', '')
            cmd_id = row['id']

            # Mark as running
            try:
                supa._request(
                    f'/rest/v1/sync_commands?id=eq.{cmd_id}',
                    'PATCH', {
                        'status': 'running',
                        'started_at': datetime.now().isoformat(),
                    },
                    extra_headers={'Prefer': 'return=minimal'},
                )
            except Exception:
                pass

            if cmd not in self.ALLOWED_COMMANDS:
                _logger.warning('sync_commands: unknown command %s', cmd)
                try:
                    supa._request(
                        f'/rest/v1/sync_commands?id=eq.{cmd_id}',
                        'PATCH', {
                            'status': 'failed',
                            'completed_at': datetime.now().isoformat(),
                            'result': {'error': f'Unknown command: {cmd}'},
                        },
                        extra_headers={'Prefer': 'return=minimal'},
                    )
                except Exception:
                    pass
                continue

            # Execute the command
            start = time.time()
            try:
                fn = getattr(engine, cmd)
                fn()
                elapsed = round(time.time() - start, 1)
                supa._request(
                    f'/rest/v1/sync_commands?id=eq.{cmd_id}',
                    'PATCH', {
                        'status': 'completed',
                        'completed_at': datetime.now().isoformat(),
                        'result': {'elapsed_s': elapsed},
                    },
                    extra_headers={'Prefer': 'return=minimal'},
                )
                executed += 1
                _logger.info('sync_command %s completed (%.1fs)', cmd, elapsed)
            except Exception as exc:
                _logger.error('sync_command %s failed: %s', cmd, exc)
                try:
                    supa._request(
                        f'/rest/v1/sync_commands?id=eq.{cmd_id}',
                        'PATCH', {
                            'status': 'failed',
                            'completed_at': datetime.now().isoformat(),
                            'result': {'error': str(exc)[:500]},
                        },
                        extra_headers={'Prefer': 'return=minimal'},
                    )
                except Exception:
                    pass

        return executed

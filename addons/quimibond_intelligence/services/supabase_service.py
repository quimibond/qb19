"""
Quimibond Intelligence — Supabase Service
Unified Supabase persistence layer.
Inherits from base client + domain-specific mixins.
"""
import logging

from .supabase_base import SupabaseBaseClient
from .supabase_contacts import SupabaseContactsMixin
from .supabase_emails import SupabaseEmailsMixin
from .supabase_kg import SupabaseKGMixin
from .supabase_metrics import SupabaseMetricsMixin

_logger = logging.getLogger(__name__)


class SupabaseService(
    SupabaseBaseClient,
    SupabaseEmailsMixin,
    SupabaseContactsMixin,
    SupabaseKGMixin,
    SupabaseMetricsMixin,
):
    """Cliente para Supabase REST API (PostgREST)."""

    # ── Events (event sourcing) ───────────────────────────────────────────

    def log_event(self, event_type: str, source: str = 'pipeline',
                  entity_type: str = None, entity_id: int = None,
                  entity_ref: str = None, payload: dict = None):
        """Log an event to pipeline_logs for timeline tracking."""
        try:
            self._request('/rest/v1/pipeline_logs', 'POST', {
                'level': 'info',
                'phase': event_type,
                'message': f'{source}: {entity_type or ""} {entity_ref or ""}',
                'details': {
                    'source': source,
                    'entity_type': entity_type,
                    'entity_id': entity_id,
                    'entity_ref': entity_ref,
                    **(payload or {}),
                },
            })
        except Exception as exc:
            _logger.debug('log_event: %s', exc)

    # ── Sync State (Gmail history) ────────────────────────────────────────────

    def save_sync_state(self, account: str, history_id: str):
        """Persiste el historyId de Gmail en Supabase sync_state."""
        try:
            self._request(
                '/rest/v1/sync_state?on_conflict=account',
                'POST', {
                    'account': account,
                    'last_history_id': history_id,
                    'emails_synced': 0,
                }, {
                    'Prefer': 'resolution=merge-duplicates,return=minimal',
                })
        except Exception as exc:
            _logger.debug('save_sync_state: %s', exc)

    def get_sync_state(self) -> dict:
        """Carga todos los sync states: {account → history_id}."""
        try:
            rows = self._request(
                '/rest/v1/sync_state?select=account,last_history_id',
            )
            if isinstance(rows, list):
                return {
                    r['account']: r['last_history_id']
                    for r in rows if r.get('last_history_id')
                }
        except Exception as exc:
            _logger.debug('get_sync_state: %s', exc)
        return {}

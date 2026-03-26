"""
Quimibond Intelligence — Supabase Service
Unified Supabase persistence layer.
Inherits from base client + domain-specific mixins.
"""
import logging
import uuid
from datetime import datetime

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

    # ── Pipeline Runs ──────────────────────────────────────────────────────

    def start_pipeline_run(self, run_type: str) -> str:
        """Create a pipeline_runs record and return its ID."""
        run_id = str(uuid.uuid4())
        try:
            self._request('/rest/v1/pipeline_runs', 'POST', {
                'id': run_id,
                'run_type': run_type,
                'status': 'running',
                'started_at': datetime.now().isoformat(),
                'emails_processed': 0,
                'alerts_generated': 0,
                'actions_generated': 0,
                'errors': [],
                'metadata': {},
            })
        except Exception as exc:
            _logger.debug('start_pipeline_run: %s', exc)
        return run_id

    def complete_pipeline_run(self, run_id: str, status: str = 'completed',
                              metadata: dict = None, errors: list = None):
        """Update a pipeline_runs record as completed/failed."""
        try:
            from urllib.parse import quote as _q
            patch = {
                'status': status,
                'completed_at': datetime.now().isoformat(),
            }
            if metadata:
                patch['metadata'] = metadata
            if errors:
                patch['errors'] = errors
            self._request(
                f'/rest/v1/pipeline_runs?id=eq.{_q(run_id, safe="")}',
                'PATCH', patch,
                extra_headers={'Prefer': 'return=minimal'},
            )
        except Exception as exc:
            _logger.debug('complete_pipeline_run: %s', exc)

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

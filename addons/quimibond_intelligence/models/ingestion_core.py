"""
IngestionCore: thin wrapper over the ingestion.* Postgres RPCs.

Usage:
    client = SupabaseRPCClient(url, service_key)  # or SupabaseClient with .rpc()
    core = IngestionCore(client)
    run_id, watermark = core.start_run('odoo', 'odoo_invoices', 'incremental', 'cron')
    # ... do the sync, call report_batch / report_failure as you go ...
    core.complete_run(run_id, 'success', new_watermark)

The `client` object must expose a single method:
    rpc(name: str, params: dict) -> response
where `response` is whatever the RPC returned (dict, list, scalar, or None).
"""
import logging
from typing import Any, Optional

_logger = logging.getLogger(__name__)


class IngestionCore:
    def __init__(self, rpc_client: Any):
        self._c = rpc_client

    # 1. start_run → (run_id, last_watermark)
    def start_run(self, source: str, table: str, run_type: str,
                  triggered_by: str) -> tuple[str, Optional[str]]:
        resp = self._c.rpc('ingestion_start_run', {
            'p_source': source,
            'p_table': table,
            'p_run_type': run_type,
            'p_triggered_by': triggered_by,
        })
        # PostgREST returns a list of rows for table-returning functions
        row = resp[0] if isinstance(resp, list) and resp else resp or {}
        return row.get('run_id'), row.get('last_watermark')

    # 2. report_batch
    def report_batch(self, run_id: str, attempted: int,
                     succeeded: int, failed: int) -> None:
        self._c.rpc('ingestion_report_batch', {
            'p_run_id': run_id,
            'p_attempted': attempted,
            'p_succeeded': succeeded,
            'p_failed': failed,
        })

    # 3. report_failure → failure_id
    def report_failure(self, run_id: str, entity_id: str, error_code: str,
                       error_detail: str, payload: Optional[dict]) -> str:
        return self._c.rpc('ingestion_report_failure', {
            'p_run_id': run_id,
            'p_entity_id': str(entity_id),
            'p_error_code': error_code,
            'p_error_detail': error_detail or '',
            'p_payload': payload,
        })

    # 4. complete_run
    def complete_run(self, run_id: str, status: str,
                     high_watermark: Optional[str]) -> None:
        self._c.rpc('ingestion_complete_run', {
            'p_run_id': run_id,
            'p_status': status,
            'p_high_watermark': high_watermark,
        })

    # 5. report_source_count → reconciliation_id
    def report_source_count(self, source: str, table: str,
                            window_start: str, window_end: str,
                            source_count: Optional[int],
                            missing_entity_ids: Optional[list]) -> str:
        return self._c.rpc('ingestion_report_source_count', {
            'p_source': source,
            'p_table': table,
            'p_window_start': window_start,
            'p_window_end': window_end,
            'p_source_count': source_count,
            'p_missing_entity_ids': missing_entity_ids,
        })

    # 6. fetch_pending_failures → list of failure rows
    def fetch_pending_failures(self, source: str, table: str,
                               max_retries: int, limit: int) -> list:
        resp = self._c.rpc('ingestion_fetch_pending_failures', {
            'p_source': source,
            'p_table': table,
            'p_max_retries': max_retries,
            'p_limit': limit,
        })
        return resp if isinstance(resp, list) else []

    # 7. mark_resolved
    def mark_resolved(self, failure_id: str) -> None:
        self._c.rpc('ingestion_mark_failure_resolved', {
            'p_failure_id': failure_id,
        })

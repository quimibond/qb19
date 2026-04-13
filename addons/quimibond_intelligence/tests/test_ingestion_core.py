"""
Unit tests for IngestionCore. Uses an in-memory fake Supabase client
so we don't hit the network. Run with:
  cd /Users/jj/addons && pytest quimibond_intelligence/tests/test_ingestion_core.py -v
"""
import pytest
from unittest.mock import MagicMock
from quimibond_intelligence.models.ingestion_core import IngestionCore


class FakeRPCClient:
    """Captures RPC calls and returns canned responses."""
    def __init__(self):
        self.calls = []
        self.responses = {}

    def rpc(self, name, params):
        self.calls.append((name, params))
        if name in self.responses:
            resp = self.responses[name]
            return resp(params) if callable(resp) else resp
        return None


def test_start_run_returns_run_id_and_watermark():
    client = FakeRPCClient()
    client.responses['ingestion_start_run'] = [
        {'run_id': 'run-123', 'last_watermark': '2026-04-12T10:00:00Z'}
    ]
    core = IngestionCore(client)

    run_id, wm = core.start_run('odoo', 'odoo_invoices', 'incremental', 'cron')

    assert run_id == 'run-123'
    assert wm == '2026-04-12T10:00:00Z'
    assert client.calls[0][0] == 'ingestion_start_run'
    assert client.calls[0][1] == {
        'p_source': 'odoo',
        'p_table': 'odoo_invoices',
        'p_run_type': 'incremental',
        'p_triggered_by': 'cron',
    }


def test_start_run_handles_null_watermark():
    client = FakeRPCClient()
    client.responses['ingestion_start_run'] = [{'run_id': 'r1', 'last_watermark': None}]
    core = IngestionCore(client)
    run_id, wm = core.start_run('odoo', 'odoo_invoices', 'full', 'manual')
    assert run_id == 'r1'
    assert wm is None


def test_report_batch_sends_counters():
    client = FakeRPCClient()
    client.responses['ingestion_report_batch'] = None
    core = IngestionCore(client)
    core.report_batch('run-1', 200, 195, 5)
    assert client.calls[-1] == (
        'ingestion_report_batch',
        {'p_run_id': 'run-1', 'p_attempted': 200, 'p_succeeded': 195, 'p_failed': 5},
    )


def test_report_failure_sends_payload_and_returns_id():
    client = FakeRPCClient()
    client.responses['ingestion_report_failure'] = 'failure-xyz'
    core = IngestionCore(client)
    fid = core.report_failure(
        'run-1', 'E42', 'http_4xx', 'bad request',
        {'id': 42, 'name': 'inv-0042'}
    )
    assert fid == 'failure-xyz'
    assert client.calls[-1][1]['p_entity_id'] == 'E42'
    assert client.calls[-1][1]['p_payload'] == {'id': 42, 'name': 'inv-0042'}


def test_complete_run_sends_status_and_watermark():
    client = FakeRPCClient()
    client.responses['ingestion_complete_run'] = None
    core = IngestionCore(client)
    core.complete_run('run-1', 'partial', '2026-04-12T11:00:00Z')
    assert client.calls[-1] == (
        'ingestion_complete_run',
        {'p_run_id': 'run-1', 'p_status': 'partial', 'p_high_watermark': '2026-04-12T11:00:00Z'},
    )


def test_report_source_count_handles_missing_ids_list():
    client = FakeRPCClient()
    client.responses['ingestion_report_source_count'] = 'rec-1'
    core = IngestionCore(client)
    rid = core.report_source_count(
        'odoo', 'odoo_invoices',
        '2026-04-01T00:00:00Z', '2026-04-12T00:00:00Z',
        100, ['inv-1', 'inv-2']
    )
    assert rid == 'rec-1'
    call = client.calls[-1][1]
    assert call['p_source_count'] == 100
    assert call['p_missing_entity_ids'] == ['inv-1', 'inv-2']


def test_fetch_pending_failures_returns_list():
    client = FakeRPCClient()
    client.responses['ingestion_fetch_pending_failures'] = [
        {'failure_id': 'f1', 'entity_id': 'E1', 'payload_snapshot': {'x': 1}},
        {'failure_id': 'f2', 'entity_id': 'E2', 'payload_snapshot': None},
    ]
    core = IngestionCore(client)
    results = core.fetch_pending_failures('odoo', 'odoo_invoices', max_retries=3, limit=50)
    assert len(results) == 2
    assert results[0]['failure_id'] == 'f1'


def test_mark_resolved_sends_failure_id():
    client = FakeRPCClient()
    client.responses['ingestion_mark_failure_resolved'] = None
    core = IngestionCore(client)
    core.mark_resolved('f1')
    assert client.calls[-1] == ('ingestion_mark_failure_resolved', {'p_failure_id': 'f1'})

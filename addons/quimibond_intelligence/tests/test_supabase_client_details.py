"""
Tests for upsert_with_details. Uses a mocked httpx.Client so we don't hit network.
"""
from unittest.mock import MagicMock
import httpx
from quimibond_intelligence.models.supabase_client import SupabaseClient


def _make_client(mock_http):
    c = SupabaseClient('https://x.supabase.co', 'svc-key')
    c._http = mock_http
    return c


def test_upsert_with_details_all_success():
    mock = MagicMock()
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = [{'id': 1}, {'id': 2}, {'id': 3}]
    mock.post.return_value = response

    c = _make_client(mock)
    rows = [{'id': 1}, {'id': 2}, {'id': 3}]
    ok, failed = c.upsert_with_details('odoo_invoices', rows, 'id', batch_size=100)

    assert ok == 3
    assert failed == []


def test_upsert_with_details_batch_failure_records_each_row():
    mock = MagicMock()
    resp_fail = MagicMock()
    resp_fail.status_code = 400
    resp_fail.text = 'schema mismatch: column "foo" does not exist'
    # httpx.Response.raise_for_status() raises if status >= 400
    resp_fail.raise_for_status.side_effect = httpx.HTTPStatusError(
        'bad request', request=MagicMock(), response=resp_fail)
    mock.post.return_value = resp_fail

    c = _make_client(mock)
    rows = [{'id': 10}, {'id': 11}]
    ok, failed = c.upsert_with_details('odoo_invoices', rows, 'id', batch_size=100)

    assert ok == 0
    assert len(failed) == 2
    item0, err0 = failed[0]
    assert item0 == {'id': 10}
    assert err0['code'].startswith('http_4xx')
    assert 'schema mismatch' in err0['detail']


def test_rpc_strict_posts_to_rpc_endpoint():
    mock = MagicMock()
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = [{'run_id': 'r1', 'last_watermark': None}]
    mock.post.return_value = resp

    c = _make_client(mock)
    result = c.rpc_strict('ingestion_start_run', {
        'p_source': 'odoo', 'p_table': 'odoo_invoices',
        'p_run_type': 'incremental', 'p_triggered_by': 'cron',
    })

    assert result == [{'run_id': 'r1', 'last_watermark': None}]
    call_args = mock.post.call_args
    assert '/rest/v1/rpc/ingestion_start_run' in call_args[0][0]


def test_rpc_lenient_returns_none_on_error():
    """The lenient rpc() catches HTTP errors and returns None to preserve
    backward compatibility with existing callers in sync_push.py."""
    mock = MagicMock()
    resp_fail = MagicMock()
    resp_fail.status_code = 500
    resp_fail.text = 'internal server error'
    resp_fail.raise_for_status.side_effect = httpx.HTTPStatusError(
        'boom', request=MagicMock(), response=resp_fail)
    mock.post.return_value = resp_fail

    c = _make_client(mock)
    result = c.rpc('resolve_all_identities', {})
    assert result is None  # lenient catches and returns None

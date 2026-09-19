"""
Tests for SupabaseClient. Uses a mocked httpx.Client so we don't hit network.

Correr con: cd addons && python3 -m pytest quimibond_intelligence/tests
"""
from unittest.mock import MagicMock
import httpx
import pytest
from quimibond_intelligence.models.supabase_client import SupabaseClient, SupabaseError


def _make_client(mock_http):
    c = SupabaseClient('https://x.supabase.co', 'svc-key')
    c._http = mock_http
    return c


def test_upsert_counts_every_row_on_success():
    mock = MagicMock()
    response = MagicMock()
    response.status_code = 200
    mock.post.return_value = response

    c = _make_client(mock)
    rows = [{'odoo_user_id': 1}, {'odoo_user_id': 2}, {'odoo_user_id': 3}]
    synced = c.upsert('odoo_users', rows, on_conflict='odoo_user_id', batch_size=2)

    assert synced == 3
    assert mock.post.call_count == 2  # 2 chunks: [1, 2] + [3]
    assert mock.post.call_args[1]['params'] == {'on_conflict': 'odoo_user_id'}


def test_upsert_does_not_retry_4xx_and_reports_lost_rows():
    mock = MagicMock()
    resp_fail = MagicMock()
    resp_fail.status_code = 400
    resp_fail.text = 'schema mismatch: column "foo" does not exist'
    resp_fail.raise_for_status.side_effect = httpx.HTTPStatusError(
        'bad request', request=MagicMock(), response=resp_fail)
    mock.post.return_value = resp_fail

    c = _make_client(mock)
    synced = c.upsert('contacts', [{'email': 'a@x.com'}, {'email': 'b@x.com'}],
                      on_conflict='email', batch_size=100)

    assert synced == 0
    assert mock.post.call_count == 1  # 4xx no se reintenta


def test_rpc_lenient_returns_none_on_error():
    """The lenient rpc() catches HTTP errors and returns None so a transient
    failure never crashes the push."""
    mock = MagicMock()
    resp_fail = MagicMock()
    resp_fail.status_code = 500
    resp_fail.text = 'internal server error'
    resp_fail.raise_for_status.side_effect = httpx.HTTPStatusError(
        'boom', request=MagicMock(), response=resp_fail)
    mock.post.return_value = resp_fail

    c = _make_client(mock)
    result = c.rpc('memoria_brief', {'p_odoo_partner_id': 1})
    assert result is None  # lenient catches and returns None


def test_rpc_strict_returns_json_on_2xx():
    mock = MagicMock()
    resp = MagicMock(status_code=200, content=b'{"ok": true, "nuevas": 2}')
    resp.json.return_value = {'ok': True, 'nuevas': 2}
    mock.post.return_value = resp
    c = _make_client(mock)
    assert c.rpc_strict('senales_ingestar', {'p_senal': 'x'}) == {'ok': True, 'nuevas': 2}
    assert mock.post.call_args[0][0].endswith('/rest/v1/rpc/senales_ingestar')


def test_rpc_strict_raises_on_http_error():
    mock = MagicMock()
    mock.post.return_value = MagicMock(status_code=400, content=b'{"message":"bad"}', text='{"message":"bad"}')
    c = _make_client(mock)
    with pytest.raises(SupabaseError) as exc:
        c.rpc_strict('senales_ingestar', {})
    assert 'HTTP 400' in str(exc.value) and 'bad' in str(exc.value)


def test_rpc_strict_raises_on_network_error():
    mock = MagicMock()
    mock.post.side_effect = httpx.ConnectError('down')
    c = _make_client(mock)
    with pytest.raises(SupabaseError):
        c.rpc_strict('senales_ingestar', {})

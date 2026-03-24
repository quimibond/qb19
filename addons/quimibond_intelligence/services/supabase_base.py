"""
Quimibond Intelligence — Supabase Base Client
Cliente HTTP compartido para Supabase REST API (PostgREST).
Connection pooling + retry para errores transitorios.
"""
import json
import logging
import time

import httpx

_logger = logging.getLogger(__name__)

_RETRY_STATUSES = {429, 502, 503}
_MAX_RETRIES = 3


class SupabaseBaseClient:
    """Base client for Supabase PostgREST API with connection reuse and retry."""

    def __init__(self, url: str, key: str, timeout: int = 30):
        self._url = url.rstrip('/')
        self._key = key
        self._headers = {
            'apikey': key,
            'Authorization': f'Bearer {key}',
            'Content-Type': 'application/json',
        }
        self._client = httpx.Client(timeout=timeout)
        self._sync_stats = {'success': 0, 'failed': 0, 'skipped': 0}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    @property
    def sync_stats(self) -> dict:
        """Returns sync statistics for the current session."""
        return dict(self._sync_stats)

    def _track(self, success: int = 0, failed: int = 0, skipped: int = 0):
        """Increment sync stats counters."""
        self._sync_stats['success'] += success
        self._sync_stats['failed'] += failed
        self._sync_stats['skipped'] += skipped

    def _request(self, path: str, method: str = 'GET',
                 payload=None, extra_headers: dict = None):
        """HTTP request with retry for transient errors (429, 502, 503)."""
        headers = {**self._headers, **(extra_headers or {})}
        last_exc = None
        for attempt in range(_MAX_RETRIES):
            try:
                resp = self._client.request(
                    method, f'{self._url}{path}',
                    headers=headers,
                    json=payload if payload else None,
                )
                if resp.status_code in _RETRY_STATUSES and attempt < _MAX_RETRIES - 1:
                    wait = 2 ** attempt
                    _logger.warning(
                        'Supabase %d on %s %s, retry in %ds',
                        resp.status_code, method, path[:80], wait,
                    )
                    time.sleep(wait)
                    continue
                if 200 <= resp.status_code < 300:
                    text = resp.text
                    try:
                        return json.loads(text) if text else None
                    except json.JSONDecodeError:
                        return text
                raise RuntimeError(
                    f'Supabase {resp.status_code}: {resp.text[:300]}'
                )
            except httpx.TransportError as exc:
                last_exc = exc
                if attempt < _MAX_RETRIES - 1:
                    wait = 2 ** attempt
                    _logger.warning(
                        'Supabase transport error on %s %s, retry in %ds: %s',
                        method, path[:80], wait, exc,
                    )
                    time.sleep(wait)
                    continue
                raise RuntimeError(
                    f'Supabase transport error after {_MAX_RETRIES} attempts: '
                    f'{last_exc}'
                ) from last_exc
        raise RuntimeError(f'Supabase request failed: {last_exc}')

    def _upsert_batch(self, path: str, batch: list, resolution: str):
        """POST batch with Prefer header for conflict resolution.

        Validates records against sync_schema if available (logs warnings
        for unknown columns — does NOT block the write).
        """
        if batch:
            self._validate_batch(path, batch)
        self._request(path, 'POST', batch, {
            'Prefer': f'resolution={resolution},return=minimal',
        })

    def _validate_batch(self, path: str, batch: list):
        """Log warnings for records with unknown columns (possible typos).

        Extracts table name from the PostgREST path (e.g. /rest/v1/emails?...)
        and checks the first record against sync_schema.SUPABASE_SCHEMAS.
        Only runs in debug mode to avoid overhead in production.
        """
        if not _logger.isEnabledFor(logging.DEBUG) or not batch:
            return
        try:
            from .sync_schema import validate_record
            # Extract table name: /rest/v1/TABLE_NAME?...
            table = path.split('/rest/v1/')[-1].split('?')[0].strip('/')
            if not table:
                return
            warnings = validate_record(table, batch[0])
            for w in warnings:
                _logger.debug('Schema validation: %s', w)
        except Exception:
            pass  # Never block writes due to validation errors

    def close(self):
        """Close the underlying HTTP client."""
        self._client.close()

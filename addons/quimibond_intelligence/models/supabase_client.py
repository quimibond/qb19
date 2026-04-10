"""
Minimal Supabase REST client for Odoo.
Only needs httpx — no anthropic, no google-auth, no gmail.
"""
import json
import logging
import httpx

_logger = logging.getLogger(__name__)


class SupabaseClient:
    """Stateless Supabase REST API client."""

    def __init__(self, url: str, service_key: str):
        self.url = url.rstrip('/')
        self.headers = {
            'apikey': service_key,
            'Authorization': f'Bearer {service_key}',
            'Content-Type': 'application/json',
        }
        self._http = httpx.Client(timeout=60, headers=self.headers)

    def upsert(self, table: str, rows: list, on_conflict: str,
               batch_size: int = 200) -> int:
        """Upsert rows into a Supabase table with retry. Returns total rows synced."""
        if not rows:
            return 0
        synced = 0
        for i in range(0, len(rows), batch_size):
            chunk = rows[i:i + batch_size]
            last_exc = None
            for attempt in range(4):  # 0, 1, 2, 3
                try:
                    if attempt > 0:
                        import time
                        time.sleep(min(2 ** attempt, 16))
                        _logger.info('Retry %d/3 upsert %s chunk %d', attempt, table, i)
                    resp = self._http.post(
                        f'{self.url}/rest/v1/{table}',
                        content=json.dumps(chunk, default=str),
                        headers={
                            **self.headers,
                            'Prefer': 'resolution=merge-duplicates,return=minimal',
                        },
                        params={'on_conflict': on_conflict},
                    )
                    if resp.status_code in (429, 502, 503, 504) and attempt < 3:
                        last_exc = Exception(f'HTTP {resp.status_code}')
                        continue
                    if resp.status_code >= 400:
                        body = resp.text[:500]
                        _logger.error(
                            'upsert %s chunk %d (%d rows) HTTP %d: %s',
                            table, i, len(chunk), resp.status_code, body,
                        )
                    resp.raise_for_status()
                    synced += len(chunk)
                    last_exc = None
                    break
                except (httpx.NetworkError, httpx.TimeoutException) as exc:
                    last_exc = exc
                    if attempt >= 3:
                        _logger.warning('upsert %s chunk %d failed after retries: %s',
                                        table, i, exc)
                except Exception as exc:
                    _logger.warning('upsert %s chunk %d (%d rows): %s',
                                    table, i, len(chunk), exc)
                    break  # Don't retry non-retryable errors
            if last_exc:
                _logger.warning('upsert %s chunk %d gave up after retries: %s',
                                table, i, last_exc)
        lost = len(rows) - synced
        if lost > 0:
            _logger.error(
                'upsert %s: %d/%d rows LOST (%.1f%% failure rate)',
                table, lost, len(rows), 100.0 * lost / len(rows),
            )
        return synced

    def insert(self, table: str, rows: list, batch_size: int = 200) -> int:
        """Plain INSERT (no upsert) with retry. For full-refresh tables."""
        if not rows:
            return 0
        synced = 0
        for i in range(0, len(rows), batch_size):
            chunk = rows[i:i + batch_size]
            last_exc = None
            for attempt in range(4):  # 0, 1, 2, 3
                try:
                    if attempt > 0:
                        import time
                        time.sleep(min(2 ** attempt, 16))
                        _logger.info('Retry %d/3 insert %s chunk %d', attempt, table, i)
                    resp = self._http.post(
                        f'{self.url}/rest/v1/{table}',
                        content=json.dumps(chunk, default=str),
                        headers={**self.headers, 'Prefer': 'return=minimal'},
                    )
                    if resp.status_code in (429, 502, 503, 504) and attempt < 3:
                        last_exc = Exception(f'HTTP {resp.status_code}')
                        continue
                    resp.raise_for_status()
                    synced += len(chunk)
                    last_exc = None
                    break
                except (httpx.NetworkError, httpx.TimeoutException) as exc:
                    last_exc = exc
                    if attempt >= 3:
                        _logger.warning('insert %s chunk %d failed after retries: %s',
                                        table, i, exc)
                except Exception as exc:
                    _logger.warning('insert %s chunk %d (%d rows): %s',
                                    table, i, len(chunk), exc)
                    break  # Don't retry non-retryable errors
            if last_exc:
                _logger.warning('insert %s chunk %d gave up after retries: %s',
                                table, i, last_exc)
        return synced

    def delete_all(self, table: str) -> None:
        """Delete all rows from a table (for full refresh tables like activities)."""
        try:
            resp = self._http.delete(
                f'{self.url}/rest/v1/{table}',
                headers={**self.headers, 'Prefer': 'return=minimal'},
                params={'id': 'gt.0'},  # match all rows
            )
            resp.raise_for_status()
        except Exception as exc:
            _logger.warning('delete_all %s: %s', table, exc)

    def fetch(self, table: str, params: dict = None) -> list:
        """Fetch rows from a Supabase table."""
        try:
            resp = self._http.get(
                f'{self.url}/rest/v1/{table}',
                headers={**self.headers, 'Prefer': 'return=representation'},
                params=params or {},
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            _logger.warning('fetch %s: %s', table, exc)
            return []

    def patch(self, table: str, filters: str, data: dict) -> None:
        """Patch rows matching filter."""
        try:
            resp = self._http.patch(
                f'{self.url}/rest/v1/{table}?{filters}',
                content=json.dumps(data, default=str),
                headers={**self.headers, 'Prefer': 'return=minimal'},
            )
            resp.raise_for_status()
        except Exception as exc:
            _logger.warning('patch %s: %s', table, exc)

    def rpc(self, function: str, params: dict) -> dict | None:
        """Call a Supabase RPC function."""
        try:
            resp = self._http.post(
                f'{self.url}/rest/v1/rpc/{function}',
                content=json.dumps(params, default=str),
                headers=self.headers,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            _logger.warning('rpc %s: %s', function, exc)
            return None

    def close(self):
        self._http.close()

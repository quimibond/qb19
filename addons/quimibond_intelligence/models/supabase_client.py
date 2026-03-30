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
        """Upsert rows into a Supabase table. Returns total rows synced."""
        if not rows:
            return 0
        synced = 0
        for i in range(0, len(rows), batch_size):
            chunk = rows[i:i + batch_size]
            try:
                resp = self._http.post(
                    f'{self.url}/rest/v1/{table}',
                    content=json.dumps(chunk, default=str),
                    headers={
                        **self.headers,
                        'Prefer': f'resolution=merge-duplicates,return=minimal',
                    },
                    params={'on_conflict': on_conflict},
                )
                resp.raise_for_status()
                synced += len(chunk)
            except Exception as exc:
                _logger.warning('upsert %s chunk %d (%d rows): %s',
                                table, i, len(chunk), exc)
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

    def close(self):
        self._http.close()

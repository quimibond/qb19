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

    def upsert_with_details(self, table: str, rows: list, on_conflict: str,
                            batch_size: int = 200) -> tuple:
        """
        Upsert rows and return (success_count, [(row, error_dict), ...]).

        Unlike upsert(), this never swallows errors. Every batch that fails
        after retries is reported individually so the caller can record each
        lost row via IngestionCore.report_failure.

        error_dict has keys: code (str), detail (str), status (int).

        SP5.5 (2026-04-22): retries 429/502/503/504 and NetworkError/
        TimeoutException with exponential backoff (same pattern as upsert()).
        Previously a single transient 5xx permanently failed all 200 rows in
        the sub-batch, which for _push_invoices amplified into a 200-row
        report_failure fan-out and tanked the whole run.
        """
        if not rows:
            return 0, []
        import time
        ok_count = 0
        failed = []
        url = f"{self.url}/rest/v1/{table}?on_conflict={on_conflict}"
        headers = {**self.headers, 'Prefer': 'resolution=merge-duplicates,return=minimal'}

        for i in range(0, len(rows), batch_size):
            chunk = rows[i:i + batch_size]
            last_err = None  # (code, detail, status)
            for attempt in range(4):  # 0, 1, 2, 3
                try:
                    if attempt > 0:
                        time.sleep(min(2 ** attempt, 16))
                        _logger.info('Retry %d/3 upsert_with_details %s chunk %d',
                                     attempt, table, i)
                    response = self._http.post(
                        url, headers=headers,
                        content=json.dumps(chunk, default=str),
                    )
                    if response.status_code in (429, 502, 503, 504) and attempt < 3:
                        last_err = (
                            f"http_{response.status_code // 100}xx",
                            (response.text or '')[:4000],
                            response.status_code,
                        )
                        continue
                    response.raise_for_status()
                    ok_count += len(chunk)
                    last_err = None
                    break
                except httpx.HTTPStatusError as e:
                    status = e.response.status_code
                    if 400 <= status < 500 and len(chunk) > 1:
                        # El upsert es atómico: una sola fila inválida (p.ej.
                        # 23505 en un UNIQUE secundario como
                        # uq_odoo_invoices_cfdi_uuid) tiraba las 200 filas del
                        # sub-batch. 2026-09-16: dos XML de proveedor cargados
                        # dos veces en Odoo dejaron 500 facturas de septiembre
                        # sin sincronizar. Reintentamos fila por fila para
                        # perder solo la culpable.
                        ok_rows, failed_rows = self._upsert_rows_individually(
                            table, url, headers, chunk, i,
                        )
                        ok_count += ok_rows
                        failed.extend(failed_rows)
                        last_err = None
                        break
                    # 4xx en fila única (o 5xx tras reintentos): falla el batch.
                    last_err = (
                        f"http_{status // 100}xx",
                        (e.response.text or '')[:4000],
                        status,
                    )
                    break
                except (httpx.NetworkError, httpx.TimeoutException) as e:
                    last_err = ('network_error', str(e)[:4000], 0)
                    if attempt >= 3:
                        break
                    continue
                except httpx.RequestError as e:
                    last_err = ('network_error', str(e)[:4000], 0)
                    break
            if last_err:
                code, detail, status = last_err
                for row in chunk:
                    failed.append((row, {
                        'code': code,
                        'detail': detail,
                        'status': status,
                    }))
        return ok_count, failed

    def _upsert_one(self, url: str, headers: dict, row: dict):
        """Upsert a single row. Returns None on success, else (code, detail, status)."""
        import time
        for attempt in range(3):
            try:
                if attempt > 0:
                    time.sleep(min(2 ** attempt, 8))
                response = self._http.post(
                    url, headers=headers, content=json.dumps([row], default=str),
                )
                if response.status_code in (429, 502, 503, 504) and attempt < 2:
                    continue
                response.raise_for_status()
                return None
            except httpx.HTTPStatusError as e:
                return (
                    f"http_{e.response.status_code // 100}xx",
                    (e.response.text or '')[:4000],
                    e.response.status_code,
                )
            except (httpx.NetworkError, httpx.TimeoutException) as e:
                if attempt >= 2:
                    return ('network_error', str(e)[:4000], 0)
            except httpx.RequestError as e:
                return ('network_error', str(e)[:4000], 0)
        return ('http_5xx', 'exhausted retries', 0)

    def _upsert_rows_individually(self, table: str, url: str, headers: dict,
                                  rows: list, chunk_offset: int) -> tuple:
        """
        Fallback cuando un sub-batch entero recibe 4xx: sube las filas una por
        una y devuelve (ok_count, [(row, error_dict), ...]) solo con las que
        de verdad fallan.

        Caso especial — CFDI UUID duplicado (23505 en uq_odoo_invoices_cfdi_uuid):
        pasa cuando el mismo XML de proveedor se captura dos veces en Odoo. La
        fila se sube SIN uuid para no perder la factura, y se reporta con
        code='cfdi_uuid_duplicate' y el payload ya sin uuid: así queda visible
        en ingestion.sync_failure y el retry la marca resuelta en vez de
        chocar otra vez. La corrección real es cancelar el duplicado en Odoo.
        """
        ok = 0
        failed = []
        for row in rows:
            err = self._upsert_one(url, headers, row)
            if err is None:
                ok += 1
                continue
            code, detail, status = err
            if '23505' in detail and 'cfdi_uuid' in detail and row.get('cfdi_uuid'):
                degraded = dict(row, cfdi_uuid=None)
                if self._upsert_one(url, headers, degraded) is None:
                    _logger.warning(
                        'upsert %s: CFDI uuid %s duplicado en Odoo (fila %s); '
                        'sincronizada sin uuid. Cancelar el duplicado en Odoo.',
                        table, row.get('cfdi_uuid'), row.get('name') or row.get('id'),
                    )
                    failed.append((degraded, {
                        'code': 'cfdi_uuid_duplicate',
                        'detail': detail,
                        'status': status,
                    }))
                    continue
            failed.append((row, {'code': code, 'detail': detail, 'status': status}))
        _logger.warning(
            'upsert %s chunk %d: 4xx en batch, reintento fila por fila: %d ok, %d fallidas',
            table, chunk_offset, ok, len(failed),
        )
        return ok, failed

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

    def delete(self, table: str, filters: dict) -> None:
        """Delete rows matching PostgREST filters.

        Example: client.delete('odoo_payment_invoice_links',
                               {'odoo_payment_id': 'in.(1,2,3)'})
        """
        if not filters:
            raise ValueError("delete requires at least one filter (safety)")
        try:
            resp = self._http.delete(
                f'{self.url}/rest/v1/{table}',
                headers={**self.headers, 'Prefer': 'return=minimal'},
                params=filters,
            )
            resp.raise_for_status()
        except Exception as exc:
            _logger.warning('delete %s %s: %s', table, filters, exc)

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

    def rpc(self, function: str, params: dict):
        """
        Call a Supabase RPC function (lenient).
        Catches exceptions and returns None on error. Use this for ad-hoc
        / fire-and-forget RPC calls where transient failures should not
        crash the caller. For ingestion-critical RPCs use rpc_strict().
        """
        try:
            resp = self._http.post(
                f'{self.url}/rest/v1/rpc/{function}',
                content=json.dumps(params or {}, default=str),
                headers=self.headers,
            )
            resp.raise_for_status()
            if resp.status_code == 204 or not resp.content:
                return None
            return resp.json()
        except Exception as exc:
            _logger.warning('rpc %s: %s', function, exc)
            return None

    def rpc_strict(self, name: str, params: dict):
        """
        Call a Postgres function via PostgREST RPC endpoint (strict).
        Raises on any HTTP error so the caller can record the failure.
        Used by IngestionCore where every error must propagate.
        """
        url = f"{self.url}/rest/v1/rpc/{name}"
        response = self._http.post(
            url,
            content=json.dumps(params or {}, default=str),
            headers=self.headers,
        )
        response.raise_for_status()
        if response.status_code == 204 or not response.content:
            return None
        return response.json()

    def count_exact(self, table: str, params: dict = None) -> int:
        """COUNT exacto via PostgREST. Usa header Prefer: count=exact."""
        try:
            resp = self._http.get(
                f'{self.url}/rest/v1/{table}',
                params=params or {},
                headers={**self.headers,
                         'Prefer': 'count=exact',
                         'Range-Unit': 'items',
                         'Range': '0-0'},
            )
            resp.raise_for_status()
            cr = resp.headers.get('Content-Range', '')
            # formato: "0-0/1234"
            if '/' in cr:
                total = cr.split('/')[-1]
                if total.isdigit():
                    return int(total)
            return len(resp.json() or [])
        except Exception as exc:
            _logger.warning('count_exact %s: %s', table, exc)
            return 0

    def fetch_all(self, table: str, params: dict = None,
                  page_size: int = 1000) -> list:
        """Fetch con paginación automática."""
        out = []
        offset = 0
        while True:
            p = dict(params or {})
            p.setdefault('limit', str(page_size))
            p['offset'] = str(offset)
            try:
                resp = self._http.get(
                    f'{self.url}/rest/v1/{table}',
                    params=p, headers=self.headers,
                )
                resp.raise_for_status()
                batch = resp.json() or []
            except Exception as exc:
                _logger.warning('fetch_all %s offset %d: %s', table, offset, exc)
                break
            out.extend(batch)
            if len(batch) < page_size:
                break
            offset += page_size
        return out

    def close(self):
        self._http.close()

"""
Sync audit — Fase 1 cuantitativa.

Compara métricas Odoo↔Supabase via invariantes cross-check y orquesta
invariantes internos SQL en Supabase. Persiste resultados en audit_runs.

Spec: docs/superpowers/specs/2026-04-19-sync-audit-design.md
"""
import json
import logging
import traceback
import uuid
from datetime import datetime, date

from odoo import api, fields, models
from .supabase_client import SupabaseClient

_logger = logging.getLogger(__name__)

# Defaults si audit_tolerances no tiene fila para el invariant_key
DEFAULT_ABS_TOLERANCE = 0.01
DEFAULT_PCT_TOLERANCE = 0.001

# Lista de invariantes Odoo-side disponibles; scope=None ejecuta todos
ALL_ODOO_SCOPES = [
    'products',
    'invoice_lines',
    'order_lines',
    'deliveries',
    'manufacturing',
    'account_balances',
    'bank_balances',
]


class SyncAudit(models.TransientModel):
    _name = 'quimibond.sync.audit'
    _description = 'Quimibond Sync Audit (Fase 1 — cuantitativa)'

    # ---------------------------------------------------------------
    # Configuración y cliente Supabase
    # ---------------------------------------------------------------
    def _get_client(self) -> SupabaseClient:
        ICP = self.env['ir.config_parameter'].sudo()
        url = ICP.get_param('quimibond_intelligence.supabase_url')
        key = ICP.get_param('quimibond_intelligence.supabase_service_key')
        if not url or not key:
            raise ValueError('Supabase URL/key no configurados en ir.config_parameter')
        return SupabaseClient(url, key)

    def _get_tolerances(self, client: SupabaseClient) -> dict:
        """Carga tolerancias desde Supabase, devuelve dict keyed por invariant_key."""
        rows = client.fetch('audit_tolerances') or []
        return {r['invariant_key']: r for r in rows}

    # ---------------------------------------------------------------
    # Helpers de escritura a audit_runs
    # ---------------------------------------------------------------
    def _severity_for(self, diff: float, expected: float,
                     tol_abs: float, tol_pct: float) -> str:
        """Clasifica diff vs tolerancias. `expected` se usa para % (denominador)."""
        a = abs(diff or 0.0)
        if a <= tol_abs:
            return 'ok'
        denom = abs(expected or 0.0)
        if denom > 0 and (a / denom) <= tol_pct:
            return 'ok'
        # Dos cubetas: warn para < 10x tolerancia, error para más
        if a <= 10 * tol_abs:
            return 'warn'
        return 'error'

    def _record_cross(self, client, run_id, model, invariant_key, bucket_key,
                     odoo_value, supabase_value, tolerances, date_from, date_to,
                     details=None, dry_run=False):
        """Graba una medición cross-check en audit_runs. Devuelve severity."""
        odoo_v = float(odoo_value or 0)
        supa_v = float(supabase_value or 0)
        diff = odoo_v - supa_v
        tol = tolerances.get(invariant_key, {})
        tol_abs = float(tol.get('abs_tolerance', DEFAULT_ABS_TOLERANCE))
        tol_pct = float(tol.get('pct_tolerance', DEFAULT_PCT_TOLERANCE))
        expected = odoo_v if odoo_v != 0 else supa_v
        severity = self._severity_for(diff, expected, tol_abs, tol_pct)
        row = {
            'run_id': run_id,
            'source': 'odoo',
            'model': model,
            'invariant_key': invariant_key,
            'bucket_key': bucket_key,
            'odoo_value': odoo_v,
            'supabase_value': supa_v,
            'diff': diff,
            'severity': severity,
            'date_from': str(date_from) if date_from else None,
            'date_to': str(date_to) if date_to else None,
            'details': details or {},
        }
        if not dry_run:
            client.upsert('audit_runs', [row],
                          on_conflict='run_id,source,model,invariant_key,bucket_key')
        return severity

    def _record_error(self, client, run_id, model, invariant_key, exception,
                     date_from, date_to, dry_run=False):
        """Graba un error de ejecución de invariante."""
        row = {
            'run_id': run_id,
            'source': 'odoo',
            'model': model,
            'invariant_key': invariant_key,
            'bucket_key': None,
            'odoo_value': None,
            'supabase_value': None,
            'diff': None,
            'severity': 'error',
            'date_from': str(date_from) if date_from else None,
            'date_to': str(date_to) if date_to else None,
            'details': {
                'exception': str(exception),
                'traceback': traceback.format_exc()[-4000:],
            },
        }
        if not dry_run:
            client.upsert('audit_runs', [row],
                          on_conflict='run_id,source,model,invariant_key,bucket_key')

    # ---------------------------------------------------------------
    # Helpers de queries Supabase (agregados)
    # ---------------------------------------------------------------
    def _supabase_count(self, client, table, filters=None) -> int:
        """COUNT via Supabase. Usa Prefer: count=exact y Range."""
        # Implementación simple: traer 1 fila y leer header Content-Range
        params = dict(filters or {})
        params['select'] = 'id'
        params['limit'] = '1'
        # Hack: postgrest devuelve total en Content-Range cuando prefer count=exact
        # Usamos client.fetch existente que sólo devuelve body; añadimos método
        return client.count_exact(table, params)

    def _supabase_sum_group(self, client, table, agg_expr, group_by,
                            filters=None) -> dict:
        """SUM/COUNT agrupado via PostgREST. Devuelve {bucket_key: value}."""
        params = dict(filters or {})
        params['select'] = f'{group_by},{agg_expr}'
        # PostgREST no agrupa por defecto; usamos RPC o view materializada.
        # Para MVP: traer filas y agrupar en Python, con paginación.
        rows = client.fetch_all(table, params)
        out = {}
        for r in rows:
            key = '|'.join(str(r.get(g, '')) for g in group_by.split(','))
            out[key] = float(r.get(agg_expr.split(':')[-1], 0) or 0)
        return out

    # ---------------------------------------------------------------
    # Orquestador
    # ---------------------------------------------------------------
    def run_all(self, date_from, date_to, scope=None, dry_run=False):
        """
        Ejecuta todos los invariantes (Odoo-side + Supabase-side).

        :param date_from: 'YYYY-MM-DD' o date
        :param date_to: 'YYYY-MM-DD' o date
        :param scope: None=todos, o lista de nombres en ALL_ODOO_SCOPES
        :param dry_run: si True, no escribe a audit_runs
        :return: {'run_id': str, 'summary': {'ok':N,'warn':N,'error':N}}
        """
        run_id = str(uuid.uuid4())
        client = self._get_client()
        tolerances = self._get_tolerances(client) if not dry_run else {}

        effective_scope = ALL_ODOO_SCOPES if scope is None else list(scope)

        _logger.info('sync_audit run_id=%s scope=%s from=%s to=%s dry_run=%s',
                     run_id, effective_scope, date_from, date_to, dry_run)

        for name in effective_scope:
            method = getattr(self, f'audit_{name}', None)
            if not method:
                _logger.warning('audit: scope %s sin método, skip', name)
                continue
            try:
                method(client, run_id, date_from, date_to, tolerances, dry_run)
            except Exception as exc:
                _logger.exception('audit %s falló: %s', name, exc)
                self._record_error(client, run_id, name, f'{name}.orchestrator',
                                   exc, date_from, date_to, dry_run)

        # Disparar invariantes internos SQL
        if not dry_run:
            try:
                client.rpc('run_internal_audits', {
                    'p_date_from': str(date_from),
                    'p_date_to': str(date_to),
                    'p_run_id': run_id,
                })
            except Exception as exc:
                _logger.exception('run_internal_audits RPC falló: %s', exc)

        summary = self._summarize(client, run_id) if not dry_run else {
            'ok': 0, 'warn': 0, 'error': 0
        }
        # Log a sync_log para visibilidad
        self.env['quimibond.sync.log'].sudo().create({
            'name': 'audit',
            'direction': 'push',
            'status': 'error' if summary.get('error', 0) > 0 else 'success',
            'summary': f"run_id={run_id} {json.dumps(summary)}",
        })
        return {'run_id': run_id, 'summary': summary}

    def _summarize(self, client, run_id) -> dict:
        rows = client.fetch('audit_runs',
                            {'run_id': f'eq.{run_id}', 'select': 'severity'}) or []
        counts = {'ok': 0, 'warn': 0, 'error': 0}
        for r in rows:
            counts[r['severity']] = counts.get(r['severity'], 0) + 1
        return counts

    # ---------------------------------------------------------------
    # Métodos audit_* — stubs (se implementan en Tasks 1.x)
    # ---------------------------------------------------------------
    def audit_products(self, client, run_id, date_from, date_to, tolerances, dry_run):
        pass

    def audit_invoice_lines(self, client, run_id, date_from, date_to,
                            tolerances, dry_run):
        pass

    def audit_order_lines(self, client, run_id, date_from, date_to,
                          tolerances, dry_run):
        pass

    def audit_deliveries(self, client, run_id, date_from, date_to,
                         tolerances, dry_run):
        pass

    def audit_manufacturing(self, client, run_id, date_from, date_to,
                            tolerances, dry_run):
        pass

    def audit_account_balances(self, client, run_id, date_from, date_to,
                                tolerances, dry_run):
        pass

    def audit_bank_balances(self, client, run_id, date_from, date_to,
                            tolerances, dry_run):
        pass

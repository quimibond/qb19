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
from datetime import datetime, date, timedelta

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
            # audit_runs.bucket_key is NOT NULL; use empty string for
            # snapshot invariants (products.*) that don't bucket by period.
            'bucket_key': bucket_key if bucket_key is not None else '',
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
            'bucket_key': '',
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
            # Savepoint so a SQL failure in one invariant does not abort the
            # outer transaction and cascade into every following method
            # (psycopg "current transaction is aborted" chain).
            try:
                with self.env.cr.savepoint():
                    method(client, run_id, date_from, date_to,
                           tolerances, dry_run)
            except Exception as exc:
                _logger.exception('audit %s falló: %s', name, exc)
                self._record_error(client, run_id, name, f'{name}.orchestrator',
                                   exc, date_from, date_to, dry_run)

        # Disparar invariantes internos SQL
        if not dry_run:
            try:
                client.rpc_strict('run_internal_audits', {
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

    def run_audit_last_year(self):
        """Convenience wrapper called by cron and UI action.

        The ir.cron / ir.actions.server `code` field runs in safe_eval
        where `from datetime import ...` is forbidden. Keep the date
        arithmetic here so the XML only does `model.run_audit_last_year()`.
        """
        today = date.today()
        start = today - timedelta(days=365)
        return self.run_all(date_from=str(start), date_to=str(today))

    # ---------------------------------------------------------------
    # CLASS-LEVEL CONSTANTS para audit_account_balances
    # ---------------------------------------------------------------
    # Catálogo SAT MX: códigos con formato XXX.YY.ZZ (ej 115.01.01).
    # Inventario = 115.* (NO 1150 como en el plan genérico Odoo).
    # CMV = 5* (501 costo ventas, 503 devoluciones compras, 505 compras MP).
    # Ingresos = 4* (401 ventas, 402 devoluciones, 403 ingresos principal).
    ACCOUNT_GROUPS = {
        'account_balances.inventory_accounts_balance': ("115%",),
        'account_balances.cogs_accounts_balance': ("5%",),
        'account_balances.revenue_accounts_balance': ("4%",),
    }

    # ---------------------------------------------------------------
    # Métodos audit_* — implementados en Tasks 1.1–1.7
    # ---------------------------------------------------------------
    def audit_products(self, client, run_id, date_from, date_to,
                       tolerances, dry_run):
        """Invariantes 1-4: snapshot de productos."""
        Product = self.env['product.product']

        # 1. count_active
        odoo_count = Product.search_count([('active', '=', True)])
        supa_count = client.count_exact('odoo_products',
                                        {'active': 'eq.true'})
        self._record_cross(client, run_id, 'products', 'products.count_active',
                          None, odoo_count, supa_count,
                          tolerances, date_from, date_to, dry_run=dry_run)

        # 2. count_with_default_code
        odoo_with_code = Product.search_count([
            ('active', '=', True), ('default_code', '!=', False),
        ])
        supa_with_code = client.count_exact('odoo_products', {
            'active': 'eq.true',
            'internal_ref': 'not.is.null',
        })
        self._record_cross(client, run_id, 'products',
                          'products.count_with_default_code',
                          None, odoo_with_code, supa_with_code,
                          tolerances, date_from, date_to, dry_run=dry_run)

        # 3. sum_standard_price
        # Odoo 17+: standard_price is a company-dependent field stored as
        # jsonb on product.template, so raw SQL SUM fails. Use ORM instead.
        active_products = Product.search([('active', '=', True)])
        odoo_sum = float(sum(active_products.mapped('standard_price') or [0]))
        supa_rows = client.fetch_all('odoo_products', {
            'active': 'eq.true', 'select': 'standard_price',
        })
        supa_sum = sum(float(r.get('standard_price') or 0) for r in supa_rows)
        self._record_cross(client, run_id, 'products',
                          'products.sum_standard_price',
                          None, odoo_sum, supa_sum,
                          tolerances, date_from, date_to, dry_run=dry_run)

        # 4. null_uom_count
        odoo_null_uom = Product.search_count([
            ('active', '=', True), ('uom_id', '=', False),
        ])
        supa_null_uom = client.count_exact('odoo_products', {
            'active': 'eq.true', 'uom_id': 'is.null',
        })
        self._record_cross(client, run_id, 'products',
                          'products.null_uom_count',
                          None, odoo_null_uom, supa_null_uom,
                          tolerances, date_from, date_to, dry_run=dry_run)

    def audit_invoice_lines(self, client, run_id, date_from, date_to,
                            tolerances, dry_run):
        """Invariantes 5-7: por bucket (year-month, move_type, company).

        Dos fixes respecto al plan original:
        1. LATERAL subquery para currency_rate (evita cartesiano: LEFT JOIN
           con `rcr.name <= invoice_date` multiplicaba cada línea por el
           número de tasas históricas disponibles).
        2. display_type filter replica lo que hace _push_invoice_lines:
           `NOT IN ('line_section','line_note','payment_term','tax','rounding')`
           tratando NULL como incluido.
        """
        self.env.cr.execute("""
            SELECT to_char(am.invoice_date, 'YYYY-MM') AS ym,
                   am.move_type,
                   am.company_id,
                   COUNT(*) AS cnt,
                   SUM(
                     CASE WHEN am.move_type IN ('out_refund','in_refund')
                          THEN -1 ELSE 1 END
                     * aml.price_subtotal
                     * COALESCE(
                         CASE WHEN am.currency_id = rc_mxn.id THEN 1.0
                              ELSE rcr.rate END,
                         1.0)
                   ) AS sum_mxn,
                   SUM(
                     CASE WHEN am.move_type IN ('out_refund','in_refund')
                          THEN -1 ELSE 1 END
                     * aml.quantity
                   ) AS sum_qty
            FROM account_move_line aml
            JOIN account_move am ON aml.move_id = am.id
            JOIN res_currency rc_mxn ON rc_mxn.name = 'MXN'
            LEFT JOIN LATERAL (
              SELECT rate
              FROM res_currency_rate
              WHERE currency_id = am.currency_id
                AND company_id = am.company_id
                AND name <= am.invoice_date
              ORDER BY name DESC
              LIMIT 1
            ) rcr ON true
            WHERE am.state = 'posted'
              AND am.move_type IN ('out_invoice','out_refund',
                                   'in_invoice','in_refund')
              AND am.invoice_date BETWEEN %s AND %s
              AND (aml.display_type IS NULL
                   OR aml.display_type NOT IN
                      ('line_section','line_note','payment_term','tax','rounding'))
            GROUP BY ym, am.move_type, am.company_id
        """, (date_from, date_to))
        odoo_buckets = {}
        for ym, move_type, company_id, cnt, sum_mxn, sum_qty in self.env.cr.fetchall():
            key = f'{ym}|{move_type}|{company_id}'
            odoo_buckets[key] = {
                'count': int(cnt or 0),
                'sum_mxn': float(sum_mxn or 0),
                'sum_qty': float(sum_qty or 0),
            }

        # -- Supabase side: via view v_audit_invoice_lines_buckets --
        supa_rows = client.fetch_all('v_audit_invoice_lines_buckets', {
            'date_from': f'gte.{date_from}',
            'date_to': f'lte.{date_to}',
        })
        supa_buckets = {r['bucket_key']: r for r in supa_rows}

        all_keys = set(odoo_buckets) | set(supa_buckets)
        for key in all_keys:
            o = odoo_buckets.get(key, {'count': 0, 'sum_mxn': 0, 'sum_qty': 0})
            s = supa_buckets.get(key, {'count': 0, 'sum_subtotal_mxn': 0,
                                       'sum_qty': 0})
            self._record_cross(
                client, run_id, 'invoice_lines',
                'invoice_lines.count_per_bucket', key,
                o['count'], s.get('count', 0), tolerances,
                date_from, date_to, dry_run=dry_run)
            self._record_cross(
                client, run_id, 'invoice_lines',
                'invoice_lines.sum_subtotal_signed_mxn', key,
                o['sum_mxn'], s.get('sum_subtotal_mxn', 0), tolerances,
                date_from, date_to, dry_run=dry_run)
            self._record_cross(
                client, run_id, 'invoice_lines',
                'invoice_lines.sum_qty_signed', key,
                o['sum_qty'], s.get('sum_qty', 0), tolerances,
                date_from, date_to, dry_run=dry_run)

    def audit_order_lines(self, client, run_id, date_from, date_to,
                          tolerances, dry_run):
        """Invariantes 8-10: sale + purchase separados."""
        # SALE — LATERAL subquery evita cartesiano con currency_rate
        self.env.cr.execute("""
            SELECT to_char(so.date_order, 'YYYY-MM') AS ym,
                   'sale' AS otype,
                   so.company_id,
                   COUNT(*) AS cnt,
                   SUM(sol.price_subtotal
                       * COALESCE(
                           CASE WHEN so.currency_id = rc_mxn.id THEN 1.0
                                ELSE rcr.rate END,
                           1.0)) AS sum_mxn,
                   SUM(sol.product_uom_qty) AS sum_qty
            FROM sale_order_line sol
            JOIN sale_order so ON sol.order_id = so.id
            JOIN res_currency rc_mxn ON rc_mxn.name = 'MXN'
            LEFT JOIN LATERAL (
              SELECT rate
              FROM res_currency_rate
              WHERE currency_id = so.currency_id
                AND company_id = so.company_id
                AND name <= so.date_order::date
              ORDER BY name DESC
              LIMIT 1
            ) rcr ON true
            WHERE so.state IN ('sale','done')
              AND so.date_order::date BETWEEN %s AND %s
            GROUP BY ym, so.company_id
        """, (date_from, date_to))
        rows_sale = self.env.cr.fetchall()

        # PURCHASE — mismo patrón LATERAL
        self.env.cr.execute("""
            SELECT to_char(po.date_order, 'YYYY-MM') AS ym,
                   'purchase' AS otype,
                   po.company_id,
                   COUNT(*) AS cnt,
                   SUM(pol.price_subtotal
                       * COALESCE(
                           CASE WHEN po.currency_id = rc_mxn.id THEN 1.0
                                ELSE rcr.rate END,
                           1.0)) AS sum_mxn,
                   SUM(pol.product_qty) AS sum_qty
            FROM purchase_order_line pol
            JOIN purchase_order po ON pol.order_id = po.id
            JOIN res_currency rc_mxn ON rc_mxn.name = 'MXN'
            LEFT JOIN LATERAL (
              SELECT rate
              FROM res_currency_rate
              WHERE currency_id = po.currency_id
                AND company_id = po.company_id
                AND name <= po.date_order::date
              ORDER BY name DESC
              LIMIT 1
            ) rcr ON true
            WHERE po.state IN ('purchase','done')
              AND po.date_order::date BETWEEN %s AND %s
            GROUP BY ym, po.company_id
        """, (date_from, date_to))
        rows_purchase = self.env.cr.fetchall()

        odoo_buckets = {}
        for ym, otype, cid, cnt, sm, sq in rows_sale + rows_purchase:
            key = f'{ym}|{otype}|{cid}'
            odoo_buckets[key] = {'count': int(cnt or 0),
                                 'sum_mxn': float(sm or 0),
                                 'sum_qty': float(sq or 0)}

        # Supabase
        supa_rows = client.fetch_all('v_audit_order_lines_buckets', {})
        supa_buckets = {r['bucket_key']: r for r in supa_rows}

        for key in set(odoo_buckets) | set(supa_buckets):
            o = odoo_buckets.get(key, {'count': 0, 'sum_mxn': 0, 'sum_qty': 0})
            s = supa_buckets.get(key, {})
            self._record_cross(client, run_id, 'order_lines',
                              'order_lines.count_per_bucket', key,
                              o['count'], s.get('count', 0), tolerances,
                              date_from, date_to, dry_run=dry_run)
            self._record_cross(client, run_id, 'order_lines',
                              'order_lines.sum_subtotal_mxn', key,
                              o['sum_mxn'], s.get('sum_subtotal_mxn', 0),
                              tolerances, date_from, date_to, dry_run=dry_run)
            self._record_cross(client, run_id, 'order_lines',
                              'order_lines.sum_qty', key,
                              o['sum_qty'], s.get('sum_qty', 0),
                              tolerances, date_from, date_to, dry_run=dry_run)

    def audit_deliveries(self, client, run_id, date_from, date_to,
                         tolerances, dry_run):
        """Invariante 11: count done per month × state × company."""
        self.env.cr.execute("""
            SELECT to_char(date_done, 'YYYY-MM') AS ym,
                   state,
                   company_id,
                   COUNT(*) AS cnt
            FROM stock_picking
            WHERE date_done IS NOT NULL
              AND date_done::date BETWEEN %s AND %s
              AND state IN ('done','cancel')
            GROUP BY ym, state, company_id
        """, (date_from, date_to))
        odoo = {f'{ym}|{st}|{cid}': int(cnt)
                for ym, st, cid, cnt in self.env.cr.fetchall()}

        supa_rows = client.fetch_all('v_audit_deliveries_buckets', {})
        supa = {r['bucket_key']: int(r['count']) for r in supa_rows}

        for key in set(odoo) | set(supa):
            self._record_cross(client, run_id, 'deliveries',
                              'deliveries.count_done_per_month', key,
                              odoo.get(key, 0), supa.get(key, 0),
                              tolerances, date_from, date_to, dry_run=dry_run)

    def audit_manufacturing(self, client, run_id, date_from, date_to,
                            tolerances, dry_run):
        """Invariantes 12-13: por state × company × month."""
        self.env.cr.execute("""
            SELECT to_char(date_start, 'YYYY-MM') AS ym,
                   state, company_id,
                   COUNT(*) AS cnt,
                   SUM(qty_produced) AS sum_qty
            FROM mrp_production
            WHERE date_start::date BETWEEN %s AND %s
            GROUP BY ym, state, company_id
        """, (date_from, date_to))
        odoo = {}
        for ym, st, cid, cnt, sq in self.env.cr.fetchall():
            key = f'{ym}|{st}|{cid}'
            odoo[key] = {'count': int(cnt or 0), 'sum_qty': float(sq or 0)}

        supa_rows = client.fetch_all('v_audit_manufacturing_buckets', {})
        supa = {r['bucket_key']: r for r in supa_rows}

        for key in set(odoo) | set(supa):
            o = odoo.get(key, {'count': 0, 'sum_qty': 0})
            s = supa.get(key, {})
            self._record_cross(client, run_id, 'manufacturing',
                              'manufacturing.count_per_state', key,
                              o['count'], int(s.get('count') or 0),
                              tolerances, date_from, date_to, dry_run=dry_run)
            self._record_cross(client, run_id, 'manufacturing',
                              'manufacturing.sum_qty_produced', key,
                              o['sum_qty'], float(s.get('sum_qty') or 0),
                              tolerances, date_from, date_to, dry_run=dry_run)

    def audit_account_balances(self, client, run_id, date_from, date_to,
                                tolerances, dry_run):
        """Invariantes 14-16: balance de cuentas por período × company.

        Odoo 17+: account.account.code es per-company vía code_store_ids.
        Resolvemos los ids por compañía vía ORM con with_company(cid) y
        filtramos en SQL por account_id IN (...) en vez de aa.code LIKE ...
        """
        Account = self.env['account.account']
        Company = self.env['res.company']
        companies = Company.search([])

        for invariant_key, patterns in self.ACCOUNT_GROUPS.items():
            prefixes = tuple(p.rstrip('%') for p in patterns)

            # Agregamos odoo per (month, company) igual que antes, pero los
            # accounts se resuelven en ORM con contexto por compañía.
            odoo = {}
            for company in companies:
                accounts_ctx = Account.with_company(company.id).search([])
                matching_ids = [
                    a.id for a in accounts_ctx
                    if (a.code or '').startswith(prefixes)
                ]
                if not matching_ids:
                    continue
                self.env.cr.execute("""
                    SELECT to_char(aml.date, 'YYYY-MM') AS ym,
                           SUM(aml.balance) AS bal
                    FROM account_move_line aml
                    JOIN account_move am ON aml.move_id = am.id
                    WHERE am.state = 'posted'
                      AND aml.date BETWEEN %s AND %s
                      AND aml.account_id IN %s
                      AND aml.company_id = %s
                    GROUP BY ym
                """, (date_from, date_to, tuple(matching_ids), company.id))
                for ym, bal in self.env.cr.fetchall():
                    odoo[f'{ym}|{company.id}'] = float(bal or 0)

            supa_rows = client.fetch_all('v_audit_account_balances_buckets',
                                        {'invariant_key': f'eq.{invariant_key}'})
            supa = {r['bucket_key']: float(r['balance']) for r in supa_rows}

            for key in set(odoo) | set(supa):
                self._record_cross(client, run_id, 'account_balances',
                                  invariant_key, key,
                                  odoo.get(key, 0), supa.get(key, 0),
                                  tolerances, date_from, date_to, dry_run=dry_run)

    def audit_bank_balances(self, client, run_id, date_from, date_to,
                            tolerances, dry_run):
        """Invariantes 17-18: snapshot por journal."""
        # 17. count per journal
        self.env.cr.execute("""
            SELECT id, company_id FROM account_journal
            WHERE type IN ('bank','cash') AND active = true
        """)
        odoo_journals = {f'journal_{jid}|{cid}': 1
                         for jid, cid in self.env.cr.fetchall()}
        odoo_count = len(odoo_journals)
        supa_count = client.count_exact('odoo_bank_balances',
                                        {'active': 'eq.true'})
        self._record_cross(client, run_id, 'bank_balances',
                          'bank_balances.count_per_journal', None,
                          odoo_count, supa_count, tolerances,
                          date_from, date_to, dry_run=dry_run)

        # 18. native_balance_per_journal
        # Usamos la misma lógica que sync_push._push_bank_balances
        Journal = self.env['account.journal']
        journals = Journal.search([
            ('type', 'in', ['bank', 'cash']), ('active', '=', True),
        ])
        for j in journals:
            # balance nativo: suma de asientos en la cuenta del journal
            # en su currency propia (sin convertir)
            default_account = j.default_account_id
            if not default_account:
                continue
            self.env.cr.execute("""
                SELECT COALESCE(SUM(
                    CASE WHEN aml.currency_id IS NOT NULL
                         THEN aml.amount_currency
                         ELSE aml.balance END
                ), 0)
                FROM account_move_line aml
                JOIN account_move am ON aml.move_id = am.id
                WHERE aml.account_id = %s
                  AND am.state = 'posted'
            """, (default_account.id,))
            odoo_bal = float(self.env.cr.fetchone()[0] or 0)
            supa_rows = client.fetch('odoo_bank_balances', {
                'journal_id': f'eq.{j.id}',
                'odoo_company_id': f'eq.{j.company_id.id}',
                'select': 'native_balance',
            }) or []
            supa_bal = float(supa_rows[0]['native_balance']
                             if supa_rows else 0)
            key = f'journal_{j.id}|{j.company_id.id}'
            self._record_cross(client, run_id, 'bank_balances',
                              'bank_balances.native_balance_per_journal',
                              key, odoo_bal, supa_bal, tolerances,
                              date_from, date_to, dry_run=dry_run)

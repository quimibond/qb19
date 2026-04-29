"""
Push Odoo operational data to Supabase.

One cron job, one function: push_to_supabase().
Reads from Odoo ORM, writes to Supabase REST API.
No Claude, no Gmail, no enrichment logic.
"""
import logging
import re
from datetime import datetime, timedelta

from odoo import api, fields, models

from .ingestion_core import IngestionCore
from .supabase_client import SupabaseClient

_logger = logging.getLogger(__name__)

# Email validation regex
_EMAIL_RE = re.compile(r'^[^@\s]+@[^@\s]+\.[^@\s]+$')

# Posted-without-timbrado grace window. If an invoice is posted but has no
# l10n_mx_edi CFDI UUID yet, we give it this many minutes to finish timbrado
# before logging a WARNING (operational signal that timbrado may have failed).
STALE_UUID_THRESHOLD_MINUTES = 5


def _get_client(env) -> SupabaseClient | None:
    """Build Supabase client from Odoo config parameters."""
    get = lambda k: env['ir.config_parameter'].sudo().get_param(k) or ''
    url = get('quimibond_intelligence.supabase_url')
    key = get('quimibond_intelligence.supabase_service_key')
    if not url or not key:
        _logger.error('Supabase URL or service key not configured')
        return None
    return SupabaseClient(url, key)


def _build_cfdi_map(env, invoice_ids: list, seen_uuids: set | None = None) -> dict:
    """Build {invoice_id: {uuid, sat}} from l10n_mx_edi.document via ORM.

    The stored field l10n_mx_edi_cfdi_uuid on account.move is stale after
    the Odoo 17→19 migration — .read() returns NULL. The UI shows the UUID
    because loading the form triggers the recompute chain; the sync doesn't.

    Instead of reading the stored field, we go straight to the source:
    l10n_mx_edi.document records, keyed by doc.move_id (1:1 FK to account.move).

    FIXED (2026-04-20, SP0): Previously iterated doc.invoice_ids (M2M) which
    caused complemento de pago (tipo P) documents to assign their UUID to every
    invoice they covered. Now queries by doc.move_id and post-filters to invoice
    move_types only, so payment complement UUIDs never bleed into odoo_invoices.
    See memory: project_cfdi_uuid_bug_2026_04_20.md

    `seen_uuids` (SP5.5 2026-04-22): optional set shared across chunks to
    guarantee UUID uniqueness globally, not just within a single chunk.
    ~11 UUIDs in current Odoo data are claimed by 1,700+ moves (pre-SP0
    migration residue in l10n_mx_edi.document). Without a global set,
    first-wins within chunk N ≠ first-wins within chunk M, and the chunks
    collide at the Supabase UNIQUE constraint uq_odoo_invoices_cfdi_uuid
    with HTTP 409. Caller should init once per push:
        seen_uuids = set()
        for chunk ...: cfdi_map = _build_cfdi_map(env, chunk_ids, seen_uuids)

    SP10.4 (2026-04-23): when multiple moves claim the same UUID, group docs
    by uuid and pick a winner by score: sat_state='valid' > move.state='posted'
    > highest move.id (recency). Old logic ('id desc' first-wins) gave the UUID
    to legacy phantom moves and starved the real posted+paid invoices. 22 real
    Quimibond out_invoice post-2025 paid had NULL uuid for this reason.
    """
    if not invoice_ids:
        return {}
    if seen_uuids is None:
        seen_uuids = set()

    result = {}
    try:
        Document = env['l10n_mx_edi.document'].sudo()
        docs = Document.search([
            ('move_id', 'in', invoice_ids),
            ('attachment_uuid', '!=', False),
        ])

        # Group invoice-typed docs by uuid (skip payment complements).
        docs_by_uuid: dict = {}
        for doc in docs:
            if not doc.move_id:
                continue
            if doc.move_id.move_type not in (
                'out_invoice', 'out_refund', 'in_invoice', 'in_refund'
            ):
                continue
            # SP10.6: normalize UUID to lowercase. Odoo stores XML-source UUIDs
            # as uppercase (in_invoice from supplier XML); SAT/Syntage uses lowercase.
            # Lowercase at source guarantees case-insensitive match in canonical layer.
            uuid_lc = (doc.attachment_uuid or '').lower()
            if not uuid_lc:
                continue
            docs_by_uuid.setdefault(uuid_lc, []).append(doc)

        # Pick winner per uuid by (sat_valid, posted, move.id) tuple.
        def _score(d):
            sat_valid = 1 if d.sat_state == 'valid' else 0
            posted = 1 if d.move_id.state == 'posted' else 0
            return (sat_valid, posted, d.move_id.id)

        for uuid_val, group in docs_by_uuid.items():
            if uuid_val in seen_uuids:
                continue
            winner = max(group, key=_score)
            result[winner.move_id.id] = {
                'uuid': uuid_val,
                'sat': winner.sat_state or None,
            }
            seen_uuids.add(uuid_val)
    except Exception as exc:
        _logger.warning('CFDI map build failed: %s', exc)

    return result


def _commercial_partner_id(partner) -> int | None:
    """Resolve commercial partner ID (parent company)."""
    cp = partner.commercial_partner_id
    return cp.id if cp else partner.id


# H9 — partner name validation (audit 2026-04-16)
# Odoo produce partners con names como "8141", "5806" o strings de 1-2
# caracteres, a menudo importados desde sistemas legacy. Antes se
# pusheaban tal cual y aparecían como "8141" en /companies. Este helper
# devuelve el mejor nombre disponible o None (skip) aplicando la misma
# regla de frontend `sanitizeCompanyName`.
_NUMERIC_ONLY = re.compile(r'^[0-9]+$')


def _best_partner_name(partner) -> str | None:
    """Devuelve el mejor nombre disponible para un partner.

    Orden de preferencia:
      1. `partner.name` si es real (no vacío, no numérico puro, >=3 chars)
      2. `commercial_partner_id.name` si es real (partner pertenece a una
         empresa padre con nombre bueno)
      3. `partner.vat` (RFC) — identificable aunque feo
      4. Dominio del primer email (`@acme.com` → `acme.com`)
      5. None — el caller debe skip.
    """
    def _clean(s):
        if not s:
            return None
        t = s.strip()
        if not t or len(t) < 3 or _NUMERIC_ONLY.match(t):
            return None
        return t

    # 1. Partner.name directo
    name = _clean(partner.name)
    if name:
        return name

    # 2. Commercial parent
    try:
        cp = partner.commercial_partner_id
        if cp and cp.id != partner.id:
            name = _clean(cp.name)
            if name:
                return name
    except Exception:
        pass

    # 3. VAT / RFC
    try:
        name = _clean(partner.vat)
        if name:
            return name
    except Exception:
        pass

    # 4. Email domain
    try:
        raw = (partner.email or '').split(',')[0].split(';')[0].strip()
        if '@' in raw:
            dom = raw.split('@')[-1].strip().lower()
            dom = _clean(dom)
            if dom and dom not in {
                'gmail.com', 'hotmail.com', 'outlook.com', 'yahoo.com',
                'live.com', 'icloud.com', 'protonmail.com', 'outlook.es',
            }:
                return dom
    except Exception:
        pass

    return None


def _build_payment_date_map(env, invoice_ids: list) -> dict:
    """Build {invoice_id: date} from account.partial.reconcile via ORM.

    For each paid/in_payment invoice, finds the last reconciliation date
    by looking at partial reconcile records on the invoice's receivable/
    payable lines.  This is the real payment date (not write_date proxy).
    """
    if not invoice_ids:
        return {}

    result = {}
    try:
        Reconcile = env['account.partial.reconcile'].sudo()
        MoveLine = env['account.move.line'].sudo()

        # Get receivable/payable lines for these invoices
        lines = MoveLine.search([
            ('move_id', 'in', invoice_ids),
            ('account_type', 'in', [
                'asset_receivable', 'liability_payable',
            ]),
        ])
        if not lines:
            return result

        line_ids = lines.ids
        # Map line_id → move_id for quick lookup
        line_to_move = {ln.id: ln.move_id.id for ln in lines}

        # Find all partial reconcile records touching these lines
        reconciles = Reconcile.search([
            '|',
            ('debit_move_id', 'in', line_ids),
            ('credit_move_id', 'in', line_ids),
        ])

        for rec in reconciles:
            rec_date = rec.create_date.date() if rec.create_date else None
            if not rec_date:
                continue
            # Check which side is our invoice line
            for line_id in (rec.debit_move_id.id, rec.credit_move_id.id):
                move_id = line_to_move.get(line_id)
                if move_id and move_id in invoice_ids:
                    # Keep the latest reconciliation date per invoice
                    if move_id not in result or rec_date > result[move_id]:
                        result[move_id] = rec_date
    except Exception as exc:
        _logger.warning('Payment date map build failed: %s', exc)

    return result


class QuimibondSync(models.TransientModel):
    _name = 'quimibond.sync'
    _description = 'Quimibond Sync Engine'

    # Main operating company. All accounting, bank balances, manufacturing,
    # and orderpoints are filtered to this company to avoid mixing data from
    # personal/test companies in the same Odoo instance.
    # Set via config param quimibond_intelligence.company_id (default: 1).
    def _get_company_id(self):
        """Return the operating company ID for filtering multi-company data."""
        ICP = self.env['ir.config_parameter'].sudo()
        cid = ICP.get_param('quimibond_intelligence.company_id', '1')
        return int(cid)

    # Multi-company: if quimibond_intelligence.company_ids is set (comma-
    # separated list), usa esa lista. Si no, cae al single-company legacy
    # [_get_company_id()] para backward-compat. Esto permite opt-in a
    # multi-company sin romper el setup existente. Ejemplo:
    #   env['ir.config_parameter'].sudo().set_param(
    #     'quimibond_intelligence.company_ids', '1,2,3,4,16')
    def _get_company_ids(self):
        """Return the list of company IDs to include in the push."""
        ICP = self.env['ir.config_parameter'].sudo()
        raw = (ICP.get_param('quimibond_intelligence.company_ids') or '').strip()
        if raw:
            try:
                return [int(x.strip()) for x in raw.split(',') if x.strip()]
            except (ValueError, TypeError):
                pass
        return [self._get_company_id()]

    # Tablas que SIEMPRE hacen full push (no incremental), incluso cuando
    # last_sync esta seteado. Son catalogos pequenos donde el riesgo de
    # perderlos por incremental fallido es mayor al costo de re-enviarlos.
    # Se detecto el 13-abr-2026 que chart_of_accounts (5d), orderpoints (8d),
    # employees/departments (12d) y crm_leads (18d) quedaban stale porque
    # el filtro write_date no los tocaba entre runs.
    #
    # SP12.2 (2026-04-27): aliviada la lista. Las 9 tablas pesadas
    # (invoices, invoice_lines, order_lines, sale_orders, purchase_orders,
    # products, deliveries, manufacturing, account_payments) salieron del
    # full hourly y volvieron a incremental por write_date. El runtime del
    # cron baja de ~3:44 → ~50s. Para cubrir el riesgo de write_date no
    # tocado por recompute (FX, amount_residual, payment_state), se agrega
    # un cron diario `push_to_supabase_full()` a las 3am que setea
    # `force_full_sync=1` y resyncroniza TODO sin filtro de last_sync.
    # Drift máximo: 24h. Si el frontend muestra una factura desfasada,
    # disparar manual via shell con `force_full_sync=1` antes del nightly.
    FULL_PUSH_METHODS = frozenset([
        # Catálogos chicos donde write_date no se toca al editar campos
        # calculados o al toggle del flag active. El costo de re-pushar es
        # trivial (<1s c/u), el riesgo de stale es alto.
        'employees', 'departments', 'orderpoints', 'chart_of_accounts',
        'crm_leads', 'bank_balances', 'users',
        # BOMs: el flag active no siempre refleja en write_date. Siempre full.
        'boms',
        # stock_locations: ~68 rows, catálogo chico.
        'stock_locations',
        # SP12 (2026-04-23): workcenters es un catálogo chico (~10-20 rows).
        # workorders se pushea incremental via write_date.
        'workcenters',
    ])

    def _run_push(self, client, label, method_fn, last_sync=None):
        """Ejecuta un metodo _push_* aislado: cualquier excepcion queda
        capturada (no tumba el resto del sync) y loggea a Supabase
        pipeline_logs con phase='odoo_push' — asi podemos auditar desde
        el frontend sin necesidad de shell de Odoo.sh.

        Para tablas en FULL_PUSH_METHODS fuerza last_sync=None.
        """
        method_start = datetime.now()
        status = 'success'
        error_msg = None
        rows = 0
        effective_last_sync = None if label in self.FULL_PUSH_METHODS else last_sync

        try:
            try:
                rows = method_fn(client, last_sync=effective_last_sync) or 0
            except TypeError:
                # Metodos que no aceptan last_sync (ej: _push_activities)
                rows = method_fn(client) or 0
        except Exception as exc:
            status = 'error'
            error_msg = str(exc)[:500]
            _logger.exception('Push %s failed', label)

        elapsed = (datetime.now() - method_start).total_seconds()

        # Loggea a Supabase (best-effort: si el log mismo falla, seguimos).
        try:
            client.insert('pipeline_logs', [{
                'level': 'error' if status == 'error' else 'info',
                'phase': 'odoo_push',
                'message': (
                    f'[{label}] {rows} rows pushed in {elapsed:.1f}s'
                    if status == 'success'
                    else f'[{label}] FAILED after {elapsed:.1f}s: {error_msg}'
                ),
                'details': {
                    'method': label,
                    'rows': rows,
                    'status': status,
                    'elapsed_s': round(elapsed, 1),
                    'error': error_msg,
                    'last_sync': last_sync.strftime('%Y-%m-%d %H:%M:%S') if last_sync else None,
                    'full_push': label in self.FULL_PUSH_METHODS,
                },
            }])
        except Exception as log_exc:
            _logger.warning('Failed to log push metric: %s', log_exc)

        return rows

    @api.model
    def push_to_supabase(self):
        """Main cron entry point: push all Odoo data to Supabase."""
        client = _get_client(self.env)
        if not client:
            return

        # Get last sync timestamp for incremental sync
        ICP = self.env['ir.config_parameter'].sudo()
        last_sync_str = ICP.get_param('quimibond_intelligence.last_sync_date', '')

        # One-time full sync: if force_full_sync is set, ignore last_sync
        # and clear the flag after completion.
        force_full = ICP.get_param('quimibond_intelligence.force_full_sync', '')
        if force_full:
            last_sync_str = ''
            _logger.info('Full sync forced via force_full_sync parameter')

        incremental = bool(last_sync_str)
        last_sync = None
        if incremental:
            try:
                last_sync = datetime.strptime(last_sync_str, '%Y-%m-%d %H:%M:%S')
                # Add 1-minute overlap to avoid missing records
                last_sync = last_sync - timedelta(minutes=1)
            except (ValueError, TypeError):
                last_sync = None
                incremental = False

        _start = datetime.now()
        try:
            # Mapeo label → (metodo). _run_push aisla errores por metodo y
            # loggea cada resultado a Supabase pipeline_logs (phase='odoo_push').
            methods = [
                ('contacts', self._push_contacts),
                ('products', self._push_products),
                ('order_lines', self._push_order_lines),
                ('users', self._push_users),
                ('invoices', self._push_invoices),
                ('invoice_lines', self._push_invoice_lines),
                ('deliveries', self._push_deliveries),
                ('crm_leads', self._push_crm_leads),
                ('activities', self._push_activities),
                ('manufacturing', self._push_manufacturing),
                ('employees', self._push_employees),
                ('departments', self._push_departments),
                ('sale_orders', self._push_sale_orders),
                ('purchase_orders', self._push_purchase_orders),
                ('orderpoints', self._push_orderpoints),
                ('account_payments', self._push_account_payments),
                # SP5.5 (2026-04-22): payment_invoice_links + uoms removed.
                # Both Supabase tables were dropped in Silver SP5 (frontend
                # now reads canonical_payments + relations). Every hourly
                # push returned 404 for 4,333 + 76 rows respectively.
                # The _push_payment_invoice_links / _push_uoms helpers are
                # kept in this file in case the tables are ever re-added,
                # but are no longer dispatched by the cron.
                ('chart_of_accounts', self._push_chart_of_accounts),
                ('account_balances', self._push_account_balances),
                ('bank_balances', self._push_bank_balances),
                ('currency_rates', self._push_currency_rates),
                ('boms', self._push_boms),
                # SP11: stock vs accounting reconciliation (2026-04-23)
                ('stock_locations', self._push_stock_locations),
                ('stock_moves', self._push_stock_moves),
                ('account_entries_stock', self._push_account_entries_stock),
                # SP12: manufacturing cost audit (2026-04-23)
                ('workcenters', self._push_workcenters),
                ('workorders', self._push_workorders),
            ]
            totals = {}
            for label, fn in methods:
                totals[label] = self._run_push(client, label, fn, last_sync=last_sync)

            summary = ', '.join(f'{k}={v}' for k, v in totals.items() if v)
            failed = [k for k, v in totals.items() if v == 0]
            _logger.info('✓ Push to Supabase: %s', summary or 'no changes')
            if failed:
                _logger.warning('Push methods with 0 rows: %s', ', '.join(failed))
            elapsed = (datetime.now() - _start).total_seconds()
            self.env['quimibond.sync.log'].sudo().create({
                'name': 'Push completo',
                'direction': 'push',
                'status': 'success',
                'summary': summary or 'sin cambios',
                'duration_seconds': round(elapsed, 1),
            })
            # Save sync timestamp for next incremental run
            ICP.set_param('quimibond_intelligence.last_sync_date',
                          _start.strftime('%Y-%m-%d %H:%M:%S'))
            # Clear one-time full sync flag
            if force_full:
                ICP.set_param('quimibond_intelligence.force_full_sync', '')

            # Trigger identity resolution after successful push
            try:
                client.rpc('resolve_all_identities', {})
                _logger.info('Identity resolution triggered after push')
            except Exception as exc:
                _logger.warning('Identity resolution RPC failed: %s', exc)

            # Export schema catalog once per day
            try:
                last_schema = ICP.get_param(
                    'quimibond_intelligence.last_schema_export', '')
                today = _start.strftime('%Y-%m-%d')
                if last_schema != today:
                    self.push_schema_catalog()
                    ICP.set_param(
                        'quimibond_intelligence.last_schema_export', today)
                    _logger.info('Schema catalog exported for %s', today)
            except Exception as exc:
                _logger.warning('Schema catalog export failed: %s', exc)

            self.env.cr.commit()
        except Exception as exc:
            _logger.error('Push to Supabase failed: %s', exc)
            try:
                self.env['quimibond.sync.log'].sudo().create({
                    'name': 'Push fallido',
                    'direction': 'push',
                    'status': 'error',
                    'summary': str(exc)[:500],
                })
                self.env.cr.commit()
            except Exception:
                pass
        finally:
            client.close()

    # ── Nightly full sync (SP12.2 — 2026-04-27) ──────────────────────────
    # El push hourly (push_to_supabase) corre incremental por write_date para
    # las 9 tablas grandes (invoices, invoice_lines, order_lines, sale_orders,
    # purchase_orders, products, deliveries, manufacturing, account_payments).
    # Eso baja el runtime de ~3:44 → ~50s. El riesgo: campos calculados que
    # Odoo recompute sin tocar write_date (FX rate, amount_residual,
    # payment_state) quedan stale. Este cron a las 3am setea force_full_sync=1
    # y dispara push_to_supabase, que ignora last_sync y resyncroniza TODO.
    # Drift máximo: 24h.

    @api.model
    def push_to_supabase_full(self):
        """Force a full push of all tables, ignoring last_sync.

        Wraps push_to_supabase() by setting the force_full_sync config
        parameter. push_to_supabase() reads the flag, runs full, and clears
        the flag in the success path.
        """
        ICP = self.env['ir.config_parameter'].sudo()
        ICP.set_param('quimibond_intelligence.force_full_sync', '1')
        self.env.cr.commit()
        _logger.info('Nightly full sync triggered (force_full_sync=1)')
        self.push_to_supabase()

    # ── Schema Catalog ────────────────────────────────────────────────────

    @api.model
    def push_schema_catalog(self):
        """Export all installed Odoo models and fields to Supabase.

        This lets the intelligence layer know exactly what data exists
        in Odoo without guessing field names. Run manually or on deploy.
        """
        client = _get_client(self.env)
        if not client:
            return

        # Models we care about for business intelligence
        MODELS = [
            'res.partner', 'product.product', 'product.template',
            'sale.order', 'sale.order.line',
            'purchase.order', 'purchase.order.line',
            'account.move', 'account.move.line',
            'account.payment', 'account.payment.term',
            'account.account', 'account.journal',
            'account.tax',
            'stock.picking', 'stock.move',
            'stock.warehouse.orderpoint', 'stock.quant',
            'crm.lead', 'mail.activity',
            'hr.employee', 'hr.department',
            'mrp.production', 'mrp.bom',
            'res.currency', 'res.company',
            'product.pricelist', 'product.pricelist.item',
            'res.partner.category',
        ]

        rows = []
        for model_name in MODELS:
            try:
                Model = self.env[model_name].sudo()
            except KeyError:
                _logger.info('Model %s not available, skipping', model_name)
                continue

            model_desc = Model._description or model_name

            for fname, field in Model._fields.items():
                # Skip internal/private fields
                if fname.startswith('_') or fname in ('id', 'create_uid',
                    'write_uid', 'create_date', 'write_date', '__last_update'):
                    continue

                relation = None
                if field.type in ('many2one', 'many2many', 'one2many'):
                    relation = field.comodel_name

                selection_values = None
                if field.type == 'selection':
                    try:
                        sel = field.selection
                        if callable(sel):
                            sel = sel(Model)
                        selection_values = sel
                    except Exception:
                        pass

                rows.append({
                    'model_name': model_name,
                    'model_description': model_desc,
                    'field_name': fname,
                    'field_type': field.type,
                    'field_description': field.string or fname,
                    'required': bool(field.required),
                    'readonly': bool(field.readonly),
                    'relation': relation,
                    'selection_values': selection_values,
                    'synced_to_supabase': fname in self._get_synced_fields(model_name),
                })

        if rows:
            # Full refresh: delete and re-insert
            client.delete_all('odoo_schema_catalog')
            count = client.insert('odoo_schema_catalog', rows, batch_size=500)
            _logger.info('Schema catalog: %d fields exported from %d models',
                         count, len(MODELS))

        client.close()

    def _get_synced_fields(self, model_name):
        """Return set of field names that are currently synced for a model."""
        # Map of model → fields we push to Supabase
        SYNCED = {
            'res.partner': {'name', 'email', 'vat', 'customer_rank', 'supplier_rank',
                           'is_company', 'parent_id', 'commercial_partner_id',
                           'country_id', 'city', 'category_id',
                           'property_payment_term_id', 'property_supplier_payment_term_id',
                           'credit_limit', 'credit', 'debit', 'total_invoiced',
                           'total_overdue'},
            'product.product': {'name', 'default_code', 'categ_id', 'uom_id',
                               'type', 'qty_available', 'virtual_available',
                               'standard_price', 'list_price', 'active', 'barcode',
                               'avg_cost', 'weight'},
            'sale.order': {'name', 'partner_id', 'state', 'amount_total',
                          'amount_untaxed', 'date_order', 'user_id', 'margin',
                          'margin_percent'},
            'purchase.order': {'name', 'partner_id', 'state', 'amount_total',
                              'date_order', 'user_id'},
            'account.move': {'name', 'partner_id', 'move_type', 'state',
                           'amount_total', 'amount_residual', 'amount_tax',
                           'amount_untaxed', 'currency_id',
                           'invoice_date', 'invoice_date_due', 'payment_state', 'ref',
                           'invoice_payment_term_id',
                           'l10n_mx_edi_cfdi_uuid', 'l10n_mx_edi_cfdi_sat_state'},
            'account.move.line': {'account_id', 'debit', 'credit', 'balance',
                                 'date', 'name', 'partner_id', 'journal_id'},
            'account.payment': {'name', 'partner_id', 'amount', 'payment_type',
                               'partner_type', 'date', 'ref', 'state', 'journal_id',
                               'payment_method_line_id', 'is_matched', 'is_reconciled',
                               'amount_company_currency_signed'},
            'account.account': {'code', 'name', 'account_type', 'reconcile'},
            'account.journal': {'name', 'type', 'currency_id', 'bank_account_id',
                               'default_account_id'},
            'stock.picking': {'name', 'partner_id', 'picking_type_id', 'state',
                            'scheduled_date', 'date_done', 'origin'},
            'crm.lead': {'name', 'partner_id', 'type', 'stage_id', 'user_id',
                        'expected_revenue', 'probability', 'date_deadline', 'active'},
            'hr.employee': {'name', 'work_email', 'department_id', 'job_id',
                          'job_title', 'parent_id', 'coach_id'},
        }
        return SYNCED.get(model_name, set())

    # ── Retry failed rows from previous runs ─────────────────────────────

    def _retry_failures(self):
        """
        Called every 30 minutes by ir_cron_retry_failures. For Plan 1 scope,
        fetches up to 50 pending failures per table and re-upserts them using
        the saved payload snapshot. Successful retries are marked resolved;
        persistent failures bump retry_count via a fresh retry run.
        """
        client = _get_client(self.env)
        if client is None:
            return
        core = IngestionCore(client)
        # SP14 audit (2026-04-29): on_conflict must match an existing UNIQUE
        # index. odoo_invoices has UNIQUE on odoo_invoice_id only; the legacy
        # (partner_id,name) compound was dropped 2026-04-20 because it
        # collided across multi-company. Retry kept the obsolete tuple, so
        # every retry batch hit 42P10 ("no unique constraint matching ON
        # CONFLICT"). 20,610 rows piled up in ingestion.sync_failure with
        # retry_count up to 98. Aligning retry's on_conflict to the same
        # column the main push uses (sync_push.py:1560) drains them.
        tables = [
            ('odoo', 'odoo_invoices', 'odoo_invoice_id'),
        ]
        max_retries = 5
        for source, table, conflict in tables:
            pending = core.fetch_pending_failures(source, table, max_retries, limit=50)
            if not pending:
                continue
            # Build the row list from payload snapshots; drop any with no payload
            rows_with_meta = [
                (p, p.get('payload_snapshot'))
                for p in pending
                if p.get('payload_snapshot')
            ]
            if not rows_with_meta:
                continue
            rows = [r for _, r in rows_with_meta]
            ok_count, failed = client.upsert_with_details(
                table, rows, on_conflict=conflict, batch_size=200
            )
            # Mark resolved: anything not in the failed list succeeded
            failed_names = {
                str(row.get('name') or '')
                for row, _ in failed
            }
            for p, row in rows_with_meta:
                if str(row.get('name') or '') not in failed_names:
                    core.mark_resolved(p['failure_id'])
            # Report the still-failing ones under a fresh retry run
            if failed:
                run_id, _ = core.start_run(source, table, 'retry', 'cron')
                core.report_batch(run_id, len(rows), ok_count, len(failed))
                for row, err in failed:
                    core.report_failure(
                        run_id=run_id,
                        entity_id=str(row.get('name') or ''),
                        error_code=err['code'],
                        error_detail=err['detail'],
                        payload=row,
                    )
                core.complete_run(run_id, 'partial', None)

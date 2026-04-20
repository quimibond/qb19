"""
Push Odoo operational data to Supabase.

One cron job, one function: push_to_supabase().
Reads from Odoo ORM, writes to Supabase REST API.
No Claude, no Gmail, no enrichment logic.
"""
import logging
import re
from datetime import datetime, timedelta

from odoo import api, models

from .ingestion_core import IngestionCore
from .supabase_client import SupabaseClient

_logger = logging.getLogger(__name__)

# Email validation regex
_EMAIL_RE = re.compile(r'^[^@\s]+@[^@\s]+\.[^@\s]+$')


def _get_client(env) -> SupabaseClient | None:
    """Build Supabase client from Odoo config parameters."""
    get = lambda k: env['ir.config_parameter'].sudo().get_param(k) or ''
    url = get('quimibond_intelligence.supabase_url')
    key = get('quimibond_intelligence.supabase_service_key')
    if not url or not key:
        _logger.error('Supabase URL or service key not configured')
        return None
    return SupabaseClient(url, key)


def _build_cfdi_map(env, invoice_ids: list) -> dict:
    """Build {invoice_id: {uuid, sat}} from l10n_mx_edi.document via ORM.

    The stored field l10n_mx_edi_cfdi_uuid on account.move is stale after
    the Odoo 17→19 migration — .read() returns NULL. The UI shows the UUID
    because loading the form triggers the recompute chain; the sync doesn't.

    Instead of reading the stored field, we go straight to the source:
    l10n_mx_edi.document records linked via invoice_ids (M2M).
    """
    if not invoice_ids:
        return {}

    result = {}
    try:
        Document = env['l10n_mx_edi.document'].sudo()
        docs = Document.search([
            ('invoice_ids', 'in', invoice_ids),
            ('attachment_uuid', '!=', False),
        ], order='id desc')
        for doc in docs:
            for inv_id in doc.invoice_ids.ids:
                if inv_id not in result and inv_id in invoice_ids:
                    result[inv_id] = {
                        'uuid': doc.attachment_uuid,
                        'sat': doc.sat_state or None,
                    }
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

    # Tablas que SIEMPRE hacen full push (no incremental), incluso cuando
    # last_sync esta seteado. Son catalogos pequenos donde el riesgo de
    # perderlos por incremental fallido es mayor al costo de re-enviarlos.
    # Se detecto el 13-abr-2026 que chart_of_accounts (5d), orderpoints (8d),
    # employees/departments (12d) y crm_leads (18d) quedaban stale porque
    # el filtro write_date no los tocaba entre runs.
    FULL_PUSH_METHODS = frozenset([
        'employees', 'departments', 'orderpoints', 'chart_of_accounts',
        'crm_leads', 'bank_balances', 'users',
        # Added 2026-04-13: these tables were stuck at near-zero rows because
        # incremental write_date filter missed records not recently edited.
        'products', 'sale_orders', 'purchase_orders', 'invoice_lines',
        'deliveries', 'manufacturing', 'account_payments',
        # Force full push for invoices/payments so new fields
        # (salesperson_name, amount_*_mxn, payment_category) get populated.
        # Can be removed after first successful full sync.
        'invoices', 'payments',
        # BOMs are a small catalog (~hundreds) and active flag changes
        # are not always reflected in write_date. Always full push.
        'boms',
        # Sprint 13e: UoM master is tiny + line UoM was added later, so
        # order_lines need full re-push to populate the new column.
        'uoms', 'order_lines',
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
                ('payments', self._push_payments),
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
                ('payment_invoice_links', self._push_payment_invoice_links),
                ('chart_of_accounts', self._push_chart_of_accounts),
                ('account_balances', self._push_account_balances),
                ('bank_balances', self._push_bank_balances),
                ('currency_rates', self._push_currency_rates),
                ('boms', self._push_boms),
                ('uoms', self._push_uoms),
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

    # ── Contacts & Companies ─────────────────────────────────────────────

    def _push_contacts(self, client: SupabaseClient, last_sync=None) -> int:
        """Push res.partner → contacts + companies tables."""
        Partner = self.env['res.partner'].sudo()

        # Base: partners with email that are customers or suppliers
        domain = [
            ('email', '!=', False),
            ('email', '!=', ''),
            '|', ('customer_rank', '>', 0), ('supplier_rank', '>', 0),
        ]
        if last_sync:
            domain.append(('write_date', '>=', last_sync.strftime('%Y-%m-%d %H:%M:%S')))
        partners = Partner.search(domain)

        # Also include partners referenced by invoices/orders but missing ranks
        # These are the ones that cause orphan invoices in Supabase
        try:
            Move = self.env['account.move'].sudo()
            cutoff = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
            cid = self._get_company_id()
            invoice_partner_ids = Move.search([
                ('company_id', '=', cid),
                ('move_type', 'in', ['out_invoice', 'out_refund', 'in_invoice', 'in_refund']),
                ('state', '=', 'posted'),
                ('invoice_date', '>=', cutoff),
            ]).mapped('partner_id.commercial_partner_id').ids

            missing_ids = set(invoice_partner_ids) - set(partners.ids)
            if missing_ids:
                extra = Partner.browse(list(missing_ids)).filtered(
                    lambda p: p.email and p.email.strip()
                )
                if extra:
                    partners = partners | extra
                    _logger.info('Added %d partners from invoices (missing rank)', len(extra))
        except Exception as exc:
            _logger.warning('Extra partner fetch: %s', exc)

        companies = {}  # canonical_name → {fields}
        contacts = []   # [{fields}]

        for p in partners:
            emails = [
                e.strip().lower()
                for e in re.split(r'[;,\s]+', p.email or '')
                if _EMAIL_RE.match(e.strip())
            ]
            if not emails:
                continue

            cp_id = _commercial_partner_id(p)
            is_customer = p.customer_rank > 0
            is_supplier = p.supplier_rank > 0

            # Extract partner tags/categories
            tags = []
            try:
                if p.category_id:
                    tags = [t.name for t in p.category_id]
            except Exception:
                pass

            # Extract payment terms (customer and supplier)
            payment_term = None
            supplier_payment_term = None
            try:
                if hasattr(p, 'property_payment_term_id') and p.property_payment_term_id:
                    payment_term = p.property_payment_term_id.name
                if hasattr(p, 'property_supplier_payment_term_id') and p.property_supplier_payment_term_id:
                    supplier_payment_term = p.property_supplier_payment_term_id.name
            except Exception:
                pass

            # Extract domain from email
            domain = None
            if emails:
                d = emails[0].split('@')[-1] if '@' in emails[0] else None
                if d and d not in ('gmail.com', 'hotmail.com', 'outlook.com',
                                   'yahoo.com', 'live.com', 'icloud.com',
                                   'protonmail.com', 'outlook.es'):
                    domain = d

            # Only treat as company if Odoo marks it as company,
            # or if it's a standalone partner whose commercial_partner is itself.
            # Avoids creating fake "companies" from individual contacts
            # (e.g., "Acosta, Mario" instead of "CONTINENTAL").
            cp = p.commercial_partner_id
            is_real_company = p.is_company or (
                not p.parent_id and cp and cp.id == p.id
                and not _EMAIL_RE.match((p.name or '').strip())
                and '@' not in (p.name or '')
            )
            if is_real_company:
                # This is a company (top-level partner).
                # H9: usar _best_partner_name para caer a commercial_parent/
                # vat/email si partner.name es basura ("8141", "—", etc.).
                cn = _best_partner_name(p)
                if cn and cn not in companies:
                    rfc = (p.vat or '').strip() or None
                    # Financial totals (computed by Odoo from account.move)
                    total_receivable = total_payable = total_invoiced_odoo = None
                    total_overdue_odoo = credit_limit = None
                    try:
                        total_receivable = round(p.credit, 2) if hasattr(p, 'credit') else None
                        total_payable = round(p.debit, 2) if hasattr(p, 'debit') else None
                        total_invoiced_odoo = round(p.total_invoiced, 2) if hasattr(p, 'total_invoiced') else None
                        total_overdue_odoo = round(p.total_overdue, 2) if hasattr(p, 'total_overdue') else None
                        if hasattr(p, 'credit_limit') and p.credit_limit:
                            credit_limit = round(p.credit_limit, 2)
                    except Exception:
                        pass

                    # Build odoo_context with only non-null values
                    odoo_ctx = {}
                    if payment_term:
                        odoo_ctx['payment_term'] = payment_term
                    if supplier_payment_term:
                        odoo_ctx['supplier_payment_term'] = supplier_payment_term
                    if tags:
                        odoo_ctx['tags'] = tags

                    companies[cn] = {
                        'canonical_name': cn,
                        'name': cn,
                        'odoo_partner_id': cp_id,
                        'is_customer': is_customer,
                        'is_supplier': is_supplier,
                        'rfc': rfc,
                        'domain': domain,
                        'country': p.country_id.name if p.country_id else None,
                        'city': p.city or None,
                        'credit_limit': credit_limit,
                        'total_receivable': total_receivable,
                        'total_payable': total_payable,
                        'total_invoiced_odoo': total_invoiced_odoo,
                        'total_overdue_odoo': total_overdue_odoo,
                        'odoo_context': odoo_ctx,
                    }

            contact_name = p.name or None

            # Resolve company canonical name for linking
            # Use commercial_partner_id for better resolution (handles
            # contacts like "Acosta, Mario" → "CONTINENTAL" parent)
            company_cn = None
            if p.parent_id:
                company_cn = (p.parent_id.name or '').strip() or None
            elif p.is_company:
                company_cn = (p.name or '').strip() or None
            else:
                # Standalone contact — try commercial_partner_id
                cp = p.commercial_partner_id
                if cp and cp.id != p.id:
                    company_cn = (cp.name or '').strip() or None

            for i, email in enumerate(emails):
                # Always set odoo_partner_id on the FIRST email so we never
                # lose the Odoo link. Previously, partners with 2+ emails got
                # None on all entries — breaking joins to invoices/orders.
                contacts.append({
                    'email': email,
                    'name': contact_name,
                    'contact_type': 'external',
                    'odoo_partner_id': p.id if i == 0 else None,
                    'is_customer': is_customer,
                    'is_supplier': is_supplier,
                })

        # Also push companies from invoice partners that may lack email
        # (these cause orphan invoices worth millions)
        try:
            existing_partner_ids = {c['odoo_partner_id'] for c in companies.values()
                                    if c.get('odoo_partner_id')}
            Move = self.env['account.move'].sudo()
            cutoff = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
            cid = self._get_company_id()
            inv_partners = Move.search([
                ('company_id', '=', cid),
                ('move_type', 'in', ['out_invoice', 'out_refund', 'in_invoice', 'in_refund']),
                ('state', '=', 'posted'),
                ('invoice_date', '>=', cutoff),
            ]).mapped('partner_id.commercial_partner_id')

            for p in inv_partners:
                cp_id = p.id
                if cp_id in existing_partner_ids:
                    continue
                # H9: igual que arriba, usar helper con fallback chain.
                cn = _best_partner_name(p)
                if not cn:
                    continue
                if cn not in companies:
                    # Financial totals (same as main path)
                    total_receivable = total_payable = None
                    total_invoiced_odoo = total_overdue_odoo = credit_limit = None
                    try:
                        total_receivable = round(p.credit, 2) if hasattr(p, 'credit') else None
                        total_payable = round(p.debit, 2) if hasattr(p, 'debit') else None
                        total_invoiced_odoo = round(p.total_invoiced, 2) if hasattr(p, 'total_invoiced') else None
                        total_overdue_odoo = round(p.total_overdue, 2) if hasattr(p, 'total_overdue') else None
                        if hasattr(p, 'credit_limit') and p.credit_limit:
                            credit_limit = round(p.credit_limit, 2)
                    except Exception:
                        pass
                    companies[cn] = {
                        'canonical_name': cn,
                        'name': cn,
                        'odoo_partner_id': cp_id,
                        'is_customer': p.customer_rank > 0,
                        'is_supplier': p.supplier_rank > 0,
                        'rfc': (p.vat or '').strip() or None,
                        'domain': None,
                        'country': p.country_id.name if p.country_id else None,
                        'city': p.city or None,
                        'credit_limit': credit_limit,
                        'total_receivable': total_receivable,
                        'total_payable': total_payable,
                        'total_invoiced_odoo': total_invoiced_odoo,
                        'total_overdue_odoo': total_overdue_odoo,
                    }
                    existing_partner_ids.add(cp_id)
        except Exception as exc:
            _logger.warning('Invoice partner companies: %s', exc)

        synced = 0
        if companies:
            # Use odoo_partner_id as conflict key (not canonical_name) so that
            # company renames in Odoo update the existing row instead of
            # creating a duplicate. All Odoo-sourced companies have partner_id.
            synced += client.upsert(
                'companies', list(companies.values()),
                on_conflict='odoo_partner_id', batch_size=100,
            )
            # Backfill financial data via RPC (PostgREST upsert may miss
            # columns added after schema cache was built)
            fin_map = {}
            for c in companies.values():
                pid = c.get('odoo_partner_id')
                if not pid:
                    continue
                fin = {}
                if c.get('total_receivable') is not None:
                    fin['total_receivable'] = c['total_receivable']
                if c.get('total_payable') is not None:
                    fin['total_payable'] = c['total_payable']
                if c.get('total_invoiced_odoo') is not None:
                    fin['total_invoiced_odoo'] = c['total_invoiced_odoo']
                if c.get('total_overdue_odoo') is not None:
                    fin['total_overdue_odoo'] = c['total_overdue_odoo']
                if c.get('odoo_context'):
                    fin['odoo_context'] = c['odoo_context']
                if fin:
                    fin_map[str(pid)] = fin
            if fin_map:
                client.rpc('backfill_company_financials', {'data': fin_map})

            # Update RFC via RPC
            rfc_map = {str(c['odoo_partner_id']): c['rfc']
                       for c in companies.values()
                       if c.get('rfc') and c.get('odoo_partner_id')}
            if rfc_map:
                client.rpc('backfill_rfc_from_json', {'data': rfc_map})
        if contacts:
            synced += client.upsert(
                'contacts', contacts,
                on_conflict='email', batch_size=50,
            )
        return synced

    # ── Products ─────────────────────────────────────────────────────────

    def _push_products(self, client: SupabaseClient, last_sync=None) -> int:
        Product = self.env['product.product'].sudo()
        # Active products
        domain = [('active', '=', True)]
        if last_sync:
            domain.append(('write_date', '>=', last_sync.strftime('%Y-%m-%d %H:%M:%S')))
        products = Product.search(domain)

        # Also include inactive products referenced in posted invoices/orders
        # (otherwise invoice_lines and order_lines have orphan product IDs)
        if not last_sync:
            try:
                self.env.cr.execute("""
                    SELECT DISTINCT product_id FROM account_move_line
                    WHERE product_id IS NOT NULL AND move_id IN (
                        SELECT id FROM account_move
                        WHERE state = 'posted'
                        AND move_type IN ('out_invoice','out_refund','in_invoice','in_refund')
                    )
                    UNION
                    SELECT DISTINCT product_id FROM sale_order_line
                    WHERE product_id IS NOT NULL
                    UNION
                    SELECT DISTINCT product_id FROM purchase_order_line
                    WHERE product_id IS NOT NULL
                """)
                all_product_ids = {r[0] for r in self.env.cr.fetchall()}
                missing_ids = list(all_product_ids - set(products.ids))
                if missing_ids:
                    inactive = Product.with_context(active_test=False).browse(missing_ids).exists()
                    products |= inactive
                    _logger.info('Products: added %d inactive/archived products referenced in lines', len(inactive))
            except Exception as exc:
                _logger.warning('Products: failed to fetch inactive products: %s', exc)

        # Pre-fetch all reorder rules in one query (avoids N+1)
        orderpoint_map = {}  # product_id -> {min, max}
        try:
            Orderpoint = self.env['stock.warehouse.orderpoint'].sudo()
            all_orderpoints = Orderpoint.search([
                ('product_id', 'in', products.ids),
            ])
            for op in all_orderpoints:
                pid = op.product_id.id
                if pid not in orderpoint_map:
                    orderpoint_map[pid] = {
                        'min': op.product_min_qty,
                        'max': op.product_max_qty,
                    }
        except Exception:
            pass

        rows = []
        for p in products:
            # Use computed fields from product.product which aggregate stock.quant
            # These are more reliable than manual quant queries and handle
            # warehouse contexts correctly.
            stock_qty = 0.0
            reserved_qty = 0.0
            try:
                # qty_available = on hand, virtual_available = forecasted
                # outgoing_qty = reserved for outgoing
                stock_qty = p.qty_available or 0.0
                reserved_qty = (p.qty_available or 0.0) - (p.free_qty or 0.0)
            except Exception:
                # Fallback: try stock.quant directly
                try:
                    Quant = self.env['stock.quant'].sudo()
                    quants = Quant.search([
                        ('product_id', '=', p.id),
                        ('location_id.usage', '=', 'internal'),
                    ])
                    for q in quants:
                        stock_qty += q.quantity
                        reserved_qty += getattr(q, 'reserved_quantity', 0.0)
                except Exception:
                    pass

            # Determine product type string
            ptype = getattr(p, 'detailed_type', None) or getattr(p, 'type', 'consu')

            # Get reorder rules from pre-fetched map
            reorder_min = reorder_max = 0.0
            if p.id in orderpoint_map:
                reorder_min = orderpoint_map[p.id]['min']
                reorder_max = orderpoint_map[p.id]['max']

            # Get full category path for better classification
            category = ''
            try:
                if p.categ_id:
                    category = p.categ_id.complete_name or p.categ_id.name or ''
            except Exception:
                category = p.categ_id.name if p.categ_id else ''

            rows.append({
                'odoo_product_id': p.id,
                'name': p.name,
                'internal_ref': p.default_code or '',
                'category': category,
                'uom': p.uom_id.name if p.uom_id else '',
                'uom_id': p.uom_id.id if p.uom_id else None,
                'product_type': ptype,
                'stock_qty': round(stock_qty, 2),
                'reserved_qty': round(reserved_qty, 2),
                'available_qty': round(stock_qty - reserved_qty, 2),
                'reorder_min': round(reorder_min, 2),
                'reorder_max': round(reorder_max, 2),
                'standard_price': round(p.standard_price, 2),
                'list_price': round(p.lst_price, 2),
                'avg_cost': round(p.avg_cost, 2) if hasattr(p, 'avg_cost') and p.avg_cost else None,
                'weight': round(p.weight, 4) if hasattr(p, 'weight') and p.weight else None,
                'active': p.active,
                'odoo_company_id': p.company_id.id if p.company_id else None,
                'updated_at': datetime.now().isoformat(),
            })

        return client.upsert('odoo_products', rows, on_conflict='odoo_product_id', batch_size=100)

    # ── Order Lines (Sale + Purchase, ALL history) ────────────────────

    def _push_order_lines(self, client: SupabaseClient, last_sync=None) -> int:
        rows = []

        # Sale order lines
        try:
            SOLine = self.env['sale.order.line'].sudo()
            so_domain = [
                ('order_id.state', 'in', ['sale', 'done']),
                ('display_type', '=', False),
            ]
            if last_sync:
                so_domain.append(('write_date', '>=', last_sync.strftime('%Y-%m-%d %H:%M:%S')))
            lines = SOLine.search(so_domain)
            for l in lines:
                o = l.order_id
                # MXN conversion: use order's amount_total vs company currency
                currency = o.currency_id.name if o.currency_id else 'MXN'
                mxn_ratio = 1.0
                if currency != 'MXN':
                    try:
                        # sale.order doesn't have amount_total_signed, use
                        # the currency rate at order date as approximation
                        company_cur = o.company_id.currency_id if o.company_id else None
                        if company_cur and o.currency_id and company_cur != o.currency_id:
                            mxn_ratio = o.currency_id._convert(
                                1.0, company_cur, o.company_id,
                                o.date_order or datetime.now().date(),
                            )
                    except Exception:
                        pass
                line_uom_obj = getattr(l, 'product_uom', None) or getattr(l, 'product_uom_id', None)
                rows.append({
                    'odoo_line_id': l.id,
                    'odoo_order_id': o.id,
                    'odoo_partner_id': _commercial_partner_id(o.partner_id),
                    'odoo_product_id': l.product_id.id if l.product_id else None,
                    'order_name': o.name,
                    'order_date': o.date_order.strftime('%Y-%m-%d') if o.date_order else None,
                    'order_type': 'sale',
                    'order_state': o.state,
                    'product_name': l.product_id.name if l.product_id else '',
                    'product_ref': l.product_id.default_code or '' if l.product_id else '',
                    'qty': round(l.product_uom_qty, 2),
                    'qty_delivered': round(getattr(l, 'qty_delivered', 0) or 0, 2),
                    'qty_invoiced': round(getattr(l, 'qty_invoiced', 0) or 0, 2),
                    'price_unit': round(l.price_unit, 2),
                    'discount': round(l.discount, 2),
                    'subtotal': round(l.price_subtotal, 2),
                    'subtotal_mxn': round(l.price_subtotal * mxn_ratio, 2),
                    'currency': currency,
                    'line_uom': line_uom_obj.name if line_uom_obj else None,
                    'line_uom_id': line_uom_obj.id if line_uom_obj else None,
                    'salesperson_name': o.user_id.name if o.user_id else None,
                    'odoo_company_id': o.company_id.id if o.company_id else None,
                })
        except Exception as exc:
            _logger.warning('Sale order lines: %s', exc)

        # Purchase order lines
        try:
            POLine = self.env['purchase.order.line'].sudo()
            po_domain = [
                ('order_id.state', 'in', ['purchase', 'done']),
                ('display_type', '=', False),
            ]
            if last_sync:
                po_domain.append(('write_date', '>=', last_sync.strftime('%Y-%m-%d %H:%M:%S')))
            po_lines = POLine.search(po_domain)
            for l in po_lines:
                o = l.order_id
                currency = o.currency_id.name if o.currency_id else 'MXN'
                mxn_ratio = 1.0
                if currency != 'MXN':
                    try:
                        company_cur = o.company_id.currency_id if o.company_id else None
                        if company_cur and o.currency_id and company_cur != o.currency_id:
                            mxn_ratio = o.currency_id._convert(
                                1.0, company_cur, o.company_id,
                                o.date_order or datetime.now().date(),
                            )
                    except Exception:
                        pass
                line_uom_obj = getattr(l, 'product_uom', None) or getattr(l, 'product_uom_id', None)
                rows.append({
                    'odoo_line_id': -l.id,  # Negative to avoid collision with sale lines
                    'odoo_order_id': o.id,
                    'odoo_partner_id': _commercial_partner_id(o.partner_id),
                    'odoo_product_id': l.product_id.id if l.product_id else None,
                    'order_name': o.name,
                    'order_date': o.date_order.strftime('%Y-%m-%d') if o.date_order else None,
                    'order_type': 'purchase',
                    'order_state': o.state,
                    'product_name': l.product_id.name if l.product_id else '',
                    'product_ref': l.product_id.default_code or '' if l.product_id else '',
                    'qty': round(getattr(l, 'product_uom_qty', l.product_qty), 2),
                    'qty_delivered': round(getattr(l, 'qty_received', 0) or 0, 2),
                    'qty_invoiced': round(getattr(l, 'qty_invoiced', 0) or 0, 2),
                    'price_unit': round(l.price_unit, 2),
                    'discount': 0,
                    'subtotal': round(l.price_subtotal, 2),
                    'subtotal_mxn': round(l.price_subtotal * mxn_ratio, 2),
                    'currency': currency,
                    'line_uom': line_uom_obj.name if line_uom_obj else None,
                    'line_uom_id': line_uom_obj.id if line_uom_obj else None,
                    'salesperson_name': o.user_id.name if o.user_id else None,
                    'odoo_company_id': o.company_id.id if o.company_id else None,
                })
        except Exception as exc:
            _logger.warning('Purchase order lines: %s', exc)

        return client.upsert('odoo_order_lines', rows, on_conflict='odoo_line_id', batch_size=200)

    # ── Users ────────────────────────────────────────────────────────────

    def _push_users(self, client: SupabaseClient, last_sync=None) -> int:
        User = self.env['res.users'].sudo()
        domain = [('active', '=', True), ('share', '=', False)]
        if last_sync:
            domain.append(('write_date', '>=', last_sync.strftime('%Y-%m-%d %H:%M:%S')))
        users = User.search(domain, limit=200)
        today = datetime.now().date()

        # Pre-fetch all activities for efficiency
        Activity = self.env['mail.activity'].sudo()
        all_activities = Activity.search([
            ('user_id', 'in', users.ids),
        ])
        # Group by user
        user_activities = {}
        for a in all_activities:
            uid = a.user_id.id
            if uid not in user_activities:
                user_activities[uid] = {'pending': 0, 'overdue': 0}
            user_activities[uid]['pending'] += 1
            if a.date_deadline and a.date_deadline < today:
                user_activities[uid]['overdue'] += 1

        # Pre-fetch employee records for department/job
        employee_map = {}  # user_id -> {dept, job, manager}
        try:
            Employee = self.env['hr.employee'].sudo()
            employees = Employee.search([
                ('user_id', 'in', users.ids),
                ('active', '=', True),
            ])
            for emp in employees:
                employee_map[emp.user_id.id] = {
                    'department': emp.department_id.name if emp.department_id else None,
                    'job_title': emp.job_id.name if emp.job_id else (emp.job_title or None),
                    'manager': emp.parent_id.name if emp.parent_id else None,
                    'work_location': getattr(emp, 'work_location_name', None),
                }
        except Exception as exc:
            _logger.warning('HR employee fetch: %s', exc)

        rows = []
        for u in users:
            acts = user_activities.get(u.id, {'pending': 0, 'overdue': 0})
            emp = employee_map.get(u.id, {})

            rows.append({
                'odoo_user_id': u.id,
                'name': u.name,
                'email': u.email or u.login,
                'department': emp.get('department'),
                'job_title': emp.get('job_title') or getattr(u, 'job_title', None),
                'pending_activities_count': acts['pending'],
                'overdue_activities_count': acts['overdue'],
                'activities_json': [],  # Will be populated below
                'odoo_company_id': u.company_id.id if u.company_id else None,
                'updated_at': datetime.now().isoformat(),
            })

        return client.upsert('odoo_users', rows, on_conflict='odoo_user_id')

    # ── Invoices (ALL history) ──────────────────────────────────────────

    def _push_invoices(self, client: SupabaseClient, last_sync=None) -> int:
        core = IngestionCore(client)
        run_id, core_watermark = core.start_run(
            source='odoo',
            table='odoo_invoices',
            run_type='full' if not last_sync else 'incremental',
            triggered_by='cron',
        )
        effective_watermark = core_watermark or (last_sync.isoformat() if last_sync else None)
        status = 'success'
        final_watermark = effective_watermark
        ok = 0
        try:
            Move = self.env['account.move'].sudo()
            cid = self._get_company_id()
            domain = [
                ('company_id', '=', cid),
                ('move_type', 'in', [
                    'out_invoice', 'out_refund',
                    'in_invoice', 'in_refund',
                ]),
                ('state', '=', 'posted'),
            ]
            if last_sync:
                domain.append(('write_date', '>=', last_sync.strftime('%Y-%m-%d %H:%M:%S')))
            invoices = Move.search(domain)

            # CFDI UUID + SAT state: bypasses the stored computed field on
            # account.move which is stale for post-migration invoices (Jul 2025+).
            # Reads directly from l10n_mx_edi.document via SQL (M2M aware) and
            # ORM XML parsing (filestore-safe). See _build_cfdi_map docstring.
            cfdi_map = _build_cfdi_map(self.env, invoices.ids)

            # Payment dates from account.partial.reconcile (real payment date)
            payment_date_map = _build_payment_date_map(self.env, invoices.ids)

            today = datetime.now().date()
            rows = []
            for inv in invoices:
                pid = _commercial_partner_id(inv.partner_id)
                if not pid:
                    continue

                days_overdue = 0
                if inv.payment_state in ('not_paid', 'partial') and inv.invoice_date_due:
                    if inv.invoice_date_due < today:
                        days_overdue = (today - inv.invoice_date_due).days

                # Payment term
                pay_term = None
                try:
                    if inv.invoice_payment_term_id:
                        pay_term = inv.invoice_payment_term_id.name
                except Exception:
                    pass

                # CFDI fields from pre-read map
                cfdi = cfdi_map.get(inv.id, {})
                cfdi_uuid = cfdi.get('uuid')
                cfdi_sat = cfdi.get('sat')

                # Payment date and days_to_pay from reconciliation
                pay_date = payment_date_map.get(inv.id)
                pay_date_str = pay_date.strftime('%Y-%m-%d') if pay_date else None
                days_to_pay = None
                if pay_date and inv.invoice_date:
                    delta = (pay_date - inv.invoice_date).days
                    days_to_pay = max(delta, 0)

                # MXN amounts: amount_total_signed is always in company
                # currency (MXN).  For MXN invoices the value equals
                # amount_total; for USD/EUR it is the converted amount.
                # sign: out_invoice positive, in_invoice negative by Odoo
                # convention — we store absolute value so sums make sense.
                amt_signed = getattr(inv, 'amount_total_signed', None)
                amount_total_mxn = round(abs(amt_signed), 2) if amt_signed is not None else None
                # amount_untaxed_signed doesn't exist, derive from ratio
                if amount_total_mxn and inv.amount_total:
                    ratio = abs(amt_signed) / inv.amount_total if inv.amount_total else 1.0
                    amount_untaxed_mxn = round(inv.amount_untaxed * ratio, 2)
                    amount_residual_mxn = round(inv.amount_residual * ratio, 2)
                else:
                    amount_untaxed_mxn = round(inv.amount_untaxed, 2)
                    amount_residual_mxn = round(inv.amount_residual, 2)

                # Salesperson: from linked sale order or invoice's user
                salesperson_name = None
                salesperson_user_id = None
                try:
                    # Prefer the invoice's own user_id (commercial responsible)
                    if inv.invoice_user_id:
                        salesperson_name = inv.invoice_user_id.name
                        salesperson_user_id = inv.invoice_user_id.id
                    elif inv.user_id:
                        salesperson_name = inv.user_id.name
                        salesperson_user_id = inv.user_id.id
                except Exception:
                    pass

                rows.append({
                    'odoo_invoice_id': inv.id,
                    'odoo_partner_id': pid,
                    'name': inv.name,
                    'move_type': inv.move_type,
                    'amount_total': round(inv.amount_total, 2),
                    'amount_residual': round(inv.amount_residual, 2),
                    'amount_tax': round(inv.amount_tax, 2) if hasattr(inv, 'amount_tax') else None,
                    'amount_untaxed': round(inv.amount_untaxed, 2) if hasattr(inv, 'amount_untaxed') else None,
                    'amount_paid': round(inv.amount_total - inv.amount_residual, 2),
                    'amount_total_mxn': amount_total_mxn,
                    'amount_untaxed_mxn': amount_untaxed_mxn,
                    'amount_residual_mxn': amount_residual_mxn,
                    'currency': inv.currency_id.name if inv.currency_id else 'MXN',
                    'invoice_date': inv.invoice_date.strftime('%Y-%m-%d') if inv.invoice_date else None,
                    'due_date': inv.invoice_date_due.strftime('%Y-%m-%d') if inv.invoice_date_due else None,
                    'state': inv.state,
                    'payment_state': inv.payment_state,
                    'days_overdue': days_overdue,
                    'days_to_pay': days_to_pay,
                    'payment_date': pay_date_str,
                    'payment_term': pay_term,
                    'cfdi_uuid': cfdi_uuid,
                    'cfdi_sat_state': cfdi_sat,
                    'salesperson_name': salesperson_name,
                    'salesperson_user_id': salesperson_user_id,
                    'ref': inv.ref or '',
                    'write_date': inv.write_date.strftime('%Y-%m-%dT%H:%M:%S') if inv.write_date else None,
                    'odoo_company_id': inv.company_id.id if inv.company_id else None,
                })

            # Deduplicate: keep last occurrence per (odoo_partner_id, name)
            # to avoid "ON CONFLICT DO UPDATE cannot affect row a second time"
            seen = {}
            for row in rows:
                seen[(row['odoo_partner_id'], row['name'])] = row
            rows = list(seen.values())

            # === swap upsert → upsert_with_details ===
            ok, failed = client.upsert_with_details(
                'odoo_invoices', rows, on_conflict='odoo_partner_id,name', batch_size=200
            )
            core.report_batch(run_id, attempted=len(rows), succeeded=ok, failed=len(failed))
            for row, err in failed:
                core.report_failure(
                    run_id=run_id,
                    entity_id=str(row.get('name') or row.get('odoo_partner_id') or ''),
                    error_code=err['code'],
                    error_detail=err['detail'],
                    payload=row,
                )
            if failed:
                status = 'partial'
            if rows:
                final_watermark = max(
                    (r.get('write_date') for r in rows if r.get('write_date')),
                    default=effective_watermark,
                )
        except Exception as e:
            status = 'failed'
            _logger.exception('push_invoices failed: %s', e)
            core.complete_run(run_id, status=status, high_watermark=effective_watermark)
            raise
        core.complete_run(run_id, status=status, high_watermark=final_watermark)
        return ok

    # ── Invoice Lines (ALL history) ──────────────────────────────────────

    def _compute_invoice_fx_ratio(self, inv) -> float:
        """Resuelve MXN-per-native-unit para una factura.

        Fallback chain (H12):
          1. amount_total_signed / amount_total (sanity-checked)
          2. currency_id._convert() a company currency en invoice_date
          3. res.currency.rate.rate en la fecha de la factura
          4. 1.0 (MXN-native o no resoluble)

        Extraído como helper para ser reutilizable + testeable.
        """
        inv_currency = inv.currency_id.name if inv.currency_id else 'MXN'
        mxn_ratio = 1.0

        amt_signed = getattr(inv, 'amount_total_signed', None)
        if amt_signed is not None and inv.amount_total:
            ratio_from_signed = abs(amt_signed) / inv.amount_total
            # Sanity: en non-MXN un ratio ≈1.0 es FX no aplicada, forzar fallback.
            if ratio_from_signed > 0 and not (
                inv_currency != 'MXN' and abs(ratio_from_signed - 1.0) < 0.001
            ):
                mxn_ratio = ratio_from_signed

        if mxn_ratio == 1.0 and inv_currency != 'MXN' and inv.currency_id:
            try:
                company = inv.company_id or self.env.company
                target = company.currency_id
                on_date = inv.invoice_date or inv.date or datetime.now().date()
                converted = inv.currency_id._convert(
                    1.0, target, company, on_date, round=False,
                )
                if converted and converted > 0:
                    mxn_ratio = float(converted)
            except Exception as exc:
                _logger.debug('FX _convert failed for %s: %s', inv.name, exc)

        if mxn_ratio == 1.0 and inv_currency != 'MXN':
            try:
                Rate = self.env['res.currency.rate'].sudo()
                on_date = inv.invoice_date or datetime.now().date()
                rate_row = Rate.search(
                    [
                        ('currency_id', '=', inv.currency_id.id),
                        ('name', '<=', on_date.strftime('%Y-%m-%d')),
                    ],
                    order='name desc',
                    limit=1,
                )
                if rate_row and rate_row.rate:
                    mxn_ratio = 1.0 / float(rate_row.rate)
            except Exception as exc:
                _logger.debug(
                    'FX res.currency.rate fallback failed for %s: %s',
                    inv.name, exc,
                )

        return mxn_ratio

    def _push_invoice_lines(self, client: SupabaseClient, last_sync=None) -> int:
        """Push account.move.line → odoo_invoice_lines table.

        H11 refactor (2026-04-17): antes iteraba invoice por invoice y accedía
        `inv.invoice_line_ids` lazy, causando N+1 queries ORM. Para 14,520
        facturas esto rebasaba el timeout del cron → 97% de invoices sin
        lines pushed. Ahora:
          1. Search invoices + precompute FX ratios (UNA vez)
          2. Bulk search de TODAS las lines con move_id IN (...)
          3. Single pass sobre lines usando inv_map precomputado
        """
        Move = self.env['account.move'].sudo()
        cid = self._get_company_id()

        domain = [
            ('company_id', '=', cid),
            ('move_type', 'in', [
                'out_invoice', 'out_refund', 'in_invoice', 'in_refund',
            ]),
            ('state', '=', 'posted'),
        ]
        if last_sync:
            domain.append(('write_date', '>=', last_sync.strftime('%Y-%m-%d %H:%M:%S')))
        invoices = Move.search(domain)

        if not invoices:
            return 0

        # Precompute metadata + FX ratios (un loop sobre invoices solo).
        inv_map: dict[int, dict] = {}
        ratios: dict[int, float] = {}
        for inv in invoices:
            pid = _commercial_partner_id(inv.partner_id)
            if not pid:
                continue
            inv_map[inv.id] = {
                'pid': pid,
                'name': inv.name,
                'move_type': inv.move_type,
                'invoice_date': (
                    inv.invoice_date.strftime('%Y-%m-%d') if inv.invoice_date else None
                ),
                'currency': inv.currency_id.name if inv.currency_id else 'MXN',
                'company_id': inv.company_id.id if inv.company_id else None,
            }
            ratios[inv.id] = self._compute_invoice_fx_ratio(inv)

        if not inv_map:
            return 0

        _logger.info(
            '_push_invoice_lines: %d invoices, fetching lines in bulk',
            len(inv_map),
        )

        # Bulk fetch de lines — reemplaza el lazy loop `for inv: for line in inv.invoice_line_ids`.
        Line = self.env['account.move.line'].sudo()
        lines = Line.search([
            ('move_id', 'in', list(inv_map.keys())),
            ('display_type', 'not in',
             ['line_section', 'line_note', 'payment_term', 'tax', 'rounding']),
        ])

        rows = []
        for line in lines:
            mv_id = line.move_id.id
            ctx = inv_map.get(mv_id)
            if not ctx:
                continue
            ratio = ratios.get(mv_id, 1.0)
            line_uom_obj = getattr(line, 'product_uom_id', None)
            rows.append({
                'odoo_line_id': line.id,
                'odoo_move_id': mv_id,
                'odoo_partner_id': ctx['pid'],
                'move_name': ctx['name'],
                'move_type': ctx['move_type'],
                'invoice_date': ctx['invoice_date'],
                'odoo_product_id': line.product_id.id if line.product_id else None,
                'product_name': (
                    line.product_id.name if line.product_id else (line.name or '')[:200]
                ),
                'product_ref': line.product_id.default_code or '' if line.product_id else '',
                # price_unit y quantity con 6 decimales — Odoo internamente
                # usa Product Price precision (6 por default), que al
                # redondear a 2 en items con cantidad enorme (millones) causa
                # drift de miles $$ vs price_subtotal (el "oficial" de Odoo).
                # Fase 2 fix — audit invariant invoice_lines.price_recompute
                # detectó 31,883 líneas con drift por este redondeo.
                'quantity': round(line.quantity, 6),
                'price_unit': round(line.price_unit, 6),
                'discount': round(line.discount, 2),
                'price_subtotal': round(line.price_subtotal, 2),
                'price_total': round(line.price_total, 2),
                'currency': ctx['currency'],
                'price_subtotal_mxn': round(line.price_subtotal * ratio, 2),
                'price_total_mxn': round(line.price_total * ratio, 2),
                'line_uom': line_uom_obj.name if line_uom_obj else None,
                'line_uom_id': line_uom_obj.id if line_uom_obj else None,
                'odoo_company_id': ctx['company_id'],
            })

        _logger.info(
            '_push_invoice_lines: upserting %d rows from %d invoices',
            len(rows), len(inv_map),
        )
        return client.upsert('odoo_invoice_lines', rows,
                              on_conflict='odoo_line_id', batch_size=200)

    # ── Payments (last 180 days) ─────────────────────────────────────────

    def _push_payments(self, client: SupabaseClient, last_sync=None) -> int:
        """Push payment data extracted from paid/partial invoices.

        Odoo uses bank reconciliation (not account.payment records),
        so we extract payment info from invoice amount_residual changes.

        NOTE: This is a "proxy" payment table. odoo_account_payments has the
        real account.payment records with bank/method details. This table is
        kept for backward-compat but now includes payment_category and
        correct payment_date from account.partial.reconcile.
        """
        core = IngestionCore(client)
        run_id, core_watermark = core.start_run(
            source='odoo',
            table='odoo_payments',
            run_type='full' if not last_sync else 'incremental',
            triggered_by='cron',
        )
        effective_watermark = core_watermark or (last_sync.isoformat() if last_sync else None)
        status = 'success'
        final_watermark = effective_watermark
        ok = 0
        try:
            Move = self.env['account.move'].sudo()

            # Get invoices that have been paid or partially paid
            # EXCLUDE payroll (entry type) — only customer/supplier invoices
            cid = self._get_company_id()
            domain = [
                ('company_id', '=', cid),
                ('move_type', 'in', [
                    'out_invoice', 'out_refund',
                    'in_invoice', 'in_refund',
                ]),
                ('state', '=', 'posted'),
                ('payment_state', 'in', ['paid', 'in_payment', 'partial']),
            ]
            if last_sync:
                domain.append(('write_date', '>=', last_sync.strftime('%Y-%m-%d %H:%M:%S')))
            invoices = Move.search(domain)

            # Real payment dates from account.partial.reconcile
            payment_date_map = _build_payment_date_map(self.env, invoices.ids)

            rows = []
            for inv in invoices:
                pid = _commercial_partner_id(inv.partner_id)
                if not pid:
                    continue

                amount_paid = inv.amount_total - inv.amount_residual
                if amount_paid <= 0:
                    continue

                # Determine payment category
                if inv.move_type in ('out_invoice', 'out_refund'):
                    pay_category = 'customer'
                elif inv.move_type in ('in_invoice', 'in_refund'):
                    pay_category = 'supplier'
                else:
                    pay_category = 'other'

                # Use REAL payment date from reconciliation (not write_date)
                real_pay_date = payment_date_map.get(inv.id)
                if real_pay_date:
                    payment_date_str = real_pay_date.strftime('%Y-%m-%d')
                elif inv.invoice_date:
                    # Fallback: invoice_date (better than write_date)
                    payment_date_str = inv.invoice_date.strftime('%Y-%m-%d')
                else:
                    payment_date_str = None

                # MXN conversion
                amt_signed = getattr(inv, 'amount_total_signed', None)
                if amt_signed is not None and inv.amount_total:
                    mxn_ratio = abs(amt_signed) / inv.amount_total
                    amount_mxn = round(amount_paid * mxn_ratio, 2)
                else:
                    amount_mxn = round(amount_paid, 2)

                rows.append({
                    'odoo_partner_id': pid,
                    'name': f'PAY-{inv.name}',
                    'payment_type': 'inbound' if inv.move_type in ('out_invoice', 'in_refund') else 'outbound',
                    'amount': round(amount_paid, 2),
                    'amount_mxn': amount_mxn,
                    'currency': inv.currency_id.name if inv.currency_id else 'MXN',
                    'payment_date': payment_date_str,
                    'payment_category': pay_category,
                    'state': 'posted',
                    'write_date': inv.write_date.strftime('%Y-%m-%dT%H:%M:%S') if inv.write_date else None,
                    'odoo_company_id': inv.company_id.id if inv.company_id else None,
                })

            # === swap upsert → upsert_with_details ===
            ok, failed = client.upsert_with_details(
                'odoo_payments', rows, on_conflict='odoo_partner_id,name', batch_size=200
            )
            core.report_batch(run_id, attempted=len(rows), succeeded=ok, failed=len(failed))
            for row, err in failed:
                core.report_failure(
                    run_id=run_id,
                    entity_id=str(row.get('name') or row.get('odoo_partner_id') or ''),
                    error_code=err['code'],
                    error_detail=err['detail'],
                    payload=row,
                )
            if failed:
                status = 'partial'
            if rows:
                final_watermark = max(
                    (r.get('write_date') for r in rows if r.get('write_date')),
                    default=effective_watermark,
                )
        except Exception as e:
            status = 'failed'
            _logger.exception('push_payments failed: %s', e)
            core.complete_run(run_id, status=status, high_watermark=effective_watermark)
            raise
        core.complete_run(run_id, status=status, high_watermark=final_watermark)
        return ok

    # ── Deliveries (pending + last 90 days) ──────────────────────────────

    def _push_deliveries(self, client: SupabaseClient, last_sync=None) -> int:
        Picking = self.env['stock.picking'].sudo()
        # Cutoff: 365 días para cubrir reportes 12m rolling. Antes 90d
        # dejaba todo 2025 sin sincronizar (audit invariant expuso el gap
        # 2026-04-20). Para backfill histórico completo ver
        # manual_backfill_deliveries() en sync_backfill.
        cutoff = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        # Include BOTH outgoing (to customers) and incoming (from suppliers)
        # for full OTD tracking on both sides of the supply chain.
        domain = [
            ('picking_type_code', 'in', ['outgoing', 'incoming']),
            '|',
            ('state', 'not in', ['done', 'cancel']),
            ('date_done', '>=', cutoff),
        ]
        if last_sync:
            domain.append(('write_date', '>=', last_sync.strftime('%Y-%m-%d %H:%M:%S')))
        pickings = Picking.search(domain)

        now = datetime.now()
        rows = []
        for pk in pickings:
            pid = _commercial_partner_id(pk.partner_id) if pk.partner_id else None
            if not pid:
                continue

            is_late = (
                pk.state not in ('done', 'cancel')
                and pk.scheduled_date
                and pk.scheduled_date < now
            )
            lead_time = None
            if pk.state == 'done' and pk.date_done and pk.create_date:
                lead_time = round((pk.date_done - pk.create_date).total_seconds() / 86400, 1)

            rows.append({
                'odoo_picking_id': pk.id,
                'odoo_partner_id': pid,
                'name': pk.name,
                'picking_type': pk.picking_type_id.name if pk.picking_type_id else '',
                'picking_type_code': pk.picking_type_code or '',
                'origin': pk.origin or '',
                'scheduled_date': pk.scheduled_date.strftime('%Y-%m-%d') if pk.scheduled_date else None,
                'date_done': pk.date_done.isoformat() if pk.date_done else None,
                'create_date': pk.create_date.strftime('%Y-%m-%d') if pk.create_date else None,
                'state': pk.state,
                'is_late': is_late,
                'lead_time_days': lead_time,
                'odoo_company_id': pk.company_id.id if pk.company_id else None,
            })

        return client.upsert('odoo_deliveries', rows,
                              on_conflict='odoo_picking_id', batch_size=200)

    # ── CRM Leads ────────────────────────────────────────────────────────

    def _push_crm_leads(self, client: SupabaseClient, last_sync=None) -> int:
        Lead = self.env['crm.lead'].sudo()
        cid = self._get_company_id()
        domain = [('active', '=', True), ('company_id', '=', cid)]
        if last_sync:
            domain.append(('write_date', '>=', last_sync.strftime('%Y-%m-%d %H:%M:%S')))
        leads = Lead.search(domain)
        now = datetime.now()

        rows = []
        for l in leads:
            days_open = (now - l.create_date).days if l.create_date else 0
            rows.append({
                'odoo_lead_id': l.id,
                'odoo_partner_id': _commercial_partner_id(l.partner_id) if l.partner_id else None,
                'name': l.name,
                'lead_type': l.type or 'lead',
                'stage': l.stage_id.name if l.stage_id else '',
                'expected_revenue': round(l.expected_revenue, 2),
                'probability': round(l.probability, 1),
                'date_deadline': l.date_deadline.strftime('%Y-%m-%d') if l.date_deadline else None,
                'create_date': l.create_date.strftime('%Y-%m-%d') if l.create_date else None,
                'days_open': days_open,
                'assigned_user': l.user_id.name if l.user_id else '',
                'active': l.active,
                'odoo_company_id': l.company_id.id if l.company_id else None,
            })

        return client.upsert('odoo_crm_leads', rows,
                              on_conflict='odoo_lead_id', batch_size=200)

    # ── Activities (full refresh) ────────────────────────────────────────

    def _push_activities(self, client: SupabaseClient) -> int:
        Activity = self.env['mail.activity'].sudo()
        activities = Activity.search([])
        today = datetime.now().date()

        rows = []
        for a in activities:
            # Resolve partner from related model
            pid = self._resolve_activity_partner(a)
            rows.append({
                'odoo_partner_id': pid,
                'activity_type': a.activity_type_id.name if a.activity_type_id else 'Tarea',
                'summary': a.summary or (a.note or '')[:200],
                'res_model': a.res_model,
                'res_id': a.res_id,
                'date_deadline': a.date_deadline.strftime('%Y-%m-%d') if a.date_deadline else None,
                'assigned_to': a.user_id.name if a.user_id else '',
                'is_overdue': a.date_deadline < today if a.date_deadline else False,
                'synced_at': datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
            })

        # Full refresh: delete then insert. If insert fails, activities
        # are temporarily empty until next successful sync (every 1h).
        # This is acceptable since activities are non-critical display data.
        client.delete_all('odoo_activities')
        return client.insert('odoo_activities', rows, batch_size=200)

    def _resolve_activity_partner(self, activity) -> int | None:
        """Resolve partner ID from activity's related model."""
        if activity.res_model == 'res.partner':
            return activity.res_id

        partner_models = ['sale.order', 'account.move', 'purchase.order', 'crm.lead']
        if activity.res_model in partner_models:
            try:
                record = self.env[activity.res_model].sudo().browse(activity.res_id)
                if record.exists() and record.partner_id:
                    return _commercial_partner_id(record.partner_id)
            except Exception:
                pass
        return None

    # ── Manufacturing Orders ─────────────────────────────────────────────

    def _push_manufacturing(self, client: SupabaseClient, last_sync=None) -> int:
        """Push mrp.production → odoo_manufacturing table."""
        try:
            MO = self.env['mrp.production'].sudo()
        except KeyError:
            _logger.info('mrp.production not available, skipping manufacturing sync')
            return 0

        cid = self._get_company_id()
        cutoff = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
        domain = [
            ('company_id', '=', cid),
            '|',
            ('state', 'not in', ['done', 'cancel']),
            ('date_start', '>=', cutoff),
        ]
        if last_sync:
            domain.append(('write_date', '>=', last_sync.strftime('%Y-%m-%d %H:%M:%S')))
        productions = MO.search(domain)

        rows = []
        for mo in productions:
            rows.append({
                'odoo_production_id': mo.id,
                'name': mo.name,
                'product_name': mo.product_id.name if mo.product_id else '',
                'odoo_product_id': mo.product_id.id if mo.product_id else None,
                'qty_planned': round(mo.product_qty, 2),
                'qty_produced': round(getattr(mo, 'qty_produced', 0) or 0, 2),
                'state': mo.state,
                'date_start': mo.date_start.isoformat() if mo.date_start else None,
                'date_finished': mo.date_finished.isoformat() if mo.date_finished else None,
                'create_date': mo.create_date.strftime('%Y-%m-%d') if mo.create_date else None,
                'assigned_user': mo.user_id.name if mo.user_id else '',
                'origin': mo.origin or '',
                'odoo_company_id': mo.company_id.id if mo.company_id else None,
            })

        return client.upsert('odoo_manufacturing', rows,
                              on_conflict='odoo_production_id', batch_size=200)

    # ── HR Employees ─────────────────────────────────────────────────────

    def _push_employees(self, client: SupabaseClient, last_sync=None) -> int:
        """Push hr.employee → odoo_employees table."""
        try:
            Employee = self.env['hr.employee'].sudo()
        except KeyError:
            _logger.info('hr.employee not available, skipping')
            return 0

        cid = self._get_company_id()
        domain = [('active', '=', True), ('company_id', '=', cid)]
        if last_sync:
            domain.append(('write_date', '>=', last_sync.strftime('%Y-%m-%d %H:%M:%S')))
        employees = Employee.search(domain, limit=500)
        rows = []
        for emp in employees:
            rows.append({
                'odoo_employee_id': emp.id,
                'odoo_user_id': emp.user_id.id if emp.user_id else None,
                'name': emp.name,
                'work_email': emp.work_email or (emp.user_id.email if emp.user_id else None),
                'work_phone': emp.work_phone or emp.mobile_phone or None,
                'department_name': emp.department_id.name if emp.department_id else None,
                'department_id': emp.department_id.id if emp.department_id else None,
                'job_title': emp.job_title or None,
                'job_name': emp.job_id.name if emp.job_id else None,
                'manager_name': emp.parent_id.name if emp.parent_id else None,
                'manager_id': emp.parent_id.id if emp.parent_id else None,
                'coach_name': emp.coach_id.name if emp.coach_id else None,
                'is_active': emp.active,
                'odoo_company_id': emp.company_id.id if emp.company_id else None,
            })

        return client.upsert('odoo_employees', rows,
                              on_conflict='odoo_employee_id', batch_size=100)

    # ── HR Departments ───────────────────────────────────────────────────

    def _push_departments(self, client: SupabaseClient, last_sync=None) -> int:
        """Push hr.department → odoo_departments table."""
        try:
            Dept = self.env['hr.department'].sudo()
        except KeyError:
            _logger.info('hr.department not available, skipping')
            return 0

        cid = self._get_company_id()
        domain = [('active', '=', True), ('company_id', '=', cid)]
        if last_sync:
            domain.append(('write_date', '>=', last_sync.strftime('%Y-%m-%d %H:%M:%S')))
        departments = Dept.search(domain, limit=200)
        rows = []
        for dept in departments:
            # Count members
            member_count = 0
            try:
                member_count = len(dept.member_ids) if hasattr(dept, 'member_ids') else 0
            except Exception:
                pass

            rows.append({
                'odoo_department_id': dept.id,
                'name': dept.name,
                'parent_name': dept.parent_id.name if dept.parent_id else None,
                'parent_id': dept.parent_id.id if dept.parent_id else None,
                'manager_name': dept.manager_id.name if dept.manager_id else None,
                'manager_id': dept.manager_id.id if dept.manager_id else None,
                'member_count': member_count,
                'odoo_company_id': dept.company_id.id if dept.company_id else None,
            })

        return client.upsert('odoo_departments', rows,
                              on_conflict='odoo_department_id', batch_size=100)

    # ── Sale Orders (headers) ────────────────────────────────────────────

    def _push_sale_orders(self, client: SupabaseClient, last_sync=None) -> int:
        """Push sale.order headers → odoo_sale_orders table."""
        try:
            SO = self.env['sale.order'].sudo()
        except KeyError:
            _logger.info('sale.order not available, skipping')
            return 0

        cid = self._get_company_id()
        domain = [
            ('company_id', '=', cid),
            ('state', 'in', ['sale', 'done']),
        ]
        if last_sync:
            domain.append(('write_date', '>=', last_sync.strftime('%Y-%m-%d %H:%M:%S')))
        orders = SO.search(domain)

        rows = []
        for o in orders:
            pid = _commercial_partner_id(o.partner_id) if o.partner_id else None

            # Margin calculation
            margin = None
            margin_pct = None
            try:
                margin = round(o.margin, 2) if hasattr(o, 'margin') else None
                if margin is not None and o.amount_untaxed > 0:
                    margin_pct = round(margin / o.amount_untaxed * 100, 1)
            except Exception:
                pass

            # MXN conversion for multi-currency orders
            currency = o.currency_id.name if o.currency_id else 'MXN'
            amount_total_mxn = round(o.amount_total, 2)
            amount_untaxed_mxn = round(o.amount_untaxed, 2)
            if currency != 'MXN':
                try:
                    company_cur = o.company_id.currency_id if o.company_id else None
                    if company_cur and o.currency_id and company_cur != o.currency_id:
                        rate = o.currency_id._convert(
                            1.0, company_cur, o.company_id,
                            o.date_order or datetime.now().date(),
                        )
                        amount_total_mxn = round(o.amount_total * rate, 2)
                        amount_untaxed_mxn = round(o.amount_untaxed * rate, 2)
                except Exception:
                    pass

            rows.append({
                'odoo_order_id': o.id,
                'name': o.name,
                'odoo_partner_id': pid,
                'salesperson_name': o.user_id.name if o.user_id else None,
                'salesperson_email': o.user_id.email if o.user_id else None,
                'salesperson_user_id': o.user_id.id if o.user_id else None,
                'team_name': o.team_id.name if hasattr(o, 'team_id') and o.team_id else None,
                'amount_total': round(o.amount_total, 2),
                'amount_untaxed': round(o.amount_untaxed, 2),
                'amount_total_mxn': amount_total_mxn,
                'amount_untaxed_mxn': amount_untaxed_mxn,
                'margin': margin,
                'margin_percent': margin_pct,
                'currency': currency,
                'state': o.state,
                'date_order': o.date_order.strftime('%Y-%m-%d') if o.date_order else None,
                'commitment_date': o.commitment_date.strftime('%Y-%m-%d') if hasattr(o, 'commitment_date') and o.commitment_date else None,
                'create_date': o.create_date.strftime('%Y-%m-%d') if o.create_date else None,
                'odoo_company_id': o.company_id.id if o.company_id else None,
            })

        return client.upsert('odoo_sale_orders', rows,
                              on_conflict='odoo_order_id', batch_size=200)

    # ── Purchase Orders (headers) ────────────────────────────────────────

    def _push_purchase_orders(self, client: SupabaseClient, last_sync=None) -> int:
        """Push purchase.order headers → odoo_purchase_orders table."""
        try:
            PO = self.env['purchase.order'].sudo()
        except KeyError:
            _logger.info('purchase.order not available, skipping')
            return 0

        cid = self._get_company_id()
        domain = [
            ('company_id', '=', cid),
            ('state', 'in', ['purchase', 'done']),
        ]
        if last_sync:
            domain.append(('write_date', '>=', last_sync.strftime('%Y-%m-%d %H:%M:%S')))
        orders = PO.search(domain)

        rows = []
        for o in orders:
            pid = _commercial_partner_id(o.partner_id) if o.partner_id else None

            # MXN conversion
            currency = o.currency_id.name if o.currency_id else 'MXN'
            amount_total_mxn = round(o.amount_total, 2)
            amount_untaxed_mxn = round(o.amount_untaxed, 2)
            if currency != 'MXN':
                try:
                    company_cur = o.company_id.currency_id if o.company_id else None
                    if company_cur and o.currency_id and company_cur != o.currency_id:
                        rate = o.currency_id._convert(
                            1.0, company_cur, o.company_id,
                            o.date_order or datetime.now().date(),
                        )
                        amount_total_mxn = round(o.amount_total * rate, 2)
                        amount_untaxed_mxn = round(o.amount_untaxed * rate, 2)
                except Exception:
                    pass

            rows.append({
                'odoo_order_id': o.id,
                'name': o.name,
                'odoo_partner_id': pid,
                'buyer_name': o.user_id.name if o.user_id else None,
                'buyer_email': o.user_id.email if o.user_id else None,
                'buyer_user_id': o.user_id.id if o.user_id else None,
                'amount_total': round(o.amount_total, 2),
                'amount_untaxed': round(o.amount_untaxed, 2),
                'amount_total_mxn': amount_total_mxn,
                'amount_untaxed_mxn': amount_untaxed_mxn,
                'currency': currency,
                'state': o.state,
                'date_order': o.date_order.strftime('%Y-%m-%d') if o.date_order else None,
                'date_approve': o.date_approve.strftime('%Y-%m-%d') if hasattr(o, 'date_approve') and o.date_approve else None,
                'create_date': o.create_date.strftime('%Y-%m-%d') if o.create_date else None,
                'odoo_company_id': o.company_id.id if o.company_id else None,
            })

        return client.upsert('odoo_purchase_orders', rows,
                              on_conflict='odoo_order_id', batch_size=200)

    # ── Stock Reorder Rules (orderpoints) ────────────────────────────────

    def _push_orderpoints(self, client: SupabaseClient, last_sync=None) -> int:
        """Push stock.warehouse.orderpoint → odoo_orderpoints table.
        Critical for desabasto (stockout) detection."""
        try:
            Orderpoint = self.env['stock.warehouse.orderpoint'].sudo()
        except KeyError:
            _logger.info('stock.warehouse.orderpoint not available, skipping')
            return 0

        cid = self._get_company_id()
        domain = [('active', '=', True), ('company_id', '=', cid)]
        if last_sync:
            domain.append(('write_date', '>=', last_sync.strftime('%Y-%m-%d %H:%M:%S')))
        orderpoints = Orderpoint.search(domain, limit=5000)

        rows = []
        for op in orderpoints:
            product = op.product_id
            qty_on_hand = 0.0
            qty_forecast = 0.0
            try:
                qty_on_hand = product.qty_available or 0.0
                qty_forecast = product.virtual_available or 0.0
            except Exception:
                pass

            rows.append({
                'odoo_orderpoint_id': op.id,
                'odoo_product_id': product.id if product else None,
                'product_name': product.name if product else '',
                'warehouse_name': op.warehouse_id.name if op.warehouse_id else '',
                'location_name': op.location_id.complete_name if op.location_id else '',
                'product_min_qty': round(op.product_min_qty, 2),
                'product_max_qty': round(op.product_max_qty, 2),
                'qty_to_order': round(getattr(op, 'qty_to_order', 0) or 0, 2),
                'qty_on_hand': round(qty_on_hand, 2),
                'qty_forecast': round(qty_forecast, 2),
                'trigger_type': getattr(op, 'trigger', 'auto'),
                'active': op.active,
                'odoo_company_id': op.company_id.id if op.company_id else None,
            })

        return client.upsert('odoo_orderpoints', rows,
                              on_conflict='odoo_orderpoint_id', batch_size=200)

    # ── Account Payments (real payment records) ─────────────────────────

    def _push_account_payments(self, client: SupabaseClient, last_sync=None) -> int:
        """Push account.payment → odoo_account_payments table.

        Real payment records from Odoo (not proxy from invoices).
        Includes payment method, journal, bank reconciliation status.
        """
        try:
            Payment = self.env['account.payment'].sudo()
        except KeyError:
            _logger.info('account.payment not available, skipping')
            return 0

        try:
            cid = self._get_company_id()
            domain = [('company_id', '=', cid)]
            # Skip incremental filter on first run (table may be empty)
            if last_sync:
                existing = client.fetch('odoo_account_payments', {'limit': '1', 'select': 'id'})
                if existing:
                    domain.append(('write_date', '>=', last_sync.strftime('%Y-%m-%d %H:%M:%S')))
            payments = Payment.search(domain, limit=5000)
            _logger.info('account_payments: found %d records', len(payments))

            rows = []
            for p in payments:
                try:
                    pid = _commercial_partner_id(p.partner_id) if p.partner_id else None

                    journal_name = None
                    try:
                        journal_name = p.journal_id.name if p.journal_id else None
                    except Exception:
                        pass

                    payment_method = None
                    try:
                        if hasattr(p, 'payment_method_line_id') and p.payment_method_line_id:
                            payment_method = p.payment_method_line_id.name
                        elif hasattr(p, 'payment_method_id') and p.payment_method_id:
                            payment_method = p.payment_method_id.name
                    except Exception:
                        pass

                    rows.append({
                        'odoo_payment_id': p.id,
                        'odoo_partner_id': pid,
                        'name': p.name or '',
                        'payment_type': p.payment_type or '',
                        'partner_type': p.partner_type or '',
                        'amount': round(p.amount or 0, 2),
                        'amount_signed': round(p.amount_company_currency_signed, 2) if hasattr(p, 'amount_company_currency_signed') and p.amount_company_currency_signed else None,
                        'currency': p.currency_id.name if p.currency_id else 'MXN',
                        'date': p.date.strftime('%Y-%m-%d') if p.date else None,
                        'ref': (p.ref or '') if hasattr(p, 'ref') else '',
                        'journal_name': journal_name,
                        'payment_method': payment_method,
                        'state': p.state or '',
                        'is_matched': bool(getattr(p, 'is_matched', False)),
                        'is_reconciled': bool(getattr(p, 'is_reconciled', False)),
                        'reconciled_invoices_count': int(getattr(p, 'reconciled_invoices_count', 0) or 0),
                        'odoo_company_id': p.company_id.id if p.company_id else None,
                    })
                except Exception as exc:
                    _logger.warning('account_payment %s: %s', p.id, exc)

            _logger.info('account_payments: pushing %d rows', len(rows))
            return client.upsert('odoo_account_payments', rows,
                                  on_conflict='odoo_payment_id', batch_size=200)
        except Exception as exc:
            _logger.error('_push_account_payments failed: %s', exc)
            return 0

    def _push_payment_invoice_links(self, client: SupabaseClient, last_sync=None) -> int:
        """Push account.payment.reconciled_invoice_ids → odoo_payment_invoice_links.

        Expone la relación payment↔invoice que Odoo mantiene en el m2m
        `reconciled_invoice_ids`. Habilita matching Syntage↔Odoo via CFDI UUID:
            Syntage doctos_relacionados[].uuid_docto
              → odoo_invoices.cfdi_uuid
              → odoo_payment_invoice_links.odoo_invoice_id
              → odoo_payment_invoice_links.odoo_payment_id
              → odoo_account_payments
        """
        try:
            Payment = self.env['account.payment'].sudo()
        except KeyError:
            _logger.info('account.payment not available, skipping payment_invoice_links')
            return 0

        try:
            cid = self._get_company_id()
            domain = [
                ('company_id', '=', cid),
                ('reconciled_invoice_ids', '!=', False),
            ]
            # Incremental por write_date del payment (si una reconciliación cambia,
            # Odoo actualiza el payment). Skip si la tabla está vacía.
            if last_sync:
                existing = client.fetch(
                    'odoo_payment_invoice_links', {'limit': '1', 'select': 'id'}
                )
                if existing:
                    domain.append(
                        ('write_date', '>=', last_sync.strftime('%Y-%m-%d %H:%M:%S'))
                    )

            payments = Payment.search(domain, limit=5000)
            _logger.info('payment_invoice_links: scanning %d payments', len(payments))

            rows = []
            touched_payment_ids = []
            for p in payments:
                try:
                    invoices = p.reconciled_invoice_ids
                    if not invoices:
                        continue
                    touched_payment_ids.append(p.id)
                    comp_id = p.company_id.id if p.company_id else None
                    for inv in invoices:
                        rows.append({
                            'odoo_payment_id': p.id,
                            'odoo_invoice_id': inv.id,
                            'odoo_company_id': comp_id,
                        })
                except Exception as exc:
                    _logger.warning('payment_invoice_links %s: %s', p.id, exc)

            # Full replace por payment_id tocado: los m2m pueden perder filas
            # (reconciliaciones deshechas). Borrar + re-insertar garantiza
            # consistencia sin correr full scan.
            if touched_payment_ids:
                # Supabase REST DELETE con filter IN. Batching para URL length.
                batch = 500
                for i in range(0, len(touched_payment_ids), batch):
                    chunk = touched_payment_ids[i:i + batch]
                    try:
                        client.delete(
                            'odoo_payment_invoice_links',
                            {'odoo_payment_id': f'in.({",".join(str(x) for x in chunk)})'},
                        )
                    except Exception as exc:
                        _logger.warning('payment_invoice_links delete chunk failed: %s', exc)

            _logger.info('payment_invoice_links: pushing %d link rows', len(rows))
            if not rows:
                return 0
            return client.upsert(
                'odoo_payment_invoice_links', rows,
                on_conflict='odoo_payment_id,odoo_invoice_id',
                batch_size=500,
            )
        except Exception as exc:
            _logger.error('_push_payment_invoice_links failed: %s', exc)
            return 0

    # ── Chart of Accounts ───────────────────────────────────────────────

    def _push_chart_of_accounts(self, client: SupabaseClient, last_sync=None) -> int:
        """Push account.account → odoo_chart_of_accounts table.

        The chart of accounts is the foundation for P&L and Balance Sheet.
        Always full sync (small table, ~100 rows).
        """
        try:
            Account = self.env['account.account'].sudo()
        except KeyError:
            _logger.info('account.account not available, skipping')
            return 0

        try:
            # Odoo 17+: account.account.code es computed per-company via
            # code_store_ids. Para Quimibond MX con multi-company (12+ orgs
            # con catálogos SAT propios), iteramos por compañía y resolvemos
            # el code en el contexto de cada una. Antes se usaba solo
            # self._get_company_id() (company 1), dejando 1,044 cuentas de
            # las otras 11 companies con code='' (audit expuso 2026-04-20).
            Account = Account.with_context(active_test=False)
            Company = self.env['res.company'].sudo()
            companies = Company.search([])

            rows = []
            seen_acc_ids = set()
            for company in companies:
                cid = company.id
                # Try the direct company_id filter first; if empty (Odoo 17+
                # shared chart mode) or if it raises, fall back to all.
                try:
                    accounts = Account.search([('company_id', '=', cid)])
                    if not accounts:
                        # Shared chart: todas las cuentas accesibles por esta
                        # company via company_ids many2many (o all).
                        accounts = Account.search([])
                except Exception:
                    accounts = Account.search([])

                accounts_ctx = accounts.with_company(cid)
                for acc in accounts_ctx:
                    try:
                        code = acc.code or ''
                        # Fallback adicional: leer directamente code_store_ids
                        if not code and hasattr(acc, 'code_store_ids'):
                            mapping = acc.code_store_ids.filtered(
                                lambda m: m.company_id.id == cid
                            )
                            if mapping:
                                code = mapping[0].code or ''

                        # Skip si la cuenta no tiene code en este contexto
                        # (significa que no "pertenece" a esta company).
                        if not code and acc.id in seen_acc_ids:
                            continue

                        acc_type = getattr(acc, 'account_type', None) or ''
                        rows.append({
                            'odoo_account_id': acc.id,
                            'code': code,
                            'name': acc.name or '',
                            'account_type': acc_type,
                            'reconcile': bool(acc.reconcile) if hasattr(acc, 'reconcile') else False,
                            'deprecated': bool(getattr(acc, 'deprecated', False)),
                            'active': bool(getattr(acc, 'active', True)),
                            'odoo_company_id': cid,
                        })
                        seen_acc_ids.add(acc.id)
                    except Exception as exc:
                        _logger.warning('chart_of_accounts %s: %s', acc.id, exc)

            _logger.info('chart_of_accounts: pushing %d rows across %d companies',
                         len(rows), len(companies))
            return client.upsert('odoo_chart_of_accounts', rows,
                                  on_conflict='odoo_account_id', batch_size=200)
        except Exception as exc:
            _logger.error('_push_chart_of_accounts failed: %s', exc)
            return 0

    # ── Account Balances (monthly, for P&L) ─────────────────────────────

    def _push_account_balances(self, client: SupabaseClient, last_sync=None) -> int:
        """Push monthly account balances → odoo_account_balances table.

        Aggregates account.move.line by account + month for P&L and
        Balance Sheet reporting. Only posted entries.
        """
        try:
            Line = self.env['account.move.line'].sudo()
        except KeyError:
            _logger.info('account.move.line not available, skipping')
            return 0

        # Use read_group for efficient aggregation in Odoo
        # Filter to operating company to avoid mixing P&L from 8 companies
        cid = self._get_company_id()
        try:
            groups = Line.read_group(
                domain=[
                    ('parent_state', '=', 'posted'),
                    ('display_type', 'not in', ['line_section', 'line_note']),
                    ('company_id', '=', cid),
                ],
                fields=['account_id', 'debit:sum', 'credit:sum', 'balance:sum'],
                groupby=['account_id', 'date:month'],
                lazy=False,
            )
        except Exception as exc:
            _logger.warning('read_group account balances failed: %s', exc)
            return 0

        # Build account cache for names/codes. Mismo patrón multi-company
        # que _push_chart_of_accounts: iteramos res.company para que el
        # compute de `code` (Odoo 17+ code_store_ids) se resuelva
        # correctamente. Antes un solo with_company(cid) dejaba el cache
        # vacío (si company_id filter raised) o con codes incompletos,
        # causando account_code='' en 100% de odoo_account_balances.
        account_cache = {}
        try:
            Account = self.env['account.account'].sudo()
            Company = self.env['res.company'].sudo()
            for company in Company.search([]):
                company_cid = company.id
                try:
                    accounts = Account.search([('company_id', '=', company_cid)])
                    if not accounts:
                        accounts = Account.search([])
                except Exception:
                    accounts = Account.search([])
                for acc in accounts.with_company(company_cid):
                    code = acc.code or ''
                    if not code and hasattr(acc, 'code_store_ids'):
                        mapping = acc.code_store_ids.filtered(
                            lambda m: m.company_id.id == company_cid
                        )
                        if mapping:
                            code = mapping[0].code or ''
                    # Solo registrar si tenemos code (cuenta "pertenece" a
                    # esta company) o si no hay entry previa para la acc.
                    if code or acc.id not in account_cache:
                        account_cache[acc.id] = {
                            'code': code,
                            'name': acc.name or '',
                            'account_type': getattr(acc, 'account_type', '') or '',
                        }
        except Exception as exc:
            _logger.warning('account_balances cache build failed: %s', exc)

        # Month name → number mapping for Spanish locale (Odoo read_group
        # returns localized month names like "enero 2026", "febrero 2026").
        _MONTH_ES = {
            'enero': '01', 'febrero': '02', 'marzo': '03', 'abril': '04',
            'mayo': '05', 'junio': '06', 'julio': '07', 'agosto': '08',
            'septiembre': '09', 'octubre': '10', 'noviembre': '11',
            'diciembre': '12',
        }
        # English fallback
        _MONTH_EN = {
            'january': '01', 'february': '02', 'march': '03', 'april': '04',
            'may': '05', 'june': '06', 'july': '07', 'august': '08',
            'september': '09', 'october': '10', 'november': '11',
            'december': '12',
        }

        def _normalize_period(raw: str) -> str:
            """Convert 'abril 2026' or 'April 2026' → '2026-04'."""
            if not raw:
                return raw
            parts = raw.strip().lower().split()
            if len(parts) == 2:
                month_name, year = parts
                month_num = _MONTH_ES.get(month_name) or _MONTH_EN.get(month_name)
                if month_num and year.isdigit():
                    return f'{year}-{month_num}'
            return raw  # fallback: return as-is

        rows = []
        for g in groups:
            acc_id = g['account_id'][0] if g['account_id'] else None
            if not acc_id:
                continue

            acc_info = account_cache.get(acc_id, {})
            # date:month returns 'abril 2026' format — normalize to '2026-04'
            month_str = _normalize_period(g.get('date:month', ''))

            rows.append({
                'odoo_account_id': acc_id,
                'account_code': acc_info.get('code', ''),
                'account_name': acc_info.get('name', ''),
                'account_type': acc_info.get('account_type', ''),
                'period': month_str,
                'debit': round(g.get('debit', 0) or 0, 2),
                'credit': round(g.get('credit', 0) or 0, 2),
                'balance': round(g.get('balance', 0) or 0, 2),
            })

        # Full refresh (balances change as entries are posted)
        if rows:
            client.delete_all('odoo_account_balances')
            return client.insert('odoo_account_balances', rows, batch_size=500)
        return 0

    # ── Retry Failures ─────────────────────────────────────────────────

    @api.model
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
        tables = [
            ('odoo', 'odoo_invoices', 'odoo_partner_id,name'),
            ('odoo', 'odoo_payments', 'odoo_partner_id,name'),
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

    # ── Bank Balances ───────────────────────────────────────────────────

    def _push_bank_balances(self, client: SupabaseClient, last_sync=None) -> int:
        """Push bank journal balances → odoo_bank_balances table.

        Shows current cash position from bank-type journals.
        """
        try:
            Journal = self.env['account.journal'].sudo()
        except KeyError:
            _logger.info('account.journal not available, skipping')
            return 0

        # Only sync journals from the operating company
        cid = self._get_company_id()
        journals = Journal.search([
            ('type', 'in', ['bank', 'cash']),
            ('company_id', '=', cid),
        ])

        rows = []
        for j in journals:
            # Compute both company-currency balance (MXN) and foreign-currency
            # native balance from the same read_group.
            #
            # account.move.line.balance        = debit - credit (company ccy, MXN)
            # account.move.line.amount_currency = native foreign-currency amount
            #
            # For USD/EUR journals we want BOTH:
            #   - current_balance_mxn -> MXN ledger value (used for aggregations)
            #   - current_balance     -> native foreign value (for display)
            balance_mxn = 0.0
            balance_native = 0.0
            try:
                if hasattr(j, 'default_account_id') and j.default_account_id:
                    Line = self.env['account.move.line'].sudo()
                    result = Line.read_group(
                        domain=[
                            ('account_id', '=', j.default_account_id.id),
                            ('parent_state', '=', 'posted'),
                        ],
                        fields=['balance:sum', 'amount_currency:sum'],
                        groupby=[],
                    )
                    if result:
                        balance_mxn = result[0].get('balance', 0) or 0
                        balance_native = result[0].get('amount_currency', 0) or 0
            except Exception as exc:
                _logger.warning('Bank balance for %s: %s', j.name, exc)

            bank_account = None
            try:
                if hasattr(j, 'bank_account_id') and j.bank_account_id:
                    bank_account = j.bank_account_id.acc_number
                elif hasattr(j, 'bank_acc_number'):
                    bank_account = j.bank_acc_number
            except Exception:
                pass

            company_currency = (
                j.company_id.currency_id.name
                if j.company_id and j.company_id.currency_id else 'MXN'
            )
            journal_currency = j.currency_id.name if j.currency_id else company_currency

            # If journal operates in a foreign currency AND has amount_currency
            # data, current_balance = native foreign value. Otherwise,
            # current_balance = MXN ledger balance (same as _mxn).
            if journal_currency != company_currency and balance_native:
                current_balance = round(balance_native, 2)
            else:
                current_balance = round(balance_mxn, 2)

            # Detect credit card journals. Odoo's account.journal.type only
            # has 'bank' and 'cash' — credit cards are configured as type='bank'
            # with a default_account_id whose account_type is
            # 'liability_credit_card'. Downstream dashboards need to distinguish
            # them (display as "Tarjeta" and classify as cc_debt in cash
            # bucketing), so we override journal_type='credit' when detected.
            effective_type = j.type
            try:
                acc = j.default_account_id
                if acc and getattr(acc, 'account_type', None) == 'liability_credit_card':
                    effective_type = 'credit'
            except Exception:
                pass

            rows.append({
                'odoo_journal_id': j.id,
                'name': j.name,
                'journal_type': effective_type,  # bank / cash / credit
                'currency': journal_currency,
                'bank_account': bank_account,
                'current_balance': current_balance,
                'current_balance_mxn': round(balance_mxn, 2),
                'odoo_company_id': j.company_id.id if j.company_id else None,
                'company_name': j.company_id.name if j.company_id else None,
                'updated_at': datetime.now().isoformat(),
            })

        # Full refresh (small table, balances change)
        if rows:
            client.delete_all('odoo_bank_balances')
            return client.insert('odoo_bank_balances', rows, batch_size=50)
        return 0

    # ── Currency Rates ───────────────────────────────────────────────────

    def _push_currency_rates(self, client: SupabaseClient, last_sync=None) -> int:
        """Push res.currency.rate → odoo_currency_rates table.

        Pushes the latest rate for each active foreign currency so Supabase
        views can convert USD/EUR amounts to MXN using real Odoo rates
        instead of hardcoded values.
        """
        try:
            Rate = self.env['res.currency.rate'].sudo()
        except KeyError:
            _logger.info('res.currency.rate not available, skipping')
            return 0

        cid = self._get_company_id()
        company = self.env['res.company'].sudo().browse(cid)
        company_currency = company.currency_id.name if company.currency_id else 'MXN'

        # Get all currencies that have rates defined
        Currency = self.env['res.currency'].sudo()
        currencies = Currency.search([('active', '=', True)])

        rows = []
        for cur in currencies:
            if cur.name == company_currency:
                continue  # Skip MXN→MXN

            # Get the latest rate for this currency in the company
            rates = Rate.search([
                ('currency_id', '=', cur.id),
                ('company_id', 'in', [cid, False]),
            ], order='name desc', limit=30)  # last 30 rate entries

            for r in rates:
                # Odoo stores inverse rate: 1 / (foreign per base)
                # e.g. if 1 USD = 19.5 MXN, Odoo stores rate = 1/19.5 = 0.05128
                inverse_rate = r.rate or 0
                if inverse_rate > 0:
                    mxn_rate = round(1.0 / inverse_rate, 6)
                else:
                    continue

                rows.append({
                    'currency': cur.name,
                    'rate': mxn_rate,
                    'inverse_rate': round(inverse_rate, 10),
                    'rate_date': r.name.strftime('%Y-%m-%d') if r.name else None,
                    'odoo_company_id': cid,
                })

        if rows:
            return client.upsert('odoo_currency_rates', rows,
                                  on_conflict='currency,rate_date,odoo_company_id',
                                  batch_size=200)
        return 0

    # ── BOMs (mrp.bom + mrp.bom.line) ────────────────────────────────────

    def _push_boms(self, client: SupabaseClient, last_sync=None) -> int:
        """Push mrp.bom + mrp.bom.line → mrp_boms / mrp_bom_lines tables.

        BOMs unlock real manufacturing cost: instead of relying on the
        cached standard_price (often stale or zero for finished goods),
        we can roll down each BOM to sum component standard_prices and
        derive the actual unit cost of each manufactured product.

        Returns the number of bom headers pushed (0 if mrp not installed).
        """
        try:
            Bom = self.env['mrp.bom'].sudo()
        except KeyError:
            _logger.info('mrp.bom not available, skipping')
            return 0

        cid = self._get_company_id()
        domain = [
            ('active', '=', True),
            '|', ('company_id', '=', cid), ('company_id', '=', False),
        ]
        if last_sync:
            domain.append(('write_date', '>=', last_sync.strftime('%Y-%m-%d %H:%M:%S')))

        boms = Bom.search(domain)
        if not boms:
            return 0

        bom_rows = []
        line_rows = []
        for bom in boms:
            # Resolve product (variant) and template
            tmpl = bom.product_tmpl_id
            variant = bom.product_id  # may be empty (BOM applies to all variants)
            display_product = variant if variant else (tmpl.product_variant_id if tmpl else False)

            bom_rows.append({
                'odoo_bom_id': bom.id,
                'odoo_product_tmpl_id': tmpl.id if tmpl else None,
                'odoo_product_id': display_product.id if display_product else None,
                'product_name': display_product.name if display_product else (tmpl.name if tmpl else ''),
                'product_ref': (display_product.default_code or '') if display_product else '',
                'product_qty': float(bom.product_qty or 1.0),
                'product_uom': bom.product_uom_id.name if bom.product_uom_id else '',
                'code': bom.code or '',
                'bom_type': bom.type or 'normal',
                'active': bool(bom.active),
                'odoo_company_id': bom.company_id.id if bom.company_id else None,
                'synced_at': datetime.now().isoformat(),
            })

            for line in bom.bom_line_ids:
                comp = line.product_id
                line_rows.append({
                    'odoo_bom_line_id': line.id,
                    'odoo_bom_id': bom.id,
                    'odoo_product_id': comp.id if comp else None,
                    'product_name': comp.name if comp else '',
                    'product_ref': (comp.default_code or '') if comp else '',
                    'product_qty': float(line.product_qty or 0.0),
                    'product_uom': line.product_uom_id.name if line.product_uom_id else '',
                    'synced_at': datetime.now().isoformat(),
                })

        # Push headers first, then lines (FK soft via odoo_bom_id)
        client.upsert('mrp_boms', bom_rows,
                      on_conflict='odoo_bom_id', batch_size=200)
        if line_rows:
            client.upsert('mrp_bom_lines', line_rows,
                          on_conflict='odoo_bom_line_id', batch_size=500)
        return len(bom_rows)

    # ── UoMs (uom.uom master table) ──────────────────────────────────────

    def _push_uoms(self, client: SupabaseClient, last_sync=None) -> int:
        """Push uom.uom -> odoo_uoms table.

        Sprint 13e: needed to convert sale/invoice line quantities back
        to the product's canonical UoM when they differ. Conversion is
        within a UoM category (length, weight, volume); cross-category
        conversion is product-dependent and only flagged downstream.

        Odoo convention for `factor`: ratio relative to the category
        reference UoM. A SMALLER unit has factor > 1 (e.g. cm.factor =
        100 if m is the reference). Conversion math:
            qty_in_target = qty * (target.factor / source.factor)
        when both share the same category_id.
        """
        try:
            Uom = self.env['uom.uom'].sudo()
        except KeyError:
            _logger.info('uom.uom not available, skipping')
            return 0

        uoms = Uom.search([])
        rows = []
        for u in uoms:
            try:
                cat = getattr(u, 'category_id', None)
                rows.append({
                    'odoo_uom_id': u.id,
                    'name': u.name or '',
                    'category_id': cat.id if cat else None,
                    'category_name': cat.name if cat else None,
                    'factor': float(u.factor) if hasattr(u, 'factor') else None,
                    'factor_inv': float(u.factor_inv) if hasattr(u, 'factor_inv') else None,
                    'uom_type': getattr(u, 'uom_type', None),
                    'active': bool(getattr(u, 'active', True)),
                    'rounding': float(getattr(u, 'rounding', 0) or 0),
                    'synced_at': datetime.now().isoformat(),
                })
            except Exception as exc:
                _logger.warning('uom %s: %s', u.id, exc)

        if not rows:
            return 0
        return client.upsert('odoo_uoms', rows,
                             on_conflict='odoo_uom_id', batch_size=200)

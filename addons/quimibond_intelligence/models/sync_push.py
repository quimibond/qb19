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


def _commercial_partner_id(partner) -> int | None:
    """Resolve commercial partner ID (parent company)."""
    cp = partner.commercial_partner_id
    return cp.id if cp else partner.id


class QuimibondSync(models.TransientModel):
    _name = 'quimibond.sync'
    _description = 'Quimibond Sync Engine'

    @api.model
    def push_to_supabase(self):
        """Main cron entry point: push all Odoo data to Supabase."""
        client = _get_client(self.env)
        if not client:
            return

        # Get last sync timestamp for incremental sync
        ICP = self.env['ir.config_parameter'].sudo()
        last_sync_str = ICP.get_param('quimibond_intelligence.last_sync_date', '')
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
            totals = {}
            totals['contacts'] = self._push_contacts(client, last_sync=last_sync)
            totals['products'] = self._push_products(client, last_sync=last_sync)
            totals['order_lines'] = self._push_order_lines(client, last_sync=last_sync)
            totals['users'] = self._push_users(client, last_sync=last_sync)
            totals['invoices'] = self._push_invoices(client, last_sync=last_sync)
            totals['invoice_lines'] = self._push_invoice_lines(client, last_sync=last_sync)
            totals['payments'] = self._push_payments(client, last_sync=last_sync)
            totals['deliveries'] = self._push_deliveries(client, last_sync=last_sync)
            totals['crm_leads'] = self._push_crm_leads(client, last_sync=last_sync)
            totals['activities'] = self._push_activities(client)
            totals['manufacturing'] = self._push_manufacturing(client, last_sync=last_sync)
            totals['employees'] = self._push_employees(client, last_sync=last_sync)
            totals['departments'] = self._push_departments(client, last_sync=last_sync)
            totals['sale_orders'] = self._push_sale_orders(client, last_sync=last_sync)
            totals['purchase_orders'] = self._push_purchase_orders(client, last_sync=last_sync)
            totals['orderpoints'] = self._push_orderpoints(client, last_sync=last_sync)
            totals['account_payments'] = self._push_account_payments(client, last_sync=last_sync)
            totals['chart_of_accounts'] = self._push_chart_of_accounts(client, last_sync=last_sync)
            totals['account_balances'] = self._push_account_balances(client, last_sync=last_sync)
            totals['bank_balances'] = self._push_bank_balances(client, last_sync=last_sync)

            summary = ', '.join(f'{k}={v}' for k, v in totals.items() if v)
            _logger.info('✓ Push to Supabase: %s', summary or 'no changes')
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
            invoice_partner_ids = Move.search([
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
                # This is a company (top-level partner)
                cn = (p.name or '').strip()
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

            for email in emails:
                contacts.append({
                    'email': email,
                    'name': contact_name,
                    'contact_type': 'external',
                    'odoo_partner_id': p.id if len(emails) == 1 else None,
                    'company': company_cn,
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
            inv_partners = Move.search([
                ('move_type', 'in', ['out_invoice', 'out_refund', 'in_invoice', 'in_refund']),
                ('state', '=', 'posted'),
                ('invoice_date', '>=', cutoff),
            ]).mapped('partner_id.commercial_partner_id')

            for p in inv_partners:
                cp_id = p.id
                if cp_id in existing_partner_ids:
                    continue
                cn = (p.name or '').strip()
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
            synced += client.upsert(
                'companies', list(companies.values()),
                on_conflict='canonical_name', batch_size=100,
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
        # In Odoo 19, 'type' was renamed to 'detailed_type' in some versions.
        # Use a broad search and filter by checking the product is storable.
        domain = [('active', '=', True)]
        if last_sync:
            domain.append(('write_date', '>=', last_sync.strftime('%Y-%m-%d %H:%M:%S')))
        products = Product.search(domain, limit=6000)

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
                'updated_at': datetime.now().isoformat(),
            })

        return client.upsert('odoo_products', rows, on_conflict='odoo_product_id', batch_size=100)

    # ── Order Lines (Sale + Purchase, last 12 months) ────────────────────

    def _push_order_lines(self, client: SupabaseClient, last_sync=None) -> int:
        cutoff = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        rows = []

        # Sale order lines
        try:
            SOLine = self.env['sale.order.line'].sudo()
            so_domain = [
                ('order_id.date_order', '>=', cutoff),
                ('order_id.state', 'in', ['sale', 'done']),
                ('display_type', '=', False),
            ]
            if last_sync:
                so_domain.append(('write_date', '>=', last_sync.strftime('%Y-%m-%d %H:%M:%S')))
            lines = SOLine.search(so_domain)
            for l in lines:
                o = l.order_id
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
                    'price_unit': round(l.price_unit, 2),
                    'discount': round(l.discount, 2),
                    'subtotal': round(l.price_subtotal, 2),
                    'currency': o.currency_id.name if o.currency_id else 'MXN',
                })
        except Exception as exc:
            _logger.warning('Sale order lines: %s', exc)

        # Purchase order lines
        try:
            POLine = self.env['purchase.order.line'].sudo()
            po_domain = [
                ('order_id.date_order', '>=', cutoff),
                ('order_id.state', 'in', ['purchase', 'done']),
                ('display_type', '=', False),
            ]
            if last_sync:
                po_domain.append(('write_date', '>=', last_sync.strftime('%Y-%m-%d %H:%M:%S')))
            po_lines = POLine.search(po_domain)
            for l in po_lines:
                o = l.order_id
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
                    'price_unit': round(l.price_unit, 2),
                    'discount': 0,
                    'subtotal': round(l.price_subtotal, 2),
                    'currency': o.currency_id.name if o.currency_id else 'MXN',
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
                'updated_at': datetime.now().isoformat(),
            })

        return client.upsert('odoo_users', rows, on_conflict='odoo_user_id')

    # ── Invoices (last 12 months) ────────────────────────────────────────

    def _push_invoices(self, client: SupabaseClient, last_sync=None) -> int:
        Move = self.env['account.move'].sudo()
        cutoff = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        domain = [
            ('move_type', 'in', [
                'out_invoice', 'out_refund',
                'in_invoice', 'in_refund',
            ]),
            ('state', '=', 'posted'),
            ('invoice_date', '>=', cutoff),
        ]
        if last_sync:
            domain.append(('write_date', '>=', last_sync.strftime('%Y-%m-%d %H:%M:%S')))
        invoices = Move.search(domain)

        # Batch-read CFDI computed fields via .read() to avoid prefetch failures.
        # l10n_mx_edi_cfdi_uuid is a non-stored computed field that depends on
        # l10n_mx_edi_document_ids. Attribute access (getattr) can fail silently
        # when the prefetch batch contains records with broken EDI documents.
        # .read() forces per-record computation and returns dicts reliably.
        cfdi_map = {}
        try:
            cfdi_fields = ['l10n_mx_edi_cfdi_uuid', 'l10n_mx_edi_cfdi_sat_state']
            for batch_start in range(0, len(invoices), 200):
                batch = invoices[batch_start:batch_start + 200]
                try:
                    for row in batch.read(cfdi_fields):
                        uuid_val = row.get('l10n_mx_edi_cfdi_uuid')
                        sat_val = row.get('l10n_mx_edi_cfdi_sat_state')
                        cfdi_map[row['id']] = {
                            'uuid': uuid_val if uuid_val else None,
                            'sat': sat_val if sat_val else None,
                        }
                except Exception as exc:
                    # If batch read fails, try individual reads
                    _logger.warning('CFDI batch read failed, trying individual: %s', exc)
                    for inv in batch:
                        try:
                            data = inv.read(cfdi_fields)[0]
                            uuid_val = data.get('l10n_mx_edi_cfdi_uuid')
                            sat_val = data.get('l10n_mx_edi_cfdi_sat_state')
                            cfdi_map[inv.id] = {
                                'uuid': uuid_val if uuid_val else None,
                                'sat': sat_val if sat_val else None,
                            }
                        except Exception:
                            cfdi_map[inv.id] = {'uuid': None, 'sat': None}
        except Exception as exc:
            _logger.error('CFDI field reading failed entirely: %s', exc)

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

            rows.append({
                'odoo_partner_id': pid,
                'name': inv.name,
                'move_type': inv.move_type,
                'amount_total': round(inv.amount_total, 2),
                'amount_residual': round(inv.amount_residual, 2),
                'amount_tax': round(inv.amount_tax, 2) if hasattr(inv, 'amount_tax') else None,
                'amount_untaxed': round(inv.amount_untaxed, 2) if hasattr(inv, 'amount_untaxed') else None,
                'amount_paid': round(inv.amount_total - inv.amount_residual, 2),
                'currency': inv.currency_id.name if inv.currency_id else 'MXN',
                'invoice_date': inv.invoice_date.strftime('%Y-%m-%d') if inv.invoice_date else None,
                'due_date': inv.invoice_date_due.strftime('%Y-%m-%d') if inv.invoice_date_due else None,
                'state': inv.state,
                'payment_state': inv.payment_state,
                'days_overdue': days_overdue,
                'payment_term': pay_term,
                'cfdi_uuid': cfdi_uuid,
                'cfdi_sat_state': cfdi_sat,
                'ref': inv.ref or '',
            })

        return client.upsert('odoo_invoices', rows,
                              on_conflict='odoo_partner_id,name', batch_size=200)

    # ── Invoice Lines (last 12 months) ────────────────────────────────────

    def _push_invoice_lines(self, client: SupabaseClient, last_sync=None) -> int:
        """Push account.move.line → odoo_invoice_lines table."""
        Move = self.env['account.move'].sudo()
        cutoff = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

        domain = [
            ('move_type', 'in', [
                'out_invoice', 'out_refund', 'in_invoice', 'in_refund',
            ]),
            ('state', '=', 'posted'),
            ('invoice_date', '>=', cutoff),
        ]
        if last_sync:
            domain.append(('write_date', '>=', last_sync.strftime('%Y-%m-%d %H:%M:%S')))
        invoices = Move.search(domain)

        rows = []
        for inv in invoices:
            pid = _commercial_partner_id(inv.partner_id)
            if not pid:
                continue

            inv_date = inv.invoice_date.strftime('%Y-%m-%d') if inv.invoice_date else None

            for line in inv.invoice_line_ids:
                # Skip section/note lines (Odoo 19 uses 'product' for real lines)
                if line.display_type in ('line_section', 'line_note',
                                         'payment_term', 'tax', 'rounding'):
                    continue

                rows.append({
                    'odoo_line_id': line.id,
                    'odoo_move_id': inv.id,
                    'odoo_partner_id': pid,
                    'move_name': inv.name,
                    'move_type': inv.move_type,
                    'invoice_date': inv_date,
                    'odoo_product_id': line.product_id.id if line.product_id else None,
                    'product_name': line.product_id.name if line.product_id else (line.name or '')[:200],
                    'product_ref': line.product_id.default_code or '' if line.product_id else '',
                    'quantity': round(line.quantity, 2),
                    'price_unit': round(line.price_unit, 2),
                    'discount': round(line.discount, 2),
                    'price_subtotal': round(line.price_subtotal, 2),
                    'price_total': round(line.price_total, 2),
                })

        return client.upsert('odoo_invoice_lines', rows,
                              on_conflict='odoo_line_id', batch_size=200)

    # ── Payments (last 180 days) ─────────────────────────────────────────

    def _push_payments(self, client: SupabaseClient, last_sync=None) -> int:
        """Push payment data extracted from paid/partial invoices.

        Odoo uses bank reconciliation (not account.payment records),
        so we extract payment info from invoice amount_residual changes.
        """
        Move = self.env['account.move'].sudo()
        cutoff = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

        # Get invoices that have been paid or partially paid
        # Include both customer (out_) and supplier (in_) invoices
        domain = [
            ('move_type', 'in', [
                'out_invoice', 'out_refund',
                'in_invoice', 'in_refund',
            ]),
            ('state', '=', 'posted'),
            ('payment_state', 'in', ['paid', 'in_payment', 'partial']),
            ('invoice_date', '>=', cutoff),
        ]
        if last_sync:
            domain.append(('write_date', '>=', last_sync.strftime('%Y-%m-%d %H:%M:%S')))
        invoices = Move.search(domain)

        rows = []
        for inv in invoices:
            pid = _commercial_partner_id(inv.partner_id)
            if not pid:
                continue

            amount_paid = inv.amount_total - inv.amount_residual
            if amount_paid <= 0:
                continue

            # Use write_date as proxy for payment date (closer to actual payment
            # than invoice_date). invoice_date is when invoice was created, not paid.
            payment_date = inv.write_date or inv.invoice_date
            rows.append({
                'odoo_partner_id': pid,
                'name': f'PAY-{inv.name}',
                'payment_type': 'inbound' if inv.move_type in ('out_invoice', 'in_refund') else 'outbound',
                'amount': round(amount_paid, 2),
                'currency': inv.currency_id.name if inv.currency_id else 'MXN',
                'payment_date': payment_date.strftime('%Y-%m-%d') if payment_date else None,
                'state': 'posted',
            })

        return client.upsert('odoo_payments', rows,
                              on_conflict='odoo_partner_id,name', batch_size=200)

    # ── Deliveries (pending + last 90 days) ──────────────────────────────

    def _push_deliveries(self, client: SupabaseClient, last_sync=None) -> int:
        Picking = self.env['stock.picking'].sudo()
        cutoff = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
        domain = [
            ('picking_type_code', '=', 'outgoing'),
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
                'odoo_partner_id': pid,
                'name': pk.name,
                'picking_type': pk.picking_type_id.name if pk.picking_type_id else '',
                'origin': pk.origin or '',
                'scheduled_date': pk.scheduled_date.strftime('%Y-%m-%d') if pk.scheduled_date else None,
                'date_done': pk.date_done.isoformat() if pk.date_done else None,
                'create_date': pk.create_date.strftime('%Y-%m-%d') if pk.create_date else None,
                'state': pk.state,
                'is_late': is_late,
                'lead_time_days': lead_time,
            })

        return client.upsert('odoo_deliveries', rows,
                              on_conflict='odoo_partner_id,name', batch_size=200)

    # ── CRM Leads ────────────────────────────────────────────────────────

    def _push_crm_leads(self, client: SupabaseClient, last_sync=None) -> int:
        Lead = self.env['crm.lead'].sudo()
        domain = [('active', '=', True)]
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
            })

        return client.upsert('odoo_crm_leads', rows,
                              on_conflict='odoo_lead_id', batch_size=200)

    # ── Activities (full refresh) ────────────────────────────────────────

    def _push_activities(self, client: SupabaseClient) -> int:
        Activity = self.env['mail.activity'].sudo()
        activities = Activity.search([], limit=5000)
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
            })

        # Full refresh: delete all then insert
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

        cutoff = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
        domain = [
            '|',
            ('state', 'not in', ['done', 'cancel']),
            ('date_start', '>=', cutoff),
        ]
        if last_sync:
            domain.append(('write_date', '>=', last_sync.strftime('%Y-%m-%d %H:%M:%S')))
        productions = MO.search(domain, limit=500)

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

        domain = [('active', '=', True)]
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

        domain = [('active', '=', True)]
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

        cutoff = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        domain = [
            ('date_order', '>=', cutoff),
            ('state', 'in', ['sale', 'done']),
        ]
        if last_sync:
            domain.append(('write_date', '>=', last_sync.strftime('%Y-%m-%d %H:%M:%S')))
        orders = SO.search(domain, limit=2000)

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
                'margin': margin,
                'margin_percent': margin_pct,
                'currency': o.currency_id.name if o.currency_id else 'MXN',
                'state': o.state,
                'date_order': o.date_order.strftime('%Y-%m-%d') if o.date_order else None,
                'commitment_date': o.commitment_date.strftime('%Y-%m-%d') if hasattr(o, 'commitment_date') and o.commitment_date else None,
                'create_date': o.create_date.strftime('%Y-%m-%d') if o.create_date else None,
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

        cutoff = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        domain = [
            ('date_order', '>=', cutoff),
            ('state', 'in', ['purchase', 'done']),
        ]
        if last_sync:
            domain.append(('write_date', '>=', last_sync.strftime('%Y-%m-%d %H:%M:%S')))
        orders = PO.search(domain, limit=2000)

        rows = []
        for o in orders:
            pid = _commercial_partner_id(o.partner_id) if o.partner_id else None

            rows.append({
                'odoo_order_id': o.id,
                'name': o.name,
                'odoo_partner_id': pid,
                'buyer_name': o.user_id.name if o.user_id else None,
                'buyer_email': o.user_id.email if o.user_id else None,
                'buyer_user_id': o.user_id.id if o.user_id else None,
                'amount_total': round(o.amount_total, 2),
                'amount_untaxed': round(o.amount_untaxed, 2),
                'currency': o.currency_id.name if o.currency_id else 'MXN',
                'state': o.state,
                'date_order': o.date_order.strftime('%Y-%m-%d') if o.date_order else None,
                'date_approve': o.date_approve.strftime('%Y-%m-%d') if hasattr(o, 'date_approve') and o.date_approve else None,
                'create_date': o.create_date.strftime('%Y-%m-%d') if o.create_date else None,
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

        domain = [('active', '=', True)]
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
            cutoff = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
            domain = [('date', '>=', cutoff)]
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
                    })
                except Exception as exc:
                    _logger.warning('account_payment %s: %s', p.id, exc)

            _logger.info('account_payments: pushing %d rows', len(rows))
            return client.upsert('odoo_account_payments', rows,
                                  on_conflict='odoo_payment_id', batch_size=200)
        except Exception as exc:
            _logger.error('_push_account_payments failed: %s', exc)
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
            # Always full sync — chart of accounts rarely changes and is small
            accounts = Account.search([])
            _logger.info('chart_of_accounts: found %d accounts', len(accounts))

            rows = []
            for acc in accounts:
                try:
                    acc_type = getattr(acc, 'account_type', None) or ''
                    rows.append({
                        'odoo_account_id': acc.id,
                        'code': acc.code or '',
                        'name': acc.name or '',
                        'account_type': acc_type,
                        'reconcile': bool(acc.reconcile) if hasattr(acc, 'reconcile') else False,
                        'deprecated': bool(getattr(acc, 'deprecated', False)),
                    })
                except Exception as exc:
                    _logger.warning('chart_of_accounts %s: %s', acc.id, exc)

            _logger.info('chart_of_accounts: pushing %d rows', len(rows))
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

        cutoff = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

        # Use read_group for efficient aggregation in Odoo
        try:
            groups = Line.read_group(
                domain=[
                    ('parent_state', '=', 'posted'),
                    ('date', '>=', cutoff),
                    ('display_type', 'not in', ['line_section', 'line_note']),
                ],
                fields=['account_id', 'debit:sum', 'credit:sum', 'balance:sum'],
                groupby=['account_id', 'date:month'],
                lazy=False,
            )
        except Exception as exc:
            _logger.warning('read_group account balances failed: %s', exc)
            return 0

        # Build account cache for names/codes
        account_cache = {}
        try:
            Account = self.env['account.account'].sudo()
            for acc in Account.search([]):
                account_cache[acc.id] = {
                    'code': acc.code or '',
                    'name': acc.name or '',
                    'account_type': getattr(acc, 'account_type', '') or '',
                }
        except Exception:
            pass

        rows = []
        for g in groups:
            acc_id = g['account_id'][0] if g['account_id'] else None
            if not acc_id:
                continue

            acc_info = account_cache.get(acc_id, {})
            # date:month returns 'April 2026' format
            month_str = g.get('date:month', '')

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

        journals = Journal.search([('type', 'in', ['bank', 'cash'])])

        rows = []
        for j in journals:
            # Get the default debit/credit account for balance
            balance = 0.0
            try:
                # In Odoo 19, journal has default_account_id
                if hasattr(j, 'default_account_id') and j.default_account_id:
                    # Sum all posted journal items on this account
                    Line = self.env['account.move.line'].sudo()
                    result = Line.read_group(
                        domain=[
                            ('account_id', '=', j.default_account_id.id),
                            ('parent_state', '=', 'posted'),
                        ],
                        fields=['balance:sum'],
                        groupby=[],
                    )
                    if result:
                        balance = result[0].get('balance', 0) or 0
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

            rows.append({
                'odoo_journal_id': j.id,
                'name': j.name,
                'journal_type': j.type,  # bank / cash
                'currency': j.currency_id.name if j.currency_id else (
                    j.company_id.currency_id.name if j.company_id and j.company_id.currency_id else 'MXN'
                ),
                'bank_account': bank_account,
                'current_balance': round(balance, 2),
                'updated_at': datetime.now().isoformat(),
            })

        # Full refresh (small table, balances change)
        if rows:
            client.delete_all('odoo_bank_balances')
            return client.insert('odoo_bank_balances', rows, batch_size=50)
        return 0

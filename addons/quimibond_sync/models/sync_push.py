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

        try:
            totals = {}
            totals['contacts'] = self._push_contacts(client)
            totals['products'] = self._push_products(client)
            totals['order_lines'] = self._push_order_lines(client)
            totals['users'] = self._push_users(client)
            totals['invoices'] = self._push_invoices(client)
            totals['payments'] = self._push_payments(client)
            totals['deliveries'] = self._push_deliveries(client)
            totals['crm_leads'] = self._push_crm_leads(client)
            totals['activities'] = self._push_activities(client)

            summary = ', '.join(f'{k}={v}' for k, v in totals.items() if v)
            _logger.info('✓ Push to Supabase: %s', summary or 'no changes')
        except Exception as exc:
            _logger.error('Push to Supabase failed: %s', exc)
        finally:
            client.close()

    # ── Contacts & Companies ─────────────────────────────────────────────

    def _push_contacts(self, client: SupabaseClient) -> int:
        """Push res.partner → contacts + companies tables."""
        Partner = self.env['res.partner'].sudo()
        partners = Partner.search([
            ('email', '!=', False),
            ('email', '!=', ''),
            '|', ('customer_rank', '>', 0), ('supplier_rank', '>', 0),
        ])

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

            if p.is_company or p.parent_id is False:
                # This is a company
                cn = (p.name or '').strip()
                if cn and cn not in companies:
                    companies[cn] = {
                        'canonical_name': cn,
                        'name': cn,
                        'odoo_partner_id': cp_id,
                        'is_customer': is_customer,
                        'is_supplier': is_supplier,
                    }

            for email in emails:
                contacts.append({
                    'email': email,
                    'name': p.name if not p.is_company else None,
                    'contact_type': 'external',
                    'odoo_partner_id': p.id if len(emails) == 1 else None,
                    'is_customer': is_customer,
                    'is_supplier': is_supplier,
                })

        synced = 0
        if companies:
            synced += client.upsert(
                'companies', list(companies.values()),
                on_conflict='canonical_name', batch_size=100,
            )
        if contacts:
            synced += client.upsert(
                'contacts', contacts,
                on_conflict='email', batch_size=50,
            )
        return synced

    # ── Products ─────────────────────────────────────────────────────────

    def _push_products(self, client: SupabaseClient) -> int:
        Product = self.env['product.product'].sudo()
        products = Product.search([('active', '=', True), ('type', '!=', 'service')])

        try:
            Quant = self.env['stock.quant'].sudo()
        except KeyError:
            Quant = None

        rows = []
        for p in products:
            stock_qty = reserved_qty = 0.0
            if Quant:
                quants = Quant.search([
                    ('product_id', '=', p.id),
                    ('location_id.usage', '=', 'internal'),
                ])
                for q in quants:
                    stock_qty += q.quantity
                    reserved_qty += q.reserved_quantity

            rows.append({
                'odoo_product_id': p.id,
                'name': p.name,
                'internal_ref': p.default_code or '',
                'category': p.categ_id.name if p.categ_id else '',
                'uom': p.uom_id.name if p.uom_id else '',
                'product_type': p.type,
                'stock_qty': round(stock_qty, 2),
                'reserved_qty': round(reserved_qty, 2),
                'available_qty': round(stock_qty - reserved_qty, 2),
                'standard_price': round(p.standard_price, 2),
                'list_price': round(p.lst_price, 2),
                'active': p.active,
                'updated_at': datetime.now().isoformat(),
            })

        return client.upsert('odoo_products', rows, on_conflict='odoo_product_id', batch_size=100)

    # ── Order Lines (Sale + Purchase, last 12 months) ────────────────────

    def _push_order_lines(self, client: SupabaseClient) -> int:
        cutoff = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        rows = []

        # Sale order lines
        try:
            SOLine = self.env['sale.order.line'].sudo()
            lines = SOLine.search([
                ('order_id.date_order', '>=', cutoff),
                ('order_id.state', 'in', ['sale', 'done']),
                ('display_type', '=', False),
            ])
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
            po_lines = POLine.search([
                ('order_id.date_order', '>=', cutoff),
                ('order_id.state', 'in', ['purchase', 'done']),
                ('display_type', '=', False),
            ])
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
                    'qty': round(l.product_qty, 2),
                    'price_unit': round(l.price_unit, 2),
                    'discount': 0,
                    'subtotal': round(l.price_subtotal, 2),
                    'currency': o.currency_id.name if o.currency_id else 'MXN',
                })
        except Exception as exc:
            _logger.warning('Purchase order lines: %s', exc)

        return client.upsert('odoo_order_lines', rows, on_conflict='odoo_line_id', batch_size=200)

    # ── Users ────────────────────────────────────────────────────────────

    def _push_users(self, client: SupabaseClient) -> int:
        User = self.env['res.users'].sudo()
        users = User.search([('active', '=', True), ('share', '=', False)], limit=200)
        today = datetime.now().date()

        rows = []
        for u in users:
            # Count activities
            pending = overdue = 0
            try:
                Activity = self.env['mail.activity'].sudo()
                acts = Activity.search([('user_id', '=', u.id)], limit=20)
                for a in acts:
                    pending += 1
                    if a.date_deadline and a.date_deadline < today:
                        overdue += 1
            except Exception:
                pass

            rows.append({
                'odoo_user_id': u.id,
                'name': u.name,
                'email': u.email or u.login,
                'department': u.department_id.name if u.department_id else None,
                'job_title': u.job_title or None,
                'pending_activities_count': pending,
                'overdue_activities_count': overdue,
                'updated_at': datetime.now().isoformat(),
            })

        return client.upsert('odoo_users', rows, on_conflict='odoo_user_id')

    # ── Invoices (last 12 months) ────────────────────────────────────────

    def _push_invoices(self, client: SupabaseClient) -> int:
        Move = self.env['account.move'].sudo()
        cutoff = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        invoices = Move.search([
            ('move_type', 'in', ['out_invoice', 'out_refund']),
            ('state', '=', 'posted'),
            ('invoice_date', '>=', cutoff),
        ])

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

            rows.append({
                'odoo_partner_id': pid,
                'name': inv.name,
                'move_type': inv.move_type,
                'amount_total': round(inv.amount_total, 2),
                'amount_residual': round(inv.amount_residual, 2),
                'currency': inv.currency_id.name if inv.currency_id else 'MXN',
                'invoice_date': inv.invoice_date.strftime('%Y-%m-%d') if inv.invoice_date else None,
                'due_date': inv.invoice_date_due.strftime('%Y-%m-%d') if inv.invoice_date_due else None,
                'state': inv.state,
                'payment_state': inv.payment_state,
                'days_overdue': days_overdue,
                'ref': inv.ref or '',
            })

        return client.upsert('odoo_invoices', rows,
                              on_conflict='odoo_partner_id,name', batch_size=200)

    # ── Payments (last 180 days) ─────────────────────────────────────────

    def _push_payments(self, client: SupabaseClient) -> int:
        Payment = self.env['account.payment'].sudo()
        cutoff = (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')
        payments = Payment.search([
            ('state', '=', 'posted'),
            ('date', '>=', cutoff),
        ])

        rows = []
        for p in payments:
            pid = _commercial_partner_id(p.partner_id)
            if not pid:
                continue
            rows.append({
                'odoo_partner_id': pid,
                'name': p.name,
                'payment_type': p.payment_type or 'inbound',
                'amount': round(p.amount, 2),
                'currency': p.currency_id.name if p.currency_id else 'MXN',
                'payment_date': p.date.strftime('%Y-%m-%d') if p.date else None,
                'state': p.state,
            })

        return client.upsert('odoo_payments', rows,
                              on_conflict='odoo_partner_id,name', batch_size=200)

    # ── Deliveries (pending + last 90 days) ──────────────────────────────

    def _push_deliveries(self, client: SupabaseClient) -> int:
        Picking = self.env['stock.picking'].sudo()
        cutoff = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
        pickings = Picking.search([
            ('picking_type_code', '=', 'outgoing'),
            '|',
            ('state', 'not in', ['done', 'cancel']),
            ('date_done', '>=', cutoff),
        ])

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

    def _push_crm_leads(self, client: SupabaseClient) -> int:
        Lead = self.env['crm.lead'].sudo()
        leads = Lead.search([('active', '=', True)])
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
            })

        # Full refresh: delete all then insert
        client.delete_all('odoo_activities')
        return client.upsert('odoo_activities', rows, on_conflict='', batch_size=200)

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

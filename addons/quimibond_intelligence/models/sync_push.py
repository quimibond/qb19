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

        _start = datetime.now()
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
            totals['manufacturing'] = self._push_manufacturing(client)
            totals['employees'] = self._push_employees(client)
            totals['departments'] = self._push_departments(client)
            totals['sale_orders'] = self._push_sale_orders(client)
            totals['purchase_orders'] = self._push_purchase_orders(client)

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

            # Extract partner tags/categories
            tags = []
            try:
                if p.category_id:
                    tags = [t.name for t in p.category_id]
            except Exception:
                pass

            # Extract payment terms
            payment_term = None
            try:
                if hasattr(p, 'property_payment_term_id') and p.property_payment_term_id:
                    payment_term = p.property_payment_term_id.name
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

            if p.is_company or not p.parent_id:
                # This is a company (top-level partner)
                cn = (p.name or '').strip()
                if cn and cn not in companies:
                    companies[cn] = {
                        'canonical_name': cn,
                        'name': cn,
                        'odoo_partner_id': cp_id,
                        'is_customer': is_customer,
                        'is_supplier': is_supplier,
                        'domain': domain,
                        'country': p.country_id.name if p.country_id else None,
                        'city': p.city or None,
                    }

            # Resolve the display name:
            # - For contacts with a parent company, use the contact's own name
            # - For company partners, use the company name (better than NULL)
            # - For contacts without parent, use their name directly
            contact_name = p.name or None
            if p.is_company and p.parent_id:
                # Child company — use name
                contact_name = p.name
            elif not p.is_company and p.parent_id:
                # Contact under a company — use contact name
                contact_name = p.name

            for email in emails:
                contacts.append({
                    'email': email,
                    'name': contact_name,
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
        # In Odoo 19, 'type' was renamed to 'detailed_type' in some versions.
        # Use a broad search and filter by checking the product is storable.
        products = Product.search([('active', '=', True)], limit=6000)

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

            # Get reorder rules if available
            reorder_min = reorder_max = 0.0
            try:
                orderpoints = self.env['stock.warehouse.orderpoint'].sudo().search([
                    ('product_id', '=', p.id),
                ], limit=1)
                if orderpoints:
                    reorder_min = orderpoints[0].product_min_qty
                    reorder_max = orderpoints[0].product_max_qty
            except Exception:
                pass

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
        """Push payment data extracted from paid/partial invoices.

        Odoo uses bank reconciliation (not account.payment records),
        so we extract payment info from invoice amount_residual changes.
        """
        Move = self.env['account.move'].sudo()
        cutoff = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

        # Get invoices that have been paid or partially paid
        invoices = Move.search([
            ('move_type', 'in', ['out_invoice', 'out_refund']),
            ('state', '=', 'posted'),
            ('payment_state', 'in', ['paid', 'in_payment', 'partial']),
            ('invoice_date', '>=', cutoff),
        ])

        rows = []
        for inv in invoices:
            pid = _commercial_partner_id(inv.partner_id)
            if not pid:
                continue

            amount_paid = inv.amount_total - inv.amount_residual
            if amount_paid <= 0:
                continue

            rows.append({
                'odoo_partner_id': pid,
                'name': f'PAY-{inv.name}',
                'payment_type': 'inbound' if inv.move_type == 'out_invoice' else 'outbound',
                'amount': round(amount_paid, 2),
                'currency': inv.currency_id.name if inv.currency_id else 'MXN',
                'payment_date': inv.invoice_date.strftime('%Y-%m-%d') if inv.invoice_date else None,
                'state': 'posted',
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

    def _push_manufacturing(self, client: SupabaseClient) -> int:
        """Push mrp.production → odoo_manufacturing table."""
        try:
            MO = self.env['mrp.production'].sudo()
        except KeyError:
            _logger.info('mrp.production not available, skipping manufacturing sync')
            return 0

        cutoff = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
        productions = MO.search([
            '|',
            ('state', 'not in', ['done', 'cancel']),
            ('date_start', '>=', cutoff),
        ], limit=500)

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

    def _push_employees(self, client: SupabaseClient) -> int:
        """Push hr.employee → odoo_employees table."""
        try:
            Employee = self.env['hr.employee'].sudo()
        except KeyError:
            _logger.info('hr.employee not available, skipping')
            return 0

        employees = Employee.search([('active', '=', True)], limit=500)
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

    def _push_departments(self, client: SupabaseClient) -> int:
        """Push hr.department → odoo_departments table."""
        try:
            Dept = self.env['hr.department'].sudo()
        except KeyError:
            _logger.info('hr.department not available, skipping')
            return 0

        departments = Dept.search([('active', '=', True)], limit=200)
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

    def _push_sale_orders(self, client: SupabaseClient) -> int:
        """Push sale.order headers → odoo_sale_orders table."""
        try:
            SO = self.env['sale.order'].sudo()
        except KeyError:
            _logger.info('sale.order not available, skipping')
            return 0

        cutoff = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        orders = SO.search([
            ('date_order', '>=', cutoff),
            ('state', 'in', ['sale', 'done']),
        ], limit=2000)

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

    def _push_purchase_orders(self, client: SupabaseClient) -> int:
        """Push purchase.order headers → odoo_purchase_orders table."""
        try:
            PO = self.env['purchase.order'].sudo()
        except KeyError:
            _logger.info('purchase.order not available, skipping')
            return 0

        cutoff = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        orders = PO.search([
            ('date_order', '>=', cutoff),
            ('state', 'in', ['purchase', 'done']),
        ], limit=2000)

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

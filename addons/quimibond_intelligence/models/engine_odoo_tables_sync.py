"""
Engine — Sync Odoo operational tables to Supabase

Sincroniza datos estructurados de Odoo a tablas dedicadas en Supabase:
- odoo_products: catálogo con stock en tiempo real
- odoo_order_lines: detalle de ventas/compras por producto
- odoo_users: equipo con actividades pendientes

Corre como parte del enrichment (cada 6h) o manualmente.
"""
import json
import logging
import time
from datetime import datetime, timedelta

from odoo import api, fields, models

from .intelligence_config import acquire_lock, release_lock

_logger = logging.getLogger(__name__)


class IntelligenceEngine(models.Model):
    _inherit = 'intelligence.engine'

    @api.model
    def run_sync_odoo_tables(self):
        """Sync productos, líneas de orden, y usuarios a Supabase."""
        lock = 'quimibond_intelligence.odoo_tables_sync_running'
        if not acquire_lock(self.env, lock):
            return
        start = time.time()

        try:
            cfg = self._load_config()
            if not cfg:
                return

            from ..services.supabase_service import SupabaseService

            with SupabaseService(cfg['supabase_url'], cfg['supabase_key']) as supa:
                products = self._sync_products(supa)
                lines = self._sync_order_lines(supa)
                users = self._sync_users(supa)
                invoices = self._sync_invoices(supa)
                payments = self._sync_payments(supa)
                deliveries = self._sync_deliveries(supa)
                crm = self._sync_crm_leads(supa)
                activities = self._sync_activities(supa)

                _logger.info(
                    '✓ Odoo tables sync: %d products, %d order lines, '
                    '%d users, %d invoices, %d payments, %d deliveries, '
                    '%d CRM leads, %d activities (%.1fs)',
                    products, lines, users, invoices, payments,
                    deliveries, crm, activities, time.time() - start,
                )

                # Neural network: resolve ALL connections
                try:
                    result = supa._request(
                        '/rest/v1/rpc/resolve_all_connections',
                        'POST', {},
                    )
                    _logger.info('Neural network connections: %s', result)
                except Exception as exc:
                    _logger.debug('resolve_connections: %s', exc)
        except Exception as exc:
            _logger.error('run_sync_odoo_tables: %s', exc, exc_info=True)
        finally:
            release_lock(self.env, lock)

    # ── Products ──────────────────────────────────────────────────────────────

    def _sync_products(self, supa) -> int:
        """Sync product catalog with real-time stock."""
        try:
            Product = self.env['product.product'].sudo()
        except KeyError:
            return 0

        products = Product.search([
            ('active', '=', True),
            ('type', '!=', 'service'),
        ], limit=2000)

        if not products:
            return 0

        # Cargar stock.quant y orderpoints en batch
        try:
            Quant = self.env['stock.quant'].sudo()
            Orderpoint = self.env['stock.warehouse.orderpoint'].sudo()
        except KeyError:
            Quant = None
            Orderpoint = None

        batch = []
        for p in products:
            stock_qty = 0
            reserved_qty = 0
            reorder_min = 0
            reorder_max = 0

            if Quant:
                quants = Quant.search([
                    ('product_id', '=', p.id),
                    ('location_id.usage', '=', 'internal'),
                ])
                stock_qty = sum(q.quantity for q in quants)
                reserved_qty = sum(q.reserved_quantity for q in quants)

            if Orderpoint:
                op = Orderpoint.search([
                    ('product_id', '=', p.id),
                ], limit=1)
                if op:
                    reorder_min = op.product_min_qty
                    reorder_max = op.product_max_qty

            batch.append({
                'odoo_product_id': p.id,
                'name': p.name,
                'internal_ref': p.default_code or '',
                'category': p.categ_id.name if p.categ_id else '',
                'category_id': p.categ_id.id if p.categ_id else None,
                'uom': p.uom_id.name if p.uom_id else 'Unidad',
                'stock_qty': round(stock_qty, 2),
                'reserved_qty': round(reserved_qty, 2),
                'reorder_min': round(reorder_min, 2),
                'reorder_max': round(reorder_max, 2),
                'standard_price': round(p.standard_price or 0, 2),
                'list_price': round(p.lst_price or 0, 2),
                'active': p.active,
                'product_type': p.type,
                'barcode': p.barcode or '',
                'weight': round(p.weight or 0, 3),
                'updated_at': datetime.now().isoformat(),
            })

        # Upsert en batches de 100
        synced = 0
        for i in range(0, len(batch), 100):
            chunk = batch[i:i + 100]
            try:
                supa._request(
                    '/rest/v1/odoo_products?on_conflict=odoo_product_id',
                    'POST', chunk,
                    extra_headers={
                        'Prefer': 'resolution=merge-duplicates',
                    },
                )
                synced += len(chunk)
            except Exception as exc:
                _logger.debug('sync_products batch: %s', exc)

        return synced

    # ── Order Lines ───────────────────────────────────────────────────────────

    def _sync_order_lines(self, supa) -> int:
        """Sync sale/purchase order lines (last 12 months)."""
        cutoff = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        synced = 0

        # Sale order lines
        try:
            SOLine = self.env['sale.order.line'].sudo()
            lines = SOLine.search([
                ('order_id.date_order', '>=', cutoff),
                ('order_id.state', 'in', ['sale', 'done']),
                ('display_type', '=', False),
            ], limit=5000)

            batch = []
            for line in lines:
                order = line.order_id
                cpid = (order.partner_id.commercial_partner_id.id
                        if order.partner_id.commercial_partner_id
                        else order.partner_id.id)
                batch.append({
                    'odoo_line_id': line.id,
                    'odoo_order_id': order.id,
                    'odoo_partner_id': cpid,
                    'order_name': order.name,
                    'order_date': (order.date_order.strftime('%Y-%m-%d')
                                   if order.date_order else None),
                    'order_type': 'sale',
                    'order_state': order.state,
                    'product_name': line.product_id.name if line.product_id else '',
                    'odoo_product_id': line.product_id.id if line.product_id else None,
                    'qty': round(line.product_uom_qty or 0, 2),
                    'price_unit': round(line.price_unit or 0, 2),
                    'discount': round(line.discount or 0, 2),
                    'subtotal': round(line.price_subtotal or 0, 2),
                    'currency': order.currency_id.name if order.currency_id else 'MXN',
                })

            for i in range(0, len(batch), 200):
                chunk = batch[i:i + 200]
                try:
                    supa._request(
                        '/rest/v1/odoo_order_lines?on_conflict=odoo_line_id',
                        'POST', chunk,
                        extra_headers={
                            'Prefer': 'resolution=merge-duplicates',
                        },
                    )
                    synced += len(chunk)
                except Exception as exc:
                    _logger.debug('sync sale lines batch: %s', exc)

        except Exception as exc:
            _logger.debug('sync sale lines: %s', exc)

        # Purchase order lines
        try:
            POLine = self.env['purchase.order.line'].sudo()
            po_lines = POLine.search([
                ('order_id.date_order', '>=', cutoff),
                ('order_id.state', 'in', ['purchase', 'done']),
                ('display_type', '=', False),
            ], limit=5000)

            batch = []
            for line in po_lines:
                order = line.order_id
                cpid = (order.partner_id.commercial_partner_id.id
                        if order.partner_id.commercial_partner_id
                        else order.partner_id.id)
                # Use negative IDs for PO lines to avoid collision with SO lines
                batch.append({
                    'odoo_line_id': -line.id,
                    'odoo_order_id': order.id,
                    'odoo_partner_id': cpid,
                    'order_name': order.name,
                    'order_date': (order.date_order.strftime('%Y-%m-%d')
                                   if order.date_order else None),
                    'order_type': 'purchase',
                    'order_state': order.state,
                    'product_name': line.product_id.name if line.product_id else '',
                    'odoo_product_id': line.product_id.id if line.product_id else None,
                    'qty': round(line.product_qty or 0, 2),
                    'price_unit': round(line.price_unit or 0, 2),
                    'discount': 0,
                    'subtotal': round(line.price_subtotal or 0, 2),
                    'currency': order.currency_id.name if order.currency_id else 'MXN',
                })

            for i in range(0, len(batch), 200):
                chunk = batch[i:i + 200]
                try:
                    supa._request(
                        '/rest/v1/odoo_order_lines?on_conflict=odoo_line_id',
                        'POST', chunk,
                        extra_headers={
                            'Prefer': 'resolution=merge-duplicates',
                        },
                    )
                    synced += len(chunk)
                except Exception as exc:
                    _logger.debug('sync purchase lines batch: %s', exc)

        except Exception as exc:
            _logger.debug('sync purchase lines: %s', exc)

        return synced

    # ── Users / Team ──────────────────────────────────────────────────────────

    def _sync_users(self, supa) -> int:
        """Sync active internal users with their pending activities."""
        User = self.env['res.users'].sudo()
        users = User.search([
            ('active', '=', True),
            ('share', '=', False),  # Only internal users
        ], limit=200)

        if not users:
            return 0

        today = fields.Date.today()
        try:
            Activity = self.env['mail.activity'].sudo()
        except KeyError:
            Activity = None

        batch = []
        for u in users:
            pending = 0
            overdue = 0
            activities = []

            if Activity:
                acts = Activity.search([
                    ('user_id', '=', u.id),
                ], limit=20, order='date_deadline asc')
                pending = len(acts)
                overdue = len([a for a in acts if a.date_deadline < today])
                activities = [{
                    'type': (a.activity_type_id.name
                             if a.activity_type_id else 'Tarea'),
                    'summary': a.summary or '',
                    'deadline': a.date_deadline.strftime('%Y-%m-%d'),
                    'overdue': a.date_deadline < today,
                    'model': a.res_model or '',
                } for a in acts[:10]]

            dept = ''
            if hasattr(u, 'department_id') and u.department_id:
                dept = u.department_id.name
            job = ''
            if hasattr(u, 'job_title'):
                job = u.job_title or ''

            batch.append({
                'odoo_user_id': u.id,
                'name': u.name,
                'email': u.email or u.login,
                'department': dept,
                'job_title': job,
                'pending_activities_count': pending,
                'overdue_activities_count': overdue,
                'activities_json': activities,
                'updated_at': datetime.now().isoformat(),
            })

        try:
            supa._request(
                '/rest/v1/odoo_users?on_conflict=odoo_user_id',
                'POST', batch,
                extra_headers={
                    'Prefer': 'resolution=merge-duplicates',
                },
            )
            return len(batch)
        except Exception as exc:
            _logger.debug('sync_users: %s', exc)
            return 0

    # ── Invoices ─────────────────────────────────────────────────────────────

    def _sync_invoices(self, supa) -> int:
        """Sync invoices (out_invoice + out_refund) from account.move."""
        try:
            Move = self.env['account.move'].sudo()
        except KeyError:
            return 0

        today = fields.Date.today()
        cutoff = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

        invoices = Move.search([
            ('move_type', 'in', ['out_invoice', 'out_refund']),
            ('state', '=', 'posted'),
            ('invoice_date', '>=', cutoff),
        ], limit=5000)

        if not invoices:
            return 0

        batch = []
        for inv in invoices:
            cpid = (inv.partner_id.commercial_partner_id.id
                    if inv.partner_id.commercial_partner_id
                    else inv.partner_id.id)

            days_overdue = 0
            if (inv.payment_state in ('not_paid', 'partial')
                    and inv.invoice_date_due and inv.invoice_date_due < today):
                days_overdue = (today - inv.invoice_date_due).days

            days_to_pay = None
            payment_status = None
            if inv.payment_state in ('paid', 'in_payment'):
                # Find payment date from reconciled lines
                try:
                    pay_date = max(
                        (l.date for l in inv.line_ids.mapped(
                            'matched_credit_ids.credit_move_id')
                         if l.date),
                        default=None,
                    )
                    if pay_date and inv.invoice_date_due:
                        days_to_pay = (pay_date - inv.invoice_date_due).days
                        if days_to_pay <= 3:
                            payment_status = 'on_time'
                        elif days_to_pay < 0:
                            payment_status = 'early'
                        else:
                            payment_status = 'late'
                except Exception:
                    pass

            batch.append({
                'odoo_partner_id': cpid,
                'name': inv.name,
                'move_type': inv.move_type,
                'amount_total': round(inv.amount_total, 2),
                'amount_residual': round(inv.amount_residual, 2),
                'currency': inv.currency_id.name if inv.currency_id else 'MXN',
                'invoice_date': (inv.invoice_date.strftime('%Y-%m-%d')
                                 if inv.invoice_date else None),
                'due_date': (inv.invoice_date_due.strftime('%Y-%m-%d')
                             if inv.invoice_date_due else None),
                'state': inv.state,
                'payment_state': inv.payment_state,
                'days_overdue': days_overdue,
                'days_to_pay': days_to_pay,
                'payment_status': payment_status,
                'ref': inv.ref or '',
            })

        synced = 0
        for i in range(0, len(batch), 200):
            chunk = batch[i:i + 200]
            try:
                supa._request(
                    '/rest/v1/odoo_invoices?on_conflict=odoo_partner_id,name',
                    'POST', chunk,
                    extra_headers={'Prefer': 'resolution=merge-duplicates'},
                )
                synced += len(chunk)
            except Exception as exc:
                _logger.debug('sync invoices batch %d: %s', i, exc)

        return synced

    # ── Payments ─────────────────────────────────────────────────────────────

    def _sync_payments(self, supa) -> int:
        """Sync posted payments from account.payment."""
        try:
            Payment = self.env['account.payment'].sudo()
        except KeyError:
            return 0

        cutoff = (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')

        payments = Payment.search([
            ('state', '=', 'posted'),
            ('date', '>=', cutoff),
        ], limit=5000)

        if not payments:
            return 0

        batch = []
        for p in payments:
            cpid = (p.partner_id.commercial_partner_id.id
                    if p.partner_id and p.partner_id.commercial_partner_id
                    else (p.partner_id.id if p.partner_id else 0))
            if not cpid:
                continue

            batch.append({
                'odoo_partner_id': cpid,
                'name': p.name,
                'payment_type': p.payment_type or 'inbound',
                'amount': round(p.amount, 2),
                'currency': p.currency_id.name if p.currency_id else 'MXN',
                'payment_date': p.date.strftime('%Y-%m-%d') if p.date else None,
                'state': p.state,
            })

        synced = 0
        for i in range(0, len(batch), 200):
            chunk = batch[i:i + 200]
            try:
                supa._request(
                    '/rest/v1/odoo_payments?on_conflict=odoo_partner_id,name',
                    'POST', chunk,
                    extra_headers={'Prefer': 'resolution=merge-duplicates'},
                )
                synced += len(chunk)
            except Exception as exc:
                _logger.debug('sync payments batch %d: %s', i, exc)

        return synced

    # ── Deliveries ───────────────────────────────────────────────────────────

    def _sync_deliveries(self, supa) -> int:
        """Sync outgoing deliveries from stock.picking."""
        try:
            Picking = self.env['stock.picking'].sudo()
        except KeyError:
            return 0

        cutoff = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
        today_dt = fields.Datetime.now()

        # Pending + recent completed
        pickings = Picking.search([
            ('picking_type_code', '=', 'outgoing'),
            '|',
            ('state', 'not in', ['done', 'cancel']),
            ('date_done', '>=', cutoff),
        ], limit=3000)

        if not pickings:
            return 0

        batch = []
        for pk in pickings:
            cpid = (pk.partner_id.commercial_partner_id.id
                    if pk.partner_id and pk.partner_id.commercial_partner_id
                    else (pk.partner_id.id if pk.partner_id else 0))
            if not cpid:
                continue

            is_late = (pk.state not in ('done', 'cancel')
                       and pk.scheduled_date
                       and pk.scheduled_date < today_dt)

            lead_time = None
            if pk.state == 'done' and pk.date_done and pk.create_date:
                lead_time = round(
                    (pk.date_done - pk.create_date).total_seconds() / 86400, 1)

            batch.append({
                'odoo_partner_id': cpid,
                'name': pk.name,
                'picking_type': (pk.picking_type_id.name
                                 if pk.picking_type_id else ''),
                'origin': pk.origin or '',
                'scheduled_date': (pk.scheduled_date.strftime('%Y-%m-%d')
                                   if pk.scheduled_date else None),
                'date_done': (pk.date_done.isoformat()
                              if pk.date_done else None),
                'create_date': (pk.create_date.strftime('%Y-%m-%d')
                                if pk.create_date else None),
                'state': pk.state,
                'is_late': is_late,
                'lead_time_days': lead_time,
            })

        synced = 0
        for i in range(0, len(batch), 200):
            chunk = batch[i:i + 200]
            try:
                supa._request(
                    '/rest/v1/odoo_deliveries?on_conflict=odoo_partner_id,name',
                    'POST', chunk,
                    extra_headers={'Prefer': 'resolution=merge-duplicates'},
                )
                synced += len(chunk)
            except Exception as exc:
                _logger.debug('sync deliveries batch %d: %s', i, exc)

        return synced

    # ── CRM Leads ────────────────────────────────────────────────────────────

    def _sync_crm_leads(self, supa) -> int:
        """Sync active CRM leads and opportunities."""
        try:
            Lead = self.env['crm.lead'].sudo()
        except KeyError:
            return 0

        today = fields.Date.today()
        leads = Lead.search([
            ('active', '=', True),
        ], limit=2000)

        if not leads:
            return 0

        batch = []
        for lead in leads:
            cpid = (lead.partner_id.commercial_partner_id.id
                    if lead.partner_id and lead.partner_id.commercial_partner_id
                    else (lead.partner_id.id if lead.partner_id else None))

            days_open = 0
            if lead.create_date:
                days_open = (datetime.now() - lead.create_date).days

            batch.append({
                'odoo_partner_id': cpid,
                'odoo_lead_id': lead.id,
                'name': lead.name or '',
                'lead_type': lead.type or 'lead',
                'stage': lead.stage_id.name if lead.stage_id else '',
                'expected_revenue': round(lead.expected_revenue or 0, 2),
                'probability': round(lead.probability or 0, 1),
                'date_deadline': (lead.date_deadline.strftime('%Y-%m-%d')
                                  if lead.date_deadline else None),
                'create_date': (lead.create_date.strftime('%Y-%m-%d')
                                if lead.create_date else None),
                'days_open': days_open,
                'assigned_user': lead.user_id.name if lead.user_id else '',
                'active': lead.active,
            })

        synced = 0
        for i in range(0, len(batch), 200):
            chunk = batch[i:i + 200]
            try:
                supa._request(
                    '/rest/v1/odoo_crm_leads?on_conflict=odoo_lead_id',
                    'POST', chunk,
                    extra_headers={'Prefer': 'resolution=merge-duplicates'},
                )
                synced += len(chunk)
            except Exception as exc:
                _logger.debug('sync CRM leads batch %d: %s', i, exc)

        return synced

    # ── Activities ───────────────────────────────────────────────────────────

    def _sync_activities(self, supa) -> int:
        """Sync pending activities (all models) to Supabase."""
        try:
            Activity = self.env['mail.activity'].sudo()
        except KeyError:
            return 0

        today = fields.Date.today()

        # All pending activities
        activities = Activity.search([], limit=5000)

        if not activities:
            return 0

        # Delete all existing activities (they're recreated each time)
        try:
            supa._request('/rest/v1/odoo_activities?id=gt.0', 'DELETE',
                          extra_headers={'Prefer': 'return=minimal'})
        except Exception:
            pass

        batch = []
        for act in activities:
            # Resolve partner from activity's record
            partner_id = None
            if act.res_model == 'res.partner':
                partner_id = act.res_id
            elif act.res_model in ('sale.order', 'account.move',
                                    'purchase.order', 'crm.lead'):
                try:
                    record = self.env[act.res_model].sudo().browse(act.res_id)
                    if record.exists() and record.partner_id:
                        cpid = record.partner_id.commercial_partner_id
                        partner_id = cpid.id if cpid else record.partner_id.id
                except Exception:
                    pass

            batch.append({
                'odoo_partner_id': partner_id,
                'activity_type': (act.activity_type_id.name
                                  if act.activity_type_id else 'Tarea'),
                'summary': act.summary or act.note or '',
                'res_model': act.res_model or '',
                'res_id': act.res_id,
                'date_deadline': (act.date_deadline.strftime('%Y-%m-%d')
                                  if act.date_deadline else None),
                'assigned_to': act.user_id.name if act.user_id else '',
                'is_overdue': (act.date_deadline < today
                               if act.date_deadline else False),
            })

        synced = 0
        for i in range(0, len(batch), 200):
            chunk = batch[i:i + 200]
            try:
                supa._request(
                    '/rest/v1/odoo_activities',
                    'POST', chunk,
                    extra_headers={'Prefer': 'return=minimal'},
                )
                synced += len(chunk)
            except Exception as exc:
                _logger.debug('sync activities batch %d: %s', i, exc)

        return synced

"""Sync push: sale orders, purchase orders, order lines, deliveries, CRM, activities."""
import logging
from datetime import datetime, timedelta

from odoo import models

from .supabase_client import SupabaseClient
from .sync_push import _commercial_partner_id

_logger = logging.getLogger(__name__)


class QuimibondSyncOrders(models.TransientModel):
    _inherit = 'quimibond.sync'

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
                    'product_ref': ((l.product_id.default_code or '').strip() or None) if l.product_id else None,
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
                    'product_ref': ((l.product_id.default_code or '').strip() or None) if l.product_id else None,
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
        cids = self._get_company_ids()
        domain = [('active', '=', True), ('company_id', 'in', cids)]
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

    def _push_sale_orders(self, client: SupabaseClient, last_sync=None) -> int:
        """Push sale.order headers → odoo_sale_orders table."""
        try:
            SO = self.env['sale.order'].sudo()
        except KeyError:
            _logger.info('sale.order not available, skipping')
            return 0

        cids = self._get_company_ids()
        domain = [
            ('company_id', 'in', cids),
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

        cids = self._get_company_ids()
        domain = [
            ('company_id', 'in', cids),
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

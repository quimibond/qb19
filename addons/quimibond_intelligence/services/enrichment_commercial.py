"""
Quimibond Intelligence — Commercial Mixin
Sales, CRM, purchase patterns, and cross-sell for OdooEnrichmentService.
"""
import logging
from collections import defaultdict
from datetime import datetime, timedelta

from .enrichment_helpers import _safe_sum_aggregate

_logger = logging.getLogger(__name__)


class CommercialMixin:
    """Sales, invoices, purchases, CRM, activities, purchase patterns, cross-sell."""

    def enrich_partner(self, partner, models, date_90d, date_30d,
                       date_7d, today) -> dict:
        """Construye el perfil completo de un partner con todos los modelos."""
        pid = partner.id
        # Use commercial_partner_id for invoice/sale queries.
        # In Odoo, invoices/sales use commercial_partner_id (the company),
        # not the individual contact's partner_id.
        cpid = partner.commercial_partner_id.id if partner.commercial_partner_id else pid
        summary_parts = []

        # ── 1. Datos básicos ────────────────────────────────────────────────
        ctx = {
            'id': pid,
            'commercial_partner_id': cpid,
            'name': partner.name,
            'email': partner.email,
            'phone': partner.phone or '',
            'is_company': partner.is_company,
            'company_name': (partner.parent_id.name
                             if partner.parent_id else
                             (partner.name if partner.is_company else '')),
            'customer_rank': partner.customer_rank,
            'supplier_rank': partner.supplier_rank,
            'is_customer': partner.customer_rank > 0,
            'is_supplier': partner.supplier_rank > 0,
            'credit_limit': getattr(partner, 'credit_limit', 0),
            'total_invoiced': partner.total_invoiced or 0,
        }

        # ── 2. Ventas recientes (sale.order) ────────────────────────────────
        if models.get('sale_order'):
            # child_of matches the partner AND all its children
            sales = models['sale_order'].search([
                ('partner_id', 'child_of', cpid),
                ('date_order', '>=', date_90d),
            ], order='date_order desc', limit=10)

            ctx['recent_sales'] = [{
                'name': s.name,
                'date': s.date_order.strftime('%Y-%m-%d') if s.date_order else '',
                'amount': s.amount_total,
                'state': s.state,
                'currency': s.currency_id.name,
            } for s in sales]

            if ctx['is_customer'] and ctx['recent_sales']:
                total_sales = sum(s['amount'] for s in ctx['recent_sales'])
                summary_parts.append(
                    f"VENTAS: {len(ctx['recent_sales'])} pedidos "
                    f"(${total_sales:,.0f}) en 90d"
                )

        # ── 3. Facturas pendientes (account.move) ──────────────────────────
        if models.get('account_move'):
            invoices = models['account_move'].search([
                ('partner_id', 'child_of', cpid),
                ('move_type', 'in', ['out_invoice', 'out_refund']),
                ('payment_state', 'in', ['not_paid', 'partial']),
            ], order='invoice_date desc', limit=10)

            ctx['pending_invoices'] = [{
                'name': inv.name,
                'date': (inv.invoice_date.strftime('%Y-%m-%d')
                         if inv.invoice_date else ''),
                'amount': inv.amount_total,
                'amount_residual': inv.amount_residual,
                'state': inv.state,
                'currency': inv.currency_id.name,
                'days_overdue': (
                    (today - inv.invoice_date_due).days
                    if inv.invoice_date_due and inv.invoice_date_due < today
                    else 0
                ),
            } for inv in invoices]

            if ctx['pending_invoices']:
                total_pend = sum(
                    i['amount_residual'] for i in ctx['pending_invoices']
                )
                overdue = [
                    i for i in ctx['pending_invoices'] if i['days_overdue'] > 0
                ]
                if overdue:
                    max_overdue = max(i['days_overdue'] for i in overdue)
                    summary_parts.append(
                        f"FACTURAS: ${total_pend:,.0f} pendiente "
                        f"({len(overdue)} vencidas, máx {max_overdue}d)"
                    )
                else:
                    summary_parts.append(
                        f"FACTURAS: ${total_pend:,.0f} pendiente (al corriente)"
                    )

        # ── 4. Compras (purchase.order) ─────────────────────────────────────
        if models.get('purchase_order') and partner.supplier_rank > 0:
            purchases = models['purchase_order'].search([
                ('partner_id', 'child_of', cpid),
                ('date_order', '>=', date_90d),
            ], order='date_order desc', limit=10)

            ctx['recent_purchases'] = [{
                'name': p.name,
                'date': (p.date_order.strftime('%Y-%m-%d')
                         if p.date_order else ''),
                'amount': p.amount_total,
                'state': p.state,
                'currency': p.currency_id.name,
            } for p in purchases]

            if ctx['recent_purchases']:
                total_purch = sum(
                    p['amount'] for p in ctx['recent_purchases']
                )
                summary_parts.append(
                    f"COMPRAS: {len(ctx['recent_purchases'])} OC "
                    f"(${total_purch:,.0f}) en 90d"
                )

        # ── 5. Pagos recibidos/emitidos (account.payment) ──────────────────
        if models.get('account_payment'):
            payments = models['account_payment'].search([
                ('partner_id', 'child_of', cpid),
                ('state', '=', 'posted'),
                ('date', '>=', date_30d),
            ], order='date desc', limit=10)

            ctx['recent_payments'] = [{
                'name': pay.name,
                'date': pay.date.strftime('%Y-%m-%d') if pay.date else '',
                'amount': pay.amount,
                'payment_type': pay.payment_type,
                'currency': pay.currency_id.name,
            } for pay in payments]

            if ctx['recent_payments']:
                inbound = [
                    p for p in ctx['recent_payments']
                    if p['payment_type'] == 'inbound'
                ]
                outbound = [
                    p for p in ctx['recent_payments']
                    if p['payment_type'] == 'outbound'
                ]
                if inbound:
                    total_in = sum(p['amount'] for p in inbound)
                    summary_parts.append(
                        f"COBROS: ${total_in:,.0f} recibido (30d)"
                    )
                if outbound:
                    total_out = sum(p['amount'] for p in outbound)
                    summary_parts.append(
                        f"PAGOS: ${total_out:,.0f} pagado (30d)"
                    )

        # ── 6. Comunicación interna - Chatter (mail.message) ───────────────
        if models.get('mail_message'):
            messages = models['mail_message'].search([
                ('res_id', '=', pid),
                ('model', '=', 'res.partner'),
                ('message_type', 'in', ['comment', 'email']),
                ('date', '>=', date_7d),
            ], order='date desc', limit=10)

            ctx['recent_chatter'] = [{
                'date': msg.date.strftime('%Y-%m-%d %H:%M') if msg.date else '',
                'author': msg.author_id.name if msg.author_id else 'Sistema',
                'type': msg.message_type,
                'preview': (msg.body or '')[:200].replace('<br>', ' ')
                           .replace('<p>', '').replace('</p>', ''),
                'subtype': (msg.subtype_id.name
                            if msg.subtype_id else ''),
            } for msg in messages]

            # Buscar también mensajes en modelos relacionados (SO, PO, etc.)
            related_msgs = models['mail_message'].search([
                ('partner_ids', 'in', pid),
                ('message_type', 'in', ['comment', 'email']),
                ('date', '>=', date_7d),
                ('model', '!=', 'res.partner'),
            ], order='date desc', limit=10)

            ctx['related_chatter'] = [{
                'date': (msg.date.strftime('%Y-%m-%d %H:%M')
                         if msg.date else ''),
                'author': (msg.author_id.name
                           if msg.author_id else 'Sistema'),
                'model': msg.model or '',
                'res_id': msg.res_id,
                'preview': (msg.body or '')[:200].replace('<br>', ' ')
                           .replace('<p>', '').replace('</p>', ''),
            } for msg in related_msgs]

            total_msgs = len(ctx['recent_chatter']) + len(ctx['related_chatter'])
            if total_msgs > 0:
                summary_parts.append(
                    f"COMUNICACION ODOO: {total_msgs} mensajes en 7d"
                )

        # Continúa en _enrich_partner_dims_7_17
        self._enrich_partner_dims_7_17(
            partner, models, ctx, summary_parts, pid, date_90d, date_7d, today,
        )

        # ── Resumen consolidado ─────────────────────────────────────────────
        ctx['_summary'] = ' | '.join(summary_parts) if summary_parts else ''
        return ctx

    def _analyze_purchase_patterns(self, pid, models, date_90d, today):
        """Deep analysis of purchase patterns per product for a partner.

        Returns (products_list, patterns_dict) where patterns_dict contains:
        - product_details: per-product stats (frequency, trend, discount)
        - volume_drops: products with significant volume decrease
        - discount_anomalies: products with unusual discount on last order
        - cross_sell: products bought by similar clients but not this one
        """
        SO = models['sale_order']
        Product = models['product_product']

        # Fetch all confirmed orders in the last 12 months for deeper analysis
        date_12m = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        date_6m = (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')

        all_orders = SO.search([
            ('partner_id', 'child_of', pid),
            ('state', 'in', ['sale', 'done']),
            ('date_order', '>=', date_12m),
        ], order='date_order desc')

        if not all_orders:
            return [], {}

        # ── Collect per-product purchase history ────────────────────────────
        # product_id → list of {date, qty, price_unit, discount, subtotal}
        product_history = defaultdict(list)
        product_meta = {}  # product_id → {name, uom, stock_qty, category}

        for so in all_orders:
            order_date = so.date_order
            if not order_date:
                continue
            date_str = order_date.strftime('%Y-%m-%d')
            for line in so.order_line:
                prod = line.product_id
                if not prod or not prod.active:
                    continue
                # Skip service/section/note lines
                if line.display_type:
                    continue
                product_history[prod.id].append({
                    'date': date_str,
                    'qty': line.product_uom_qty,
                    'price_unit': line.price_unit,
                    'discount': line.discount or 0.0,
                    'subtotal': line.price_subtotal,
                })
                if prod.id not in product_meta:
                    categ = prod.categ_id
                    product_meta[prod.id] = {
                        'name': prod.name,
                        'uom': (line.product_uom.name
                                if line.product_uom else ''),
                        'stock_qty': prod.qty_available,
                        'categ_name': categ.name if categ else '',
                        'categ_id': categ.id if categ else None,
                    }

        if not product_history:
            return [], {}

        # ── Analyze each product ────────────────────────────────────────────
        product_details = []
        volume_drops = []
        discount_anomalies = []
        date_6m_str = date_6m

        for prod_id, history in product_history.items():
            meta = product_meta[prod_id]
            history_sorted = sorted(history, key=lambda x: x['date'])
            total_orders = len(history_sorted)
            total_qty = sum(h['qty'] for h in history_sorted)
            total_revenue = sum(h['subtotal'] for h in history_sorted)
            avg_price = (
                sum(h['price_unit'] for h in history_sorted) / total_orders
            )
            avg_discount = (
                sum(h['discount'] for h in history_sorted) / total_orders
            )

            # Last purchase info
            last = history_sorted[-1]
            first = history_sorted[0]

            # Purchase frequency (avg days between orders)
            avg_frequency_days = None
            if total_orders >= 2:
                first_dt = datetime.strptime(first['date'], '%Y-%m-%d')
                last_dt = datetime.strptime(last['date'], '%Y-%m-%d')
                span_days = (last_dt - first_dt).days
                if span_days > 0:
                    avg_frequency_days = round(span_days / (total_orders - 1))

            # Volume trend: compare recent 6m vs previous 6m
            recent_qty = sum(
                h['qty'] for h in history_sorted
                if h['date'] >= date_6m_str
            )
            previous_qty = sum(
                h['qty'] for h in history_sorted
                if h['date'] < date_6m_str
            )
            volume_trend_pct = None
            if previous_qty > 0:
                volume_trend_pct = round(
                    (recent_qty - previous_qty) / previous_qty * 100
                )

            # Discount anomaly: last discount vs average
            discount_delta = last['discount'] - avg_discount
            is_discount_anomaly = (
                abs(discount_delta) > 5 and total_orders >= 3
            )

            detail = {
                'name': meta['name'],
                'categ': meta['categ_name'],
                'uom': meta['uom'],
                'stock_qty': meta['stock_qty'],
                'total_orders': total_orders,
                'total_qty': total_qty,
                'total_revenue': round(total_revenue, 2),
                'avg_price': round(avg_price, 2),
                'avg_discount': round(avg_discount, 2),
                'avg_frequency_days': avg_frequency_days,
                'last_date': last['date'],
                'last_qty': last['qty'],
                'last_price': last['price_unit'],
                'last_discount': last['discount'],
                'recent_6m_qty': recent_qty,
                'previous_6m_qty': previous_qty,
                'volume_trend_pct': volume_trend_pct,
            }
            product_details.append(detail)

            # Flag volume drops (>30% decrease with at least some history)
            if (volume_trend_pct is not None
                    and volume_trend_pct <= -30
                    and previous_qty > 0):
                volume_drops.append({
                    'product': meta['name'],
                    'trend_pct': volume_trend_pct,
                    'recent_qty': recent_qty,
                    'previous_qty': previous_qty,
                })

            # Flag discount anomalies
            if is_discount_anomaly:
                discount_anomalies.append({
                    'product': meta['name'],
                    'last_discount': last['discount'],
                    'avg_discount': round(avg_discount, 2),
                    'delta': round(discount_delta, 2),
                })

        # Sort by total revenue descending
        product_details.sort(key=lambda x: x['total_revenue'], reverse=True)

        # ── Build backward-compatible products list ─────────────────────────
        products_list = [{
            'name': d['name'],
            'last_price': d['last_price'],
            'last_qty': d['last_qty'],
            'last_date': d['last_date'],
            'stock_qty': d['stock_qty'],
            'uom': d['uom'],
        } for d in product_details[:10]]

        # ── Cross-sell: find products bought by similar clients ─────────────
        cross_sell = self._detect_cross_sell(
            pid, product_history.keys(), models, product_meta,
        )

        patterns = {
            'product_details': product_details[:15],
            'volume_drops': volume_drops,
            'discount_anomalies': discount_anomalies,
            'cross_sell': cross_sell,
            'total_products': len(product_details),
            'total_revenue_12m': round(
                sum(d['total_revenue'] for d in product_details), 2,
            ),
        }
        return products_list, patterns

    def _detect_cross_sell(self, pid, bought_product_ids, models,
                           product_meta):
        """Find products bought by similar clients (same category) that
        this client hasn't bought.

        'Similar clients' = clients who bought at least 2 of the same products
        in the last 6 months.
        """
        SO = models['sale_order']
        bought_ids = set(bought_product_ids)
        if not bought_ids or len(bought_ids) < 2:
            return []

        try:
            date_6m = (
                datetime.now() - timedelta(days=180)
            ).strftime('%Y-%m-%d')

            # Find other partners who ordered the same products recently
            similar_orders = SO.search([
                ('partner_id', '!=', pid),
                ('state', 'in', ['sale', 'done']),
                ('date_order', '>=', date_6m),
                ('order_line.product_id', 'in', list(bought_ids)),
            ], limit=30)

            # Count overlap per partner and collect their other products
            partner_overlap = defaultdict(set)  # partner_id → bought_ids
            partner_other = defaultdict(set)    # partner_id → other products
            for so in similar_orders:
                for line in so.order_line:
                    if line.display_type or not line.product_id:
                        continue
                    p_id = line.product_id.id
                    if p_id in bought_ids:
                        partner_overlap[so.partner_id.id].add(p_id)
                    else:
                        partner_other[so.partner_id.id].add(p_id)

            # Keep only partners with >= 2 common products
            similar_partners = {
                p_id for p_id, overlap in partner_overlap.items()
                if len(overlap) >= 2
            }

            if not similar_partners:
                return []

            # Aggregate other products from similar clients
            candidate_counts = defaultdict(int)  # product_id → count
            for p_id in similar_partners:
                for prod_id in partner_other[p_id]:
                    if prod_id not in bought_ids:
                        candidate_counts[prod_id] += 1

            # Top 5 most common among similar clients
            top_candidates = sorted(
                candidate_counts.items(), key=lambda x: x[1], reverse=True,
            )[:5]

            cross_sell = []
            Product = models['product_product']
            for prod_id, count in top_candidates:
                if count < 2:
                    continue
                try:
                    prod = Product.browse(prod_id)
                    if prod.exists() and prod.active:
                        cross_sell.append({
                            'product': prod.name,
                            'similar_clients_buying': count,
                            'total_similar_clients': len(similar_partners),
                        })
                except Exception:
                    pass
            return cross_sell

        except Exception as exc:
            _logger.debug('Cross-sell detection: %s', exc)
            return []

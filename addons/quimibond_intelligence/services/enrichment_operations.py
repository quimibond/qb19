"""
Quimibond Intelligence — Operations Mixin
Stock, MRP, inventory, credit/aging for OdooEnrichmentService.
"""
import logging
from datetime import datetime, timedelta

from odoo import fields

from .enrichment_helpers import _safe_sum_aggregate

_logger = logging.getLogger(__name__)


class OperationsMixin:
    """Dims 7-17: activities, CRM, stock, MRP, calendar, lifetime, aging,
    related contacts, credit notes, delivery performance, inventory intelligence,
    payment behavior."""

    def _enrich_partner_dims_7_17(self, partner, models, ctx, summary_parts,
                                   pid, date_90d, date_7d, today):
        """Dimensiones 7-17 del enriquecimiento de partner."""
        cpid = ctx.get('commercial_partner_id', pid)

        # ── 7. Actividades pendientes (mail.activity) ──────────────────────
        if models.get('mail_activity'):
            activities = models['mail_activity'].search([
                ('res_id', '=', pid),
                ('res_model', '=', 'res.partner'),
            ], order='date_deadline asc', limit=10)

            partner_activities = list(activities)
            for model_name in ('sale.order', 'account.move', 'purchase.order',
                               'crm.lead'):
                try:
                    related = models['mail_activity'].search([
                        ('res_model', '=', model_name),
                        ('res_id', 'in', self.get_partner_record_ids(
                            partner, model_name, models,
                        )),
                    ], limit=10)
                    partner_activities.extend(related)
                except Exception:
                    pass

            ctx['pending_activities'] = [{
                'type': act.activity_type_id.name if act.activity_type_id else 'Tarea',
                'summary': act.summary or act.note or '',
                'deadline': (act.date_deadline.strftime('%Y-%m-%d')
                             if act.date_deadline else ''),
                'assigned_to': act.user_id.name if act.user_id else '',
                'is_overdue': (
                    act.date_deadline < today if act.date_deadline else False
                ),
                'model': act.res_model or '',
            } for act in partner_activities]

            overdue_acts = [
                a for a in ctx['pending_activities'] if a['is_overdue']
            ]
            pending_acts = [
                a for a in ctx['pending_activities'] if not a['is_overdue']
            ]
            if overdue_acts or pending_acts:
                parts = []
                if overdue_acts:
                    parts.append(f"{len(overdue_acts)} VENCIDAS")
                if pending_acts:
                    parts.append(f"{len(pending_acts)} pendientes")
                summary_parts.append(
                    f"ACTIVIDADES: {', '.join(parts)}"
                )

        # ── 8. Pipeline CRM (crm.lead) ─────────────────────────────────────
        if models.get('crm_lead'):
            leads = models['crm_lead'].search([
                ('partner_id', 'child_of', cpid),
                ('active', '=', True),
            ], order='create_date desc', limit=5)

            ctx['crm_leads'] = [{
                'name': lead.name,
                'stage': lead.stage_id.name if lead.stage_id else '',
                'expected_revenue': lead.expected_revenue or 0,
                'probability': lead.probability or 0,
                'date_deadline': (lead.date_deadline.strftime('%Y-%m-%d')
                                  if lead.date_deadline else ''),
                'user': lead.user_id.name if lead.user_id else '',
                'type': 'opportunity' if lead.type == 'opportunity' else 'lead',
                'days_open': (
                    (today - lead.create_date.date()).days
                    if lead.create_date else 0
                ),
            } for lead in leads]

            opps = [l for l in ctx['crm_leads'] if l['type'] == 'opportunity']
            if opps:
                total_rev = sum(l['expected_revenue'] for l in opps)
                summary_parts.append(
                    f"CRM: {len(opps)} oportunidades "
                    f"(${total_rev:,.0f} esperado)"
                )

        # ── 9. Entregas y recepciones (stock.picking) ──────────────────────
        if models.get('stock_picking'):
            pickings = models['stock_picking'].search([
                ('partner_id', 'child_of', cpid),
                ('state', 'not in', ['done', 'cancel']),
            ], order='scheduled_date asc', limit=10)

            ctx['pending_deliveries'] = [{
                'name': pick.name,
                'type': pick.picking_type_id.name if pick.picking_type_id else '',
                'scheduled': (pick.scheduled_date.strftime('%Y-%m-%d')
                              if pick.scheduled_date else ''),
                'state': pick.state,
                'is_late': (
                    pick.scheduled_date.date() < today
                    if pick.scheduled_date else False
                ),
                'origin': pick.origin or '',
            } for pick in pickings]

            if ctx['pending_deliveries']:
                late = [d for d in ctx['pending_deliveries'] if d['is_late']]
                if late:
                    summary_parts.append(
                        f"ENTREGAS: {len(ctx['pending_deliveries'])} "
                        f"pendientes ({len(late)} RETRASADAS)"
                    )
                else:
                    summary_parts.append(
                        f"ENTREGAS: {len(ctx['pending_deliveries'])} pendientes"
                    )

        # ── 10. Reuniones agendadas (calendar.event) ───────────────────────
        if models.get('calendar_event'):
            events = models['calendar_event'].search([
                ('partner_ids', 'in', pid),
                ('start', '>=', fields.Datetime.now()),
            ], order='start asc', limit=5)

            ctx['upcoming_meetings'] = [{
                'name': ev.name,
                'start': ev.start.strftime('%Y-%m-%d %H:%M') if ev.start else '',
                'attendees': [
                    att.display_name for att in (ev.attendee_ids or [])
                ][:5],
                'description': (ev.description or '')[:200],
            } for ev in events]

            if ctx['upcoming_meetings']:
                next_meeting = ctx['upcoming_meetings'][0]
                summary_parts.append(
                    f"REUNION: {next_meeting['name']} ({next_meeting['start']})"
                )

        # ── 11. Manufactura (mrp.production) ────────────────────────────────
        if models.get('mrp_production'):
            try:
                sale_orders = (models.get('sale_order') or self.env['sale.order'].sudo())
                so_ids = sale_orders.search([
                    ('partner_id', 'child_of', cpid),
                    ('state', 'in', ['sale', 'done']),
                    ('date_order', '>=', date_90d),
                ]).ids
                if so_ids:
                    productions = models['mrp_production'].search([
                        ('origin', 'like', 'SO'),
                        ('state', 'not in', ['done', 'cancel']),
                    ], limit=20)
                    so_names = set(
                        sale_orders.browse(so_ids).mapped('name')
                    )
                    partner_prods = [
                        p for p in productions
                        if p.origin and any(
                            sn in (p.origin or '') for sn in so_names
                        )
                    ]

                    ctx['manufacturing'] = [{
                        'name': mo.name,
                        'product': mo.product_id.name if mo.product_id else '',
                        'qty': mo.product_qty,
                        'state': mo.state,
                        'date_start': (mo.date_start.strftime('%Y-%m-%d')
                                       if mo.date_start else ''),
                        'origin': mo.origin or '',
                    } for mo in partner_prods[:5]]

                    if ctx.get('manufacturing'):
                        summary_parts.append(
                            f"PRODUCCION: {len(ctx['manufacturing'])} "
                            f"OMs en proceso"
                        )
            except Exception as exc:
                _logger.debug('MRP enrichment skip: %s', exc)

        # Continúa en _enrich_partner_dims_12_17
        self._enrich_partner_dims_12_17(
            partner, models, ctx, summary_parts, pid, date_90d, today,
        )

    def _enrich_partner_dims_12_17(self, partner, models, ctx, summary_parts,
                                    pid, date_90d, today):
        """Dimensiones 12-17 del enriquecimiento de partner."""
        cpid = ctx.get('commercial_partner_id', pid)

        # ── 12. Lifetime Value y tendencia histórica ────────────────────────
        if models.get('account_move') and ctx.get('is_customer'):
            try:
                AM = models['account_move']
                inv_domain = [
                    ('partner_id', 'child_of', cpid),
                    ('move_type', '=', 'out_invoice'),
                    ('state', '=', 'posted'),
                ]
                lifetime_total = _safe_sum_aggregate(
                    AM, inv_domain, 'amount_total')

                if not lifetime_total:
                    # Fallback: use partner.total_invoiced (Odoo computed)
                    lifetime_total = ctx.get('total_invoiced', 0)

                if lifetime_total:
                    first_inv = AM.search(
                        inv_domain + [('invoice_date', '!=', False)],
                        order='invoice_date asc', limit=1,
                    )
                    first_date = first_inv.invoice_date if first_inv else today
                    months_active = max(1, (today - first_date).days // 30)
                    monthly_avg = lifetime_total / months_active

                    date_3m = (
                        datetime.now() - timedelta(days=90)
                    ).strftime('%Y-%m-%d')
                    date_6m = (
                        datetime.now() - timedelta(days=180)
                    ).strftime('%Y-%m-%d')
                    recent_3m = _safe_sum_aggregate(
                        AM,
                        inv_domain + [('invoice_date', '>=', date_3m)],
                        'amount_total',
                    )
                    prev_3m = _safe_sum_aggregate(
                        AM,
                        inv_domain + [
                            ('invoice_date', '>=', date_6m),
                            ('invoice_date', '<', date_3m),
                        ],
                        'amount_total',
                    )

                    if prev_3m > 0:
                        trend_pct = round(
                            (recent_3m - prev_3m) / prev_3m * 100)
                        trend_dir = '\U0001f4c8' if trend_pct > 0 else '\U0001f4c9'
                    else:
                        trend_pct = 0
                        trend_dir = '\u2192'

                    ctx['lifetime'] = {
                        'total_invoiced': lifetime_total,
                        'first_invoice': first_date.strftime('%Y-%m-%d'),
                        'months_active': months_active,
                        'monthly_avg': round(monthly_avg, 2),
                        'recent_3m': recent_3m or 0,
                        'prev_3m': prev_3m or 0,
                        'trend_pct': trend_pct,
                    }
                    summary_parts.append(
                        f"LTV: ${lifetime_total:,.0f} en {months_active} meses "
                        f"(prom ${monthly_avg:,.0f}/mes) "
                        f"{trend_dir} {trend_pct:+d}% vs trimestre anterior"
                    )
            except Exception as exc:
                _logger.warning('Lifetime enrichment pid=%s: %s', pid, exc)

        # ── 13. Product Purchase Intelligence ─────────────────────────────
        if models.get('sale_order') and models.get('product_product'):
            try:
                ctx['products'], ctx['purchase_patterns'] = (
                    self._analyze_purchase_patterns(
                        cpid, models, date_90d, today,
                    )
                )
                patterns = ctx['purchase_patterns']
                if ctx['products']:
                    in_stock = sum(
                        1 for p in ctx['products'] if p['stock_qty'] > 0)
                    parts = [
                        f"{len(ctx['products'])} productos "
                        f"({in_stock} con stock)",
                    ]
                    if patterns.get('volume_drops'):
                        parts.append(
                            f"{len(patterns['volume_drops'])} con baja")
                    if patterns.get('discount_anomalies'):
                        parts.append(
                            f"{len(patterns['discount_anomalies'])} "
                            f"descuento inusual")
                    summary_parts.append(
                        f"PRODUCTOS: {' | '.join(parts)}"
                    )
            except Exception as exc:
                _logger.warning('Product enrichment pid=%s: %s', pid, exc)

        # ── 14. Cartera por antigüedad (aging) ───────────────────────────
        if models.get('account_move') and ctx.get('pending_invoices'):
            aging = {'current': 0, '1_30': 0, '31_60': 0,
                     '61_90': 0, '90_plus': 0}
            for inv in ctx['pending_invoices']:
                d = inv.get('days_overdue', 0)
                amt = inv.get('amount_residual', 0)
                if d <= 0:
                    aging['current'] += amt
                elif d <= 30:
                    aging['1_30'] += amt
                elif d <= 60:
                    aging['31_60'] += amt
                elif d <= 90:
                    aging['61_90'] += amt
                else:
                    aging['90_plus'] += amt
            ctx['aging'] = aging
            aging_parts = []
            if aging['1_30']:
                aging_parts.append(f"1-30d: ${aging['1_30']:,.0f}")
            if aging['31_60']:
                aging_parts.append(f"31-60d: ${aging['31_60']:,.0f}")
            if aging['61_90']:
                aging_parts.append(f"61-90d: ${aging['61_90']:,.0f}")
            if aging['90_plus']:
                aging_parts.append(f"90+d: ${aging['90_plus']:,.0f}")
            if aging_parts:
                summary_parts.append(
                    f"CARTERA VENCIDA: {' | '.join(aging_parts)}"
                )

        # ── 15. Contactos relacionados (misma empresa) ───────────────────
        company_id = partner.parent_id.id if partner.parent_id else (
            pid if partner.is_company else None
        )
        if company_id and models.get('partner'):
            try:
                siblings = models['partner'].search([
                    '|',
                    ('parent_id', '=', company_id),
                    ('id', '=', company_id),
                    ('id', '!=', pid),
                    ('email', '!=', False),
                ], limit=10)
                if siblings:
                    related = []
                    for sib in siblings:
                        info = {'name': sib.name, 'email': sib.email or ''}
                        if models.get('crm_lead'):
                            sib_leads = models['crm_lead'].search_count([
                                ('partner_id', '=', sib.id),
                                ('active', '=', True),
                            ])
                            if sib_leads:
                                info['active_opportunities'] = sib_leads
                        related.append(info)
                    ctx['related_contacts'] = related
                    summary_parts.append(
                        f"RED: {len(related)} contactos en misma empresa"
                    )
            except Exception as exc:
                _logger.debug('Related contacts: %s', exc)

        # ── 16. Devoluciones y notas de crédito ──────────────────────────
        if models.get('account_move'):
            try:
                credit_notes = models['account_move'].search([
                    ('partner_id', 'child_of', cpid),
                    ('move_type', '=', 'out_refund'),
                    ('state', '=', 'posted'),
                    ('invoice_date', '>=', date_90d),
                ], order='invoice_date desc', limit=5)
                if credit_notes:
                    cn_total = sum(cn.amount_total for cn in credit_notes)
                    ctx['credit_notes'] = [{
                        'name': cn.name,
                        'date': (cn.invoice_date.strftime('%Y-%m-%d')
                                 if cn.invoice_date else ''),
                        'amount': cn.amount_total,
                        'ref': cn.ref or '',
                    } for cn in credit_notes]
                    summary_parts.append(
                        f"DEVOLUCIONES: {len(credit_notes)} NC "
                        f"(${cn_total:,.0f}) en 90d \u26a0\ufe0f"
                    )
            except Exception as exc:
                _logger.debug('Credit notes: %s', exc)

        # ── 17. Performance de entrega (on-time rate) ────────────────────
        if models.get('stock_picking'):
            try:
                done_picks = models['stock_picking'].search([
                    ('partner_id', 'child_of', cpid),
                    ('state', '=', 'done'),
                    ('picking_type_code', '=', 'outgoing'),
                    ('date_done', '>=', date_90d),
                ], limit=50)
                if done_picks:
                    on_time = sum(
                        1 for p in done_picks
                        if p.scheduled_date and p.date_done
                        and p.date_done <= p.scheduled_date
                    )
                    total_done = len(done_picks)
                    otd_rate = round(on_time / total_done * 100)
                    avg_days = 0
                    lead_times = []
                    for p in done_picks:
                        if p.create_date and p.date_done:
                            lt = (p.date_done - p.create_date).days
                            if lt >= 0:
                                lead_times.append(lt)
                    if lead_times:
                        avg_days = round(
                            sum(lead_times) / len(lead_times), 1)

                    ctx['delivery_performance'] = {
                        'total_delivered': total_done,
                        'on_time_rate': otd_rate,
                        'avg_lead_time_days': avg_days,
                    }
                    otd_emoji = '\u2705' if otd_rate >= 90 else (
                        '\u26a0\ufe0f' if otd_rate >= 70 else '\U0001f534')
                    summary_parts.append(
                        f"ENTREGA OTD: {otd_rate}% {otd_emoji} "
                        f"({total_done} env\u00edos, lead time {avg_days}d)"
                    )
            except Exception as exc:
                _logger.warning('Delivery performance pid=%s: %s', pid, exc)

        # ── 18. Inventory Intelligence ─────────────────────────────────────
        # Analyze stock levels for products this client buys,
        # estimate days of inventory, flag stockout risks.
        product_details = ctx.get('purchase_patterns', {}).get(
            'product_details', [])
        if product_details and models.get('stock_quant'):
            try:
                ctx['inventory_intelligence'] = (
                    self._analyze_inventory_for_partner(
                        product_details, models, today,
                    )
                )
                inv_intel = ctx['inventory_intelligence']
                if inv_intel.get('at_risk'):
                    summary_parts.append(
                        f"INVENTARIO: {len(inv_intel['at_risk'])} productos "
                        f"en riesgo de desabasto"
                    )
                elif inv_intel.get('products'):
                    healthy = sum(
                        1 for p in inv_intel['products']
                        if p['status'] == 'healthy'
                    )
                    summary_parts.append(
                        f"INVENTARIO: {len(inv_intel['products'])} productos "
                        f"monitoreados ({healthy} sanos)"
                    )
            except Exception as exc:
                _logger.warning('Inventory intelligence pid=%s: %s', pid, exc)

        # ── 19. Payment Behavior Intelligence ──────────────────────────────
        # Compare agreed payment terms vs actual payment dates.
        if models.get('account_move') and ctx.get('is_customer'):
            try:
                ctx['payment_behavior'] = (
                    self._analyze_payment_behavior(cpid, models, today)
                )
                pb = ctx['payment_behavior']
                if pb.get('invoices_analyzed', 0) >= 3:
                    compliance = pb.get('compliance_score', 0)
                    avg_delay = pb.get('avg_days_late', 0)
                    trend = pb.get('trend', 'stable')
                    trend_icon = (
                        '\u2191' if trend == 'improving'
                        else '\u2193' if trend == 'worsening'
                        else '\u2192'
                    )
                    parts = [f"compliance {compliance}%"]
                    if avg_delay > 0:
                        parts.append(f"prom +{avg_delay:.0f}d tarde")
                    elif avg_delay < 0:
                        parts.append(f"prom {avg_delay:.0f}d antes")
                    parts.append(f"tendencia {trend_icon}")
                    summary_parts.append(
                        f"PAGO: {' | '.join(parts)}"
                    )
            except Exception as exc:
                _logger.warning('Payment behavior pid=%s: %s', pid, exc)

    def _analyze_inventory_for_partner(self, product_details, models, today):
        """Analyze inventory levels for products a client regularly buys.

        Uses purchase_patterns product_details (from dim 13) to know which
        products to check, then queries stock.quant for actual stock and
        estimates days of inventory based on recent consumption.

        Returns dict with:
        - products: list of per-product inventory status
        - at_risk: products with < 15 days of estimated inventory
        - total_stock_value: estimated value of relevant stock
        """
        Quant = models['stock_quant']
        Orderpoint = models.get('orderpoint')
        SOLine = models.get('sale_order_line')

        result_products = []
        at_risk = []
        total_stock_value = 0

        # Only analyze products that the client has ordered at least twice
        relevant = [
            p for p in product_details if p.get('total_orders', 0) >= 2
        ]
        if not relevant:
            return {'products': [], 'at_risk': [], 'total_stock_value': 0}

        # Pre-load product records for name lookup
        Product = models['product_product']

        # Get global daily consumption rate from sale.order.line (last 90d)
        date_90d = (
            datetime.now() - timedelta(days=90)
        ).strftime('%Y-%m-%d')

        for prod_detail in relevant[:15]:
            prod_name = prod_detail['name']
            try:
                # Find the product record
                prod = Product.search(
                    [('name', '=', prod_name), ('active', '=', True)],
                    limit=1,
                )
                if not prod:
                    continue

                # Current stock across all internal locations
                quants = Quant.search([
                    ('product_id', '=', prod.id),
                    ('location_id.usage', '=', 'internal'),
                ])
                current_qty = sum(q.quantity - q.reserved_quantity
                                  for q in quants)
                stock_value = current_qty * (prod.standard_price or 0)
                total_stock_value += stock_value

                # Estimate daily consumption (global, all clients, last 90d)
                daily_consumption = 0
                if SOLine:
                    try:
                        total_sold_90d = _safe_sum_aggregate(
                            SOLine,
                            [
                                ('product_id', '=', prod.id),
                                ('order_id.state', 'in', ['sale', 'done']),
                                ('order_id.date_order', '>=', date_90d),
                            ],
                            'product_uom_qty',
                        )
                        daily_consumption = round(total_sold_90d / 90, 2)
                    except Exception:
                        pass

                # Days of inventory
                days_of_inventory = None
                if daily_consumption > 0 and current_qty > 0:
                    days_of_inventory = round(
                        current_qty / daily_consumption)

                # Reorder point info
                reorder_min = None
                reorder_max = None
                if Orderpoint:
                    try:
                        op = Orderpoint.search([
                            ('product_id', '=', prod.id),
                        ], limit=1)
                        if op:
                            reorder_min = op.product_min_qty
                            reorder_max = op.product_max_qty
                    except Exception:
                        pass

                # Determine status
                if current_qty <= 0:
                    status = 'stockout'
                elif days_of_inventory is not None and days_of_inventory < 7:
                    status = 'critical'
                elif days_of_inventory is not None and days_of_inventory < 15:
                    status = 'low'
                elif (reorder_min is not None
                      and current_qty <= reorder_min):
                    status = 'below_reorder'
                else:
                    status = 'healthy'

                # This client's share of consumption
                client_freq_days = prod_detail.get('avg_frequency_days')
                client_avg_qty = (
                    prod_detail['total_qty'] / prod_detail['total_orders']
                    if prod_detail['total_orders'] > 0 else 0
                )
                next_order_estimate = None
                if client_freq_days and prod_detail.get('last_date'):
                    try:
                        last_dt = datetime.strptime(
                            prod_detail['last_date'], '%Y-%m-%d'
                        ).date()
                        days_since = (today - last_dt).days
                        next_order_estimate = max(
                            0, client_freq_days - days_since)
                    except (ValueError, TypeError):
                        pass

                info = {
                    'product': prod_name,
                    'current_qty': round(current_qty, 2),
                    'stock_value': round(stock_value, 2),
                    'daily_consumption': daily_consumption,
                    'days_of_inventory': days_of_inventory,
                    'reorder_min': reorder_min,
                    'reorder_max': reorder_max,
                    'status': status,
                    'client_avg_qty_per_order': round(client_avg_qty, 2),
                    'client_frequency_days': client_freq_days,
                    'client_next_order_days': next_order_estimate,
                    'can_fulfill_next_order': (
                        current_qty >= client_avg_qty
                        if client_avg_qty > 0 else None
                    ),
                }
                result_products.append(info)

                if status in ('stockout', 'critical', 'low',
                              'below_reorder'):
                    at_risk.append({
                        'product': prod_name,
                        'status': status,
                        'current_qty': round(current_qty, 2),
                        'days_of_inventory': days_of_inventory,
                        'daily_consumption': daily_consumption,
                        'can_fulfill_next_order': info[
                            'can_fulfill_next_order'],
                        'client_next_order_days': next_order_estimate,
                    })

            except Exception as exc:
                _logger.debug('Inventory check %s: %s', prod_name, exc)

        return {
            'products': result_products,
            'at_risk': at_risk,
            'total_stock_value': round(total_stock_value, 2),
        }

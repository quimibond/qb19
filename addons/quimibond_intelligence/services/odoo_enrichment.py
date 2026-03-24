"""
Quimibond Intelligence — Odoo Enrichment Service
Extraído de intelligence_engine.py: enriquecimiento profundo con Odoo ORM.
"""
import logging
from collections import defaultdict
from datetime import datetime, timedelta

from odoo import fields

_logger = logging.getLogger(__name__)


def _safe_sum_aggregate(model, domain, field_name):
    """Read an aggregate SUM compatible with Odoo 17+ and 19.

    Odoo 17+ deprecated read_group in favor of _read_group.
    Odoo 19 may have removed read_group entirely.
    Returns the numeric total or 0.
    """
    # Try new API first (_read_group, Odoo 17+)
    if hasattr(model, '_read_group'):
        try:
            result = model._read_group(
                domain, aggregates=[f'{field_name}:sum'],
            )
            # _read_group returns [(val,)] when no groupby
            if result and len(result) > 0:
                row = result[0]
                if isinstance(row, (list, tuple)):
                    return row[0] or 0
                # Some versions return dict
                if isinstance(row, dict):
                    return row.get(f'{field_name}', 0) or 0
            return 0
        except Exception as exc:
            _logger.debug('_read_group fallback: %s', exc)

    # Fallback: old API (Odoo 16 and earlier)
    try:
        rows = model.read_group(domain, [field_name], [])
        return rows[0][field_name] if rows else 0
    except Exception as exc:
        _logger.warning('read_group failed for %s: %s', field_name, exc)

    # Final fallback: brute-force search + sum
    try:
        records = model.search(domain)
        return sum(getattr(r, field_name, 0) or 0 for r in records)
    except Exception:
        return 0

# ── Dominios genéricos (Gmail, Outlook, etc.) ────────────────────────────────

GENERIC_DOMAINS = frozenset({
    'gmail.com', 'googlemail.com', 'outlook.com', 'hotmail.com',
    'yahoo.com', 'yahoo.com.mx', 'live.com', 'live.com.mx',
    'icloud.com', 'aol.com', 'protonmail.com', 'proton.me',
    'msn.com', 'mail.com', 'zoho.com', 'yandex.com',
    'google.com', 'vercel.com', 'github.com',
})


def is_generic_domain(domain: str) -> bool:
    """Retorna True si el dominio es genérico (Gmail, Outlook, etc.)."""
    return domain.lower() in GENERIC_DOMAINS


class OdooEnrichmentService:
    """Enriquecimiento profundo de contactos usando Odoo ORM (17 dimensiones)."""

    def __init__(self, env):
        self.env = env

    # ── Cargar modelos ORM disponibles ────────────────────────────────────

    def load_models(self) -> dict:
        """Carga los modelos ORM disponibles. Graceful si alguno no existe."""
        models = {}
        model_map = {
            'partner': 'res.partner',
            'sale_order': 'sale.order',
            'account_move': 'account.move',
            'purchase_order': 'purchase.order',
            'mail_message': 'mail.message',
            'mail_activity': 'mail.activity',
            'crm_lead': 'crm.lead',
            'stock_picking': 'stock.picking',
            'account_payment': 'account.payment',
            'calendar_event': 'calendar.event',
            'mrp_production': 'mrp.production',
            'product_product': 'product.product',
            'stock_quant': 'stock.quant',
            'orderpoint': 'stock.warehouse.orderpoint',
            'sale_order_line': 'sale.order.line',
            'payment_term': 'account.payment.term',
        }
        for key, model_name in model_map.items():
            try:
                models[key] = self.env[model_name].sudo()
            except KeyError:
                _logger.debug('Modelo %s no disponible (módulo no instalado)',
                              model_name)
        return models

    # ── Extracción de contactos ───────────────────────────────────────────

    def extract_contacts(self) -> list:
        """Extrae contactos de Odoo (fuente de verdad).

        Retorna lista de dicts con email, name, contact_type para uso
        en enrich() y otros métodos del pipeline.
        """
        models = self.load_models()
        if 'partner' not in models:
            _logger.warning('extract_contacts: res.partner no disponible')
            return []

        Partner = models['partner']
        partners = Partner.search([
            ('email', '!=', False),
            ('email', '!=', ''),
            ('active', '=', True),
            '|',
            ('customer_rank', '>', 0),
            ('supplier_rank', '>', 0),
        ], order='customer_rank desc, supplier_rank desc')

        contacts = []
        seen = set()
        for p in partners:
            email = (p.email or '').strip().lower()
            if not email or '@' not in email or email in seen:
                continue
            seen.add(email)
            contacts.append({
                'email': email,
                'name': p.name or '',
                'contact_type': 'external',
            })

        _logger.info('Odoo: %d partners activos con email', len(contacts))
        return contacts

    # ── IDs de registros asociados a un partner ───────────────────────────

    def get_partner_record_ids(self, partner, model_name, models) -> list:
        """IDs de registros de un modelo asociados a un partner."""
        try:
            if model_name == 'sale.order' and models.get('sale_order'):
                return models['sale_order'].search([
                    ('partner_id', '=', partner.id),
                ]).ids[:20]
            if model_name == 'account.move' and models.get('account_move'):
                return models['account_move'].search([
                    ('partner_id', '=', partner.id),
                ]).ids[:20]
            if model_name == 'purchase.order' and models.get('purchase_order'):
                return models['purchase_order'].search([
                    ('partner_id', '=', partner.id),
                ]).ids[:20]
            if model_name == 'crm.lead' and models.get('crm_lead'):
                return models['crm_lead'].search([
                    ('partner_id', '=', partner.id),
                ]).ids[:20]
        except Exception:
            pass
        return []

    # ══════════════════════════════════════════════════════════════════════════
    #   ENRICH PARTNER — 17 dimensiones (parte 1: dims 1-6)
    # ══════════════════════════════════════════════════════════════════════════

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
            # Search by both partner_id and commercial_partner_id
            sales = models['sale_order'].search([
                '|',
                ('partner_id', '=', pid),
                ('partner_id', '=', cpid),
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
                '|',
                ('partner_id', '=', pid),
                ('partner_id', '=', cpid),
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
                '|',
                ('partner_id', '=', pid),
                ('partner_id', '=', cpid),
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
                '|',
                ('partner_id', '=', pid),
                ('partner_id', '=', cpid),
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

    # ══════════════════════════════════════════════════════════════════════════
    #   ENRICH PARTNER — dimensiones 7-17
    # ══════════════════════════════════════════════════════════════════════════

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
                '|',
                ('partner_id', '=', pid),
                ('partner_id', '=', cpid),
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
                '|',
                ('partner_id', '=', pid),
                ('partner_id', '=', cpid),
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
                    '|',
                    ('partner_id', '=', pid),
                    ('partner_id', '=', cpid),
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
                    '|',
                    ('partner_id', '=', pid),
                    ('partner_id', '=', cpid),
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
                        trend_dir = '📈' if trend_pct > 0 else '📉'
                    else:
                        trend_pct = 0
                        trend_dir = '→'

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
                    '|',
                    ('partner_id', '=', pid),
                    ('partner_id', '=', cpid),
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
                        f"(${cn_total:,.0f}) en 90d ⚠️"
                    )
            except Exception as exc:
                _logger.debug('Credit notes: %s', exc)

        # ── 17. Performance de entrega (on-time rate) ────────────────────
        if models.get('stock_picking'):
            try:
                done_picks = models['stock_picking'].search([
                    '|',
                    ('partner_id', '=', pid),
                    ('partner_id', '=', cpid),
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
                    otd_emoji = '✅' if otd_rate >= 90 else (
                        '⚠️' if otd_rate >= 70 else '🔴')
                    summary_parts.append(
                        f"ENTREGA OTD: {otd_rate}% {otd_emoji} "
                        f"({total_done} envíos, lead time {avg_days}d)"
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
                        '↑' if trend == 'improving'
                        else '↓' if trend == 'worsening'
                        else '→'
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

    # ══════════════════════════════════════════════════════════════════════════
    #   PAYMENT BEHAVIOR INTELLIGENCE
    # ══════════════════════════════════════════════════════════════════════════

    def _analyze_payment_behavior(self, pid, models, today):
        """Analyze payment behavior: agreed terms vs actual payment dates.

        Looks at paid invoices (last 12 months) to calculate:
        - Compliance score (0-100): % of invoices paid on time or early
        - Average days late/early vs due date
        - Trend: comparing recent 6m behavior vs previous 6m
        - Payment term info from the partner
        - Per-invoice detail for the most recent ones

        Returns dict with compliance_score, avg_days_late, trend, details.
        """
        AM = models['account_move']
        date_12m = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        date_6m = (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')

        # Get paid invoices with both due date and payment date
        paid_invoices = AM.search([
            ('partner_id', '=', pid),
            ('move_type', '=', 'out_invoice'),
            ('state', '=', 'posted'),
            ('payment_state', 'in', ['paid', 'in_payment']),
            ('invoice_date', '>=', date_12m),
            ('invoice_date_due', '!=', False),
        ], order='invoice_date desc', limit=50)

        if not paid_invoices:
            return {'invoices_analyzed': 0}

        # Analyze each invoice: days between due date and actual payment
        invoice_details = []
        recent_delays = []   # last 6 months
        previous_delays = []  # 6-12 months ago

        for inv in paid_invoices:
            due_date = inv.invoice_date_due
            # Find actual payment date from reconciled payments
            payment_date = self._get_invoice_payment_date(inv)
            if not payment_date or not due_date:
                continue

            days_diff = (payment_date - due_date).days  # positive = late
            invoice_date_str = (
                inv.invoice_date.strftime('%Y-%m-%d')
                if inv.invoice_date else ''
            )

            detail = {
                'invoice': inv.name,
                'amount': inv.amount_total,
                'invoice_date': invoice_date_str,
                'due_date': due_date.strftime('%Y-%m-%d'),
                'payment_date': payment_date.strftime('%Y-%m-%d'),
                'days_diff': days_diff,
                'status': (
                    'early' if days_diff < 0
                    else 'on_time' if days_diff <= 3
                    else 'late'
                ),
            }
            invoice_details.append(detail)

            if invoice_date_str >= date_6m:
                recent_delays.append(days_diff)
            else:
                previous_delays.append(days_diff)

        if not invoice_details:
            return {'invoices_analyzed': 0}

        # Compliance score: % paid on time (within 3 day grace period)
        on_time_count = sum(
            1 for d in invoice_details if d['days_diff'] <= 3
        )
        compliance_score = round(on_time_count / len(invoice_details) * 100)

        # Average days late (negative = early)
        all_delays = [d['days_diff'] for d in invoice_details]
        avg_days_late = round(sum(all_delays) / len(all_delays), 1)

        # Trend: compare recent vs previous average delay
        trend = 'stable'
        recent_avg = None
        previous_avg = None
        if recent_delays and previous_delays:
            recent_avg = round(
                sum(recent_delays) / len(recent_delays), 1)
            previous_avg = round(
                sum(previous_delays) / len(previous_delays), 1)
            diff = recent_avg - previous_avg
            if diff <= -3:
                trend = 'improving'
            elif diff >= 3:
                trend = 'worsening'

        # Payment term from the partner
        Partner = models['partner']
        partner = Partner.browse(pid)
        payment_term_name = ''
        payment_term_days = None
        if hasattr(partner, 'property_payment_term_id') and \
                partner.property_payment_term_id:
            pt = partner.property_payment_term_id
            payment_term_name = pt.name or ''
            # Estimate days from the term lines
            try:
                if hasattr(pt, 'line_ids') and pt.line_ids:
                    max_days = max(
                        line.nb_days for line in pt.line_ids
                        if hasattr(line, 'nb_days')
                    )
                    payment_term_days = max_days
            except (ValueError, AttributeError):
                pass

        # Worst offenders (most late invoices)
        worst = sorted(
            invoice_details, key=lambda x: x['days_diff'], reverse=True,
        )[:3]

        return {
            'invoices_analyzed': len(invoice_details),
            'compliance_score': compliance_score,
            'avg_days_late': avg_days_late,
            'median_days_late': sorted(all_delays)[len(all_delays) // 2],
            'max_days_late': max(all_delays),
            'min_days_late': min(all_delays),
            'on_time_count': on_time_count,
            'late_count': len(invoice_details) - on_time_count,
            'trend': trend,
            'recent_6m_avg': recent_avg,
            'previous_6m_avg': previous_avg,
            'payment_term': payment_term_name,
            'payment_term_days': payment_term_days,
            'recent_invoices': invoice_details[:10],
            'worst_offenders': worst,
        }

    @staticmethod
    def _get_invoice_payment_date(invoice):
        """Get the actual payment date for a paid invoice.

        Tries reconciled payment first, falls back to write_date.
        """
        try:
            # Try to find reconciled payments via the invoice's
            # reconciled move lines
            for partial in (invoice._get_reconciled_payments() or []):
                if hasattr(partial, 'date') and partial.date:
                    return partial.date
            # Fallback: if payment_state is paid, use the last write_date
            # as an approximation
            if invoice.payment_state in ('paid', 'in_payment'):
                # Use invoice_date_due + a small buffer as conservative
                # estimate, or write_date
                if hasattr(invoice, 'write_date') and invoice.write_date:
                    return invoice.write_date.date()
        except Exception:
            pass
        # Final fallback
        if hasattr(invoice, 'write_date') and invoice.write_date:
            return invoice.write_date.date()
        return None

    # ══════════════════════════════════════════════════════════════════════════
    #   INVENTORY INTELLIGENCE
    # ══════════════════════════════════════════════════════════════════════════

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

    # ══════════════════════════════════════════════════════════════════════════
    #   PRODUCT PURCHASE INTELLIGENCE
    # ══════════════════════════════════════════════════════════════════════════

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
            ('partner_id', '=', pid),
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

    # ══════════════════════════════════════════════════════════════════════════
    #   ENRICH — orquesta todo el enriquecimiento
    # ══════════════════════════════════════════════════════════════════════════

    def enrich(self, contacts: list, emails: list = None) -> dict:
        """Enriquecimiento profundo: 10 modelos de Odoo + verificación de acciones."""
        emails = emails or []
        odoo_ctx = {
            'partners': {},
            'business_summary': {},
            'action_followup': {},
            'global_pipeline': {},
            'team_activities': {},
        }
        external_emails = [
            c['email'] for c in contacts
            if c['contact_type'] == 'external' and c['email']
        ]

        if not external_emails:
            return odoo_ctx

        models = self.load_models()
        if 'partner' not in models:
            return odoo_ctx

        Partner = models['partner']
        date_90d = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
        date_30d = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        date_7d = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        today = datetime.now().date()

        # ── Pre-cargar partners por dominio para fallback ─────────────────
        domain_map = {}
        for ea in external_emails:
            domain = ea.rsplit('@', 1)[-1].lower() if '@' in ea else ''
            if domain and not is_generic_domain(domain):
                domain_map.setdefault(domain, []).append(ea)

        domain_partners_cache = {}
        for domain in domain_map:
            try:
                domain_partners_cache[domain] = Partner.search(
                    [('email', '=ilike', f'%@{domain}')], limit=50,
                )
            except Exception:
                domain_partners_cache[domain] = Partner

        # ── Enriquecer por contacto externo ─────────────────────────────────
        matched = 0
        skipped = 0
        for email_addr in external_emails:
            try:
                partner = Partner.search(
                    [('email', '=ilike', email_addr)], limit=1,
                )

                if not partner:
                    domain = (email_addr.rsplit('@', 1)[-1].lower()
                              if '@' in email_addr else '')
                    domain_partners = domain_partners_cache.get(domain)
                    if domain_partners:
                        best = None
                        for dp in domain_partners:
                            if not best:
                                best = dp
                            elif dp.is_company and not best.is_company:
                                best = dp
                            elif (dp.customer_rank + dp.supplier_rank
                                  > best.customer_rank + best.supplier_rank):
                                best = dp
                        partner = best

                if not partner:
                    skipped += 1
                    continue

                matched += 1
                ctx = self.enrich_partner(
                    partner, models, date_90d, date_30d, date_7d, today,
                )
                odoo_ctx['partners'][email_addr] = ctx
                odoo_ctx['business_summary'][email_addr] = (
                    ctx.get('_summary', '')
                )

            except Exception as exc:
                _logger.warning('Odoo enrichment skip %s: %s', email_addr, exc)

        _logger.info(
            'Odoo match: %d/%d matched, %d skipped (no partner found)',
            matched, len(external_emails), skipped,
        )

        # ── Verificación de acciones sugeridas previamente ──────────────────
        odoo_ctx['action_followup'] = self.verify_pending_actions(today)

        # ── Contexto global del pipeline comercial ──────────────────────────
        if models.get('crm_lead'):
            odoo_ctx['global_pipeline'] = self.get_global_pipeline(
                models['crm_lead'],
            )

        # ── Actividades del equipo ──────────────────────────────────────────
        if models.get('mail_activity'):
            odoo_ctx['team_activities'] = self.get_team_activities(
                models['mail_activity'], today,
            )

        _logger.info(
            '✓ %d contactos enriquecidos (deep) | %d acciones verificadas',
            len(odoo_ctx['partners']),
            len(odoo_ctx.get('action_followup', {}).get('items', [])),
        )
        return odoo_ctx

    # ══════════════════════════════════════════════════════════════════════════
    #   VERIFY PENDING ACTIONS
    # ══════════════════════════════════════════════════════════════════════════

    def verify_pending_actions(self, today) -> dict:
        """Verifica si las acciones sugeridas previamente se ejecutaron."""
        result = {
            'items': [],
            'completion_rate': 0,
            'overdue_count': 0,
            'completed_today': 0,
        }
        try:
            ActionItem = self.env['intelligence.action.item'].sudo()
            pending = ActionItem.search([
                ('state', 'in', ['open', 'in_progress']),
            ], order='priority_seq asc, due_date asc', limit=30)

            if not pending:
                return result

            completed = ActionItem.search_count([
                ('state', '=', 'done'),
                ('write_date', '>=', today.strftime('%Y-%m-%d')),
            ])
            result['completed_today'] = completed

            for action in pending:
                item = {
                    'id': action.id,
                    'description': action.name,
                    'type': action.action_type,
                    'priority': action.priority,
                    'due_date': (action.due_date.strftime('%Y-%m-%d')
                                 if action.due_date else ''),
                    'assigned_to': (action.assignee_id.name
                                    if action.assignee_id else ''),
                    'partner': (action.partner_id.name
                                if action.partner_id else ''),
                    'days_open': (
                        (today - action.create_date.date()).days
                        if action.create_date else 0
                    ),
                    'is_overdue': action.is_overdue,
                    'evidence_of_action': [],
                }

                # Buscar evidencia de que alguien actuó
                if action.partner_id:
                    try:
                        MailMsg = self.env['mail.message'].sudo()
                        recent_msgs = MailMsg.search([
                            ('res_id', '=', action.partner_id.id),
                            ('model', '=', 'res.partner'),
                            ('date', '>=', action.create_date),
                            ('message_type', 'in', ['comment', 'email']),
                        ], limit=3, order='date desc')

                        for msg in recent_msgs:
                            item['evidence_of_action'].append({
                                'type': 'chatter_message',
                                'date': msg.date.strftime('%Y-%m-%d %H:%M'),
                                'author': (msg.author_id.name
                                           if msg.author_id else ''),
                                'preview': (msg.body or '')[:100],
                            })

                        MailActivity = self.env['mail.activity'].sudo()
                        activity_type_map = {
                            'call': 'Llamada',
                            'email': 'Correo',
                            'meeting': 'Reunión',
                        }
                        act_type_name = activity_type_map.get(
                            action.action_type, '',
                        )
                        if act_type_name:
                            scheduled = MailActivity.search([
                                ('res_id', '=', action.partner_id.id),
                                ('res_model', '=', 'res.partner'),
                            ], limit=3)
                            for act in scheduled:
                                item['evidence_of_action'].append({
                                    'type': 'scheduled_activity',
                                    'activity': (act.activity_type_id.name
                                                 if act.activity_type_id
                                                 else ''),
                                    'deadline': (
                                        act.date_deadline.strftime('%Y-%m-%d')
                                        if act.date_deadline else ''
                                    ),
                                    'assigned_to': (act.user_id.name
                                                    if act.user_id else ''),
                                })
                    except Exception:
                        pass

                if action.is_overdue:
                    result['overdue_count'] += 1

                result['items'].append(item)

            # Tasa de completado de los últimos 7 días
            week_ago = (
                today - timedelta(days=7)
            ).strftime('%Y-%m-%d')
            total_week = ActionItem.search_count([
                ('create_date', '>=', week_ago),
            ])
            done_week = ActionItem.search_count([
                ('create_date', '>=', week_ago),
                ('state', '=', 'done'),
            ])
            result['completion_rate'] = (
                round(done_week / total_week * 100)
                if total_week > 0 else 0
            )

        except Exception as exc:
            _logger.warning('Action verification error: %s', exc)

        return result

    # ══════════════════════════════════════════════════════════════════════════
    #   GLOBAL PIPELINE + TEAM ACTIVITIES
    # ══════════════════════════════════════════════════════════════════════════

    def get_global_pipeline(self, CrmLead) -> dict:
        """Resumen global del pipeline comercial."""
        try:
            all_opps = CrmLead.search([
                ('type', '=', 'opportunity'),
                ('active', '=', True),
            ])
            if not all_opps:
                return {}

            by_stage = defaultdict(lambda: {'count': 0, 'revenue': 0})
            for opp in all_opps:
                stage_name = opp.stage_id.name if opp.stage_id else 'Sin etapa'
                by_stage[stage_name]['count'] += 1
                by_stage[stage_name]['revenue'] += opp.expected_revenue or 0

            total_revenue = sum(s['revenue'] for s in by_stage.values())
            return {
                'total_opportunities': len(all_opps),
                'total_expected_revenue': total_revenue,
                'by_stage': dict(by_stage),
            }
        except Exception as exc:
            _logger.debug('Pipeline error: %s', exc)
            return {}

    def get_team_activities(self, MailActivity, today) -> dict:
        """Actividades pendientes del equipo agrupadas por usuario."""
        try:
            all_activities = MailActivity.search([
                ('date_deadline', '<=',
                 (today + timedelta(days=3)).strftime('%Y-%m-%d')),
            ], order='date_deadline asc', limit=50)

            by_user = defaultdict(lambda: {
                'pending': 0, 'overdue': 0, 'items': [],
            })
            for act in all_activities:
                user_name = act.user_id.name if act.user_id else 'Sin asignar'
                is_overdue = (
                    act.date_deadline < today if act.date_deadline else False
                )
                by_user[user_name]['pending'] += 1
                if is_overdue:
                    by_user[user_name]['overdue'] += 1
                if len(by_user[user_name]['items']) < 5:
                    by_user[user_name]['items'].append({
                        'type': (act.activity_type_id.name
                                 if act.activity_type_id else 'Tarea'),
                        'summary': act.summary or '',
                        'deadline': (act.date_deadline.strftime('%Y-%m-%d')
                                     if act.date_deadline else ''),
                        'model': act.res_model or '',
                        'overdue': is_overdue,
                    })
            return dict(by_user)
        except Exception as exc:
            _logger.debug('Team activities error: %s', exc)
            return {}

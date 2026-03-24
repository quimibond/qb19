"""
Quimibond Intelligence — Odoo Enrichment Service
Extraído de intelligence_engine.py: enriquecimiento profundo con Odoo ORM.
"""
import logging
from collections import defaultdict
from datetime import datetime, timedelta

from odoo import fields

_logger = logging.getLogger(__name__)

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
        summary_parts = []

        # ── 1. Datos básicos ────────────────────────────────────────────────
        ctx = {
            'id': pid,
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
            sales = models['sale_order'].search([
                ('partner_id', '=', pid),
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
                ('partner_id', '=', pid),
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
                ('partner_id', '=', pid),
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
                ('partner_id', '=', pid),
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
                ('partner_id', '=', pid),
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
                ('partner_id', '=', pid),
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
                    ('partner_id', '=', pid),
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

        # ── 12. Lifetime Value y tendencia histórica ────────────────────────
        if models.get('account_move') and ctx.get('is_customer'):
            try:
                AM = models['account_move']
                inv_domain = [
                    ('partner_id', '=', pid),
                    ('move_type', '=', 'out_invoice'),
                    ('state', '=', 'posted'),
                ]
                totals = AM.read_group(
                    inv_domain, ['amount_total'], [],
                )
                lifetime_total = totals[0]['amount_total'] if totals else 0

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
                    r3 = AM.read_group(
                        inv_domain + [('invoice_date', '>=', date_3m)],
                        ['amount_total'], [],
                    )
                    p3 = AM.read_group(
                        inv_domain + [
                            ('invoice_date', '>=', date_6m),
                            ('invoice_date', '<', date_3m),
                        ],
                        ['amount_total'], [],
                    )
                    recent_3m = r3[0]['amount_total'] if r3 else 0
                    prev_3m = p3[0]['amount_total'] if p3 else 0

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
                _logger.debug('Lifetime enrichment: %s', exc)

        # ── 13. Stock disponible y precios recientes ─────────────────────
        if models.get('sale_order') and models.get('product_product'):
            try:
                recent_so = models['sale_order'].search([
                    ('partner_id', '=', pid),
                    ('state', 'in', ['sale', 'done']),
                ], order='date_order desc', limit=5)
                product_info = {}
                for so in recent_so:
                    for line in so.order_line:
                        prod = line.product_id
                        if not prod or prod.id in product_info:
                            continue
                        product_info[prod.id] = {
                            'name': prod.name,
                            'last_price': line.price_unit,
                            'last_qty': line.product_uom_qty,
                            'last_date': (so.date_order.strftime('%Y-%m-%d')
                                          if so.date_order else ''),
                            'stock_qty': prod.qty_available,
                            'uom': (line.product_uom.name
                                    if line.product_uom else ''),
                        }
                if product_info:
                    ctx['products'] = list(product_info.values())[:10]
                    in_stock = sum(
                        1 for p in ctx['products'] if p['stock_qty'] > 0)
                    summary_parts.append(
                        f"PRODUCTOS: {len(ctx['products'])} "
                        f"comprados ({in_stock} con stock)"
                    )
            except Exception as exc:
                _logger.debug('Product enrichment: %s', exc)

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
                    ('partner_id', '=', pid),
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
                    ('partner_id', '=', pid),
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
                _logger.debug('Delivery performance: %s', exc)

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

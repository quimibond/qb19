"""
Quimibond Intelligence — Odoo Enrichment Service
Extraído de intelligence_engine.py: enriquecimiento profundo con Odoo ORM.
"""
import logging
from datetime import datetime, timedelta

from .enrichment_helpers import is_generic_domain
from .enrichment_financial import FinancialMixin
from .enrichment_commercial import CommercialMixin
from .enrichment_operations import OperationsMixin
from .enrichment_social import SocialMixin

_logger = logging.getLogger(__name__)


class OdooEnrichmentService(
    FinancialMixin,
    CommercialMixin,
    OperationsMixin,
    SocialMixin,
):
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
            raw_email = (p.email or '').strip().lower()
            if not raw_email or '@' not in raw_email:
                continue
            # Odoo partners can have multiple emails separated by ; or ,
            # Use only the first valid email address
            first_email = raw_email.split(';')[0].split(',')[0].strip()
            if not first_email or '@' not in first_email or first_email in seen:
                continue
            seen.add(first_email)
            contacts.append({
                'email': first_email,
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
            '\u2713 %d contactos enriquecidos (deep) | %d acciones verificadas',
            len(odoo_ctx['partners']),
            len(odoo_ctx.get('action_followup', {}).get('items', [])),
        )
        return odoo_ctx

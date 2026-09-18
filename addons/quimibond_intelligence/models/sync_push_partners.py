"""Sync push: contactos/empresas (`contacts` + `companies`) y usuarios (`odoo_users`).

Mixin con los dos únicos métodos _push_* que quedan en quimibond.sync. Son
el puente mínimo para que la memoria de correo en Supabase ligue correos
con partners y usuarios de Odoo (2026-09-18).
"""
import logging
import re
from datetime import datetime, timedelta

from odoo import models

from .supabase_client import SupabaseClient
from .sync_push import _commercial_partner_id, _best_partner_name, _EMAIL_RE

_logger = logging.getLogger(__name__)


class QuimibondSyncPartners(models.TransientModel):
    _inherit = 'quimibond.sync'

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
            cids = self._get_company_ids()
            invoice_partner_ids = Move.search([
                ('company_id', 'in', cids),
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
                # This is a company (top-level partner).
                # H9: usar _best_partner_name para caer a commercial_parent/
                # vat/email si partner.name es basura ("8141", "—", etc.).
                cn = _best_partner_name(p)
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
            # FIXME: `company_cn` se calcula con tres ramas (padre,
            # is_company, commercial_partner_id) y nunca se usa — el
            # dict de contacto no lleva el nombre de la empresa. O
            # falta pasarlo a Supabase, o sobran estas diez líneas.
            # Se deja como está: decidirlo es de quien mantiene el
            # sync, no de este cambio de lint.
            company_cn = None  # noqa: F841
            if p.parent_id:
                company_cn = (p.parent_id.name or '').strip() or None
            elif p.is_company:
                company_cn = (p.name or '').strip() or None
            else:
                # Standalone contact — try commercial_partner_id
                cp = p.commercial_partner_id
                if cp and cp.id != p.id:
                    company_cn = (cp.name or '').strip() or None

            for i, email in enumerate(emails):
                # Always set odoo_partner_id on the FIRST email so we never
                # lose the Odoo link. Previously, partners with 2+ emails got
                # None on all entries — breaking joins to invoices/orders.
                contacts.append({
                    'email': email,
                    'name': contact_name,
                    'contact_type': 'external',
                    'odoo_partner_id': p.id if i == 0 else None,
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
            cids = self._get_company_ids()
            inv_partners = Move.search([
                ('company_id', 'in', cids),
                ('move_type', 'in', ['out_invoice', 'out_refund', 'in_invoice', 'in_refund']),
                ('state', '=', 'posted'),
                ('invoice_date', '>=', cutoff),
            ]).mapped('partner_id.commercial_partner_id')

            for p in inv_partners:
                cp_id = p.id
                if cp_id in existing_partner_ids:
                    continue
                # H9: igual que arriba, usar helper con fallback chain.
                cn = _best_partner_name(p)
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
            # Use odoo_partner_id as conflict key (not canonical_name) so that
            # company renames in Odoo update the existing row instead of
            # creating a duplicate. All Odoo-sourced companies have partner_id.
            #
            # Normalizar shape: ambos loops (partners con email + partners
            # derivados de invoices) construyen dicts con conjuntos de
            # claves distintos (p.ej. el primero agrega odoo_context cuando
            # hay payment_term/tags, el segundo nunca). PostgREST rechaza
            # chunks heterogéneos con "All object keys must match"
            # (PGRST102). Garantizamos shape uniforme fusionando contra un
            # template de todas las claves vistas.
            company_rows = list(companies.values())
            all_keys: set = set()
            for row in company_rows:
                all_keys.update(row.keys())
            template = {k: None for k in all_keys}
            company_rows = [{**template, **row} for row in company_rows]
            # rfc, totales financieros y odoo_context van en el mismo upsert
            # (las columnas existen en `companies`). Los RPC de backfill
            # (backfill_company_financials / backfill_rfc_from_json) se
            # borraron de Supabase el 2026-09-18.
            synced += client.upsert(
                'companies', company_rows,
                on_conflict='odoo_partner_id', batch_size=100,
            )
        if contacts:
            # Dedupe by email before upsert. Sin esto Postgres rompía el
            # chunk entero con "ON CONFLICT DO UPDATE command cannot affect
            # row a second time" (500) cuando dos partners distintos
            # compartían email (muy común en Odoo — contactos de misma
            # empresa con email genérico). 2026-04-20 observed 45% failure
            # rate en contacts push.
            #
            # Regla de merge: si hay colisión, preferir la row con
            # odoo_partner_id no-null (para no perder el link a Odoo).
            dedup_contacts: dict[str, dict] = {}
            for row in contacts:
                key = (row.get('email') or '').strip().lower()
                if not key:
                    continue
                existing = dedup_contacts.get(key)
                if existing is None:
                    dedup_contacts[key] = row
                elif (existing.get('odoo_partner_id') is None
                      and row.get('odoo_partner_id') is not None):
                    dedup_contacts[key] = row
            contacts = list(dedup_contacts.values())

            # Second-pass dedup by odoo_partner_id (2026-04-22): aun después
            # del dedup por email, múltiples rows pueden compartir
            # odoo_partner_id (un mismo contacto de Odoo con varios emails).
            # Supabase tiene UNIQUE(odoo_partner_id) además del UNIQUE(email).
            by_partner: dict[int, dict] = {}
            no_partner: list[dict] = []
            for row in contacts:
                pid = row.get('odoo_partner_id')
                if not pid:
                    no_partner.append(row)
                    continue
                existing = by_partner.get(pid)
                if existing is None:
                    by_partner[pid] = row
                elif not existing.get('email') and row.get('email'):
                    by_partner[pid] = row
            rows_with_pid = list(by_partner.values())

            # Split upsert 2026-04-22: rows con odoo_partner_id usan
            # on_conflict=odoo_partner_id (el identificador real del contacto
            # en Odoo). Esto resuelve el caso crítico "partner ya existe en
            # Supabase con email distinto": antes con on_conflict=email, el
            # upsert no matcheaba el row viejo, tiraba INSERT, chocaba con
            # UNIQUE(odoo_partner_id) → 23505, 16.2% failure rate hourly.
            # Rows sin pid (raro) siguen con on_conflict=email para no perder
            # contacts importados manualmente en Supabase.
            if rows_with_pid:
                synced += client.upsert(
                    'contacts', rows_with_pid,
                    on_conflict='odoo_partner_id', batch_size=50,
                )
            if no_partner:
                synced += client.upsert(
                    'contacts', no_partner,
                    on_conflict='email', batch_size=50,
                )
        return synced

    # ── Users ────────────────────────────────────────────────────────────
    # odoo_users la lee el grafo nocturno de la memoria (kg_refresh_deterministic:
    # nodos `usuario` y quién atiende a quién). Siempre full push (<200 filas).

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
                'odoo_company_id': u.company_id.id if u.company_id else None,
                'updated_at': datetime.now().isoformat(),
            })

        return client.upsert('odoo_users', rows, on_conflict='odoo_user_id')

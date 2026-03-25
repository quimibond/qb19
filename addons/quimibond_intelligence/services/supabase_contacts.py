"""
Quimibond Intelligence — Supabase Contacts Mixin
Contact and company persistence for Supabase.
"""
import logging
from datetime import datetime
from urllib.parse import quote as url_quote

from .supabase_utils import _postgrest_in_list

_logger = logging.getLogger(__name__)


class SupabaseContactsMixin:
    """Contact and company persistence for Supabase."""

    # ── Contactos ────────────────────────────────────────────────────────────

    def save_contacts(self, contacts: list):
        """Guarda contactos en lotes via RPC, con fallback individual."""
        if not contacts:
            return
        # Batch RPC calls in chunks to reduce overhead
        chunk = 50
        saved = 0
        failed = 0
        for i in range(0, len(contacts), chunk):
            batch = contacts[i:i + chunk]
            for c in batch:
                try:
                    params = {
                        'p_email': c['email'],
                        'p_name': c.get('name', ''),
                        'p_contact_type': c['contact_type'],
                        'p_department': c.get('department'),
                    }
                    company = c.get('company')
                    if company:
                        params['p_company_name'] = company
                    self._request('/rest/v1/rpc/upsert_contact', 'POST', params)
                    saved += 1
                except Exception as exc:
                    failed += 1
                    _logger.warning('save_contact %s: %s', c.get('email'), exc)
        self._track(success=saved, failed=failed)
        _logger.info('✓ %d contactos guardados (%d fallidos)', saved, failed)

    # ── Person Profiles (aprendizaje acumulativo) ─────────────────────────

    def upsert_person_profile(self, profile: dict):
        """Actualiza campos de perfil/personalidad directamente en contacts.

        Cada vez que el sistema procesa emails, actualiza el perfil con
        nueva información. Los datos se acumulan — no se sobrescriben
        a menos que haya info más reciente.
        """
        email = profile.get('email')
        if not email:
            return None

        # Build patch with only non-None profile fields
        profile_fields = (
            'role', 'decision_power', 'communication_style',
            'language_preference', 'key_interests', 'personality_notes',
            'negotiation_style', 'response_pattern', 'influence_on_deals',
        )
        patch = {}
        for field in profile_fields:
            val = profile.get(field)
            if val is not None:
                patch[field] = val
        if profile.get('department'):
            patch['department'] = profile['department']

        if not patch:
            return None

        try:
            encoded = url_quote(email.lower().strip(), safe='')
            self._request(
                f'/rest/v1/contacts?email=eq.{encoded}',
                'PATCH', patch,
                extra_headers={'Prefer': 'return=minimal'},
            )
        except Exception as exc:
            _logger.debug('upsert_person_profile: %s', exc)
        return None

    def get_person_profile(self, email=None, name=None):
        """Obtiene el perfil acumulado de una persona desde contacts."""
        if email:
            key = url_quote(email.lower().strip(), safe='')
            filter_param = f'email=eq.{key}'
        elif name:
            key = url_quote(name.lower().strip(), safe='')
            filter_param = f'name=eq.{key}'
        else:
            return None
        try:
            result = self._request(
                f'/rest/v1/contacts?{filter_param}&limit=1'
                '&select=id,email,name,company,company_id,role,department,'
                'decision_power,communication_style,language_preference,'
                'key_interests,personality_notes,negotiation_style,'
                'response_pattern,influence_on_deals,interaction_count',
            )
            return result[0] if result else None
        except Exception:
            return None

    # ── Contact Odoo Sync ─────────────────────────────────────────────────────

    def sync_contact_odoo_data(self, email: str, odoo_data: dict,
                               _company_cache: dict = None):
        """Actualiza un contacto en Supabase con datos de Odoo.

        Si company_name está en odoo_data, resuelve o crea la empresa
        en la tabla companies y asigna company_id al contacto.
        Pass _company_cache to avoid repeated company lookups.
        """
        try:
            # Resolve company_name → company_id
            company_name = odoo_data.get('company')
            if company_name and 'company_id' not in odoo_data:
                company_id = self._resolve_or_create_company(
                    company_name, odoo_data, _cache=_company_cache,
                )
                if company_id:
                    odoo_data['company_id'] = company_id

            encoded = url_quote(email, safe='')
            self._request(
                f'/rest/v1/contacts?email=eq.{encoded}',
                'PATCH', odoo_data,
                extra_headers={'Prefer': 'return=minimal'},
            )
        except Exception as exc:
            _logger.warning('sync_contact_odoo %s: %s', email, exc)

    # ── Companies ─────────────────────────────────────────────────────────

    def batch_resolve_companies(self, names: list) -> dict:
        """Batch-resolve company names to IDs (chunked queries).

        Returns dict: {canonical_name → company_id}
        """
        if not names:
            return {}
        canonicals = [n.lower().strip() for n in names if n and n.strip()]
        if not canonicals:
            return {}
        result = {}
        chunk = 50
        try:
            for i in range(0, len(canonicals), chunk):
                part = canonicals[i:i + chunk]
                enc = _postgrest_in_list(part)
                if not enc:
                    continue
                rows = self._request(
                    f'/rest/v1/companies?canonical_name=in.({enc})'
                    '&select=id,canonical_name',
                ) or []
                for r in rows:
                    cn = r.get('canonical_name', '')
                    if cn:
                        result[cn] = r['id']
        except Exception as exc:
            _logger.warning('batch_resolve_companies: %s', exc)
        return result

    def _resolve_or_create_company(self, name: str,
                                    odoo_data: dict = None,
                                    _cache: dict = None) -> int:
        """Busca o crea una empresa por nombre. Retorna company_id.

        Pass _cache (from batch_resolve_companies) to skip the lookup query.
        """
        if not name or not name.strip():
            return None
        canonical = name.lower().strip()

        # Check cache first (from batch_resolve_companies)
        if _cache and canonical in _cache:
            return _cache[canonical]

        try:
            encoded = url_quote(canonical, safe='')
            resp = self._request(
                f'/rest/v1/companies?canonical_name=eq.{encoded}&select=id',
            )
            if resp and isinstance(resp, list) and resp:
                cid = resp[0]['id']
                if _cache is not None:
                    _cache[canonical] = cid
                return cid
        except Exception:
            pass

        # Create new company
        company_data = {
            'name': name.strip(),
            'canonical_name': canonical,
        }
        if odoo_data:
            if odoo_data.get('odoo_partner_id'):
                company_data['odoo_partner_id'] = odoo_data['odoo_partner_id']
            if odoo_data.get('is_customer') is not None:
                company_data['is_customer'] = odoo_data['is_customer']
            if odoo_data.get('is_supplier') is not None:
                company_data['is_supplier'] = odoo_data['is_supplier']
        try:
            result = self._request(
                '/rest/v1/companies', 'POST', company_data,
                extra_headers={'Prefer': 'return=representation'},
            )
            if result and isinstance(result, list) and result:
                cid = result[0]['id']
                if _cache is not None:
                    _cache[canonical] = cid
                return cid
        except Exception as exc:
            _logger.debug('create_company %s: %s', name, exc)
        return None

    def sync_company_odoo_data(self, company_id: int, data: dict):
        """Actualiza datos agregados de una empresa desde Odoo.

        Acepta tanto métricas escalares (lifetime_value, etc.) como
        odoo_context JSONB con detalle operacional completo.
        """
        try:
            self._request(
                f'/rest/v1/companies?id=eq.{company_id}',
                'PATCH', data,
                extra_headers={'Prefer': 'return=minimal'},
            )
        except Exception as exc:
            _logger.warning('sync_company_odoo %s: %s', company_id, exc)

    def save_company_snapshots(self, snapshots: list):
        """Guarda snapshots diarios de métricas operacionales por empresa.

        Upsert por (company_id, snapshot_date) — si ya existe el snapshot
        del día, lo actualiza con datos frescos.
        """
        if not snapshots:
            return
        try:
            self._upsert_batch(
                '/rest/v1/odoo_snapshots'
                '?on_conflict=company_id,snapshot_date',
                snapshots, 'merge-duplicates',
            )
            self._track(success=len(snapshots))
            _logger.info('✓ %d company snapshots guardados', len(snapshots))
        except Exception as exc:
            self._track(failed=len(snapshots))
            _logger.warning('save_company_snapshots: %s', exc)

    def get_company_contacts(self, company_id: int) -> list:
        """Obtiene todos los contactos de una empresa."""
        try:
            return self._request(
                f'/rest/v1/contacts?company_id=eq.{company_id}'
                '&select=id,email,name,role,decision_power,'
                'relationship_score,risk_level'
                '&order=name',
            ) or []
        except Exception:
            return []

    def get_company_360(self, company_id: int) -> dict:
        """Vista 360 de una empresa (contactos, alertas, revenue, facts)."""
        try:
            return self._request(
                '/rest/v1/rpc/get_company_360', 'POST',
                {'p_company_id': company_id},
            ) or {}
        except Exception as exc:
            _logger.debug('get_company_360: %s', exc)
            return {}

    def get_companies_needing_enrichment(self, limit: int = 20) -> list:
        """Empresas sin perfil o con perfil desactualizado (>30 días)."""
        try:
            return self._request(
                '/rest/v1/companies?enriched_at=is.null'
                '&select=id,name,canonical_name,entity_id,'
                'is_customer,is_supplier,odoo_context'
                f'&order=updated_at.desc&limit={limit}',
            ) or []
        except Exception as exc:
            _logger.warning('get_companies_needing_enrichment: %s', exc)
            return []

    def save_company_profile(self, company_id: int, profile: dict):
        """Guarda perfil enriquecido por Claude en la empresa."""
        from datetime import datetime
        patch = {'enriched_at': datetime.now().isoformat(),
                 'enrichment_source': 'claude'}
        for field in ('description', 'business_type', 'relationship_type',
                      'relationship_summary', 'industry', 'country', 'city',
                      'key_products', 'risk_signals', 'opportunity_signals',
                      'strategic_notes'):
            val = profile.get(field)
            if val is not None:
                patch[field] = val
        try:
            self._request(
                f'/rest/v1/companies?id=eq.{company_id}',
                'PATCH', patch,
                extra_headers={'Prefer': 'return=minimal'},
            )
        except Exception as exc:
            _logger.warning('save_company_profile %s: %s', company_id, exc)

"""
Engine — Enrichment & Scoring (Odoo → Supabase contacts, scores, company profiles)
"""
import json
import logging
import time
from datetime import datetime, timedelta

from odoo import api, models

from .intelligence_config import TZ_CDMX, acquire_lock, release_lock

_logger = logging.getLogger(__name__)


class IntelligenceEngine(models.Model):
    _inherit = 'intelligence.engine'

    # ══════════════════════════════════════════════════════════════════════════
    #   MICRO-PIPELINE: ENRICH CONTACTS
    # ══════════════════════════════════════════════════════════════════════════

    @api.model
    def run_enrich_only(self):
        """Enriquece contactos Odoo → Supabase. Corre cada 6h."""
        lock = 'quimibond_intelligence.enrich_running'
        if not acquire_lock(self.env, lock):
            return
        start = time.time()

        try:
            cfg = self._load_config()
            if not cfg:
                return

            from ..services.odoo_enrichment import OdooEnrichmentService
            from ..services.supabase_service import SupabaseService

            odoo_svc = OdooEnrichmentService(self.env)
            contacts = odoo_svc.extract_contacts()
            odoo_context = odoo_svc.enrich(contacts)
            today = datetime.now(TZ_CDMX).strftime('%Y-%m-%d')

            with SupabaseService(cfg['supabase_url'], cfg['supabase_key']) as supa:
                if odoo_context.get('partners'):
                    self._sync_contacts_to_supabase(odoo_context, supa, today)
                    self._link_odoo_ids(supa)
                    # Resolver company_id en todas las tablas huérfanas
                    try:
                        result = supa._request(
                            '/rest/v1/rpc/resolve_all_company_links',
                            'POST', {},
                        )
                        _logger.info('Company links resolved: %s', result)
                    except Exception as exc:
                        _logger.debug('resolve_company_links: %s', exc)
                    _logger.info('✓ %d partners sincronizados',
                                 len(odoo_context['partners']))
                else:
                    _logger.warning('Enrich: sin partners Odoo')

                supa._request('/rest/v1/events', 'POST', {
                    'event_type': 'contacts_enriched',
                    'source': 'cron_enrich_only',
                    'payload': {
                        'partners': len(odoo_context.get('partners', {})),
                        'elapsed_s': round(time.time() - start, 1),
                    },
                })

                _logger.info(
                    '✓ Enrich: %d partners (%.1fs)',
                    len(odoo_context.get('partners', {})),
                    time.time() - start,
                )
        except Exception as exc:
            _logger.error('run_enrich_only: %s', exc, exc_info=True)
        finally:
            release_lock(self.env, lock)

    # ══════════════════════════════════════════════════════════════════════════
    #   MICRO-PIPELINE: UPDATE SCORES
    # ══════════════════════════════════════════════════════════════════════════

    @api.model
    def run_update_scores(self):
        """Recalcula scores de clientes. Corre cada 12h."""
        lock = 'quimibond_intelligence.scores_running'
        if not acquire_lock(self.env, lock):
            return
        start = time.time()

        try:
            cfg = self._load_config()
            if not cfg:
                return

            from ..services.analysis_service import (
                AnalysisService,
                normalize_supabase_emails,
            )
            from ..services.odoo_enrichment import OdooEnrichmentService
            from ..services.supabase_service import SupabaseService

            odoo_svc = OdooEnrichmentService(self.env)
            contacts = odoo_svc.extract_contacts()
            odoo_context = odoo_svc.enrich(contacts)
            today = datetime.now(TZ_CDMX).strftime('%Y-%m-%d')
            analysis = AnalysisService()

            with SupabaseService(cfg['supabase_url'], cfg['supabase_key']) as supa:
                # Leer emails recientes (últimos 7 días)
                cutoff_7d = (
                    datetime.now(TZ_CDMX) - timedelta(days=7)
                ).strftime('%Y-%m-%d')
                try:
                    recent_emails = supa._request(
                        '/rest/v1/emails?order=email_date.desc'
                        '&limit=1000'
                        '&select=*'
                        f'&email_date=gte.{cutoff_7d}T00:00:00Z',
                    ) or []
                except Exception:
                    recent_emails = []

                emails = normalize_supabase_emails(recent_emails)

                threads = self._build_threads(emails, cfg)

                # Computar scores
                client_scores = analysis.compute_client_scores(
                    contacts, emails, threads, cfg,
                    odoo_ctx=odoo_context,
                )

                # Sentimientos de summaries recientes (si hay)
                contact_sentiments = {}
                try:
                    summaries = supa._request(
                        '/rest/v1/daily_summaries?order=summary_date.desc'
                        '&limit=1&select=account_summaries'
                        f'&summary_date=gte.{cutoff_7d}',
                    ) or []
                    for s in summaries:
                        for acct_s in (s.get('account_summaries') or []):
                            for ec in acct_s.get('external_contacts', []):
                                email_addr = (
                                    ec.get('email') or ''
                                ).lower()
                                if (email_addr
                                        and ec.get('sentiment_score')
                                        is not None):
                                    try:
                                        contact_sentiments[email_addr] = (
                                            float(ec['sentiment_score'])
                                        )
                                    except (ValueError, TypeError):
                                        pass
                except Exception:
                    pass

                # Guardar scores en Supabase
                try:
                    supa.save_client_scores(
                        client_scores, today,
                        contact_sentiments=contact_sentiments,
                    )
                except Exception as exc:
                    _logger.error('Error guardando client scores: %s', exc)

                # Guardar scores en Odoo
                Partner = self.env['res.partner'].sudo()
                Score = self.env['intelligence.client.score'].sudo()
                for s in client_scores:
                    try:
                        email_addr = s.get('email', '')
                        partner = Partner.search(
                            [('email', '=ilike', email_addr)], limit=1,
                        )
                        if partner:
                            Score.create({
                                'partner_id': partner.id,
                                'date': today,
                                'email': email_addr,
                                'total_score': s.get('total_score', 0),
                                'frequency_score': s.get(
                                    'frequency_score', 0),
                                'responsiveness_score': s.get(
                                    'responsiveness_score', 0),
                                'reciprocity_score': s.get(
                                    'reciprocity_score', 0),
                                'sentiment_score': s.get(
                                    'sentiment_score', 0),
                                'payment_compliance_score': s.get(
                                    'payment_compliance_score', 0),
                                'risk_level': s.get('risk_level', 'medium'),
                            })
                    except Exception as exc:
                        _logger.debug('Score save Odoo %s: %s',
                                      s.get('email'), exc)

                # Health scores
                try:
                    sb_contacts = [
                        {'email': e, 'contact_type': 'external'}
                        for e in odoo_context.get('partners', {})
                    ]
                    supa.compute_and_save_health_scores(
                        sb_contacts, [], today,
                    )
                except Exception as exc:
                    _logger.debug('Health scores: %s', exc)

                supa._request('/rest/v1/events', 'POST', {
                    'event_type': 'scores_updated',
                    'source': 'cron_update_scores',
                    'payload': {
                        'client_scores': len(client_scores),
                        'emails_analyzed': len(emails),
                        'elapsed_s': round(time.time() - start, 1),
                    },
                })

                _logger.info(
                    '✓ Scores: %d clients, %d emails (%.1fs)',
                    len(client_scores), len(emails),
                    time.time() - start,
                )
        except Exception as exc:
            _logger.error('run_update_scores: %s', exc, exc_info=True)
        finally:
            release_lock(self.env, lock)

    # ══════════════════════════════════════════════════════════════════════════
    #   HELPERS: CONTACT SYNC
    # ══════════════════════════════════════════════════════════════════════════

    def _sync_contacts_to_supabase(self, odoo_context, supa, today):
        """Sincroniza partners de Odoo → contacts + companies en Supabase.

        Usa odoo_partner_id como llave universal. Envía detalle operacional
        (facturas, pedidos, pagos, entregas) a companies directamente.
        """
        partners = odoo_context.get('partners', {})
        if not partners:
            return

        # Agrupar datos por company (commercial_partner_id)
        companies_data = {}

        synced, failed = 0, 0
        for email_addr, pdata in partners.items():
            try:
                params = {
                    'p_email': email_addr,
                    'p_name': pdata.get('name', ''),
                    'p_contact_type': 'external',
                }
                company = pdata.get('company_name') or pdata.get('company', '')
                if company:
                    params['p_company_name'] = company

                supa._request(
                    '/rest/v1/rpc/upsert_contact', 'POST', params,
                )

                # Actualizar campos extendidos del contacto
                from urllib.parse import quote as url_quote
                encoded = url_quote(email_addr.lower().strip(), safe='')
                patch = {}

                pid = pdata.get('id') or pdata.get('partner_id')
                cpid = pdata.get('commercial_partner_id')
                if pid:
                    patch['odoo_partner_id'] = pid
                if cpid:
                    patch['commercial_partner_id'] = cpid
                if pdata.get('is_customer') is not None:
                    patch['is_customer'] = pdata['is_customer']
                if pdata.get('is_supplier') is not None:
                    patch['is_supplier'] = pdata['is_supplier']
                if pdata.get('phone'):
                    patch['phone'] = pdata['phone']

                # Guardar contexto completo de Odoo como JSON
                odoo_ctx_json = {
                    k: v for k, v in pdata.items()
                    if k not in ('_summary',) and v is not None
                }
                if odoo_ctx_json:
                    patch['odoo_context'] = json.dumps(
                        odoo_ctx_json, default=str, ensure_ascii=False,
                    )

                # Acumular datos para company (por commercial_partner_id)
                if company and cpid:
                    if cpid not in companies_data:
                        companies_data[cpid] = pdata

                if patch:
                    supa._request(
                        f'/rest/v1/contacts?email=eq.{encoded}',
                        'PATCH', patch,
                        extra_headers={'Prefer': 'return=minimal'},
                    )

                synced += 1
            except Exception as exc:
                failed += 1
                _logger.debug('sync_contact %s: %s', email_addr, exc)

        # Sync operational detail to companies table
        companies_synced = 0
        for cpid, pdata in companies_data.items():
            try:
                company_patch = {}
                # Detalle operacional como JSONB
                for field in ('recent_sales', 'pending_invoices',
                              'recent_payments', 'recent_purchases',
                              'crm_leads', 'pending_deliveries',
                              'manufacturing', 'pending_activities',
                              'payment_behavior', 'aging', 'products',
                              'inventory_intelligence', 'purchase_patterns'):
                    val = pdata.get(field)
                    if val is not None:
                        company_patch[field] = (
                            json.dumps(val, default=str, ensure_ascii=False)
                            if not isinstance(val, str) else val
                        )

                # Totales
                if pdata.get('total_invoiced'):
                    company_patch['lifetime_value'] = pdata['total_invoiced']
                if pdata.get('credit_limit'):
                    company_patch['credit_limit'] = pdata['credit_limit']
                lifetime = pdata.get('lifetime', {})
                if lifetime.get('monthly_avg'):
                    company_patch['monthly_avg'] = lifetime['monthly_avg']
                if lifetime.get('trend_pct') is not None:
                    company_patch['trend_pct'] = lifetime['trend_pct']
                delivery = pdata.get('delivery_performance', {})
                if delivery.get('on_time_rate') is not None:
                    company_patch['delivery_otd_rate'] = (
                        delivery['on_time_rate'])

                if company_patch:
                    company_patch['odoo_partner_id'] = cpid
                    supa._request(
                        f'/rest/v1/companies?odoo_partner_id=eq.{cpid}',
                        'PATCH', company_patch,
                        extra_headers={'Prefer': 'return=minimal'},
                    )
                    companies_synced += 1
            except Exception as exc:
                _logger.debug('sync_company %s: %s', cpid, exc)

        _logger.info(
            'Contacts sync: %d ok, %d failed de %d total. '
            'Companies updated: %d',
            synced, failed, len(partners), companies_synced,
        )

    def _link_odoo_ids(self, supa):
        """Vincula contactos de Supabase con partners de Odoo por email."""
        try:
            unlinked = supa._request(
                '/rest/v1/contacts?odoo_partner_id=is.null'
                '&contact_type=eq.external'
                '&select=id,email'
                '&limit=200',
            ) or []
        except Exception as exc:
            _logger.warning('link_odoo_ids fetch: %s', exc)
            return

        if not unlinked:
            return

        Partner = self.env['res.partner'].sudo()
        linked = 0

        for contact in unlinked:
            email_addr = contact.get('email', '')
            if not email_addr:
                continue
            try:
                partner = Partner.search(
                    [('email', '=ilike', email_addr)], limit=1,
                )
                if partner:
                    supa._request(
                        f'/rest/v1/contacts?id=eq.{contact["id"]}',
                        'PATCH',
                        {'odoo_partner_id': partner.id},
                        extra_headers={'Prefer': 'return=minimal'},
                    )
                    linked += 1
            except Exception as exc:
                _logger.debug('link_odoo_id %s: %s', email_addr, exc)

        _logger.info('Linked %d contactos de %d sin odoo_partner_id',
                     linked, len(unlinked))

    # ══════════════════════════════════════════════════════════════════════════
    #   HELPER: COMPANY ENRICHMENT
    # ══════════════════════════════════════════════════════════════════════════

    def _enrich_companies(self, supa, claude, today):
        """Enriquece empresas con Claude basado en datos acumulados."""
        companies = supa.get_companies_needing_enrichment(limit=10)
        if not companies:
            _logger.info('Company enrichment: sin empresas pendientes')
            return

        enriched, failed = 0, 0
        for company in companies:
            company_name = company.get('name', '')
            company_id = company.get('id')
            if not company_id or not company_name:
                continue

            try:
                # Construir contexto a partir de datos acumulados
                context_parts = [
                    f'Empresa: {company_name}',
                ]
                if company.get('canonical_name'):
                    context_parts.append(
                        f'Nombre canónico: {company["canonical_name"]}')
                if company.get('is_customer'):
                    context_parts.append('Es cliente de Quimibond')
                if company.get('is_supplier'):
                    context_parts.append('Es proveedor de Quimibond')

                # Buscar datos adicionales de la entidad en KG
                entity_id = company.get('entity_id')
                if entity_id:
                    try:
                        facts = supa._request(
                            f'/rest/v1/facts?entity_id=eq.{entity_id}'
                            '&select=fact_text,fact_type,fact_date'
                            '&order=fact_date.desc&limit=20',
                        ) or []
                        if facts:
                            context_parts.append('\nHechos conocidos:')
                            for f in facts:
                                context_parts.append(
                                    f'- [{f.get("fact_type", "")}] '
                                    f'{f.get("fact_text", "")}')
                    except Exception:
                        pass

                # Contexto de Odoo si está disponible
                odoo_ctx = company.get('odoo_context')
                if odoo_ctx:
                    if isinstance(odoo_ctx, str):
                        try:
                            odoo_ctx = json.loads(odoo_ctx)
                        except (json.JSONDecodeError, TypeError):
                            odoo_ctx = None
                    if isinstance(odoo_ctx, dict):
                        context_parts.append(
                            f'\nDatos Odoo: {json.dumps(odoo_ctx, default=str, ensure_ascii=False)[:2000]}')

                context = '\n'.join(context_parts)
                profile = claude.profile_company(company_name, context)

                supa.save_company_profile(company_id, profile)

                # Guardar snapshot operacional
                try:
                    supa.save_company_snapshots([{
                        'company_id': company_id,
                        'snapshot_date': today,
                    }])
                except Exception:
                    pass

                enriched += 1
                _logger.info('  ✓ Company enriched: %s', company_name)
            except Exception as exc:
                failed += 1
                _logger.warning('  ✗ Company enrich %s: %s',
                                company_name, exc)

        _logger.info('Company enrichment: %d ok, %d failed', enriched, failed)

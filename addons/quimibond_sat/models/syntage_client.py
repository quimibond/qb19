# -*- coding: utf-8 -*-
"""Cliente de la API de Syntage.

Dos formas de traer CFDI:

* ``pull_invoices``: GET paginado de /entities/{id}/invoices. Es la forma
  robusta: Syntage solo manda webhook cuando una factura es nueva o cambia
  en SU backend, así que una extracción repetida no re-emite lo que ya tenía
  y el espejo se queda corto. El pull rellena eso y es idempotente (upsert
  por UUID).
* ``request_extraction``: POST /extractions. Le pide a Syntage que vaya al
  SAT por los CFDI de un periodo. Los datos llegan después (webhook o pull).
  Cada extracción tiene costo en Syntage: no ampliar la ventana sin motivo.

Parámetros del sistema: quimibond_sat.api_key, quimibond_sat.api_base.
"""
import logging
import time
from datetime import timedelta
from urllib.parse import urlencode

import requests

from odoo import _, api, fields, models
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)

DEFAULT_API_BASE = 'https://api.syntage.com'


class SyntageClient(models.AbstractModel):
    _name = 'sat.syntage.client'
    _description = 'Cliente de la API de Syntage'

    # ── configuración ──────────────────────────────────────────────────

    @api.model
    def _param(self, key, default=''):
        value = self.env['ir.config_parameter'].sudo().get_param('quimibond_sat.' + key, default)
        return (value or '').strip()

    @api.model
    def _api_base(self):
        return (self._param('api_base', DEFAULT_API_BASE) or DEFAULT_API_BASE).rstrip('/')

    @api.model
    def _api_key(self):
        key = self._param('api_key')
        if not key:
            raise UserError(_('Falta el parámetro del sistema quimibond_sat.api_key (API key de Syntage).'))
        return key

    @api.model
    def _headers(self, extra=None):
        headers = {
            'X-API-Key': self._api_key(),
            'Accept': 'application/ld+json',
            'Content-Type': 'application/json',
        }
        if extra:
            headers.update(extra)
        return headers

    @api.model
    def _request(self, method, path, params=None, json=None, headers=None, timeout=60):
        url = path if path.startswith('http') else self._api_base() + path
        try:
            resp = requests.request(method, url, params=params, json=json,
                                    headers=self._headers(headers), timeout=timeout)
        except requests.RequestException as exc:
            raise UserError(_('No se pudo conectar con Syntage (%s): %s') % (path, exc)) from exc
        if resp.status_code >= 400:
            raise UserError(_('Syntage respondió %s en %s: %s') % (resp.status_code, path, resp.text[:500]))
        try:
            return resp.json()
        except ValueError:
            return {}

    @api.model
    def _companies_to_sync(self):
        return self.env['res.company'].sudo().search([
            ('sat_sync_enabled', '=', True), ('vat', '!=', False),
        ])

    @api.model
    def _rfc(self, company):
        rfc = (company.vat or '').strip().upper()
        if not rfc:
            raise UserError(_('La compañía %s no tiene RFC.') % company.display_name)
        return rfc

    @api.model
    def _entity_id_for(self, company):
        """Entidad de Syntage para la compañía; se resuelve por RFC y se cachea."""
        company = company.sudo()
        if company.sat_syntage_entity_id:
            return company.sat_syntage_entity_id
        rfc = self._rfc(company)
        body = self._request('GET', '/entities', params={'taxpayer': '/taxpayers/%s' % rfc})
        members = body.get('hydra:member') or []
        entity_id = members[0].get('id') if members else None
        if not entity_id:
            raise UserError(_('Syntage no tiene una entidad para el RFC %s. Revisa que el '
                              'contribuyente esté vinculado en Syntage.') % rfc)
        company.sat_syntage_entity_id = entity_id
        return entity_id

    # ── descarga por API ───────────────────────────────────────────────

    @api.model
    def pull_invoices(self, company, date_from=None, date_to=None, max_pages=500,
                      page_size=100, commit=True):
        """Descarga los CFDI (emitidos y recibidos) del periodo y los guarda en
        sat.cfdi. Devuelve el registro de sat.sync.log."""
        start = time.time()
        Cfdi = self.env['sat.cfdi']
        entity_id = self._entity_id_for(company)
        params = {'itemsPerPage': page_size}
        if date_from:
            params['issuedAt[after]'] = fields.Date.to_string(date_from)
        if date_to:
            params['issuedAt[before]'] = fields.Date.to_string(date_to)
        url = '%s/entities/%s/invoices?%s' % (self._api_base(), entity_id, urlencode(params))
        headers = {'X-Pagination-Style': 'cursor', 'X-Pagination-Enable-Partial': '0'}

        fetched = upserted = errored = pages = 0
        errors = []
        while url and pages < max_pages:
            body = self._request('GET', url, headers=headers)
            pages += 1
            items = body.get('hydra:member') or []
            fetched += len(items)
            for obj in items:
                try:
                    with self.env.cr.savepoint():
                        Cfdi._upsert_from_syntage(obj, company, event_type='pull')
                    upserted += 1
                except Exception as exc:  # una fila mala no tira la página
                    errored += 1
                    errors.append('%s: %s' % (obj.get('uuid') or obj.get('id'), str(exc)[:200]))
                    _logger.warning('sat.cfdi upsert falló (%s): %s', obj.get('uuid'), exc)
            if commit:
                self.env.cr.commit()
            nxt = (body.get('hydra:view') or {}).get('hydra:next')
            url = (self._api_base() + nxt) if nxt else None

        status = 'success' if not errored else ('partial' if upserted else 'error')
        summary = _('%(pages)s página(s), %(fetched)s CFDI recibidos, %(upserted)s guardados, '
                    '%(errored)s con error.') % {
            'pages': pages, 'fetched': fetched, 'upserted': upserted, 'errored': errored}
        if url:
            summary += _(' Se alcanzó el máximo de páginas (%s); vuelve a correr para continuar.') % max_pages
        if errors:
            summary += '\n' + '\n'.join(errors[:50])
        return self.env['sat.sync.log'].sudo().create({
            'name': _('Descarga %(rfc)s %(from)s..%(to)s') % {
                'rfc': self._rfc(company), 'from': date_from or '', 'to': date_to or ''},
            'kind': 'pull',
            'company_id': company.id,
            'status': status,
            'summary': summary,
            'date_from': date_from,
            'date_to': date_to,
            'items_fetched': fetched,
            'items_upserted': upserted,
            'items_errored': errored,
            'duration_seconds': round(time.time() - start, 1),
        })

    # ── extracción ─────────────────────────────────────────────────────

    @api.model
    def request_extraction(self, company, date_from, date_to, include_retentions=False):
        """Pide a Syntage que extraiga del SAT los CFDI del periodo."""
        start = time.time()
        rfc = self._rfc(company)
        extractors = ['invoice', 'tax_retention'] if include_retentions else ['invoice']
        lines = []
        ok = 0
        for extractor in extractors:
            options = {
                'period': {'from': fields.Date.to_string(date_from), 'to': fields.Date.to_string(date_to)},
                'issued': True, 'received': True, 'xml': True, 'pdf': True, 'complement': -1,
            }
            if extractor == 'invoice':
                options['types'] = ['I', 'E', 'P', 'N', 'T']
            try:
                body = self._request('POST', '/extractions', json={
                    'taxpayer': '/taxpayers/%s' % rfc, 'extractor': extractor, 'options': options})
                lines.append('%s: extracción %s aceptada' % (extractor, body.get('id') or ''))
                ok += 1
            except UserError as exc:
                lines.append('%s: %s' % (extractor, exc.args[0] if exc.args else exc))
        status = 'success' if ok == len(extractors) else ('partial' if ok else 'error')
        return self.env['sat.sync.log'].sudo().create({
            'name': _('Extracción %(rfc)s %(from)s..%(to)s') % {'rfc': rfc, 'from': date_from, 'to': date_to},
            'kind': 'extraction',
            'company_id': company.id,
            'status': status,
            'summary': '\n'.join(lines),
            'date_from': date_from,
            'date_to': date_to,
            'duration_seconds': round(time.time() - start, 1),
        })

    # ── crons (los dispara sat.cfdi, modelo regular) ───────────────────

    @api.model
    def _run_daily_extraction(self, lookback_days=4):
        today = fields.Date.context_today(self)
        for company in self._companies_to_sync():
            try:
                with self.env.cr.savepoint():
                    self.request_extraction(company, today - timedelta(days=lookback_days), today)
            except Exception as exc:
                _logger.exception('Extracción diaria falló para %s', company.display_name)
                self.env['sat.sync.log'].sudo().create({
                    'name': _('Extracción %s') % (company.vat or company.display_name),
                    'kind': 'extraction', 'company_id': company.id, 'status': 'error',
                    'summary': str(exc)[:2000],
                })

    @api.model
    def _run_pull_recent(self, lookback_days=7):
        today = fields.Date.context_today(self)
        for company in self._companies_to_sync():
            try:
                self.pull_invoices(company, today - timedelta(days=lookback_days), today)
            except Exception as exc:
                _logger.exception('Descarga reciente falló para %s', company.display_name)
                self.env['sat.sync.log'].sudo().create({
                    'name': _('Descarga %s') % (company.vat or company.display_name),
                    'kind': 'pull', 'company_id': company.id, 'status': 'error',
                    'summary': str(exc)[:2000],
                })

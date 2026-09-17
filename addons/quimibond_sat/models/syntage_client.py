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
from odoo.tools import config

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
    def _commit(self):
        """Commit intermedio (descargas largas): se omite en modo test, donde
        el cursor de prueba no admite commit."""
        in_test = getattr(self.env.registry, 'in_test_mode', None)
        if (in_test and in_test()) or config['test_enable']:
            return
        self.env.cr.commit()

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
        """Entidad de Syntage para la compañía; se resuelve por RFC y se cachea.

        /entities?taxpayer=... devuelve TODAS las entidades de la organización
        (2026-09-17: la primera era la de otro contribuyente y se descargaron
        sus CFDI bajo Quimibond), así que se elige la que trae el RFC en
        taxpayer.id o credential.rfc; con varias, la de credencial válida."""
        company = company.sudo()
        if company.sat_syntage_entity_id:
            return company.sat_syntage_entity_id
        rfc = self._rfc(company)
        body = self._request('GET', '/entities', params={'taxpayer': '/taxpayers/%s' % rfc})
        members = [m for m in (body.get('hydra:member') or []) if isinstance(m, dict)]
        entity_id = self._pick_entity(members, rfc)
        if not entity_id:
            found = ', '.join(sorted({self._entity_rfc(m) or '?' for m in members})) or 'ninguna'
            raise UserError(_('Syntage no tiene una entidad para el RFC %(rfc)s (entidades encontradas: '
                              '%(found)s). Revisa que el contribuyente esté vinculado en Syntage.')
                            % {'rfc': rfc, 'found': found})
        company.sat_syntage_entity_id = entity_id
        return entity_id

    @staticmethod
    def _entity_rfc(member):
        taxpayer = member.get('taxpayer') or {}
        credential = member.get('credential') or {}
        if isinstance(taxpayer, dict):
            rfc = taxpayer.get('id') or taxpayer.get('@id')
        else:
            rfc = taxpayer
        rfc = rfc or (credential.get('rfc') if isinstance(credential, dict) else None)
        if isinstance(rfc, str) and rfc.startswith('/taxpayers/'):
            rfc = rfc.split('/')[-1]
        return (rfc or '').strip().upper() or None

    @api.model
    def _pick_entity(self, members, rfc):
        matching = [m for m in members if self._entity_rfc(m) == rfc]
        if not matching:
            return None
        valid = [m for m in matching
                 if ((m.get('credential') or {}).get('status') if isinstance(m.get('credential'), dict) else None) == 'valid']
        chosen = (valid or matching)[0]
        return chosen.get('id') or (chosen.get('@id') or '').split('/')[-1] or None

    # ── descarga por API ───────────────────────────────────────────────

    @api.model
    def pull_invoices(self, company, date_from=None, date_to=None, max_pages=500,
                      page_size=100, commit=True, log=None):
        """Descarga los CFDI (emitidos y recibidos) del periodo y los guarda en
        sat.cfdi. La bitácora se crea al arrancar (status running) y se cierra
        al final; con ``log`` se reutiliza una encolada. Devuelve el registro."""
        start = time.time()
        Cfdi = self.env['sat.cfdi']
        Log = self.env['sat.sync.log'].sudo()
        if log is None:
            log = Log.create({
                'name': _('Descarga %(rfc)s %(from)s..%(to)s') % {
                    'rfc': self._rfc(company), 'from': date_from or '', 'to': date_to or ''},
                'kind': 'pull', 'mode': 'pull', 'company_id': company.id,
                'date_from': date_from, 'date_to': date_to,
            })
        log.write({'status': 'running', 'summary': False})
        if commit:
            self._commit()
        try:
            entity_id = self._entity_id_for(company)
            params = {'itemsPerPage': page_size}
            # Syntage filtra issuedAt en UTC y el límite es a las 00:00 del día:
            # una factura del 31 a las 18:00 (México) es el 1 a las 00:00 UTC y
            # se perdía. Un día de holgura por cada lado; los CFDI de más se
            # upsertan sin efecto.
            if date_from:
                params['issuedAt[after]'] = fields.Date.to_string(
                    fields.Date.to_date(date_from) - timedelta(days=1))
            if date_to:
                params['issuedAt[before]'] = fields.Date.to_string(
                    fields.Date.to_date(date_to) + timedelta(days=2))
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
                log.write({'items_fetched': fetched, 'items_upserted': upserted, 'items_errored': errored,
                           'duration_seconds': round(time.time() - start, 1)})
                if commit:
                    self._commit()
                nxt = (body.get('hydra:view') or {}).get('hydra:next')
                url = (self._api_base() + nxt) if nxt else None
        except Exception as exc:
            log.write({'status': 'error', 'summary': str(exc)[:2000],
                       'duration_seconds': round(time.time() - start, 1)})
            if commit:
                self._commit()
            raise

        status = 'success' if not errored else ('partial' if upserted else 'error')
        summary = _('%(pages)s página(s), %(fetched)s CFDI recibidos, %(upserted)s guardados, '
                    '%(errored)s con error.') % {
            'pages': pages, 'fetched': fetched, 'upserted': upserted, 'errored': errored}
        if url:
            summary += _(' Se alcanzó el máximo de páginas (%s); vuelve a correr para continuar.') % max_pages
        if errors:
            summary += '\n' + '\n'.join(errors[:50])
        log.write({
            'status': status, 'summary': summary,
            'items_fetched': fetched, 'items_upserted': upserted, 'items_errored': errored,
            'duration_seconds': round(time.time() - start, 1),
        })
        if commit:
            self._commit()
        return log

    @api.model
    def pull_payments(self, company, date_from=None, date_to=None, max_pages=200,
                      page_size=100, commit=True, log=None):
        """Descarga los complementos de pago (InvoicePayment, uno por documento
        relacionado) de /invoices/payments para el periodo y los guarda en
        sat.cfdi.pago. Solo se guardan los que pagan una factura que ya está
        en sat.cfdi (el listado es de toda la organización). El endpoint es
        frágil con paginación profunda: pedir por meses."""
        start = time.time()
        Pago = self.env['sat.cfdi.pago']
        Cfdi = self.env['sat.cfdi'].sudo()
        Log = self.env['sat.sync.log'].sudo()
        if log is None:
            log = Log.create({
                'name': _('Pagos %(rfc)s %(from)s..%(to)s') % {
                    'rfc': self._rfc(company), 'from': date_from or '', 'to': date_to or ''},
                'kind': 'pull', 'mode': 'payments', 'company_id': company.id,
                'date_from': date_from, 'date_to': date_to,
            })
        log.write({'status': 'running', 'summary': False})
        if commit:
            self._commit()
        try:
            params = {'itemsPerPage': page_size}
            if date_from:
                params['date[after]'] = fields.Date.to_string(fields.Date.to_date(date_from) - timedelta(days=1))
            if date_to:
                params['date[before]'] = fields.Date.to_string(fields.Date.to_date(date_to) + timedelta(days=2))
            url = '%s/invoices/payments?%s' % (self._api_base(), urlencode(params))
            headers = {'X-Pagination-Style': 'cursor', 'X-Pagination-Enable-Partial': '0'}
            fetched = upserted = errored = skipped = pages = 0
            errors = []
            while url and pages < max_pages:
                body = self._request('GET', url, headers=headers)
                pages += 1
                items = body.get('hydra:member') or []
                fetched += len(items)
                uuids = {(o.get('invoiceUuid') or '').lower() for o in items if o.get('invoiceUuid')}
                known = set(Cfdi.search([('uuid', 'in', list(uuids))]).mapped(lambda c: c.uuid.lower())) if uuids else set()
                for obj in items:
                    if (obj.get('invoiceUuid') or '').lower() not in known:
                        skipped += 1
                        continue
                    try:
                        with self.env.cr.savepoint():
                            Pago._upsert_from_syntage(obj, company, event_type='pull')
                        upserted += 1
                    except Exception as exc:
                        errored += 1
                        errors.append('%s: %s' % (obj.get('id'), str(exc)[:200]))
                log.write({'items_fetched': fetched, 'items_upserted': upserted, 'items_errored': errored,
                           'duration_seconds': round(time.time() - start, 1)})
                if commit:
                    self._commit()
                nxt = (body.get('hydra:view') or {}).get('hydra:next')
                url = (self._api_base() + nxt) if nxt else None
        except Exception as exc:
            log.write({'status': 'error', 'summary': str(exc)[:2000],
                       'duration_seconds': round(time.time() - start, 1)})
            if commit:
                self._commit()
            raise
        status = 'success' if not errored else ('partial' if upserted else 'error')
        summary = _('%(pages)s página(s), %(fetched)s pagos recibidos, %(upserted)s guardados, '
                    '%(skipped)s de facturas que no están en Odoo, %(errored)s con error.') % {
            'pages': pages, 'fetched': fetched, 'upserted': upserted, 'skipped': skipped, 'errored': errored}
        if url:
            summary += _(' Se alcanzó el máximo de páginas (%s); vuelve a correr para continuar.') % max_pages
        if errors:
            summary += '\n' + '\n'.join(errors[:50])
        log.write({'status': status, 'summary': summary, 'items_fetched': fetched,
                   'items_upserted': upserted, 'items_errored': errored,
                   'duration_seconds': round(time.time() - start, 1)})
        if commit:
            self._commit()
        return log

    QUEUE_STALE_MINUTES = 30

    @api.model
    def _run_queued(self, budget_seconds=None):
        """Procesa las descargas/extracciones encoladas (status queued), una
        por una, con commit por página. Lo dispara action_pull_period(background)
        vía cron._trigger().

        El worker del cron muere a los ~15 min (Odoo.sh): se trabaja con un
        presupuesto de tiempo (quimibond_sat.queue_budget_seconds, default 600)
        y si queda cola se vuelve a disparar el cron. Una corrida que quedó en
        'running' sin avanzar en 30 min (worker muerto) se re-encola."""
        Log = self.env['sat.sync.log'].sudo()
        if budget_seconds is None:
            budget_seconds = int(self._param('quimibond_sat.queue_budget_seconds') or 600)
        start = time.time()
        stale_before = fields.Datetime.now() - timedelta(minutes=self.QUEUE_STALE_MINUTES)
        stale = Log.search([('status', '=', 'running'), ('write_date', '<', stale_before)])
        if stale:
            stale.write({'status': 'queued', 'summary': _('Re-encolado: la corrida anterior se interrumpió')})
        for index, log in enumerate(Log.search([('status', '=', 'queued')], order='id')):
            # Siempre se procesa al menos una; el presupuesto corta ANTES de la siguiente.
            if index and time.time() - start > budget_seconds:
                _logger.info('Cola SAT: presupuesto agotado, se vuelve a disparar el cron')
                self.env.ref('quimibond_sat.cron_sat_run_queued').sudo()._trigger()
                return
            try:
                if log.mode == 'extraction':
                    result = self.request_extraction(log.company_id, log.date_from, log.date_to,
                                                     log.include_retentions)
                    log.write({'status': result.status, 'summary': result.summary,
                               'duration_seconds': result.duration_seconds})
                    result.unlink()
                elif log.mode == 'payments':
                    self.pull_payments(log.company_id, log.date_from, log.date_to, log=log)
                else:
                    self.pull_invoices(log.company_id, log.date_from, log.date_to, log=log)
            except Exception as exc:
                _logger.exception('Cola SAT: falló %s', log.name)
                log.write({'status': 'error', 'summary': str(exc)[:2000]})
            self._commit()

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
            for label, fn in (('Descarga', self.pull_invoices), ('Pagos', self.pull_payments)):
                try:
                    fn(company, today - timedelta(days=lookback_days), today)
                except Exception as exc:
                    _logger.exception('%s reciente falló para %s', label, company.display_name)
                    self.env['sat.sync.log'].sudo().create({
                        'name': _('%(label)s %(rfc)s') % {'label': label, 'rfc': company.vat or company.display_name},
                        'kind': 'pull', 'company_id': company.id, 'status': 'error',
                        'summary': str(exc)[:2000],
                    })

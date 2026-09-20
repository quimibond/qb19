# -*- coding: utf-8 -*-
"""Archivos que Syntage tiene de cada CFDI (XML, PDF). Syntage no expone una
ruta "dame el XML de la factura X": el contenido se baja con
``GET /files/{fileId}/download`` y el id del archivo solo llega por el webhook
``file.created`` (``resource`` = ``/invoices/{id}``). Aquí se guarda ese mapa."""
import logging

import requests

from odoo import _, api, fields, models
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)


class SatSyntageFile(models.Model):
    _name = 'sat.syntage.file'
    _description = 'Archivo en Syntage (XML/PDF de un CFDI)'
    _order = 'id desc'
    _rec_name = 'filename'

    syntage_id = fields.Char(string='ID en Syntage', required=True, index=True)
    resource = fields.Char(string='Recurso', index=True, help='Ej. /invoices/{id}')
    file_type = fields.Char(string='Tipo', index=True, help='Ej. invoice.cfdi.xml, invoice.pdf')
    filename = fields.Char(string='Archivo')
    mime_type = fields.Char(string='MIME')
    size = fields.Integer(string='Bytes')
    company_id = fields.Many2one('res.company', string='Compañía', index=True)

    _syntage_id_uniq = models.Constraint('unique(syntage_id)', 'Ese archivo de Syntage ya está registrado.')

    @api.model
    def _upsert_from_syntage(self, obj, company):
        syntage_id = obj.get('id') or (obj.get('@id') or '').rsplit('/', 1)[-1]
        if not syntage_id:
            return self.browse()
        vals = {
            'syntage_id': syntage_id, 'resource': obj.get('resource') or False,
            'file_type': obj.get('type') or False, 'filename': obj.get('filename') or False,
            'mime_type': obj.get('mimeType') or False, 'size': int(obj.get('size') or 0),
            'company_id': company.id if company else False,
        }
        rec = self.sudo().search([('syntage_id', '=', syntage_id)], limit=1)
        if rec:
            rec.write(vals)
            return rec
        return self.sudo().create(vals)

    @api.model
    def _xml_for_invoice(self, invoice_syntage_id):
        """Archivo XML registrado para un CFDI de Syntage, o vacío."""
        if not invoice_syntage_id:
            return self.browse()
        return self.sudo().search([
            ('resource', '=', '/invoices/%s' % invoice_syntage_id),
            '|', ('file_type', 'ilike', 'xml'), ('mime_type', 'ilike', 'xml'),
        ], limit=1)

    def download_path(self):
        self.ensure_one()
        return '/files/%s/download' % self.syntage_id

    # ── carga histórica desde Supabase ───────────────────────────────
    #
    # Antes del webhook a Odoo, los eventos file.created llegaban a Supabase
    # (tabla syntage_files). Ese mapa se trae una vez por la API REST de
    # Supabase con la llave que ya tiene quimibond_intelligence.

    SUPABASE_PAGE = 1000

    @api.model
    def _supabase_conf(self):
        icp = self.env['ir.config_parameter'].sudo()
        url = (icp.get_param('quimibond_intelligence.supabase_url') or '').rstrip('/')
        key = icp.get_param('quimibond_intelligence.supabase_service_key') or ''
        if not url or not key:
            raise UserError(_('Faltan quimibond_intelligence.supabase_url / supabase_service_key.'))
        return url, key

    @api.model
    def _supabase_get(self, url, key, params):
        resp = requests.get(url + '/rest/v1/syntage_files', params=params, timeout=60,
                            headers={'apikey': key, 'Authorization': 'Bearer %s' % key})
        if resp.status_code >= 400:
            raise UserError(_('Supabase respondió %s: %s') % (resp.status_code, resp.text[:300]))
        return resp.json()

    @api.model
    def action_import_from_supabase(self, file_type='invoice.cfdi.xml', max_pages=100):
        """Trae a sat.syntage.file los archivos registrados en Supabase (por
        default solo los XML). Idempotente: los que ya existen se saltan.
        Devuelve {'fetched', 'created', 'pages'}."""
        url, key = self._supabase_conf()
        companies = {c.vat.upper(): c for c in self.env['res.company'].sudo().search([('vat', '!=', False)])}
        known = set(self.sudo().search([]).mapped('syntage_id'))
        fetched = created = pages = 0
        offset = 0
        while pages < max_pages:
            rows = self._supabase_get(url, key, {
                'select': 'syntage_id,file_type,filename,mime_type,size_bytes,taxpayer_rfc,resource:raw_payload->>resource',
                'file_type': 'eq.%s' % file_type, 'order': 'id.asc',
                'limit': self.SUPABASE_PAGE, 'offset': offset,
            })
            pages += 1
            if not rows:
                break
            vals_list = []
            for row in rows:
                fetched += 1
                sid = row.get('syntage_id')
                if not sid or sid in known:
                    continue
                known.add(sid)
                company = companies.get((row.get('taxpayer_rfc') or '').upper())
                vals_list.append({
                    'syntage_id': sid, 'resource': row.get('resource') or False,
                    'file_type': row.get('file_type') or False, 'filename': row.get('filename') or False,
                    'mime_type': row.get('mime_type') or False, 'size': int(row.get('size_bytes') or 0),
                    'company_id': company.id if company else False,
                })
            if vals_list:
                self.sudo().create(vals_list)
                created += len(vals_list)
            self.env['sat.syntage.client']._commit()
            if len(rows) < self.SUPABASE_PAGE:
                break
            offset += self.SUPABASE_PAGE
        _logger.info('sat.syntage.file: %s archivos leídos de Supabase, %s creados', fetched, created)
        return {'fetched': fetched, 'created': created, 'pages': pages}

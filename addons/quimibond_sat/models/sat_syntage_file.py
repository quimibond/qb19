# -*- coding: utf-8 -*-
"""Archivos que Syntage tiene de cada CFDI (XML, PDF). Syntage no expone una
ruta "dame el XML de la factura X": el contenido se baja con
``GET /files/{fileId}/download`` y el id del archivo solo llega por el webhook
``file.created`` (``resource`` = ``/invoices/{id}``). Aquí se guarda ese mapa."""
from odoo import api, fields, models


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

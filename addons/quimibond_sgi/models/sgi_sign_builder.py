# -*- coding: utf-8 -*-
"""Plantillas de Sign armadas por el SGI (56.18.0).

Antes, firmar un acuse o una responsiva de EPP en Sign pedía que MAST armara
a mano una plantilla en la app Firma (subir el PDF, colocar la firma) por cada
documento. Ahora el SGI la arma: toma el PDF (el documento, la responsiva),
le agrega al final una hoja de firma propia cuyas cajas están en posiciones
conocidas y crea la plantilla y la solicitud.

La hoja de firma (report/report_sign_sheet.xml) mide lo mismo que la del
cambio documental: margen superior 10 mm, título de 35 mm y renglones de
55 mm; ``_sgi_box(n)`` es la caja del renglón n.
"""
import base64
import io
import logging

from odoo import api, models

_logger = logging.getLogger(__name__)

_PAGE_MM = 279.4


class SgiSignBuilder(models.AbstractModel):
    _name = 'sgi.sign.builder'
    _description = "Plantillas de Sign armadas por el SGI"

    @api.model
    def _sgi_box(self, index):
        """Caja de firma del renglón ``index`` de la hoja de firma."""
        top = 45.0 + 55.0 * index + 12.0
        return {'posX': 0.50, 'posY': round(top / _PAGE_MM, 4),
                'width': 0.42, 'height': round(28.0 / _PAGE_MM, 4)}

    @api.model
    def _sgi_render_pdf(self, report_ref, records):
        pdf, _fmt = self.env['ir.actions.report'].sudo()._render_qweb_pdf(report_ref, res_ids=records.ids)
        return pdf

    @api.model
    def _sgi_append(self, parts, sheet):
        """(PDF, página de la hoja): las partes en orden y la hoja al final.
        Una parte que no se puede leer (PDF dañado) se omite con aviso."""
        from odoo.tools.pdf import PdfFileReader, PdfFileWriter
        writer = PdfFileWriter()
        for part in parts:
            if not part:
                continue
            try:
                for page in PdfFileReader(io.BytesIO(part), strict=False).pages:
                    writer.add_page(page)
            except Exception:  # noqa: BLE001 - se firma la hoja aunque el PDF venga mal
                _logger.warning("SGI Sign: un PDF no se pudo leer; se firma sin él.")
        sheet_pages = PdfFileReader(io.BytesIO(sheet), strict=False).pages
        for page in sheet_pages:
            writer.add_page(page)
        out = io.BytesIO()
        writer.write(out)
        return out.getvalue(), len(writer.pages)

    @api.model
    def _sgi_attachment_pdf(self, attachment_or_doc):
        """Bytes del PDF de un adjunto o documento, o None si no es PDF."""
        record = attachment_or_doc.sudo()
        if not record or record.mimetype != 'application/pdf' or not record.datas:
            return None
        return base64.b64decode(record.datas)

    @api.model
    def _sgi_signature_type(self):
        item_type = self.env.ref('sign.sign_item_type_signature', raise_if_not_found=False)
        return item_type or self.env['sign.item.type'].sudo().search([('item_type', '=', 'signature')], limit=1)

    @api.model
    def _sgi_template(self, name, pdf, page, boxes):
        """Plantilla con el PDF y una caja de firma por (renglón, papel)."""
        file = self.env['ir.attachment'].sudo().create({
            'name': "%s.pdf" % name, 'datas': base64.b64encode(pdf), 'mimetype': 'application/pdf'})
        item_type = self._sgi_signature_type()
        items = [(0, 0, dict(self._sgi_box(index), **{
            'type_id': item_type.id, 'responsible_id': role.id, 'page': page,
            'required': True, 'alignment': 'center'})) for index, role in boxes]
        return self.env['sign.template'].sudo().create({
            'name': name,
            'document_ids': [(0, 0, {'attachment_id': file.id, 'sign_item_ids': items})],
        })

    @api.model
    def _sgi_request(self, template, reference, subject, record, signers):
        """Solicitud de firma; ``signers`` = [(contacto, papel)] en orden."""
        return self.env['sign.request'].sudo().create({
            'template_id': template.id,
            'reference': reference,
            'subject': subject,
            'reference_doc': '%s,%d' % (record._name, record.id),
            'request_item_ids': [(0, 0, {'partner_id': partner.id, 'role_id': role.id, 'mail_sent_order': seq})
                                 for seq, (partner, role) in enumerate(signers, start=1)],
        })

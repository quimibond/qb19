# -*- coding: utf-8 -*-
"""«Mi procedimiento» firmado en Sign antes de entrar en vigor (56.19.0).

Con «Mi procedimiento se firma en Sign» (Ajustes → SGI), publicar ya no deja
la revisión vigente de inmediato: la deja en borrador y manda a firmar el PDF
con una hoja de firmas al final. Firman **Elaboró** (Jefe MAST) y **Aprobó**
(jefe directo del puesto: el responsable de su departamento). Con la última
firma la revisión entra en vigor sola: la anterior queda obsoleta, el archivo
del documento pasa a ser el PDF firmado y se generan los acuses de lectura.
"""
import logging

from markupsafe import Markup

from odoo import SUPERUSER_ID, api, fields, models
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)


class DocumentsDocumentMpSign(models.Model):
    _inherit = 'documents.document'

    sgi_publish_sign_request_id = fields.Many2one(
        'sign.request', string="Firma para entrar en vigor", readonly=True, copy=False,
        help="Solicitud de Sign de la que depende que esta revisión entre en vigor.")

    @api.model
    def _sgi_mp_sign_required(self):
        return self.env['ir.config_parameter'].sudo().get_param(
            'quimibond_sgi.mp_sign_required') in ('1', 'True', 'true')

    @api.model
    def _sgi_mp_pending_sign_doc(self, code, content_hash):
        """Revisión con el mismo contenido que ya espera firma (no se duplica)."""
        return self.sudo().search([
            ('sgi_code', '=', code), ('sgi_state', '=', 'borrador'),
            ('sgi_content_hash', '=', content_hash),
            ('sgi_publish_sign_request_id.state', 'in', ('sent', 'shared')),
        ], limit=1)

    def _sgi_mp_signers(self, job):
        """[(etiqueta, usuario, papel)]: MAST elabora, el jefe directo aprueba.
        Si el jefe es el mismo MAST o no tiene usuario, firma solo MAST."""
        self.ensure_one()
        mast_id = self.env['sgi.cron']._sgi_manager_user_id()
        mast = self.env['res.users'].browse(mast_id) if mast_id else self.env['res.users']
        # Quien publica firma si es de MAST (no OdooBot cuando publica el cron).
        if self.env.uid != SUPERUSER_ID and self.env.user.has_group('quimibond_sgi.group_sgi_manager'):
            mast = self.env.user
        boss = job.sudo().department_id.manager_id.user_id
        if not mast:
            raise UserError("No hay Jefe MAST (Ajustes → SGI) que firme «Elaboró».")
        signers = [("Elaboró", mast, self.env.ref('quimibond_sgi.sgi_sign_role_elaboro'))]
        if boss and boss != mast:
            signers.append(("Aprobó", boss, self.env.ref('quimibond_sgi.sgi_sign_role_aprobo')))
        missing = [user.name for _label, user, _role in signers if not user.partner_id.email]
        if missing:
            raise UserError("sin correo: %s" % ", ".join(missing))
        return signers

    def _sgi_publish_sign_rows(self):
        """Renglones de la hoja de firmas (reporte)."""
        self.ensure_one()
        job = self.sudo().sgi_job_ids[:1]
        try:
            return [(label, user.name) for label, user, _role in self._sgi_mp_signers(job)]
        except UserError:
            return [("Elaboró", ''), ("Aprobó", '')]

    def _sgi_mp_send_to_sign(self, job):
        """Arma el PDF (Mi procedimiento + hoja de firmas) y la solicitud."""
        self.ensure_one()
        signers = self._sgi_mp_signers(job)
        builder = self.env['sgi.sign.builder']
        sheet = builder._sgi_render_pdf('quimibond_sgi.action_report_publish_sign_sheet', self)
        pdf, page = builder._sgi_append([builder._sgi_attachment_pdf(self)], sheet)
        template = builder._sgi_template(
            "%s rev. %02d" % (self.sgi_code or self.name, self.sgi_revision or 0), pdf, page,
            [(index, role) for index, (_label, _user, role) in enumerate(signers)])
        request = builder._sgi_request(
            template, "%s rev. %02d — %s" % (self.sgi_code or '', self.sgi_revision or 0, job.name),
            "Firma de «Mi procedimiento» %s (%s)" % (self.sgi_code or '', job.name), self,
            [(user.partner_id, role) for _label, user, role in signers])
        self.sudo().sgi_publish_sign_request_id = request
        self.message_post(body=Markup("Revisión en borrador: entra en vigor cuando firmen %s.") % ", ".join(
            "%s (%s)" % (user.name, label) for label, user, _role in signers))
        return request

    def _sgi_publish_after_sign(self):
        """Firma completa → la revisión entra en vigor con el PDF firmado."""
        for doc in self.sudo():
            if doc.sgi_state != 'borrador' or doc.sgi_publish_sign_request_id.state != 'signed':
                continue
            signed = doc.sgi_publish_sign_request_id.completed_document_attachment_ids[:1]
            doc._obsolete_code(doc.sgi_code, exclude=doc)
            vals = {'sgi_state': 'vigente', 'sgi_issue_date': fields.Date.context_today(doc)}
            if signed and signed.datas:
                vals['datas'] = signed.datas
            doc.write(vals)
            doc._sgi_reparent_family()
            doc.action_generate_acks()
            doc.message_post(body="Firmado en Sign: la revisión %02d entra en vigor. Acuse pendiente para %d persona(s)." % (
                doc.sgi_revision or 0, len(doc.sgi_ack_ids)))
            if doc.sgi_job_ids:
                self.env['hr.employee']._sgi_mp_touch_jobs(doc.sgi_job_ids)
        return True

    @api.model
    def _sgi_sync_publish_sign(self, sign_requests=None):
        """Lo llaman Sign (al cambiar de estado) y el cron diario."""
        domain = [('sgi_state', '=', 'borrador'), ('sgi_publish_sign_request_id.state', '=', 'signed')]
        if sign_requests is not None:
            domain.append(('sgi_publish_sign_request_id', 'in', sign_requests.ids))
        docs = self.sudo().search(domain)
        docs._sgi_publish_after_sign()
        return len(docs)


class SignRequestMpSgi(models.Model):
    _inherit = 'sign.request'

    def write(self, vals):
        res = super().write(vals)
        if vals.get('state') == 'signed':
            try:
                with self.env.cr.savepoint():
                    self.env['documents.document']._sgi_sync_publish_sign(self)
            except Exception:  # noqa: BLE001 - la firma no se revierte por el SGI
                _logger.exception("SGI: no se pudo poner en vigor lo firmado; lo reintenta el cron.")
        return res


class ResConfigSettingsMpSign(models.TransientModel):
    _inherit = 'res.config.settings'

    sgi_mp_sign_required = fields.Boolean(
        string="«Mi procedimiento» se firma en Sign",
        config_parameter='quimibond_sgi.mp_sign_required',
        help="Publicar deja la revisión en borrador y la manda a firmar (MAST elabora, "
             "el jefe directo aprueba); entra en vigor sola al firmarse.")

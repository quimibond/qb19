# -*- coding: utf-8 -*-
"""Cambio documental firmado en Sign (56.17.0).

El cambio documental (F-P-G01-06) sigue siendo una solicitud de Aprobaciones,
pero se aprueba **firmando** en la app Firma, sin plantillas hechas a mano:

1. Al enviar la solicitud, el SGI arma el PDF: el F-P-G01-06 (documento,
   tipo de cambio, revisión, motivo, cambios, piloto), la versión nueva del
   documento si viene adjunta y al final una hoja de firmas.
2. Crea la plantilla de Sign con esa hoja y la solicitud de firma en orden:
   **Elaboró** (quien pide) → **Revisó** (dueño del proceso) → **Aprobó**
   (Jefe MAST, aprobador de la categoría).
3. Cada firma aprueba su renglón en Aprobaciones; con la última el cambio se
   aplica solo (revisión, obsoleta la anterior, acuses) y el PDF firmado queda
   en la solicitud y en el documento como evidencia.

En las categorías que lo piden («Se aprueba firmando en Sign») nadie aprueba
con el botón de Aprobaciones: la aprobación es la firma. Rechazar sigue
siendo desde Aprobaciones.
"""
import base64
import io
import logging

from markupsafe import Markup

from odoo import api, fields, models
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)

# Hoja de firmas (última página del PDF de la solicitud): mismas medidas que
# report/report_doc_change.xml; las cajas las pone sgi.sign.builder._sgi_box.
_SIGN_ROWS = (
    ('elaboro', "Elaboró", 'quimibond_sgi.sgi_sign_role_elaboro'),
    ('reviso', "Revisó", 'quimibond_sgi.sgi_sign_role_reviso'),
    ('aprobo', "Aprobó", 'quimibond_sgi.sgi_sign_role_aprobo'),
)


class ApprovalCategorySign(models.Model):
    _inherit = 'approval.category'

    sgi_sign_required = fields.Boolean(
        string="Se aprueba firmando en Sign",
        help="Al enviar la solicitud se crea la firma en Sign (elaboró → "
             "revisó → aprobó) y cada firma aprueba su renglón. El botón "
             "Aprobar de Aprobaciones queda bloqueado.")


class ApprovalRequestSign(models.Model):
    _inherit = 'approval.request'

    sgi_sign_required = fields.Boolean(related='category_id.sgi_sign_required',
                                       help="Indica si la categoría exige firma en Sign para aprobar.")
    sgi_sign_request_id = fields.Many2one(
        'sign.request', string="Firma (Sign)", readonly=True, copy=False,
        help="Solicitud de firma en Sign ligada a esta aprobación.")
    sgi_sign_state = fields.Selection(
        related='sgi_sign_request_id.state', string="Estado de la firma",
        help="Estado de la firma en Sign.")
    sgi_sign_progress = fields.Char(
        string="Firmas", compute='_compute_sgi_sign_progress')
    sgi_change_attachment_id = fields.Many2one(
        'ir.attachment', string="Archivo de la revisión nueva", readonly=True, copy=False,
        help="El archivo que se mandó a firmar: es el que se publica al aprobarse.")
    sgi_sign_archived = fields.Boolean(copy=False, readonly=True)
    sgi_sign_notified_state = fields.Char(copy=False, readonly=True)

    @api.depends('sgi_sign_request_id.request_item_ids.state')
    def _compute_sgi_sign_progress(self):
        for req in self:
            items = req.sgi_sign_request_id.request_item_ids
            req.sgi_sign_progress = "%d de %d" % (
                len(items.filtered(lambda i: i.state == 'completed')), len(items)) if items else False

    # ------------------------------------------------------------------
    # Quién firma
    # ------------------------------------------------------------------
    def _sgi_sign_process(self):
        self.ensure_one()
        return self.sgi_document_id.sgi_process_id or self.sgi_affected_process_ids[:1]

    def _sgi_sign_mast_users(self):
        self.ensure_one()
        users = self.category_id.approver_ids.filtered('required').user_id \
            or self.category_id.approver_ids.user_id
        if not users:
            user_id = self.env['sgi.cron']._sgi_manager_user_id()
            users = self.env['res.users'].browse(user_id) if user_id else users
        return users[:1]

    def _sgi_sign_signers(self):
        """[(clave, etiqueta, usuario)] en orden de firma; UserError con lo que
        falte."""
        self.ensure_one()
        # sudo: quien pide no siempre lee la ficha privada del dueño.
        req = self.sudo()
        owner = req.request_owner_id
        process = req._sgi_sign_process()
        reviewer = process.owner_id.user_id
        mast = req._sgi_sign_mast_users()
        problems = []
        if not owner:
            problems.append("• Falta quién pide el cambio.")
        if not process:
            problems.append("• Liga el documento a su proceso o indica los procesos afectados: "
                            "el dueño del proceso firma «Revisó».")
        elif not reviewer:
            problems.append("• El proceso %s no tiene dueño con usuario activo." % (
                process.code or process.name))
        if not mast:
            problems.append("• La categoría no tiene aprobador (Jefe MAST).")
        signers = [('elaboro', "Elaboró", owner), ('reviso', "Revisó", reviewer), ('aprobo', "Aprobó", mast)]
        for _key, label, user in signers:
            if user and not user.partner_id.email:
                problems.append("• %s (%s) no tiene correo: Sign no le puede mandar la firma." % (
                    user.name, label))
        if problems:
            raise UserError("No se puede mandar a firmar «%s»:\n%s" % (self.name or '', "\n".join(problems)))
        return signers

    def _sgi_sign_rows(self):
        """Renglones de la hoja de firmas (sin validar): [(etiqueta, usuario)]."""
        self.ensure_one()
        req = self.sudo()
        return [("Elaboró", req.request_owner_id),
                ("Revisó", req._sgi_sign_process().owner_id.user_id),
                ("Aprobó", req._sgi_sign_mast_users())]

    def _sgi_check_doc_change_ready(self):
        for req in self:
            problems = []
            if not (req.sgi_reason or '').strip():
                problems.append("• Falta el motivo del cambio.")
            if not (req.sgi_changes or '').strip():
                problems.append("• Falta la descripción de los cambios.")
            if problems:
                raise UserError("No se puede enviar el cambio «%s»:\n%s" % (
                    req.name or '', "\n".join(problems)))

    def _sgi_prepare_approvers(self, signers):
        """Los que firman «Revisó» y «Aprobó» son los aprobadores requeridos,
        en ese orden (quien pide no se aprueba a sí mismo)."""
        self.ensure_one()
        Approver = self.env['approval.approver'].sudo()
        for sequence, (key, _label, user) in zip((10, 20), signers[1:]):
            line = self.approver_ids.filtered(lambda a, u=user: a.user_id == u)
            if line:
                line.sudo().write({'required': True, 'sequence': sequence})
            elif user != self.request_owner_id or key == 'aprobo':
                Approver.create({'request_id': self.id, 'user_id': user.id,
                                 'required': True, 'sequence': sequence})

    # ------------------------------------------------------------------
    # PDF y solicitud de firma
    # ------------------------------------------------------------------
    def _sgi_render_doc_change_pdf(self):
        """PDF del F-P-G01-06 con su hoja de firmas al final."""
        self.ensure_one()
        return self.env['sgi.sign.builder']._sgi_render_pdf('quimibond_sgi.action_report_doc_change', self)

    def _sgi_sign_pdf(self):
        """(PDF a firmar, página de la hoja de firmas): la solicitud y, si la
        trae, la versión nueva del documento antes de la hoja de firmas."""
        from odoo.tools.pdf import PdfFileReader, PdfFileWriter
        self.ensure_one()
        request_pdf = self._sgi_render_doc_change_pdf()
        attachment = self.sgi_change_attachment_id
        new_version = base64.b64decode(attachment.datas) \
            if attachment and attachment.mimetype == 'application/pdf' and attachment.datas else None
        reader = PdfFileReader(io.BytesIO(request_pdf), strict=False)
        pages = list(reader.pages)
        writer = PdfFileWriter()
        for page in pages[:-1]:
            writer.add_page(page)
        if new_version:
            try:
                for page in PdfFileReader(io.BytesIO(new_version), strict=False).pages:
                    writer.add_page(page)
            except Exception:  # noqa: BLE001 - PDF dañado: se firma sin él
                _logger.warning("SGI: no se pudo unir el PDF de %s a la firma.", attachment.name)
        writer.add_page(pages[-1])
        out = io.BytesIO()
        writer.write(out)
        return out.getvalue(), len(writer.pages)

    def _sgi_send_to_sign(self):
        """Crea la plantilla (PDF + hoja de firmas) y la solicitud en Sign."""
        for req in self:
            signers = req._sgi_sign_signers()
            if not req.sgi_change_attachment_id:
                attachment = super(ApprovalRequestSign, req)._sgi_change_attachment()
                if attachment:
                    req.sgi_change_attachment_id = attachment
            pdf, sign_page = req._sgi_sign_pdf()
            builder = self.env['sgi.sign.builder']
            # Una persona que ocupa dos papeles firma una sola vez (su primer
            # papel), pero en todas sus cajas.
            role_xmlids = {key: xmlid for key, _label, xmlid in _SIGN_ROWS}
            role_of, order = {}, []
            for key, _label, user in signers:
                if user.partner_id not in role_of:
                    role_of[user.partner_id] = self.env.ref(role_xmlids[key])
                    order.append(user.partner_id)
            template = builder._sgi_template(
                "Cambio documental %s" % (req.name or ''), pdf, sign_page,
                [(index, role_of[user.partner_id]) for index, (_k, _l, user) in enumerate(signers)])
            request = builder._sgi_request(
                template, "F-P-G01-06 %s" % (req.name or ''),
                "Firma del cambio documental %s" % (req.name or ''), req,
                [(partner, role_of[partner]) for partner in order])
            req.sudo().write({'sgi_sign_request_id': request.id, 'sgi_sign_archived': False})
            req.message_post(body=Markup("Enviado a firma en Sign: %s.") % ", ".join(
                "%s (%s)" % (user.name, label) for _k, label, user in signers))
        return True

    def action_confirm(self):
        signed = self.filtered(lambda r: r.sgi_is_doc_change and r.sgi_sign_required)
        signed._sgi_check_doc_change_ready()
        for req in signed:
            req._sgi_prepare_approvers(req._sgi_sign_signers())
        if signed:
            # 57.13.1: el renglón nuevo del revisor entra al final de la caché
            # de approver_ids (después del Jefe MAST que trae la categoría).
            # Con aprobadores en orden, Aprobaciones deja «pendiente» al
            # primero que lee y «en espera» a los demás: el revisor quedaba en
            # espera, su firma no podía aprobarlo y la solicitud se atoraba.
            # Se vuelve a leer de la base, en el orden de la secuencia.
            signed.approver_ids.flush_recordset(['sequence', 'request_id'])
            signed.invalidate_recordset(['approver_ids'])
        res = super().action_confirm()
        signed._sgi_send_to_sign()
        return res

    def action_sgi_send_to_sign(self):
        """«Reenviar a firma»: la firma anterior se canceló o venció."""
        for req in self:
            if req.request_status != 'pending':
                raise UserError("Solo se manda a firmar una solicitud enviada y sin resolver.")
            if req.sgi_sign_request_id and req.sgi_sign_request_id.state in ('sent', 'shared', 'signed'):
                raise UserError("La solicitud %s ya tiene una firma en curso." % (req.name or ''))
        return self._sgi_send_to_sign()

    def action_approve(self, approver=None):
        if not self.env.context.get('sgi_sign_sync'):
            blocked = self.filtered(lambda r: r.sgi_is_doc_change and r.sgi_sign_required)
            if blocked:
                raise UserError(
                    "El cambio documental %s se aprueba firmando en Sign (elaboró → revisó → "
                    "aprobó), no con el botón Aprobar. Revise su correo o la app Firma."
                    % ", ".join(blocked.mapped('name')))
        return super().action_approve(approver=approver)

    def _sgi_change_attachment(self):
        """El archivo que se mandó a firmar manda: el PDF firmado que Sign
        deja en la solicitud no es la revisión nueva."""
        self.ensure_one()
        return self.sgi_change_attachment_id or super()._sgi_change_attachment()

    # ------------------------------------------------------------------
    # Sincronización con Sign
    # ------------------------------------------------------------------
    def _sgi_sync_sign(self):
        """Cada firma completa aprueba su renglón en Aprobaciones, en orden; la
        última aplica el cambio. Idempotente (lo llaman Sign y el cron)."""
        for req in self.sudo().filtered('sgi_sign_request_id'):
            sign = req.sgi_sign_request_id
            done = sign.request_item_ids.filtered(lambda i: i.state == 'completed').partner_id
            if req.request_status == 'pending':
                for approver in req.approver_ids.sorted(lambda a: (a.sequence, a.id)):
                    if approver.status in ('approved', 'refused', 'cancel'):
                        continue
                    if approver.user_id.partner_id not in done:
                        break
                    if approver.status == 'waiting':
                        # Sign ya impuso el orden (y los anteriores quedaron
                        # aprobados arriba): quien firmó ya puede aprobar. Cubre
                        # las solicitudes enviadas antes de 57.13.1.
                        approver.status = 'pending'
                    req.with_user(approver.user_id).sudo().with_context(
                        sgi_sign_sync=True).action_approve(approver=approver)
            if sign.state == 'signed' and not req.sgi_sign_archived:
                req._sgi_archive_signed_pdf()
            elif sign.state in ('canceled', 'expired') and req.request_status == 'pending' \
                    and req.sgi_sign_notified_state != sign.state:
                req.sgi_sign_notified_state = sign.state
                req.message_post(body="La firma en Sign quedó %s. Use «Reenviar a firma» o rechace la solicitud." % (
                    dict(sign._fields['state'].selection).get(sign.state, sign.state)))
        return True

    def _sgi_archive_signed_pdf(self):
        """El PDF firmado queda en la solicitud y en el documento."""
        self.ensure_one()
        signed = self.sgi_sign_request_id.completed_document_attachment_ids
        targets = [self] + [doc for doc in (self.sgi_new_document_id or self.sgi_document_id) if doc]
        for target in targets:
            copies = self.env['ir.attachment'].sudo()
            for att in signed:
                copies |= att.sudo().copy({'res_model': target._name, 'res_id': target.id})
            if copies:
                target.message_post(
                    body="Cambio documental %s firmado en Sign (elaboró, revisó y aprobó)." % (self.name or ''),
                    attachment_ids=copies.ids)
        self.sgi_sign_archived = True

    @api.model
    def _sgi_sync_all_sign(self):
        """Cron diario: por si Sign no avisó (firmas hechas fuera de línea)."""
        requests = self.sudo().search([('sgi_sign_request_id', '!=', False),
                                       '|', ('request_status', '=', 'pending'),
                                       ('sgi_sign_archived', '=', False)])
        requests._sgi_sync_sign()
        return len(requests)


def _sgi_sync_after_sign(env, sign_requests):
    """Aprobar al firmar sin poder tumbar la firma: si algo falla aquí, la
    firma queda hecha y el cron diario lo vuelve a intentar."""
    requests = env['approval.request'].sudo().search([('sgi_sign_request_id', 'in', sign_requests.ids)])
    if not requests:
        return
    try:
        with env.cr.savepoint():
            requests._sgi_sync_sign()
    except Exception:  # noqa: BLE001 - la firma no se revierte por el SGI
        _logger.exception("SGI: no se pudo aplicar la firma de %s; lo reintenta el cron.",
                          ", ".join(requests.mapped('name')))


class SignRequestItemSgi(models.Model):
    _inherit = 'sign.request.item'

    def write(self, vals):
        res = super().write(vals)
        if 'state' in vals:
            _sgi_sync_after_sign(self.env, self.sign_request_id)
        return res


class SignRequestSgi(models.Model):
    _inherit = 'sign.request'

    def write(self, vals):
        res = super().write(vals)
        if 'state' in vals:
            _sgi_sync_after_sign(self.env, self)
        return res

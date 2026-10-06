# -*- coding: utf-8 -*-
"""Integraciones con apps nativas ya instaladas en la instancia:

- **Sign** (Firma electrónica): el acuse de lectura puede respaldarse con una
  solicitud de firma real. MAST crea UNA plantilla de firma a partir del PDF
  del documento (colocando el campo de firma) y la liga al documento; el botón
  «Enviar acuses a firma» genera una solicitud por empleado pendiente y el
  cron diario sella el acuse cuando la solicitud queda firmada.
- **eLearning**: un curso puede otorgar una competencia (hr.skill) a un nivel
  dado. Al terminar el curso, el empleado recibe la competencia con la
  vigencia del curso (57.100.0, N-13: la línea de currículum nativa la otorga
  al momento; el cron diario es el respaldo para quien terminó sin línea),
  cerrando la brecha en la DNC sin captura manual.
"""
import logging

from odoo import models, fields, api
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)


class DocumentsDocumentSign(models.Model):
    _inherit = 'documents.document'

    sgi_sign_template_id = fields.Many2one(
        'sign.template', string="Plantilla de firma (Sign)", copy=False,
        help="Vacío: el SGI la arma sola (el PDF del documento más una hoja "
             "«Leí y entendí» con la firma colocada). Solo si se quiere otra, "
             "se elige aquí una plantilla hecha a mano en la app Firma.")
    # 56.18.0: plantilla armada por el SGI y la revisión para la que se armó
    # (si el documento cambia de revisión, se arma otra).
    sgi_sign_template_auto = fields.Boolean(copy=False, readonly=True)
    sgi_sign_template_rev = fields.Integer(copy=False, readonly=True)

    def write(self, vals):
        if 'sgi_sign_template_id' in vals and not self.env.context.get('sgi_sign_auto'):
            vals = dict(vals, sgi_sign_template_auto=False)
        return super().write(vals)

    def _sgi_ack_sign_template(self):
        """Plantilla para los acuses: la elegida a mano o, si no hay, la que
        arma el SGI (PDF del documento + hoja «Leí y entendí»)."""
        self.ensure_one()
        doc = self.sudo()
        if doc.sgi_sign_template_id and (not doc.sgi_sign_template_auto
                                         or doc.sgi_sign_template_rev == doc.sgi_revision):
            return doc.sgi_sign_template_id
        builder = self.env['sgi.sign.builder']
        sheet = builder._sgi_render_pdf('quimibond_sgi.action_report_ack_sign_sheet', doc)
        pdf, page = builder._sgi_append([builder._sgi_attachment_pdf(doc)], sheet)
        role = self.env.ref('quimibond_sgi.sgi_sign_role_empleado')
        template = builder._sgi_template(
            "Acuse %s rev. %02d" % (doc.sgi_code or doc.name, doc.sgi_revision or 0), pdf, page, [(0, role)])
        doc.with_context(sgi_sign_auto=True).write({
            'sgi_sign_template_id': template.id, 'sgi_sign_template_auto': True,
            'sgi_sign_template_rev': doc.sgi_revision})
        return template

    def action_sgi_send_sign_requests(self):
        """Crea una solicitud de firma por cada acuse pendiente sin solicitud
        viva. Idempotente: re-ejecutar solo cubre a los que faltan. La
        creación corre con sudo (el candado real es el grupo del botón)."""
        self.ensure_one()
        if not self.env.user.has_group('quimibond_sgi.group_sgi_manager'):
            raise UserError("Solo el Jefe MAST envía acuses a firma.")
        pending = self.sgi_ack_ids.filtered(
            lambda a: a.state == 'pendiente' and (
                not a.sign_request_id
                or a.sign_request_id.state in ('canceled', 'expired')))
        if not pending:
            return {
                'type': 'ir.actions.client', 'tag': 'display_notification',
                'params': {'message': "No hay acuses pendientes sin firma en curso.", 'type': 'info'},
            }
        template = self._sgi_ack_sign_template().sudo()
        roles = template.sign_item_ids.mapped('responsible_id')
        if len(roles) != 1:
            raise UserError(
                "La plantilla de firma debe tener campos de UN solo firmante "
                "(el empleado que acusa). Esta tiene %d roles." % len(roles))
        sent, skipped = 0, []
        for ack in pending:
            partner = (ack.employee_id.user_id.partner_id
                       or ack.employee_id.work_contact_id)
            if not partner or not partner.email:
                skipped.append(ack.employee_id.name)
                continue
            try:
                # 57.16.0 (G-002): savepoint por solicitud; un error SQL al
                # crear una ya no aborta la transacción para las demás.
                with self.env.cr.savepoint():
                    request = self.env['sign.request'].sudo().create({
                        'template_id': template.id,
                        'reference': "Acuse %s — %s" % (
                            self.sgi_code or self.name, ack.employee_id.name),
                        'subject': "Firma de acuse de lectura: %s" % (
                            self.sgi_code or self.name),
                        'request_item_ids': [(0, 0, {
                            'partner_id': partner.id,
                            'role_id': roles.id,
                        })],
                    })
                    ack.sudo().sign_request_id = request
                    sent += 1
            except Exception:
                _logger.exception(
                    "SGI Sign: falló la solicitud de firma del acuse de %s "
                    "en %s; continúo.", ack.employee_id.name, self.sgi_code)
                skipped.append(ack.employee_id.name)
        message = "%d solicitud(es) de firma enviada(s)." % sent
        if skipped:
            message += " Sin enviar (sin contacto/correo o con error): %s." % (
                ", ".join(skipped))
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {'message': message, 'type': 'success' if sent else 'warning'},
        }


class SgiDocumentAckSign(models.Model):
    _inherit = 'sgi.document.ack'

    sign_request_id = fields.Many2one(
        'sign.request', string="Solicitud de firma", readonly=True,
        copy=False, ondelete='set null')
    # store=True: el estado se lee del acuse sin exigir permisos de Sign al
    # empleado; el recompute corre como superusuario cuando Sign avanza.
    sign_state = fields.Selection(
        related='sign_request_id.state', string="Firma electrónica",
        store=True)

    @api.model
    def _sgi_sync_from_sign(self):
        """Acuse pendiente cuya solicitud ya está firmada → se sella como
        leído. Corre con sudo: la firma electrónica ES la evidencia de
        identidad, mejor que el click (pasa el candado A2 por env.su)."""
        signed = self.sudo().search([
            ('state', '=', 'pendiente'),
            ('sign_state', '=', 'signed'),
        ])
        for ack in signed:
            completion = ack.sign_request_id.completion_date
            ack.write({
                'state': 'leido',
                'ack_date': fields.Datetime.to_datetime(completion)
                if completion else fields.Datetime.now(),
            })
        return len(signed)


class SlideChannelSgi(models.Model):
    _inherit = 'slide.channel'

    sgi_skill_id = fields.Many2one(
        'hr.skill', string="Competencia SGI que otorga",
        help="Al terminar el curso, el empleado recibe esta competencia al "
             "nivel indicado (cierra la brecha en la DNC).")
    sgi_skill_type_id = fields.Many2one(
        related='sgi_skill_id.skill_type_id', string="Tipo de competencia",
        help="Tipo de la competencia que otorga el curso.")
    sgi_skill_level_id = fields.Many2one(
        'hr.skill.level', string="Nivel que otorga",
        domain="[('skill_type_id', '=', sgi_skill_type_id)]",
        help="Nivel de competencia que obtiene quien termina el curso.")

    def _sgi_employee_for_partner(self, partner):
        Employee = self.env['hr.employee'].sudo()
        user = self.env['res.users'].sudo().search(
            [('partner_id', '=', partner.id)], limit=1)
        employee = user and Employee.search(
            [('user_id', '=', user.id)], limit=1)
        return employee or Employee.search(
            [('work_contact_id', '=', partner.id)], limit=1)

    @api.model
    def _sgi_sync_completions(self):
        """Asistentes con curso terminado → competencia del empleado creada o
        subida de nivel (nunca bajada), con la vigencia del curso. Idempotente.

        57.100.0 (N-13): usa la misma regla que la línea de currículum
        (``hr.employee._sgi_grant_skill``) sin renovar: si el empleado ya tuvo
        la competencia a ese nivel, el cron no la vuelve a dar (con vigencia,
        «hoy + N meses» la renovaría cada noche)."""
        today = fields.Date.context_today(self)
        channels = self.sudo().search([
            ('sgi_skill_id', '!=', False),
            ('sgi_skill_level_id', '!=', False),
        ])
        granted = 0
        for channel in channels:
            done = self.env['slide.channel.partner'].sudo().search([
                ('channel_id', '=', channel.id),
                ('member_status', '=', 'completed'),
            ])
            for member in done:
                employee = channel._sgi_employee_for_partner(member.partner_id)
                if not employee:
                    continue
                if employee._sgi_grant_skill(
                        channel.sgi_skill_id, channel.sgi_skill_level_id, today,
                        channel._sgi_skill_date_to(today), 'curso', channel=channel,
                        renew=False):
                    granted += 1
        return granted

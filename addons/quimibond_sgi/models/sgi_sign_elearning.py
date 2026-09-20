# -*- coding: utf-8 -*-
"""Integraciones con apps nativas ya instaladas en la instancia:

- **Sign** (Firma electrónica): el acuse de lectura puede respaldarse con una
  solicitud de firma real. MAST crea UNA plantilla de firma a partir del PDF
  del documento (colocando el campo de firma) y la liga al documento; el botón
  «Enviar acuses a firma» genera una solicitud por empleado pendiente y el
  cron diario sella el acuse cuando la solicitud queda firmada.
- **eLearning**: un curso puede otorgar una competencia (hr.skill) a un nivel
  dado. Al terminar el curso, el cron diario registra/sube la competencia del
  empleado, cerrando la brecha en la DNC sin captura manual.
"""
import logging

from odoo import models, fields, api
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)


class DocumentsDocumentSign(models.Model):
    _inherit = 'documents.document'

    sgi_sign_template_id = fields.Many2one(
        'sign.template', string="Plantilla de firma (Sign)", copy=False,
        help="Plantilla de la app Firma hecha con el PDF de este documento y "
             "su campo de firma colocado. Habilita «Enviar acuses a firma».")

    def action_sgi_send_sign_requests(self):
        """Crea una solicitud de firma por cada acuse pendiente sin solicitud
        viva. Idempotente: re-ejecutar solo cubre a los que faltan. La
        creación corre con sudo (el candado real es el grupo del botón)."""
        self.ensure_one()
        if not self.env.user.has_group('quimibond_sgi.group_sgi_manager'):
            raise UserError("Solo el Jefe de MAST envía acuses a firma.")
        template = self.sgi_sign_template_id.sudo()
        if not template:
            raise UserError(
                "Primero liga la plantilla de firma: crea en la app Firma una "
                "plantilla con el PDF de este documento y su campo de firma, "
                "y selecciónala aquí.")
        roles = template.sign_item_ids.mapped('responsible_id')
        if len(roles) != 1:
            raise UserError(
                "La plantilla de firma debe tener campos de UN solo firmante "
                "(el empleado que acusa). Esta tiene %d roles." % len(roles))
        pending = self.sgi_ack_ids.filtered(
            lambda a: a.state == 'pendiente' and (
                not a.sign_request_id
                or a.sign_request_id.state in ('canceled', 'expired')))
        sent, skipped = 0, []
        for ack in pending:
            partner = (ack.employee_id.user_id.partner_id
                       or ack.employee_id.work_contact_id)
            if not partner or not partner.email:
                skipped.append(ack.employee_id.name)
                continue
            try:
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
        related='sgi_skill_id.skill_type_id', string="Tipo de competencia")
    sgi_skill_level_id = fields.Many2one(
        'hr.skill.level', string="Nivel que otorga",
        domain="[('skill_type_id', '=', sgi_skill_type_id)]")

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
        subida de nivel (nunca bajada). Idempotente."""
        today = fields.Date.context_today(self)
        Skill = self.env['hr.employee.skill'].sudo()
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
                current = Skill.search([
                    ('employee_id', '=', employee.id),
                    ('skill_id', '=', channel.sgi_skill_id.id),
                    '|', ('valid_to', '=', False), ('valid_to', '>=', today),
                ], limit=1)
                target = channel.sgi_skill_level_id
                if current:
                    if current.skill_level_id.level_progress >= target.level_progress:
                        continue
                    current.write({'skill_level_id': target.id})
                else:
                    Skill.create({
                        'employee_id': employee.id,
                        'skill_id': channel.sgi_skill_id.id,
                        'skill_type_id': channel.sgi_skill_id.skill_type_id.id,
                        'skill_level_id': target.id,
                    })
                granted += 1
        return granted

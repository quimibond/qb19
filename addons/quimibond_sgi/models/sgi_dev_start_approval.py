# -*- coding: utf-8 -*-
"""57.137.0 (revisión de Administración de Ventas 2026-10-08, punto 4; brief §6.8 «Aprobación para iniciar un proyecto»).

La **aprobación para iniciar el proyecto** es un documento que se genera desde el proyecto (y
la cotización, cuando ``qb_costeo_sgi`` está instalado), nunca a mano: lo llena Diseño de Producto con lo que
el cliente compartió, Administración de Ventas lo manda al cliente y el cliente lo firma, manda una orden de compra
o aprueba por WhatsApp. El documento ya lleva el **folio FT**: al generarlo se asigna si no lo
tenía (antes el folio llegaba al pasar a «Muestra»; esa asignación sigue como respaldo). La
evidencia de la respuesta del cliente se sigue registrando con «Registrar aprobación del cliente».
"""
import base64
import logging

from odoo import fields, models
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)


class ProjectProjectDevStartApproval(models.Model):
    _inherit = 'project.project'

    sgi_dev_start_approval_attachment_id = fields.Many2one(
        'ir.attachment', string="Aprobación para iniciar (PDF)", readonly=True, copy=False)
    sgi_dev_start_approval_date = fields.Datetime(string="Aprobación para iniciar generada el", readonly=True, copy=False)
    sgi_dev_start_approval_by_id = fields.Many2one('res.users', string="Generó la aprobación para iniciar",
                                                   readonly=True, copy=False)
    sgi_dev_start_approval_sent_at = fields.Datetime(string="Aprobación para iniciar enviada al cliente el",
                                                     readonly=True, copy=False)
    sgi_dev_start_approval_sent_by_id = fields.Many2one('res.users', string="La envió", readonly=True, copy=False)

    def _sgi_dev_start_approval_quote_vals(self):
        """Datos de la cotización que van en el documento. Gancho: el núcleo no conoce el cotizador;
        ``qb_costeo_sgi`` devuelve folio, precio, moneda, vigencia y condiciones de la cotización
        presentada o ganada más reciente. Vacío = el documento sale sin precio."""
        self.ensure_one()
        return {}

    def _sgi_dev_start_approval_lines(self):
        """Renglones de la tabla que van al cliente (los marcados «En especificación del cliente»)."""
        self.ensure_one()
        return self.sgi_dev_line_ids.filtered('in_customer_spec').sorted(lambda l: (l.sequence, l.id))

    def action_sgi_dev_start_approval(self):
        """Genera el PDF «Aprobación para iniciar el proyecto», asigna el folio FT si falta y lo deja
        en el proyecto y en el chatter."""
        for project in self:
            if not project.sgi_is_ft:
                raise UserError("Solo un proyecto de desarrollo lleva aprobación para iniciar.")
            if project.sgi_dev_analysis_result in ('linea', 'no_factible'):
                raise UserError("%s: un producto de línea o no factible no abre proyecto; no hay aprobación que pedir."
                                % project.display_name)
            if not project.partner_id:
                raise UserError("%s: capture el cliente antes de generar la aprobación para iniciar." % project.display_name)
            if not project.sgi_ft_folio:
                project.action_sgi_dev_assign_folio()
            report = self.env.ref('quimibond_sgi.action_report_dev_start_approval')
            pdf, _kind = self.env['ir.actions.report'].sudo()._render_qweb_pdf(report.report_name, res_ids=project.ids)
            attachment = self.env['ir.attachment'].sudo().create({
                'name': "Aprobación para iniciar %s.pdf" % (project.sgi_ft_folio or project.name),
                'type': 'binary', 'datas': base64.b64encode(pdf), 'mimetype': 'application/pdf',
                'res_model': 'project.project', 'res_id': project.id,
            })
            project.write({'sgi_dev_start_approval_attachment_id': attachment.id,
                           'sgi_dev_start_approval_date': fields.Datetime.now(),
                           'sgi_dev_start_approval_by_id': self.env.uid})
            project.message_post(body="Aprobación para iniciar el proyecto generada (%s)." % (project.sgi_ft_folio or ''),
                                 attachment_ids=attachment.ids)
        return True

    def action_sgi_dev_start_approval_mail(self):
        """Correo al cliente con el PDF (lo manda Administración de Ventas)."""
        self.ensure_one()
        if not self.sgi_dev_start_approval_attachment_id:
            self.action_sgi_dev_start_approval()
        template = self.env.ref('quimibond_sgi.mail_template_sgi_dev_start_approval')
        recipients = self.sgi_dev_contact_id or self.partner_id
        ctx = {
            'default_model': self._name, 'default_res_ids': self.ids, 'default_template_id': template.id,
            'default_composition_mode': 'comment', 'default_partner_ids': recipients.ids,
            'default_attachment_ids': self.sgi_dev_start_approval_attachment_id.ids,
            'sgi_dev_start_approval_notify': True, 'force_email': True,
        }
        return {'type': 'ir.actions.act_window', 'res_model': 'mail.compose.message', 'view_mode': 'form',
                'views': [(self.env.ref('mail.email_compose_message_wizard_form').id, 'form')],
                'target': 'new', 'context': ctx}

    def message_post(self, **kwargs):
        if self.env.context.get('sgi_dev_start_approval_notify'):
            when = fields.Datetime.now()
            for project in self.filtered(lambda p: p.sgi_is_ft and not p.sgi_dev_start_approval_sent_at):
                project.with_context(sgi_dev_start_approval_notify=False).write({
                    'sgi_dev_start_approval_sent_at': when, 'sgi_dev_start_approval_sent_by_id': self.env.uid})
        return super().message_post(**kwargs)

    def action_sgi_dev_print_start_approval(self):
        self.ensure_one()
        return self.env.ref('quimibond_sgi.action_report_dev_start_approval').report_action(self)

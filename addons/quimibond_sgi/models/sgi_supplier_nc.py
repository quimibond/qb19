# -*- coding: utf-8 -*-
"""NC-6 (53.0.0): NC a proveedor por el portal.

Desde una NC de materia prima, «Enviar al proveedor» manda por correo el
enlace del portal con la NC (lote, cantidad y evidencia); el proveedor
contesta causa y acción en el portal, sin correo de por medio. Plazo de
respuesta en días hábiles (parámetro), aviso el día que vence y escalamiento
al comprador y a MAST, como los plazos de NC-1. La NC ya cuenta en la
evaluación del proveedor (S1.08) por su `partner_id`.
"""
from odoo import api, fields, models
from odoo.exceptions import UserError

from .sgi_calendar import sgi_add_business_days


class QualityAlertSupplierPortal(models.Model):
    _name = 'quality.alert'
    _inherit = ['quality.alert', 'portal.mixin']

    sgi_supplier_id = fields.Many2one(
        'res.partner', string="Proveedor", compute='_compute_sgi_supplier_id', store=True,
        readonly=False, domain=[('supplier_rank', '>', 0)])
    sgi_supplier_state = fields.Selection([
        ('no_enviada', "Sin enviar"),
        ('enviada', "Enviada al proveedor"),
        ('contestada', "Contestada por el proveedor"),
    ], string="Respuesta del proveedor", default='no_enviada', tracking=True, copy=False)
    sgi_supplier_sent_date = fields.Date(string="Enviada el", readonly=True, copy=False)
    sgi_supplier_due_date = fields.Date(string="Respuesta antes del", readonly=True, copy=False)
    sgi_supplier_response_date = fields.Datetime(string="Contestada el", readonly=True, copy=False)
    sgi_supplier_cause = fields.Text(string="Causa según el proveedor", readonly=True, copy=False)
    sgi_supplier_action = fields.Text(string="Acción según el proveedor", readonly=True, copy=False)
    sgi_supplier_overdue = fields.Boolean(compute='_compute_sgi_supplier_overdue')

    @api.depends('partner_id')
    def _compute_sgi_supplier_id(self):
        for alert in self:
            if not alert.sgi_supplier_id and alert.partner_id and alert.partner_id.supplier_rank:
                alert.sgi_supplier_id = alert.partner_id

    @api.depends('sgi_supplier_state', 'sgi_supplier_due_date')
    def _compute_sgi_supplier_overdue(self):
        today = fields.Date.context_today(self)
        for alert in self:
            alert.sgi_supplier_overdue = bool(
                alert.sgi_supplier_state == 'enviada' and alert.sgi_supplier_due_date
                and today > alert.sgi_supplier_due_date)

    def _compute_access_url(self):
        super()._compute_access_url()
        for alert in self:
            alert.access_url = '/my/nc/%s' % alert.id

    def action_sgi_send_to_supplier(self):
        """Envía la NC al proveedor por el portal (correo con el enlace)."""
        self.ensure_one()
        if not self.sgi_folio:
            raise UserError("Solo una NC del SGI (con folio) se envía al proveedor.")
        supplier = self.sgi_supplier_id or self.partner_id
        if not supplier:
            raise UserError("Captura el proveedor en la NC antes de enviarla.")
        if not supplier.email:
            raise UserError("El proveedor %s no tiene correo." % supplier.display_name)
        try:
            days = int(self.env['ir.config_parameter'].sudo().get_param(
                'quimibond_sgi.nc_days_supplier_response', 5) or 5)
        except (TypeError, ValueError):
            days = 5
        today = fields.Date.context_today(self)
        self.write({
            'sgi_supplier_id': supplier.id,
            'partner_id': self.partner_id.id or supplier.id,
            'sgi_supplier_state': 'enviada',
            'sgi_supplier_sent_date': today,
            'sgi_supplier_due_date': sgi_add_business_days(self.env, today, days),
        })
        self._portal_ensure_token()
        template = self.env.ref('quimibond_sgi.mail_template_sgi_nc_supplier', raise_if_not_found=False)
        if template:
            template.sudo().with_context(sgi_supplier_email=supplier.email).send_mail(
                self.id, email_values={'email_to': supplier.email})
        self.message_post(body="NC enviada al proveedor %s por el portal; respuesta antes del %s." % (
            supplier.display_name, self.sgi_supplier_due_date))
        return True

    def sgi_supplier_answer(self, cause, action):
        """Respuesta del proveedor desde el portal (con token válido)."""
        self.ensure_one()
        cause = (cause or '').strip()
        action = (action or '').strip()
        if not cause or not action:
            raise UserError("Causa y acción son obligatorias.")
        self.sudo().write({
            'sgi_supplier_cause': cause, 'sgi_supplier_action': action,
            'sgi_supplier_state': 'contestada',
            'sgi_supplier_response_date': fields.Datetime.now(),
        })
        self.sudo().message_post(
            body="<b>Respuesta del proveedor</b> por el portal.<br/><b>Causa:</b> %s<br/><b>Acción:</b> %s" % (
                cause, action))
        # Cierra el aviso de respuesta pendiente y avisa a quien la sigue.
        self.sudo().activity_ids.filtered(
            lambda a: (a.summary or '').startswith("Respuesta del proveedor")).action_feedback(
            feedback="El proveedor contestó por el portal.")
        Cron = self.env['sgi.cron'].sudo()
        Cron._sgi_schedule(
            self.sudo(), "El proveedor contestó la NC %s: revisar causa y acción" % (self.sgi_folio,),
            "Revisa la respuesta del proveedor y registra las acciones en la NC.",
            self.user_id.id or Cron._sgi_manager_user_id())
        return True

    def _sgi_supplier_escalation(self, today):
        """Aviso el día que vence la respuesta y escalamiento después."""
        self.ensure_one()
        if self.sgi_supplier_state != 'enviada' or not self.sgi_supplier_due_date:
            return
        Cron = self.env['sgi.cron']
        folio = self.sgi_folio or self.name
        who = self.user_id.id or Cron._sgi_manager_user_id()
        if today >= self.sgi_supplier_due_date:
            Cron._sgi_schedule(
                self, "Respuesta del proveedor vence el %s: NC %s" % (self.sgi_supplier_due_date, folio),
                "El proveedor %s no ha contestado la NC por el portal. Reenvía el enlace o llámale." % (
                    self.sgi_supplier_id.display_name), who)
        if today > self.sgi_supplier_due_date:
            Cron._sgi_schedule(
                self, "NC %s: proveedor sin respuesta, escalada a MAST" % folio,
                "El plazo de respuesta del proveedor venció el %s." % self.sgi_supplier_due_date,
                Cron._sgi_manager_user_id())

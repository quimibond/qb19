# -*- coding: utf-8 -*-
"""Respuesta al cliente en la no conformidad (56.22.0, C5.19 y C5.20).

C5.19: al abrir la reclamación se acusa recibo al cliente en máximo 2 días
hábiles y se le da respuesta formal en 5 días hábiles (Confección), 20
(Industrial) o el plazo que pidió el cliente. Los días por línea viven en el
equipo de venta (``crm.team.sgi_complaint_response_days``); el equipo sale del
pedido de la reclamación o del cliente. La NC guarda las fechas reales, sus
plazos y si se cumplieron: de ahí se mide el indicador.

C5.20: si el producto no conforme ya se embarcó (o pudo embarcarse) se avisa
al cliente por escrito; la NC guarda la fecha del aviso y no se cierra sin él.

El cron diario de NC avisa al responsable cuando vence el acuse o la respuesta
sin fecha capturada.
"""
from odoo import api, fields, models
from odoo.exceptions import UserError

from .sgi_calendar import sgi_add_business_days

ACK_DAYS_PARAM = 'quimibond_sgi.complaint_ack_days'
RESPONSE_DAYS_PARAM = 'quimibond_sgi.complaint_response_days'


class CrmTeamComplaintDays(models.Model):
    _inherit = 'crm.team'

    sgi_complaint_response_days = fields.Integer(
        string="Días hábiles para responder una reclamación", default=5,
        help="C5.19: plazo de la respuesta formal al cliente (5 Confección, 20 Industrial). "
             "Si el cliente pidió otro plazo, se captura en la NC.")


class QualityAlertCustomerReply(models.Model):
    _inherit = 'quality.alert'

    sgi_customer_reply_required = fields.Boolean(
        string="Responde al cliente", compute='_compute_sgi_customer_reply_required', store=True,
        help="Reclamación, scorecard o NC con cliente: se mide el acuse y la respuesta (C5.19).")
    sgi_sale_team_id = fields.Many2one(
        'crm.team', string="Línea (equipo de venta)", compute='_compute_sgi_sale_team_id',
        store=True, readonly=False,
        help="Del pedido de la reclamación o, si no hay, del cliente. Fija el plazo de respuesta.")
    sgi_customer_received_date = fields.Date(
        string="Reclamación recibida el", compute='_compute_sgi_customer_received_date',
        store=True, readonly=False,
        help="Día en que llegó la reclamación (el del ticket, si lo hay). Desde aquí corren los plazos.")
    sgi_customer_ack_due = fields.Date(
        string="Acusar recibo a más tardar", compute='_compute_sgi_customer_dues', store=True)
    sgi_customer_ack_date = fields.Date(string="Acuse de recibo al cliente", tracking=True, copy=False)
    sgi_customer_deadline = fields.Date(
        string="Plazo pedido por el cliente", copy=False,
        help="Si el cliente fijó su propio plazo de respuesta, sustituye el de la línea.")
    sgi_customer_response_due = fields.Date(
        string="Responder a más tardar", compute='_compute_sgi_customer_dues', store=True)
    sgi_customer_response_date = fields.Date(string="Respuesta formal al cliente", tracking=True, copy=False)
    sgi_customer_ack_on_time = fields.Boolean(
        string="Acuse a tiempo", compute='_compute_sgi_customer_on_time', store=True)
    sgi_customer_response_on_time = fields.Boolean(
        string="Respuesta a tiempo", compute='_compute_sgi_customer_on_time', store=True)
    # C5.20
    sgi_shipped_status = fields.Selection([
        ('no', "No se embarcó"),
        ('si', "Sí, ya se embarcó"),
        ('posible', "Pudo haberse embarcado"),
    ], string="¿Producto ya embarcado?", tracking=True, copy=False,
        help="C5.20: al abrir la NC revisar si hay lotes del mismo origen ya embarcados.")
    sgi_customer_notice_date = fields.Date(
        string="Aviso escrito al cliente", tracking=True, copy=False,
        help="Fecha del aviso vía Administración de ventas con lotes, cantidades y plan.")
    sgi_customer_notice_note = fields.Char(string="Cómo se avisó (referencia)", copy=False)

    @api.depends('sgi_origin_type', 'partner_id', 'sgi_complaint_ticket_id')
    def _compute_sgi_customer_reply_required(self):
        for alert in self:
            alert.sgi_customer_reply_required = bool(
                alert.sgi_complaint_ticket_id or alert.sgi_origin_type in ('reclamacion', 'scorecard'))

    @api.depends('sgi_complaint_ticket_id.sgi_sale_order_id.team_id', 'partner_id')
    def _compute_sgi_sale_team_id(self):
        for alert in self:
            if alert.sgi_sale_team_id:
                alert.sgi_sale_team_id = alert.sgi_sale_team_id
                continue
            team = alert.sgi_complaint_ticket_id.sgi_sale_order_id.team_id
            if not team and alert.partner_id:
                team = alert.partner_id.commercial_partner_id.team_id or alert.partner_id.team_id
            alert.sgi_sale_team_id = team

    @api.depends('sgi_complaint_ticket_id.create_date', 'create_date')
    def _compute_sgi_customer_received_date(self):
        for alert in self:
            if alert.sgi_customer_received_date:
                alert.sgi_customer_received_date = alert.sgi_customer_received_date
                continue
            stamp = alert.sgi_complaint_ticket_id.create_date or alert.create_date
            alert.sgi_customer_received_date = fields.Datetime.context_timestamp(
                alert, stamp).date() if stamp else False

    @api.model
    def _sgi_int_param(self, key, default):
        try:
            return int(self.env['ir.config_parameter'].sudo().get_param(key, default) or default)
        except (TypeError, ValueError):
            return default

    @api.depends('sgi_customer_reply_required', 'sgi_customer_received_date', 'sgi_customer_deadline',
                 'sgi_sale_team_id.sgi_complaint_response_days')
    def _compute_sgi_customer_dues(self):
        ack_days = self._sgi_int_param(ACK_DAYS_PARAM, 2)
        default_days = self._sgi_int_param(RESPONSE_DAYS_PARAM, 5)
        for alert in self:
            start = alert.sgi_customer_received_date
            if not alert.sgi_customer_reply_required or not start:
                alert.sgi_customer_ack_due = alert.sgi_customer_response_due = False
                continue
            alert.sgi_customer_ack_due = sgi_add_business_days(self.env, start, ack_days, alert.company_id)
            days = alert.sgi_sale_team_id.sgi_complaint_response_days or default_days
            alert.sgi_customer_response_due = alert.sgi_customer_deadline or sgi_add_business_days(
                self.env, start, days, alert.company_id)

    @api.depends('sgi_customer_ack_date', 'sgi_customer_ack_due',
                 'sgi_customer_response_date', 'sgi_customer_response_due')
    def _compute_sgi_customer_on_time(self):
        for alert in self:
            alert.sgi_customer_ack_on_time = bool(
                alert.sgi_customer_ack_date and alert.sgi_customer_ack_due
                and alert.sgi_customer_ack_date <= alert.sgi_customer_ack_due)
            alert.sgi_customer_response_on_time = bool(
                alert.sgi_customer_response_date and alert.sgi_customer_response_due
                and alert.sgi_customer_response_date <= alert.sgi_customer_response_due)

    def _sgi_check_can_close(self):
        """C5.19 / C5.20: una reclamación no se cierra sin respuesta formal al
        cliente, ni una NC de producto embarcado sin el aviso escrito."""
        super()._sgi_check_can_close()
        for alert in self:
            problems = []
            if alert.sgi_customer_reply_required and not alert.sgi_customer_response_date:
                problems.append("• Falta la fecha de la respuesta formal al cliente (C5.19).")
            if alert.sgi_shipped_status in ('si', 'posible') and not alert.sgi_customer_notice_date:
                problems.append("• El producto se embarcó o pudo embarcarse: falta el aviso escrito al cliente (C5.20).")
            if problems:
                raise UserError("No se puede cerrar la NC %s:\n%s" % (
                    alert.sgi_folio or alert.name, "\n".join(problems)))

    def _sgi_customer_reply_escalation(self, today):
        """Aviso al responsable de la NC el día que vence el acuse o la
        respuesta sin fecha capturada."""
        Cron = self.env['sgi.cron']
        for alert in self.filtered('sgi_customer_reply_required'):
            user_id = alert.user_id.id or Cron._sgi_manager_user_id()
            folio = alert.sgi_folio or alert.name
            if not alert.sgi_customer_ack_date and alert.sgi_customer_ack_due and alert.sgi_customer_ack_due <= today:
                Cron._sgi_schedule(alert, "Acusar recibo al cliente: %s" % folio,
                                   "Venció el %s. Acusa recibo al cliente y anota la fecha en la NC (C5.19)." % (
                                       alert.sgi_customer_ack_due), user_id)
            if (not alert.sgi_customer_response_date and alert.sgi_customer_response_due
                    and alert.sgi_customer_response_due <= today):
                Cron._sgi_schedule(alert, "Responder al cliente: %s" % folio,
                                   "La respuesta formal vence el %s. Envíala con la contención y la causa, "
                                   "y anota la fecha en la NC (C5.19)." % alert.sgi_customer_response_due, user_id)


class SgiCronCustomerReply(models.AbstractModel):
    _inherit = 'sgi.cron'

    @api.model
    def cron_nonconformities(self):
        res = super().cron_nonconformities()
        today = fields.Date.context_today(self)
        self._sgi_step(
            "acuse y respuesta al cliente (C5.19)",
            lambda: self.env['quality.alert'].search([
                ('sgi_customer_reply_required', '=', True),
                ('stage_id.sgi_is_closing_stage', '=', False),
                ('stage_id.sgi_is_cancel_stage', '=', False),
                '|', ('sgi_customer_ack_date', '=', False), ('sgi_customer_response_date', '=', False),
            ])._sgi_customer_reply_escalation(today))
        return res

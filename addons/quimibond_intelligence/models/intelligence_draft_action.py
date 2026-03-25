"""
Intelligence Draft Action — Borradores de acciones preparadas por el sistema.

El sistema sugiere + prepara acciones concretas (emails, actividades, tareas)
que un humano aprueba antes de ejecutar. Patrón: sugiere → prepara → aprueba → ejecuta.
"""
import json
import logging

from odoo import api, fields, models

_logger = logging.getLogger(__name__)


class IntelligenceDraftAction(models.Model):
    _name = 'intelligence.draft.action'
    _description = 'Intelligence Draft Action'
    _order = 'create_date desc'
    _inherit = ['mail.thread']

    name = fields.Char(
        string='Descripcion', required=True, tracking=True)
    draft_type = fields.Selection([
        ('activity', 'Actividad Odoo'),
        ('email', 'Email de seguimiento'),
        ('crm_lead', 'Oportunidad CRM'),
    ], string='Tipo de borrador', required=True, index=True)
    partner_id = fields.Many2one(
        'res.partner', string='Contacto', index=True)
    action_item_id = fields.Many2one(
        'intelligence.action.item', string='Action item origen')
    alert_id = fields.Many2one(
        'intelligence.alert', string='Alerta origen')
    draft_data = fields.Text(
        string='Datos del borrador (JSON)',
        help='JSON con los datos para crear el objeto Odoo al aprobar')
    state = fields.Selection([
        ('draft', 'Borrador'),
        ('approved', 'Aprobado'),
        ('rejected', 'Rechazado'),
        ('executed', 'Ejecutado'),
    ], string='Estado', default='draft', tracking=True, index=True)
    executed_ref = fields.Char(
        string='Referencia ejecutada',
        help='Referencia al objeto creado (ej: mail.activity,123)')
    notes = fields.Text(string='Notas')

    def action_approve(self):
        """Aprueba el borrador y ejecuta la acción."""
        for rec in self:
            if rec.state != 'draft':
                continue
            try:
                ref = rec._execute_draft()
                rec.write({
                    'state': 'executed',
                    'executed_ref': ref or '',
                })
                _logger.info(
                    'Draft action %s executed: %s → %s',
                    rec.id, rec.draft_type, ref,
                )
            except Exception as exc:
                rec.write({'state': 'approved', 'notes': str(exc)})
                _logger.warning('Draft action %s execute failed: %s',
                                rec.id, exc)

    def action_reject(self):
        self.write({'state': 'rejected'})

    def action_reset(self):
        self.write({'state': 'draft'})

    def _execute_draft(self) -> str:
        """Ejecuta el borrador según su tipo. Retorna referencia."""
        data = json.loads(self.draft_data or '{}')

        if self.draft_type == 'activity':
            return self._execute_activity(data)
        elif self.draft_type == 'email':
            return self._execute_email_draft(data)
        elif self.draft_type == 'crm_lead':
            return self._execute_crm_lead(data)
        return ''

    def _execute_activity(self, data: dict) -> str:
        """Crea mail.activity en el partner."""
        partner = self.partner_id
        if not partner:
            return ''

        activity_type_xmlid = data.get(
            'activity_type', 'mail.mail_activity_data_todo')
        try:
            activity_type = self.env.ref(activity_type_xmlid)
        except ValueError:
            activity_type = self.env.ref('mail.mail_activity_data_todo')

        user_id = data.get('user_id', self.env.user.id)
        days = data.get('deadline_days', 3)

        activity = self.env['mail.activity'].sudo().create({
            'res_model_id': self.env['ir.model']._get_id('res.partner'),
            'res_id': partner.id,
            'activity_type_id': activity_type.id,
            'summary': data.get('summary', self.name)[:200],
            'note': data.get('note', ''),
            'date_deadline': fields.Date.add(
                fields.Date.today(), days=days),
            'user_id': user_id,
        })
        return f'mail.activity,{activity.id}'

    def _execute_email_draft(self, data: dict) -> str:
        """Crea borrador de email (mail.mail) sin enviar."""
        partner = self.partner_id
        if not partner or not partner.email:
            return ''

        mail = self.env['mail.mail'].sudo().create({
            'subject': data.get('subject', self.name)[:200],
            'body_html': data.get('body_html', ''),
            'email_to': partner.email,
            'auto_delete': False,
            'state': 'outgoing',  # Draft, no sent yet
        })
        return f'mail.mail,{mail.id}'

    def _execute_crm_lead(self, data: dict) -> str:
        """Crea oportunidad CRM."""
        partner = self.partner_id
        try:
            lead = self.env['crm.lead'].sudo().create({
                'name': data.get('name', self.name)[:200],
                'partner_id': partner.id if partner else False,
                'description': data.get('description', ''),
                'expected_revenue': data.get('expected_revenue', 0),
                'user_id': data.get(
                    'user_id',
                    partner.user_id.id if partner and partner.user_id
                    else self.env.user.id,
                ),
            })
            return f'crm.lead,{lead.id}'
        except Exception as exc:
            _logger.warning('CRM lead creation failed: %s', exc)
            return ''

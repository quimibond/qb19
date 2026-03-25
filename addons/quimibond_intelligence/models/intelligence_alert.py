import logging

from odoo import api, fields, models

_logger = logging.getLogger(__name__)

# Mapeo alert_type → tipo de actividad Odoo + plazo en días
ALERT_ACTIVITY_MAP = {
    'no_response': ('mail.mail_activity_data_call', 3),
    'stalled_thread': ('mail.mail_activity_data_call', 3),
    'overdue_invoice': ('mail.mail_activity_data_todo', 2),
    'at_risk_client': ('mail.mail_activity_data_meeting', 7),
    'churn_risk': ('mail.mail_activity_data_call', 2),
    'quality_issue': ('mail.mail_activity_data_todo', 3),
    'payment_delay': ('mail.mail_activity_data_todo', 2),
    'invoice_silence': ('mail.mail_activity_data_call', 2),
    'delivery_risk': ('mail.mail_activity_data_todo', 1),
    'negative_sentiment': ('mail.mail_activity_data_call', 5),
}


class IntelligenceAlert(models.Model):
    _name = 'intelligence.alert'
    _description = 'Intelligence Alert'
    _order = 'create_date desc'
    _inherit = ['mail.thread']

    name = fields.Char(string='Titulo', required=True)
    activity_created = fields.Boolean(
        string='Actividad creada', default=False,
        help='Se creó una actividad Odoo automática a partir de esta alerta')
    alert_type = fields.Selection([
        ('no_response', 'Sin respuesta'),
        ('stalled_thread', 'Thread estancado'),
        ('high_volume', 'Alto volumen'),
        ('overdue_invoice', 'Factura vencida'),
        ('at_risk_client', 'Cliente en riesgo'),
        ('accountability', 'Accion sin cumplir'),
        ('anomaly', 'Anomalia detectada'),
        ('competitor', 'Competidor mencionado'),
        ('negative_sentiment', 'Sentimiento negativo'),
        ('churn_risk', 'Riesgo de perdida'),
        ('invoice_silence', 'Factura vencida + silencio'),
        ('delivery_risk', 'Entrega retrasada'),
        ('payment_delay', 'Pago vencido'),
        ('opportunity', 'Oportunidad detectada'),
        ('quality_issue', 'Problema de calidad'),
        ('volume_drop', 'Caida de volumen'),
        ('unusual_discount', 'Descuento inusual'),
        ('cross_sell', 'Oportunidad cross-sell'),
        ('stockout_risk', 'Riesgo de desabasto'),
        ('reorder_needed', 'Reorden necesario'),
        ('payment_compliance', 'Deterioro en cumplimiento de pago'),
    ], string='Tipo', required=True, index=True)
    severity = fields.Selection([
        ('low', 'Baja'),
        ('medium', 'Media'),
        ('high', 'Alta'),
        ('critical', 'Critica'),
    ], string='Severidad', default='medium', index=True)
    state = fields.Selection([
        ('open', 'Abierta'),
        ('acknowledged', 'Reconocida'),
        ('resolved', 'Resuelta'),
        ('dismissed', 'Descartada'),
    ], string='Estado', default='open', tracking=True, index=True)
    description = fields.Text(string='Descripcion')
    account = fields.Char(string='Cuenta email')
    partner_id = fields.Many2one(
        'res.partner', string='Contacto relacionado', index=True)
    briefing_id = fields.Many2one(
        'intelligence.briefing', string='Briefing origen', index=True)
    gmail_thread_id = fields.Char(string='Thread ID')
    resolution_notes = fields.Text(string='Notas de resolucion')
    resolved_date = fields.Datetime(string='Fecha resolucion')
    supabase_id = fields.Integer(
        string='Supabase ID', index=True, copy=False)
    supabase_synced = fields.Boolean(
        string='Synced to Supabase', default=False, index=True)

    def action_acknowledge(self):
        self.write({'state': 'acknowledged', 'supabase_synced': False})

    def action_resolve(self):
        self.write({
            'state': 'resolved',
            'resolved_date': fields.Datetime.now(),
            'supabase_synced': False,
        })

    def action_dismiss(self):
        self.write({'state': 'dismissed', 'supabase_synced': False})

    def action_reopen(self):
        self.write({
            'state': 'open',
            'resolved_date': False,
            'supabase_synced': False,
        })

    # ── Auto-crear actividades Odoo desde alertas ─────────────────────────

    @api.model_create_multi
    def create(self, vals_list):
        records = super().create(vals_list)
        for rec in records:
            try:
                rec._maybe_create_activity()
            except Exception as exc:
                _logger.debug('Auto-activity for alert %s: %s', rec.id, exc)
        return records

    def _maybe_create_activity(self):
        """Crea mail.activity en el partner si la alerta es high/critical
        y el tipo tiene mapeo definido."""
        if self.severity not in ('high', 'critical'):
            return
        if not self.partner_id:
            return

        mapping = ALERT_ACTIVITY_MAP.get(self.alert_type)
        if not mapping:
            return

        activity_xmlid, days = mapping

        # Buscar tipo de actividad
        try:
            activity_type = self.env.ref(activity_xmlid)
        except ValueError:
            return

        # Determinar usuario responsable: salesperson del partner, o admin
        user = self.partner_id.user_id or self.env.user

        # No duplicar: si ya hay actividad abierta del mismo tipo en el partner
        existing = self.env['mail.activity'].sudo().search([
            ('res_model', '=', 'res.partner'),
            ('res_id', '=', self.partner_id.id),
            ('activity_type_id', '=', activity_type.id),
            ('date_deadline', '>=', fields.Date.today()),
        ], limit=1)
        if existing:
            return

        self.env['mail.activity'].sudo().create({
            'res_model_id': self.env['ir.model']._get_id('res.partner'),
            'res_id': self.partner_id.id,
            'activity_type_id': activity_type.id,
            'summary': self.name[:200],
            'note': self.description or '',
            'date_deadline': fields.Date.add(fields.Date.today(), days=days),
            'user_id': user.id,
        })
        self.write({'activity_created': True})
        _logger.info(
            'Auto-activity created: %s on partner %s (alert %s)',
            activity_type.name, self.partner_id.name, self.id,
        )

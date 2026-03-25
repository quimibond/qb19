from odoo import fields, models


class IntelligenceAlert(models.Model):
    _name = 'intelligence.alert'
    _description = 'Intelligence Alert'
    _order = 'create_date desc'
    _inherit = ['mail.thread']

    name = fields.Char(string='Titulo', required=True)
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

from odoo import api, fields, models


class IntelligenceClientScore(models.Model):
    _name = 'intelligence.client.score'
    _description = 'Client Relationship Score'
    _order = 'date desc, total_score desc'
    _rec_name = 'partner_id'

    partner_id = fields.Many2one(
        'res.partner', string='Contacto', required=True,
        index=True, ondelete='cascade')
    date = fields.Date(string='Fecha', required=True, index=True)
    email = fields.Char(string='Email', index=True)
    total_score = fields.Integer(string='Score total')
    frequency_score = fields.Integer(string='Frecuencia')
    responsiveness_score = fields.Integer(string='Capacidad resp.')
    reciprocity_score = fields.Integer(string='Reciprocidad')
    sentiment_score = fields.Integer(string='Sentimiento')
    payment_compliance_score = fields.Integer(string='Cumplimiento de pago')
    risk_level = fields.Selection([
        ('low', 'Bajo'),
        ('medium', 'Medio'),
        ('high', 'Alto'),
    ], string='Nivel de riesgo', index=True)
    score_trend = fields.Selection([
        ('up', 'Subiendo'),
        ('stable', 'Estable'),
        ('down', 'Bajando'),
    ], string='Tendencia', default='stable')

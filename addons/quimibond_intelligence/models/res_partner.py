from odoo import api, fields, models


class ResPartner(models.Model):
    _inherit = 'res.partner'

    intelligence_score = fields.Integer(
        string='Score de relacion',
        compute='_compute_intelligence_score', store=True)
    intelligence_risk = fields.Selection([
        ('low', 'Bajo'),
        ('medium', 'Medio'),
        ('high', 'Alto'),
    ], string='Riesgo', compute='_compute_intelligence_score', store=True)
    intelligence_score_ids = fields.One2many(
        'intelligence.client.score', 'partner_id',
        string='Historial de scores')
    intelligence_alert_ids = fields.One2many(
        'intelligence.alert', 'partner_id',
        string='Alertas de inteligencia')
    intelligence_alert_count = fields.Integer(
        string='Alertas abiertas',
        compute='_compute_intelligence_alert_count')

    @api.depends('intelligence_score_ids', 'intelligence_score_ids.total_score')
    def _compute_intelligence_score(self):
        Score = self.env['intelligence.client.score']
        for partner in self:
            last = Score.search([
                ('partner_id', '=', partner.id),
            ], limit=1, order='date desc')
            if last:
                partner.intelligence_score = last.total_score
                partner.intelligence_risk = last.risk_level
            else:
                partner.intelligence_score = 0
                partner.intelligence_risk = False

    def _compute_intelligence_alert_count(self):
        for partner in self:
            partner.intelligence_alert_count = self.env[
                'intelligence.alert'].search_count([
                    ('partner_id', '=', partner.id),
                    ('state', '=', 'open'),
                ])

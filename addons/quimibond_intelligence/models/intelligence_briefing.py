from odoo import api, fields, models


class IntelligenceBriefing(models.Model):
    _name = 'intelligence.briefing'
    _description = 'Intelligence Briefing'
    _order = 'date desc'
    _rec_name = 'date'

    date = fields.Date(string='Fecha', required=True, index=True)
    briefing_type = fields.Selection([
        ('daily', 'Diario'),
        ('weekly', 'Semanal'),
    ], string='Tipo', default='daily', required=True)
    html_content = fields.Html(
        string='Briefing', sanitize=True, sanitize_overridable=True,
    )
    total_emails = fields.Integer(string='Emails procesados')
    accounts_ok = fields.Integer(string='Cuentas exitosas')
    accounts_failed = fields.Integer(string='Cuentas fallidas')
    topics_count = fields.Integer(string='Temas detectados')
    topics_json = fields.Text(string='Temas (JSON)')
    execution_seconds = fields.Float(string='Tiempo ejecucion (seg)')
    alert_ids = fields.One2many(
        'intelligence.alert', 'briefing_id', string='Alertas')
    alert_count = fields.Integer(
        string='Num. alertas', compute='_compute_alert_count', store=True)
    state = fields.Selection([
        ('draft', 'Generado'),
        ('reviewed', 'Revisado'),
        ('archived', 'Archivado'),
    ], string='Estado', default='draft')

    @api.depends('alert_ids')
    def _compute_alert_count(self):
        for rec in self:
            rec.alert_count = len(rec.alert_ids)

    def action_mark_reviewed(self):
        self.write({'state': 'reviewed'})

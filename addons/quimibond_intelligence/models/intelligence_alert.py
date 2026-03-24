import logging

from odoo import api, fields, models

_logger = logging.getLogger(__name__)


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

    def action_acknowledge(self):
        self.write({'state': 'acknowledged'})
        self._sync_to_supabase('acknowledged')

    def action_resolve(self):
        self.write({
            'state': 'resolved',
            'resolved_date': fields.Datetime.now(),
        })
        self._sync_to_supabase('resolved')

    def action_dismiss(self):
        self.write({'state': 'dismissed'})
        self._sync_to_supabase('dismissed')

    def action_reopen(self):
        self.write({
            'state': 'open',
            'resolved_date': False,
        })
        self._sync_to_supabase('open')

    def _sync_to_supabase(self, state):
        """Sync alert state changes to Supabase."""
        get = lambda k, d='': (
            self.env['ir.config_parameter'].sudo()
            .get_param('quimibond_intelligence.%s' % k, d)
        )
        url = get('supabase_url')
        key = get('supabase_service_role_key') or get('supabase_key')
        if not url or not key:
            return
        try:
            from ..services.supabase_service import SupabaseService
            with SupabaseService(url, key) as supa:
                for rec in self:
                    if rec.supabase_id and rec.supabase_id > 0:
                        supa.update_alert_state_by_id(
                            rec.supabase_id, state,
                            resolution_notes=rec.resolution_notes,
                        )
                    elif rec.name:
                        _logger.info(
                            'Alert %s has no supabase_id, using title fallback',
                            rec.id,
                        )
                        supa.update_alert_state(
                            rec.name, state,
                            resolution_notes=rec.resolution_notes,
                        )
                    else:
                        _logger.warning(
                            'Alert %s has no supabase_id or title, cannot sync',
                            rec.id,
                        )
        except Exception as exc:
            _logger.warning('Alert sync to Supabase failed: %s', exc)

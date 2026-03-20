"""
Quimibond Intelligence — Configuración
Modelo transient para gestionar parámetros del sistema desde la UI de Odoo.
"""
import json
import logging

from odoo import api, fields, models

_logger = logging.getLogger(__name__)

# ── Las 22 cuentas estratégicas ──────────────────────────────────────────────
EMAIL_ACCOUNTS = [
    'direcciondeoperaciones@quimibond.com',
    'aurelio@quimibond.com',
    'gilberto@quimibond.com',
    'ventas@quimibond.com',
    'ventasindustrial@quimibond.com',
    'admon.ventas@quimibond.com',
    'info@quimibond.com',
    'manufactura@quimibond.com',
    'planeacion@quimibond.com',
    'logistica@quimibond.com',
    'ingenieriacc@quimibond.com',
    'comprasplanta@quimibond.com',
    'jefe.calidad@quimibond.com',
    'innovacion@quimibond.com',
    'producto.innovacion@quimibond.com',
    'cxp@quimibond.com',
    'cxcobrar@quimibond.com',
    'irma.luna@quimibond.com',
    'berenice.vazquez@quimibond.com',
    'recursoshumanos@quimibond.com',
    'rhmexico@quimibond.com',
    'jose.mizrahi@quimibond.com',
]

ACCOUNT_DEPARTMENTS = {
    'direcciondeoperaciones@quimibond.com': 'Dirección de Operaciones',
    'aurelio@quimibond.com': 'Dirección',
    'gilberto@quimibond.com': 'Dirección',
    'ventas@quimibond.com': 'Ventas',
    'ventasindustrial@quimibond.com': 'Ventas Industrial',
    'admon.ventas@quimibond.com': 'Admin Ventas',
    'info@quimibond.com': 'Info General',
    'manufactura@quimibond.com': 'Manufactura',
    'planeacion@quimibond.com': 'Planeación',
    'logistica@quimibond.com': 'Logística',
    'ingenieriacc@quimibond.com': 'Ingeniería CC',
    'comprasplanta@quimibond.com': 'Compras Planta',
    'jefe.calidad@quimibond.com': 'Calidad',
    'innovacion@quimibond.com': 'Innovación',
    'producto.innovacion@quimibond.com': 'Innovación Producto',
    'cxp@quimibond.com': 'Cuentas por Pagar',
    'cxcobrar@quimibond.com': 'Cuentas por Cobrar',
    'irma.luna@quimibond.com': 'Administración',
    'berenice.vazquez@quimibond.com': 'Administración',
    'recursoshumanos@quimibond.com': 'Recursos Humanos',
    'rhmexico@quimibond.com': 'RH México',
    'jose.mizrahi@quimibond.com': 'Dirección General',
}

INTERNAL_DOMAIN = 'quimibond.com'


class IntelligenceConfig(models.TransientModel):
    """Settings UI para configurar el Intelligence System desde Odoo."""
    _name = 'intelligence.config'
    _description = 'Intelligence System Configuration'

    # ── API Keys (se leen/escriben en ir.config_parameter) ───────────────────
    service_account_json = fields.Text(
        string='Google Service Account JSON',
        help='JSON completo del service account con Domain-Wide Delegation',
    )
    anthropic_api_key = fields.Char(string='Anthropic API Key')
    supabase_url = fields.Char(string='Supabase URL')
    supabase_key = fields.Char(string='Supabase Anon Key')
    voyage_api_key = fields.Char(string='Voyage AI API Key')
    recipient_email = fields.Char(
        string='Email del briefing',
        default='jose.mizrahi@quimibond.com',
    )

    # ── Umbrales ─────────────────────────────────────────────────────────────
    target_response_hours = fields.Integer(default=4, string='Meta respuesta (horas)')
    slow_response_hours = fields.Integer(default=8, string='Respuesta lenta (horas)')
    no_response_hours = fields.Integer(default=24, string='Sin respuesta (horas)')
    stalled_thread_hours = fields.Integer(default=48, string='Thread estancado (horas)')
    high_volume_threshold = fields.Integer(default=50, string='Umbral alto volumen')
    client_score_decay_days = fields.Integer(default=30, string='Ventana scoring (días)')
    cold_client_days = fields.Integer(default=14, string='Cliente frío (días)')

    # ── Helpers para ir.config_parameter ─────────────────────────────────────
    _PARAM_PREFIX = 'quimibond_intelligence'

    def _get_param(self, key, default=''):
        return self.env['ir.config_parameter'].sudo().get_param(
            f'{self._PARAM_PREFIX}.{key}', default,
        )

    def _set_param(self, key, value):
        self.env['ir.config_parameter'].sudo().set_param(
            f'{self._PARAM_PREFIX}.{key}', value or '',
        )

    @api.model
    def default_get(self, fields_list):
        """Carga valores actuales de ir.config_parameter."""
        res = super().default_get(fields_list)
        res.update({
            'service_account_json': self._get_param('service_account_json'),
            'anthropic_api_key': self._get_param('anthropic_api_key'),
            'supabase_url': self._get_param('supabase_url'),
            'supabase_key': self._get_param('supabase_key'),
            'voyage_api_key': self._get_param('voyage_api_key'),
            'recipient_email': self._get_param('recipient_email', 'jose.mizrahi@quimibond.com'),
            'target_response_hours': int(self._get_param('target_response_hours', '4')),
            'slow_response_hours': int(self._get_param('slow_response_hours', '8')),
            'no_response_hours': int(self._get_param('no_response_hours', '24')),
            'stalled_thread_hours': int(self._get_param('stalled_thread_hours', '48')),
            'high_volume_threshold': int(self._get_param('high_volume_threshold', '50')),
            'client_score_decay_days': int(self._get_param('client_score_decay_days', '30')),
            'cold_client_days': int(self._get_param('cold_client_days', '14')),
        })
        return res

    def action_save(self):
        """Guarda todos los parámetros en ir.config_parameter."""
        self.ensure_one()
        for fname in [
            'service_account_json', 'anthropic_api_key',
            'supabase_url', 'supabase_key', 'voyage_api_key',
            'recipient_email', 'target_response_hours',
            'slow_response_hours', 'no_response_hours',
            'stalled_thread_hours', 'high_volume_threshold',
            'client_score_decay_days', 'cold_client_days',
        ]:
            self._set_param(fname, str(getattr(self, fname, '')))
        return {'type': 'ir.actions.client', 'tag': 'display_notification',
                'params': {'title': 'Intelligence System',
                           'message': 'Configuración guardada correctamente.',
                           'type': 'success'}}

    def action_test_run(self):
        """Ejecuta el pipeline completo ahora (para testing)."""
        self.action_save()
        engine = self.env['intelligence.engine']
        engine.run_daily_intelligence()
        return {'type': 'ir.actions.client', 'tag': 'display_notification',
                'params': {'title': 'Intelligence System',
                           'message': 'Pipeline ejecutado. Revisa los logs.',
                           'type': 'info'}}

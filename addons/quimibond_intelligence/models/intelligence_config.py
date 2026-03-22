"""
Quimibond Intelligence — Configuración
Modelo transient para gestionar parámetros del sistema desde la UI de Odoo.
"""
import json
import logging

from odoo import api, fields, models

_logger = logging.getLogger(__name__)

# ── Valores por defecto (se usan solo si ir.config_parameter no tiene datos) ─
DEFAULT_EMAIL_ACCOUNTS = {
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


def _load_email_accounts(env):
    """Carga cuentas y departamentos desde ir.config_parameter (JSON)."""
    raw = (
        env['ir.config_parameter'].sudo()
        .get_param('quimibond_intelligence.email_accounts_json', '')
    )
    if raw:
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            _logger.warning('email_accounts_json inválido, usando defaults')
    return dict(DEFAULT_EMAIL_ACCOUNTS)


def get_email_accounts(env):
    """Retorna la lista de cuentas de email."""
    return list(_load_email_accounts(env).keys())


def get_account_departments(env):
    """Retorna el dict email→departamento."""
    return _load_email_accounts(env)


class IntelligenceConfig(models.TransientModel):
    """Settings UI para configurar el Intelligence System desde Odoo."""
    _name = 'intelligence.config'
    _description = 'Intelligence System Configuration'

    # ── API Keys (se leen/escriben en ir.config_parameter) ───────────────────
    service_account_json = fields.Text(
        string='Google Service Account JSON',
        help='JSON completo del service account con Domain-Wide Delegation',
    )
    anthropic_api_key = fields.Char(
        string='Anthropic API Key',
        groups='base.group_system',
    )
    claude_model = fields.Char(
        string='Claude Model',
        help='Model ID para Claude API (ej: claude-sonnet-4-6). Dejar vacío para usar el default.',
    )
    supabase_url = fields.Char(string='Supabase URL')
    supabase_key = fields.Char(
        string='Supabase Anon Key',
        groups='base.group_system',
    )
    supabase_service_role_key = fields.Char(
        string='Supabase Service Role Key',
        help='Requerido para escritura (bypasses RLS). Si está vacío se usa Anon Key.',
        groups='base.group_system',
    )
    voyage_api_key = fields.Char(
        string='Voyage AI API Key',
        groups='base.group_system',
    )
    recipient_email = fields.Char(string='Email destinatario del briefing')
    sender_email = fields.Char(
        string='Email remitente del briefing',
        help='Cuenta de Gmail desde la que se envían los briefings',
    )
    email_accounts_json = fields.Text(
        string='Cuentas de email (JSON)',
        help='JSON con formato {"email@dominio.com": "Departamento", ...}',
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
            'claude_model': self._get_param('claude_model'),
            'supabase_url': self._get_param('supabase_url'),
            'supabase_key': self._get_param('supabase_key'),
            'supabase_service_role_key': self._get_param('supabase_service_role_key'),
            'voyage_api_key': self._get_param('voyage_api_key'),
            'recipient_email': self._get_param('recipient_email'),
            'sender_email': self._get_param('sender_email'),
            'email_accounts_json': self._get_param(
                'email_accounts_json',
                json.dumps(DEFAULT_EMAIL_ACCOUNTS, ensure_ascii=False, indent=2),
            ),
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
        # Validar JSON de cuentas antes de guardar
        if self.email_accounts_json:
            try:
                json.loads(self.email_accounts_json)
            except (json.JSONDecodeError, TypeError):
                return {'type': 'ir.actions.client', 'tag': 'display_notification',
                        'params': {'title': 'Error',
                                   'message': 'El JSON de cuentas de email no es válido.',
                                   'type': 'danger'}}
        for fname in [
            'service_account_json', 'anthropic_api_key', 'claude_model',
            'supabase_url', 'supabase_key', 'supabase_service_role_key',
            'voyage_api_key',
            'recipient_email', 'sender_email', 'email_accounts_json',
            'target_response_hours',
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

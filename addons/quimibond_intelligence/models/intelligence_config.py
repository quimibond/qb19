"""
Quimibond Intelligence — Configuración
Modelo transient para gestionar parámetros del sistema desde la UI de Odoo.
"""
import json
import logging
import time
from datetime import datetime

from odoo import api, fields, models

_logger = logging.getLogger(__name__)

# ── Zona horaria CDMX (centralizada para todo el módulo) ─────────────────────
try:
    from zoneinfo import ZoneInfo
    TZ_CDMX = ZoneInfo('America/Mexico_City')
except ImportError:
    import pytz
    TZ_CDMX = pytz.timezone('America/Mexico_City')

# ── Lock helpers (con timeout para evitar deadlocks) ─────────────────────────

LOCK_TIMEOUT_MINUTES = 60  # Si un lock lleva más de 60 min, se libera


def acquire_lock(env, lock_name, timeout_minutes=LOCK_TIMEOUT_MINUTES):
    """Adquiere un lock basado en ir.config_parameter con timestamp.

    Retorna True si se adquirió el lock, False si ya está tomado.
    Si el lock tiene más de timeout_minutes, se fuerza el release.
    """
    ICP = env['ir.config_parameter'].sudo()
    raw = ICP.get_param(lock_name, '')
    if raw:
        try:
            locked_at = datetime.fromisoformat(raw)
            if (datetime.now() - locked_at).total_seconds() < timeout_minutes * 60:
                return False
            _logger.warning('Lock %s expirado (>%dm), forzando release',
                            lock_name, timeout_minutes)
        except (ValueError, TypeError):
            if raw == 'true':
                _logger.warning('Lock %s en formato legacy, forzando release',
                                lock_name)
    ICP.set_param(lock_name, datetime.now().isoformat())
    return True


def release_lock(env, lock_name):
    """Libera un lock basado en ir.config_parameter."""
    env['ir.config_parameter'].sudo().set_param(lock_name, '')


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

    def action_initial_setup(self):
        """Bootstrap inicial: carga datos base de Odoo y Gmail a Supabase.

        Ejecuta en secuencia (sin Claude, sin riesgo de memoria):
        1. Enrich contactos (partners/companies → Supabase)
        2. Sync productos, órdenes, equipo → Supabase
        3. Sync emails (Gmail → Supabase)
        4. Update scores (cálculo local, sin Claude)
        5. Supabase sync (push alertas/acciones pendientes)

        Después de esto, ejecutar manualmente:
        - "Analizar Emails" (usa Claude, puede tomar varios minutos)
        - "Pipeline completo" para el primer briefing
        """
        self.action_save()
        engine = self.env['intelligence.engine']
        steps = [
            ('Enrich Contactos', engine.run_enrich_only),
            ('Sync Productos/Ordenes/Equipo', engine.run_sync_odoo_tables),
            ('Sync Emails (Gmail)', engine.run_sync_emails),
            ('Actualizar Scores', engine.run_update_scores),
            ('Sync Odoo → Supabase', engine.run_supabase_sync),
        ]
        ok, failed = 0, []
        for name, fn in steps:
            try:
                _logger.info('── Setup Inicial: %s ──', name)
                # Use savepoint to isolate each step — if one fails,
                # the transaction is rolled back to the savepoint and
                # subsequent steps can still execute.
                self.env.cr.execute('SAVEPOINT setup_step')
                fn()
                self.env.cr.execute('RELEASE SAVEPOINT setup_step')
                ok += 1
            except Exception as exc:
                _logger.error('Setup Inicial — %s falló: %s', name, exc,
                              exc_info=True)
                failed.append(name)
                try:
                    self.env.cr.execute(
                        'ROLLBACK TO SAVEPOINT setup_step')
                except Exception:
                    pass  # savepoint may already be released

        if failed:
            msg = f'{ok}/{len(steps)} pasos OK. Fallaron: {", ".join(failed)}. Revisa los logs.'
            ntype = 'warning'
        else:
            msg = (f'{ok}/{len(steps)} pasos completados. '
                   'Ahora ejecuta "Analizar Emails" para el análisis con Claude.')
            ntype = 'success'
        return {'type': 'ir.actions.client', 'tag': 'display_notification',
                'params': {'title': 'Setup Inicial', 'message': msg, 'type': ntype}}

    def action_enrich_only(self):
        """Sincroniza partners de Odoo a Supabase (Odoo = fuente de verdad)."""
        self.action_save()
        engine = self.env['intelligence.engine']
        engine.run_enrich_only()
        return {'type': 'ir.actions.client', 'tag': 'display_notification',
                'params': {'title': 'Sync Odoo → Supabase',
                           'message': 'Partners sincronizados con detalle completo. Revisa los logs.',
                           'type': 'info'}}

    def action_sync_emails(self):
        """Ejecuta sync de emails Gmail → Supabase."""
        self.action_save()
        self.env['intelligence.engine'].run_sync_emails()
        return {'type': 'ir.actions.client', 'tag': 'display_notification',
                'params': {'title': 'Sync Emails',
                           'message': 'Emails sincronizados. Revisa los logs.',
                           'type': 'info'}}

    def action_analyze_emails(self):
        """Ejecuta análisis de emails con Claude."""
        self.action_save()
        self.env['intelligence.engine'].run_analyze_emails()
        return {'type': 'ir.actions.client', 'tag': 'display_notification',
                'params': {'title': 'Analizar Emails',
                           'message': 'Análisis completado. Revisa los logs.',
                           'type': 'info'}}

    def action_update_scores(self):
        """Recalcula scores de clientes."""
        self.action_save()
        self.env['intelligence.engine'].run_update_scores()
        return {'type': 'ir.actions.client', 'tag': 'display_notification',
                'params': {'title': 'Actualizar Scores',
                           'message': 'Scores actualizados. Revisa los logs.',
                           'type': 'info'}}

    def action_supabase_sync(self):
        """Empuja cambios pendientes de Odoo a Supabase."""
        self.action_save()
        self.env['intelligence.engine'].run_supabase_sync()
        return {'type': 'ir.actions.client', 'tag': 'display_notification',
                'params': {'title': 'Supabase Sync',
                           'message': 'Cambios sincronizados a Supabase. Revisa los logs.',
                           'type': 'info'}}

    def action_run_predictions(self):
        """Ejecuta pipeline de predicciones (churn, volumen, oportunidades)."""
        self.action_save()
        self.env['intelligence.engine'].run_predictions()
        return {'type': 'ir.actions.client', 'tag': 'display_notification',
                'params': {'title': 'Predicciones',
                           'message': 'Predicciones ejecutadas. Revisa alertas generadas.',
                           'type': 'info'}}

    def action_sync_odoo_tables(self):
        """Sync productos, líneas de orden y usuarios a Supabase."""
        self.action_save()
        self.env['intelligence.engine'].run_sync_odoo_tables()
        return {'type': 'ir.actions.client', 'tag': 'display_notification',
                'params': {'title': 'Sync Odoo Tables',
                           'message': 'Productos, órdenes y usuarios sincronizados. Revisa los logs.',
                           'type': 'info'}}

    def action_check_briefing_replies(self):
        """Busca replies al briefing y genera respuestas."""
        self.action_save()
        self.env['intelligence.engine'].run_check_briefing_replies()
        return {'type': 'ir.actions.client', 'tag': 'display_notification',
                'params': {'title': 'Briefing Replies',
                           'message': 'Replies procesados. Revisa los logs.',
                           'type': 'info'}}

    def action_run_diagnostics(self):
        """Ejecuta diagnóstico completo de todos los servicios.

        Resultados se imprimen en logs de Odoo.sh con formato fácil de copiar.
        """
        self.action_save()
        L = _logger.info
        W = _logger.warning
        E = _logger.error

        L('┌─────────────────────────────────────────────────────────────┐')
        L('│           QUIMIBOND INTELLIGENCE — DIAGNÓSTICO             │')
        L('└─────────────────────────────────────────────────────────────┘')

        results = {}
        start = time.time()

        # ── 1. Configuración ──
        L('──── TEST 1: Configuración ────')
        get = lambda k, d='': (
            self.env['ir.config_parameter'].sudo()
            .get_param(f'quimibond_intelligence.{k}', d)
        )
        sa_json = get('service_account_json')
        anthropic_key = get('anthropic_api_key')
        supa_url = get('supabase_url')
        supa_key = get('supabase_key')
        supa_srv_key = get('supabase_service_role_key')
        voyage_key = get('voyage_api_key')
        claude_model = get('claude_model') or 'claude-sonnet-4-6 (default)'

        checks = {
            'service_account_json': bool(sa_json),
            'anthropic_api_key': bool(anthropic_key),
            'supabase_url': bool(supa_url),
            'supabase_key': bool(supa_key or supa_srv_key),
            'voyage_api_key': bool(voyage_key),
        }
        for name, ok in checks.items():
            L('  %s %s', '✓' if ok else '✗', name)
        L('  claude_model: %s', claude_model)
        L('  supabase_url: %s', supa_url or '(vacío)')
        L('  supabase_key_type: %s',
          'service_role' if supa_srv_key else ('anon' if supa_key else 'NINGUNA'))
        results['config'] = all(checks.values())

        # ── 2. Odoo Models ──
        L('──── TEST 2: Modelos Odoo ────')
        odoo_models = {
            'res.partner': 0,
            'sale.order': 0,
            'account.move': 0,
            'purchase.order': 0,
            'crm.lead': 0,
            'stock.picking': 0,
            'mrp.production': 0,
            'mail.activity': 0,
        }
        for model_name in list(odoo_models.keys()):
            try:
                Model = self.env[model_name].sudo()
                count = Model.search_count([])
                odoo_models[model_name] = count
                L('  ✓ %s: %d registros', model_name, count)
            except Exception as exc:
                W('  ✗ %s: %s', model_name, exc)
                odoo_models[model_name] = -1

        # Partners con email (los que se sincronizan)
        try:
            partner_count = self.env['res.partner'].sudo().search_count([
                ('email', '!=', False), ('email', '!=', ''),
                ('active', '=', True),
                '|', ('customer_rank', '>', 0), ('supplier_rank', '>', 0),
            ])
            L('  ✓ partners sincronizables (email + cliente/proveedor): %d', partner_count)
        except Exception as exc:
            W('  ✗ partners sincronizables: %s', exc)
            partner_count = 0

        results['odoo'] = partner_count > 0

        # ── 3. Supabase ──
        L('──── TEST 3: Supabase ────')
        supa_effective_key = supa_srv_key or supa_key
        if supa_url and supa_effective_key:
            try:
                from ..services.supabase_service import SupabaseService
                with SupabaseService(supa_url, supa_effective_key) as supa:
                    tables_to_check = [
                        ('contacts', 'id'),
                        ('emails', 'id'),
                        ('companies', 'id'),
                        ('entities', 'id'),
                        ('facts', 'id'),
                        ('customer_health_scores', 'id'),
                        ('revenue_metrics', 'id'),
                        ('daily_summaries', 'id'),
                        ('alerts', 'id'),
                        ('account_summaries', 'id'),
                        ('threads', 'id'),
                    ]
                    supa_ok = True
                    for table, col in tables_to_check:
                        try:
                            rows = supa._request(
                                f'/rest/v1/{table}?select={col}&limit=1',
                            )
                            count_rows = supa._request(
                                f'/rest/v1/{table}?select={col}',
                                extra_headers={'Prefer': 'count=exact', 'Range-Unit': 'items', 'Range': '0-0'},
                            )
                            # Get count from content-range header via a HEAD-like approach
                            # Simple approach: just report accessible
                            L('  ✓ %s: accesible', table)
                        except Exception as exc:
                            err_str = str(exc)
                            if '404' in err_str or 'does not exist' in err_str:
                                W('  ✗ %s: tabla no existe', table)
                            else:
                                W('  ✗ %s: %s', table, err_str[:120])
                            supa_ok = False

                    # Test RPC functions
                    rpcs = [
                        'resolve_all_identities',
                        'refresh_contact_360',
                        'detect_cross_department_topics',
                    ]
                    for rpc in rpcs:
                        try:
                            supa._request(f'/rest/v1/rpc/{rpc}', 'POST', {})
                            L('  ✓ rpc/%s: OK', rpc)
                        except Exception as exc:
                            W('  ✗ rpc/%s: %s', rpc, str(exc)[:120])

                results['supabase'] = supa_ok
            except Exception as exc:
                E('  ✗ Supabase conexión fallida: %s', exc)
                results['supabase'] = False
        else:
            W('  ✗ Supabase: faltan credenciales')
            results['supabase'] = False

        # ── 4. Claude API ──
        L('──── TEST 4: Claude API ────')
        if anthropic_key:
            try:
                import anthropic as anthropic_sdk
                client = anthropic_sdk.Anthropic(
                    api_key=anthropic_key,
                    max_retries=1,
                    timeout=30.0,
                )
                # Test model access
                preferred = get('claude_model') or 'claude-sonnet-4-6'
                try:
                    model_info = client.models.retrieve(model_id=preferred)
                    L('  ✓ modelo %s: accesible (created: %s)', preferred, model_info.created_at)
                except Exception as exc:
                    W('  ✗ modelo %s: %s', preferred, exc)
                    # Try listing available models
                    try:
                        available = client.models.list(limit=20)
                        model_ids = [m.id for m in available.data if 'claude' in m.id]
                        L('  ℹ modelos disponibles: %s', ', '.join(model_ids[:10]))
                    except Exception as exc2:
                        W('  ✗ listar modelos: %s', exc2)

                # Test a minimal call
                try:
                    t0 = time.time()
                    resp = client.messages.create(
                        model=preferred,
                        max_tokens=50,
                        messages=[{'role': 'user', 'content': 'Responde solo "OK" sin nada más.'}],
                    )
                    latency = time.time() - t0
                    text = resp.content[0].text if resp.content else '(vacío)'
                    L('  ✓ test call: "%s" (%.1fs, in=%d out=%d tokens)',
                      text.strip()[:30], latency,
                      resp.usage.input_tokens, resp.usage.output_tokens)
                    L('  ✓ anthropic SDK version: %s', anthropic_sdk.__version__)
                    results['claude'] = True
                except Exception as exc:
                    E('  ✗ test call falló: %s', exc)
                    results['claude'] = False
            except Exception as exc:
                E('  ✗ Claude init: %s', exc)
                results['claude'] = False
        else:
            W('  ✗ Claude: falta anthropic_api_key')
            results['claude'] = False

        # ── 5. Gmail ──
        L('──── TEST 5: Gmail ────')
        if sa_json:
            try:
                sa_info = json.loads(sa_json)
                L('  ✓ service account parsed: %s', sa_info.get('client_email', '?'))
                from ..services.gmail_service import GmailService
                gmail = GmailService(sa_info)

                email_accounts = list(_load_email_accounts(self.env).keys())
                L('  ℹ cuentas configuradas: %d', len(email_accounts))

                # Test first account only
                if email_accounts:
                    test_acct = email_accounts[0]
                    try:
                        t0 = time.time()
                        msgs, _hid = gmail.fetch_emails(test_acct, max_results=1)
                        latency = time.time() - t0
                        L('  ✓ Gmail %s: %d msgs (%.1fs)', test_acct, len(msgs), latency)
                        results['gmail'] = True
                    except Exception as exc:
                        E('  ✗ Gmail %s: %s', test_acct, exc)
                        results['gmail'] = False
                else:
                    W('  ✗ Gmail: no hay cuentas configuradas')
                    results['gmail'] = False
            except json.JSONDecodeError:
                E('  ✗ service_account_json inválido (no es JSON)')
                results['gmail'] = False
            except Exception as exc:
                E('  ✗ Gmail: %s', exc)
                results['gmail'] = False
        else:
            W('  ✗ Gmail: falta service_account_json')
            results['gmail'] = False

        # ── 6. Voyage AI ──
        L('──── TEST 6: Voyage AI ────')
        if voyage_key:
            try:
                import httpx as _httpx
                t0 = time.time()
                resp = _httpx.post(
                    'https://api.voyageai.com/v1/embeddings',
                    headers={'Authorization': f'Bearer {voyage_key}',
                             'Content-Type': 'application/json'},
                    json={'model': 'voyage-3', 'input': ['test'], 'input_type': 'document'},
                    timeout=15.0,
                )
                latency = time.time() - t0
                if resp.status_code == 200:
                    data = resp.json()
                    dim = len(data.get('data', [{}])[0].get('embedding', []))
                    L('  ✓ Voyage AI: OK (dim=%d, %.1fs)', dim, latency)
                    results['voyage'] = True
                else:
                    W('  ✗ Voyage AI: HTTP %d — %s', resp.status_code, resp.text[:120])
                    results['voyage'] = False
            except Exception as exc:
                E('  ✗ Voyage AI: %s', exc)
                results['voyage'] = False
        else:
            L('  ⊘ Voyage AI: no configurado (opcional)')
            results['voyage'] = None

        # ── RESUMEN ──
        elapsed = time.time() - start
        L('┌─────────────────────────────────────────────────────────────┐')
        L('│                    RESUMEN DIAGNÓSTICO                     │')
        L('├─────────────────────────────────────────────────────────────┤')
        for svc, ok in results.items():
            if ok is None:
                status = '⊘ SKIP'
            elif ok:
                status = '✓ OK'
            else:
                status = '✗ FAIL'
            L('│  %-15s %s', svc, status)
        L('├─────────────────────────────────────────────────────────────┤')
        all_critical = all(v for k, v in results.items()
                          if k != 'voyage' and v is not None)
        if all_critical:
            L('│  RESULTADO: ✓ TODOS LOS SERVICIOS CRÍTICOS OK            │')
        else:
            failed = [k for k, v in results.items()
                      if v is False]
            L('│  RESULTADO: ✗ SERVICIOS CON ERROR: %-23s│', ', '.join(failed))
        L('│  Tiempo total: %.1fs                                       │', elapsed)
        L('└─────────────────────────────────────────────────────────────┘')

        ok_count = sum(1 for v in results.values() if v is True)
        total = sum(1 for v in results.values() if v is not None)
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': 'Diagnóstico completado',
                'message': f'{ok_count}/{total} servicios OK. Revisa los logs para el detalle.',
                'type': 'success' if all_critical else 'warning',
            },
        }

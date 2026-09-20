"""
Push Odoo → Supabase: el puente mínimo para que la memoria de correo ligue
correos con Odoo.

Desde el 2026-09-18 Supabase guarda solo la memoria de correo; todo lo demás
(ventas, finanzas, inventario, SAT, costeo) vive en Odoo. Lo único que se
empuja:

  contacts → tablas `contacts` + `companies`  (res.partner)
  users    → tabla `odoo_users`               (res.users + hr.employee)

Un cron horario, una función: push_to_supabase(). Lee del ORM, escribe por
REST. Los métodos `_push_contacts` y `_push_users` viven en
sync_push_partners.py.
"""
import logging
import re
from datetime import datetime, timedelta

from odoo import api, models

from .supabase_client import SupabaseClient

_logger = logging.getLogger(__name__)

# Email validation regex
_EMAIL_RE = re.compile(r'^[^@\s]+@[^@\s]+\.[^@\s]+$')


def _get_client(env) -> SupabaseClient | None:
    """Build Supabase client from Odoo config parameters."""
    get = lambda k: env['ir.config_parameter'].sudo().get_param(k) or ''
    url = get('quimibond_intelligence.supabase_url')
    key = get('quimibond_intelligence.supabase_service_key')
    if not url or not key:
        _logger.error('Supabase URL or service key not configured')
        return None
    return SupabaseClient(url, key)


def _commercial_partner_id(partner) -> int | None:
    """Resolve commercial partner ID (parent company)."""
    cp = partner.commercial_partner_id
    return cp.id if cp else partner.id


# H9 — partner name validation (audit 2026-04-16)
# Odoo produce partners con names como "8141", "5806" o strings de 1-2
# caracteres, a menudo importados desde sistemas legacy. Antes se
# pusheaban tal cual y aparecían como "8141" en /companies. Este helper
# devuelve el mejor nombre disponible o None (skip) aplicando la misma
# regla de frontend `sanitizeCompanyName`.
_NUMERIC_ONLY = re.compile(r'^[0-9]+$')


def _best_partner_name(partner) -> str | None:
    """Devuelve el mejor nombre disponible para un partner.

    Orden de preferencia:
      1. `partner.name` si es real (no vacío, no numérico puro, >=3 chars)
      2. `commercial_partner_id.name` si es real (partner pertenece a una
         empresa padre con nombre bueno)
      3. `partner.vat` (RFC) — identificable aunque feo
      4. Dominio del primer email (`@acme.com` → `acme.com`)
      5. None — el caller debe skip.
    """
    def _clean(s):
        if not s:
            return None
        t = s.strip()
        if not t or len(t) < 3 or _NUMERIC_ONLY.match(t):
            return None
        return t

    # 1. Partner.name directo
    name = _clean(partner.name)
    if name:
        return name

    # 2. Commercial parent
    try:
        cp = partner.commercial_partner_id
        if cp and cp.id != partner.id:
            name = _clean(cp.name)
            if name:
                return name
    except Exception:
        pass

    # 3. VAT / RFC
    try:
        name = _clean(partner.vat)
        if name:
            return name
    except Exception:
        pass

    # 4. Email domain
    try:
        raw = (partner.email or '').split(',')[0].split(';')[0].strip()
        if '@' in raw:
            dom = raw.split('@')[-1].strip().lower()
            dom = _clean(dom)
            if dom and dom not in {
                'gmail.com', 'hotmail.com', 'outlook.com', 'yahoo.com',
                'live.com', 'icloud.com', 'protonmail.com', 'outlook.es',
            }:
                return dom
    except Exception:
        pass

    return None


class QuimibondSync(models.TransientModel):
    _name = 'quimibond.sync'
    _description = 'Quimibond Sync Engine'

    # Compañía operativa. `_push_contacts` la usa para encontrar los partners
    # con facturas del último año aunque no tengan customer/supplier_rank.
    # Config param quimibond_intelligence.company_id (default: 1).
    def _get_company_id(self):
        """Return the operating company ID for filtering multi-company data."""
        ICP = self.env['ir.config_parameter'].sudo()
        cid = ICP.get_param('quimibond_intelligence.company_id', '1')
        return int(cid)

    # Multi-company: if quimibond_intelligence.company_ids is set (comma-
    # separated list), usa esa lista. Si no, cae al single-company legacy
    # [_get_company_id()] para backward-compat. Ejemplo:
    #   env['ir.config_parameter'].sudo().set_param(
    #     'quimibond_intelligence.company_ids', '1,2,3,4,16')
    def _get_company_ids(self):
        """Return the list of company IDs to include in the push."""
        ICP = self.env['ir.config_parameter'].sudo()
        raw = (ICP.get_param('quimibond_intelligence.company_ids') or '').strip()
        if raw:
            try:
                return [int(x.strip()) for x in raw.split(',') if x.strip()]
            except (ValueError, TypeError):
                pass
        return [self._get_company_id()]

    # Lo único que Odoo empuja a Supabase. El orden es el de ejecución.
    PUSH_MODELS = ('contacts', 'users')
    PUSH_MODELS_DEFAULT = 'contacts,users'

    # Tablas que SIEMPRE hacen full push (no incremental por write_date).
    # `users` es un catálogo chico (<200 filas) y su write_date no se toca
    # cuando cambia el empleado/departamento ligado; re-enviarlo cuesta <1s.
    FULL_PUSH_METHODS = frozenset(['users'])

    def _push_models_allowed(self):
        """Conjunto de métodos _push_* que corren. Parámetro
        quimibond_intelligence.push_models: 'all' (= contacts y users) o
        lista con comas tomada de PUSH_MODELS. Nombres desconocidos se
        ignoran con aviso: desde el 2026-09-18 ya no existe nada más que
        empujar (las tablas odoo_* de Supabase se borraron)."""
        raw = (self.env['ir.config_parameter'].sudo().get_param(
            'quimibond_intelligence.push_models') or self.PUSH_MODELS_DEFAULT).strip()
        if raw.lower() == 'all':
            return set(self.PUSH_MODELS)
        wanted = {x.strip() for x in raw.split(',') if x.strip()}
        unknown = wanted - set(self.PUSH_MODELS)
        if unknown:
            _logger.warning(
                'push_models: se ignoran %s (solo existen %s)',
                ', '.join(sorted(unknown)), ', '.join(self.PUSH_MODELS))
        return wanted & set(self.PUSH_MODELS)

    def _run_push(self, client, label, method_fn, last_sync=None):
        """Ejecuta un metodo _push_* aislado: cualquier excepcion queda
        capturada (no tumba el resto del sync) y loggea a Supabase
        pipeline_logs con phase='odoo_push'. La vista odoo_push_last_events
        (la lee el watchdog de la memoria, `memoria_watchdog`) sale de aquí.

        Para tablas en FULL_PUSH_METHODS fuerza last_sync=None.
        """
        method_start = datetime.now()
        status = 'success'
        error_msg = None
        rows = 0
        effective_last_sync = None if label in self.FULL_PUSH_METHODS else last_sync

        try:
            rows = method_fn(client, last_sync=effective_last_sync) or 0
        except Exception as exc:
            status = 'error'
            error_msg = str(exc)[:500]
            _logger.exception('Push %s failed', label)

        elapsed = (datetime.now() - method_start).total_seconds()

        # Loggea a Supabase (best-effort: si el log mismo falla, seguimos).
        try:
            client.insert('pipeline_logs', [{
                'level': 'error' if status == 'error' else 'info',
                'phase': 'odoo_push',
                'message': (
                    f'[{label}] {rows} rows pushed in {elapsed:.1f}s'
                    if status == 'success'
                    else f'[{label}] FAILED after {elapsed:.1f}s: {error_msg}'
                ),
                'details': {
                    'method': label,
                    'rows': rows,
                    'status': status,
                    'elapsed_s': round(elapsed, 1),
                    'error': error_msg,
                    'last_sync': last_sync.strftime('%Y-%m-%d %H:%M:%S') if last_sync else None,
                    'full_push': label in self.FULL_PUSH_METHODS,
                },
            }])
        except Exception as log_exc:
            _logger.warning('Failed to log push metric: %s', log_exc)

        return rows

    @api.model
    def push_to_supabase(self):
        """Cron horario: empuja contactos/empresas y usuarios a Supabase."""
        client = _get_client(self.env)
        if not client:
            return

        # Incremental por write_date desde el último push exitoso.
        ICP = self.env['ir.config_parameter'].sudo()
        last_sync_str = ICP.get_param('quimibond_intelligence.last_sync_date', '')

        # Re-push completo por única vez: quimibond_intelligence.force_full_sync=1
        # ignora last_sync y se limpia al terminar. Útil tras corregir datos
        # en Supabase o si algún contacto quedó fuera.
        force_full = ICP.get_param('quimibond_intelligence.force_full_sync', '')
        if force_full:
            last_sync_str = ''
            _logger.info('Full sync forced via force_full_sync parameter')

        last_sync = None
        if last_sync_str:
            try:
                last_sync = datetime.strptime(last_sync_str, '%Y-%m-%d %H:%M:%S')
                # Add 1-minute overlap to avoid missing records
                last_sync = last_sync - timedelta(minutes=1)
            except (ValueError, TypeError):
                last_sync = None

        _start = datetime.now()
        try:
            methods = [
                ('contacts', self._push_contacts),
                ('users', self._push_users),
            ]
            allowed = self._push_models_allowed()
            methods = [(label, fn) for label, fn in methods if label in allowed]
            totals = {}
            for label, fn in methods:
                totals[label] = self._run_push(client, label, fn, last_sync=last_sync)

            summary = ', '.join(f'{k}={v}' for k, v in totals.items() if v)
            _logger.info('✓ Push to Supabase: %s', summary or 'no changes')
            elapsed = (datetime.now() - _start).total_seconds()
            self.env['quimibond.sync.log'].sudo().create({
                'name': 'Push completo',
                'direction': 'push',
                'status': 'success',
                'summary': summary or 'sin cambios',
                'duration_seconds': round(elapsed, 1),
            })
            # Save sync timestamp for next incremental run
            ICP.set_param('quimibond_intelligence.last_sync_date',
                          _start.strftime('%Y-%m-%d %H:%M:%S'))
            # Clear one-time full sync flag
            if force_full:
                ICP.set_param('quimibond_intelligence.force_full_sync', '')
            self.env.cr.commit()
        except Exception as exc:
            _logger.error('Push to Supabase failed: %s', exc)
            try:
                self.env['quimibond.sync.log'].sudo().create({
                    'name': 'Push fallido',
                    'direction': 'push',
                    'status': 'error',
                    'summary': str(exc)[:500],
                })
                self.env.cr.commit()
            except Exception:
                pass
        finally:
            client.close()

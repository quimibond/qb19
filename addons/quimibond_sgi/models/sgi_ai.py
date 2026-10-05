# -*- coding: utf-8 -*-
"""57.100.0 (sección 7 del reporte, «IA que propone y la persona decide»;
puerta Q16): sugerencia de cláusula y clasificación y borrador de 5 porqués e
Ishikawa en la NC.

- La IA llena solo campos de sugerencia (``sgi_ai_*``, de solo lectura; por
  RPC se rechazan). La persona decide con un botón: «Usar cláusula y
  clasificación sugeridas» y «Copiar el borrador de porqués» escriben con el
  usuario que pulsa (queda en el seguimiento a su nombre). Nada escribe la
  causa raíz, la etapa, la eficacia ni cierra la NC.
- Se manda solo el texto de la NC (título, desviación, descripción, origen,
  proceso, producto, lote) y la lista de cláusulas, con correos, teléfonos y
  RFC tachados; opcionalmente la desviación y la causa raíz de hasta 3 NC
  cerradas del mismo proceso. Nunca cliente o proveedor, N° NCR, usuarios,
  responsables, adjuntos ni el chatter.
- Proveedor: Claude de Anthropic con el SDK ``anthropic`` (requirements.txt),
  sin streaming, con el respaldo del servidor ante un rechazo
  (``fallbacks: default``). Modelo en ``quimibond_sgi.ai_model``. La llave
  (``quimibond_sgi.ai_api_key``) la captura un administrador; nunca se
  siembra ni se escribe en el log.
- Apagada por omisión (``quimibond_sgi.ai_enabled = False``) hasta la
  autorización escrita de Jose. Toda falla del proveedor es un ``warning`` en
  el log (sin el texto de la NC) y un ``UserError`` en español; nunca ERROR.
"""
import json
import logging
import re

from odoo import api, fields, models
from odoo.exceptions import UserError
from odoo.tools import html2plaintext

_logger = logging.getLogger(__name__)

P_ENABLED = 'quimibond_sgi.ai_enabled'
P_BACKEND = 'quimibond_sgi.ai_backend'
P_MODEL = 'quimibond_sgi.ai_model'
P_KEY = 'quimibond_sgi.ai_api_key'
P_TIMEOUT = 'quimibond_sgi.ai_timeout'
P_HISTORY = 'quimibond_sgi.ai_include_history'
DEFAULT_BACKEND = 'anthropic'
DEFAULT_MODEL = 'claude-opus-5-5'
MAX_TOKENS = 16000
FALLBACK_BETA = 'server-side-fallback-2026-07-01'
CLASSIFICATIONS = ('mayor', 'menor', 'observacion')
SIXM = ('mano_de_obra', 'metodo', 'maquina', 'material', 'medicion', 'medio_ambiente')
SIXM_LABELS = {'mano_de_obra': "Mano de obra", 'metodo': "Método", 'maquina': "Máquina",
               'material': "Material", 'medicion': "Medición",
               'medio_ambiente': "Medio ambiente"}

_SCRUB = (
    (re.compile(r'[\w.+-]+@[\w-]+(?:\.[\w-]+)+'), '[correo]'),
    (re.compile(r'\b[A-ZÑ&]{3,4}\d{6}[A-Z0-9]{3}\b', re.I), '[RFC]'),
)
# Teléfonos: candidatos con dígitos, espacios, puntos, guiones, paréntesis o
# «+», sin pegarse a letras, «/» ni «-» (folios como NCI-2026-0012, fechas
# 05/10/2026). Se tachan con 10 o más dígitos (número de México, con o sin
# lada internacional) o con 8 o más si traen paréntesis o «+», o si los
# antecede «tel», «cel», «whatsapp» o «ext». Así sobreviven lotes, órdenes,
# fechas y cantidades («Lote 1234567», «OP 20261005», «2500 3000 kg»).
_PHONE_CANDIDATE = re.compile(r'(?<![\w/-])\+?\(?\d[\d\s().-]{6,}\d(?![\w/-])')
_PHONE_CUE = re.compile(r'(?:tel|tél|cel|móvil|movil|whats\w*|ext)\W{0,6}$', re.I)


def _scrub_phones(text):
    def repl(match):
        chunk = match.group(0)
        digits = sum(ch.isdigit() for ch in chunk)
        before = text[max(0, match.start() - 14):match.start()]
        if digits >= 10 or (digits >= 8 and ('(' in chunk or '+' in chunk
                                             or _PHONE_CUE.search(before))):
            return '[teléfono]'
        return chunk
    return _PHONE_CANDIDATE.sub(repl, text)


SCHEMA = {
    'type': 'object', 'additionalProperties': False,
    'required': ['clausula_id', 'clasificacion', 'motivo', 'porques', 'ishikawa'],
    'properties': {
        'clausula_id': {'type': 'integer'},
        'clasificacion': {'type': 'string', 'enum': list(CLASSIFICATIONS)},
        'motivo': {'type': 'string'},
        'porques': {'type': 'array', 'items': {'type': 'string'}},
        'ishikawa': {'type': 'object', 'additionalProperties': False,
                     'required': list(SIXM),
                     'properties': {k: {'type': 'string'} for k in SIXM}},
    },
}

SYSTEM = (
    "Usted apoya al responsable de una no conformidad (NC) del Sistema de Gestión "
    "Integral de una planta textil (ISO 9001, 14001 y 45001). Proponga, sin decidir: "
    "(1) la cláusula incumplida, eligiendo SOLO un id de la lista dada; (2) la "
    "clasificación: mayor (falla del sistema o riesgo al cliente o a la persona), menor "
    "(falla puntual) u observación; (3) en «motivo», por qué, en dos o tres frases; (4) "
    "una cadena de hasta 5 porqués, de lo observado a la causa probable, cada uno en "
    "una frase; (5) causas posibles por cada una de las 6M (cadena vacía si no hay). "
    "Escriba en español de México. No invente datos que no estén en el texto. Responda "
    "solo con un objeto JSON con las claves clausula_id (entero), clasificacion "
    "(mayor, menor u observacion), motivo (texto), porques (lista de textos) e ishikawa "
    "(objeto con mano_de_obra, metodo, maquina, material, medicion y medio_ambiente). "
    "Esquema: " + json.dumps(SCHEMA, ensure_ascii=False)
)


def sgi_ai_scrub(text):
    """Tacha correos, RFC y teléfonos del texto que sale a la IA."""
    text = text or ''
    for pattern, repl in _SCRUB:
        text = pattern.sub(repl, text)
    return _scrub_phones(text)


def _truthy(value):
    return str(value or '').strip().lower() in ('1', 'true', 'yes', 'si', 'sí')


def _json_from_text(text):
    """El JSON de la respuesta (acepta que venga entre ```)."""
    text = (text or '').strip()
    fenced = re.match(r'^```(?:json)?\s*(.*?)\s*```$', text, re.S)
    if fenced:
        text = fenced.group(1)
    return json.loads(text)


class SgiAiClient(models.AbstractModel):
    _name = 'sgi.ai.client'
    _description = "Cliente de IA del SGI (sugerencias)"

    @api.model
    def _sgi_ai_enabled(self):
        return _truthy(self.env['ir.config_parameter'].sudo().get_param(P_ENABLED, 'False'))

    @api.model
    def _sgi_ai_conf(self):
        """(proveedor, modelo, segundos). C-2: solo Anthropic; cualquier otro
        valor del parámetro cae a Anthropic."""
        Param = self.env['ir.config_parameter'].sudo()
        backend = (Param.get_param(P_BACKEND, DEFAULT_BACKEND) or '').strip()
        if backend != DEFAULT_BACKEND:
            backend = DEFAULT_BACKEND
        model = (Param.get_param(P_MODEL) or '').strip()
        if model in ('', 'default'):
            model = DEFAULT_MODEL
        raw = Param.get_param(P_TIMEOUT, '60')
        try:
            timeout = max(float(raw), 5.0)
        except (TypeError, ValueError):
            timeout = 60.0
        return backend, model, timeout

    @api.model
    def _sgi_ai_request(self, system, user_text, schema):
        """(dict, nombre del modelo). Toda falla del proveedor → UserError en
        español y un warning en el log, sin el texto de la NC."""
        _backend, model, timeout = self._sgi_ai_conf()
        return self._sgi_ai_request_anthropic(system, user_text, schema, model, timeout), model

    @api.model
    def _sgi_anthropic_client(self, timeout):
        try:
            import anthropic
        except ImportError:
            _logger.warning("SGI IA: el servidor no tiene el paquete «anthropic».")
            raise UserError("El servidor no tiene instalado el paquete «anthropic»; avise al "
                            "administrador.") from None
        key = (self.env['ir.config_parameter'].sudo().get_param(P_KEY) or '').strip()
        if not key:
            raise UserError("Falta la llave de la IA (parámetro quimibond_sgi.ai_api_key). "
                            "La captura un administrador.")
        return anthropic.Anthropic(api_key=key, timeout=timeout, max_retries=2)

    @api.model
    def _sgi_ai_error_message(self, exc):
        """Mensaje en «usted» según el error del SDK (el más específico primero)."""
        try:
            import anthropic
        except ImportError:
            anthropic = None
        if anthropic is not None:
            if isinstance(exc, anthropic.APITimeoutError):
                return "La IA tardó demasiado en responder. Intente de nuevo en unos minutos."
            if isinstance(exc, anthropic.APIConnectionError):
                return "No se pudo conectar con la IA. Intente de nuevo en unos minutos."
            if isinstance(exc, anthropic.RateLimitError):
                return "La IA está saturada en este momento. Intente de nuevo en unos minutos."
            if isinstance(exc, (anthropic.AuthenticationError, anthropic.PermissionDeniedError)):
                return "La llave de la IA no es válida o no tiene permiso; avise al administrador."
            if isinstance(exc, anthropic.APIStatusError):
                return "La IA no pudo atender la solicitud (error %s). Intente más tarde." % (
                    getattr(exc, 'status_code', '?'))
        return "La IA no respondió. Intente más tarde."

    @api.model
    def _sgi_ai_request_anthropic(self, system, user_text, schema, model, timeout):
        client = self._sgi_anthropic_client(timeout)
        try:
            msg = client.messages.create(
                model=model, max_tokens=MAX_TOKENS, system=system,
                messages=[{'role': 'user', 'content': user_text}],
                # Respaldo del servidor ante un rechazo de los filtros de
                # seguridad; por extra_* para no depender de la versión del SDK.
                extra_headers={'anthropic-beta': FALLBACK_BETA},
                extra_body={'fallbacks': 'default'})
        except Exception as exc:  # noqa: BLE001 — anthropic.APIError, red o SDK viejo
            _logger.warning("SGI IA: la llamada a Anthropic falló (%s).", type(exc).__name__)
            raise UserError(self._sgi_ai_error_message(exc)) from None
        stop = getattr(msg, 'stop_reason', None)
        if stop == 'refusal':
            _logger.warning("SGI IA: Anthropic rechazó la solicitud.")
            raise UserError("La IA no dio sugerencia para esta NC. Analícela sin ella.")
        if stop == 'max_tokens':
            _logger.warning("SGI IA: la respuesta se cortó por el límite de salida.")
            raise UserError("La respuesta de la IA llegó incompleta. Intente de nuevo.")
        text = "".join(getattr(block, 'text', '') or '' for block in (msg.content or [])
                       if getattr(block, 'type', None) == 'text')
        try:
            data = _json_from_text(text)
        except ValueError:
            _logger.warning("SGI IA: la respuesta no fue JSON.")
            raise UserError("La IA no dio una sugerencia utilizable.") from None
        if not isinstance(data, dict):
            _logger.warning("SGI IA: la respuesta no fue un objeto JSON.")
            raise UserError("La IA no dio una sugerencia utilizable.")
        usage = getattr(msg, 'usage', None)
        if usage is not None:
            _logger.info("SGI IA: sugerencia con %s (%s tokens de entrada, %s de salida).",
                         model, getattr(usage, 'input_tokens', '?'),
                         getattr(usage, 'output_tokens', '?'))
        return data


class QualityAlertAi(models.Model):
    _inherit = 'quality.alert'

    _SGI_AI_FIELDS = ('sgi_ai_clause_id', 'sgi_ai_classification', 'sgi_ai_reason',
                      'sgi_ai_whys', 'sgi_ai_ishikawa', 'sgi_ai_date', 'sgi_ai_model')

    sgi_ai_clause_id = fields.Many2one(
        'sgi.norm.clause', string="Cláusula sugerida (IA)", readonly=True, copy=False,
        ondelete='set null', help="Cláusula que propone la IA. No cambia la NC.")
    sgi_ai_classification = fields.Selection(
        [('mayor', "Mayor"), ('menor', "Menor"), ('observacion', "Observación")],
        string="Clasificación sugerida (IA)", readonly=True, copy=False,
        help="Clasificación que propone la IA. No cambia la NC.")
    sgi_ai_reason = fields.Text(string="Por qué lo sugiere la IA", readonly=True, copy=False)
    sgi_ai_whys = fields.Text(string="Borrador de 5 porqués (IA)", readonly=True, copy=False)
    sgi_ai_ishikawa = fields.Text(string="Borrador de Ishikawa 6M (IA)", readonly=True, copy=False)
    sgi_ai_date = fields.Datetime(string="Sugerencia del", readonly=True, copy=False)
    sgi_ai_model = fields.Char(string="Modelo de IA", readonly=True, copy=False)
    sgi_ai_available = fields.Boolean(
        string="IA disponible", compute='_compute_sgi_ai_available',
        help="La sugerencia de IA está encendida y usted puede pedirla.")

    @api.depends('sgi_folio')
    @api.depends_context('uid')
    def _compute_sgi_ai_available(self):
        on = self.env['sgi.ai.client']._sgi_ai_enabled() and \
            self.env.user.has_group('quimibond_sgi.group_sgi_user')
        for alert in self:
            alert.sgi_ai_available = bool(on and alert.sgi_folio)

    def write(self, vals):
        # Solo el método de sugerencia, con sudo: el contexto sgi_ai_write por
        # sí solo no basta (lo puede mandar cualquier cliente RPC).
        if set(vals) & set(self._SGI_AI_FIELDS) and not self.env.su:
            raise UserError("Los campos de la sugerencia de IA los llena la IA; para usarla, "
                            "pulse los botones de la sugerencia.")
        return super().write(vals)

    def _sgi_ai_check(self):
        self.ensure_one()
        if not self.env['sgi.ai.client']._sgi_ai_enabled():
            raise UserError("La sugerencia de IA está apagada.")
        if not self.env.user.has_group('quimibond_sgi.group_sgi_user'):
            raise UserError("Solo un Usuario SGI pide o usa la sugerencia de IA.")
        if not self.sgi_folio:
            raise UserError("La sugerencia de IA es solo para NC del SGI (con folio).")
        if self.stage_id.sgi_is_closing_stage or self.stage_id.sgi_is_cancel_stage:
            raise UserError("La NC está cerrada o cancelada: no se pide sugerencia.")

    def _sgi_ai_candidates(self):
        return self.env['sgi.norm.clause'].sudo().search([('norm_id.active', '=', True)])

    def _sgi_ai_payload(self):
        """(system, texto) con solo el texto de la NC y las cláusulas. Sin nombres
        de clientes, proveedores, usuarios ni empleados."""
        self.ensure_one()
        alert = self.sudo()
        origin = dict(alert._fields['sgi_origin_type']._description_selection(self.env)).get(
            alert.sgi_origin_type, '')
        parts = [
            "NC: %s" % sgi_ai_scrub(alert.title or ''),
            "Origen: %s" % origin,
            "Proceso: %s %s" % (alert.sgi_process_id.code or '', alert.sgi_process_id.name or ''),
            "Producto: %s" % sgi_ai_scrub(alert.product_tmpl_id.name or ''),
            "Lote: %s" % (alert.lot_id.name or ''),
            "Desviación: %s" % sgi_ai_scrub(alert.sgi_deviation or ''),
            "Descripción: %s" % sgi_ai_scrub(html2plaintext(alert.description or ''))[:3000],
        ]
        if _truthy(self.env['ir.config_parameter'].sudo().get_param(P_HISTORY, 'True')) \
                and alert.sgi_process_id:
            prev = alert.search([
                ('id', '!=', alert.id), ('sgi_folio', '!=', False),
                ('company_id', '=', alert.company_id.id),
                ('sgi_process_id', '=', alert.sgi_process_id.id),
                ('stage_id.sgi_is_closing_stage', '=', True)], order='id desc', limit=3)
            for p in prev:
                parts.append("NC anterior del proceso: %s | causa raíz: %s" % (
                    sgi_ai_scrub(p.sgi_deviation or '')[:500],
                    sgi_ai_scrub(p.sgi_root_cause or '')[:500]))
        parts.append("Cláusulas posibles (id, numeral, requisito, norma):")
        parts += ["[%d] %s %s — %s" % (c.id, c.code or '', c.name or '', c.norm_id.name or '')
                  for c in self._sgi_ai_candidates()]
        return SYSTEM, "\n".join(parts)

    def _sgi_ai_clean(self, data):
        """Valida la respuesta. Lo que no cuadra se descarta."""
        vals, dropped = {}, []
        try:
            clause_id = int(data.get('clausula_id') or 0)
        except (TypeError, ValueError):
            clause_id = 0
        clause = self._sgi_ai_candidates().filtered(lambda c: c.id == clause_id)
        vals['sgi_ai_clause_id'] = clause.id or False
        if not clause:
            dropped.append("cláusula")
        cls_ = data.get('clasificacion')
        vals['sgi_ai_classification'] = cls_ if cls_ in CLASSIFICATIONS else False
        if cls_ not in CLASSIFICATIONS:
            dropped.append("clasificación")
        vals['sgi_ai_reason'] = str(data.get('motivo') or '')[:600]
        whys = data.get('porques') if isinstance(data.get('porques'), list) else []
        whys = [str(w)[:300] for w in whys if str(w or '').strip()][:5]
        vals['sgi_ai_whys'] = "\n".join("%d. %s" % (i + 1, w) for i, w in enumerate(whys))
        ish = data.get('ishikawa') if isinstance(data.get('ishikawa'), dict) else {}
        vals['sgi_ai_ishikawa'] = "\n".join(
            "%s: %s" % (SIXM_LABELS[k], str(ish.get(k))[:300])
            for k in SIXM if str(ish.get(k) or '').strip())
        if not any((vals['sgi_ai_clause_id'], vals['sgi_ai_classification'], whys)):
            _logger.warning("SGI IA: la respuesta no trajo nada utilizable.")
            raise UserError("La IA no dio una sugerencia utilizable.")
        return vals, dropped

    def action_sgi_ai_suggest(self):
        self.ensure_one()
        self._sgi_ai_check()
        system, text = self._sgi_ai_payload()
        data, model = self.env['sgi.ai.client']._sgi_ai_request(system, text, SCHEMA)
        vals, dropped = self._sgi_ai_clean(data if isinstance(data, dict) else {})
        vals.update({'sgi_ai_date': fields.Datetime.now(), 'sgi_ai_model': model})
        self.sudo().with_context(sgi_ai_write=True).write(vals)
        note = ("Sugerencia de IA pedida por %s (modelo %s). Es un borrador: no cambia la NC."
                % (self.env.user.name, model))
        if dropped:
            note += " Se descartó lo que no cuadró: %s." % ", ".join(dropped)
        self.message_post(body=note)
        return True

    def action_sgi_ai_use_classification(self):
        """P18: la persona copia cláusula y clasificación; queda a su nombre."""
        self.ensure_one()
        self._sgi_ai_check()
        vals = {}
        if self.sgi_ai_classification:
            vals['sgi_classification'] = self.sgi_ai_classification
        if self.sgi_ai_clause_id:
            vals['sgi_norm_clause_id'] = self.sgi_ai_clause_id.id
        if not vals:
            raise UserError("No hay cláusula ni clasificación sugeridas.")
        self.write(vals)
        return True

    def action_sgi_ai_use_whys(self):
        """Copia el borrador solo a los porqués vacíos y a las notas Ishikawa
        si están vacías. Nunca la causa raíz."""
        self.ensure_one()
        self._sgi_ai_check()
        whys = [re.sub(r'^\d+\.\s*', '', w) for w in (self.sgi_ai_whys or '').splitlines()
                if w.strip()]
        vals = {}
        for i, why in enumerate(whys[:5], start=1):
            if not self['sgi_why_%d' % i]:
                vals['sgi_why_%d' % i] = why
        if self.sgi_ai_ishikawa and not self.sgi_ishikawa_notes:
            vals['sgi_ishikawa_notes'] = self.sgi_ai_ishikawa
        if not vals:
            raise UserError("No hay porqués vacíos que llenar ni notas Ishikawa vacías.")
        self.write(vals)
        self.message_post(body="Borrador de porqués de IA copiado por %s." % self.env.user.name)
        return True

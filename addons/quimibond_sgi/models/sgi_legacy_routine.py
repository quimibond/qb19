# -*- coding: utf-8 -*-
"""«Del Dropbox a Odoo», rutina por rutina (19.0.57.0.0; entrega 6, bloque
e6-rutinas; diseño en docs/audit/12-transicion.md §3.2, §3.3 y §3.9).

- ``sgi.legacy.routine``: cada rutina de un procedimiento del Dropbox y qué
  pasó con ella en Odoo (cubierta por una actividad, la hace Odoo, eliminada a
  propósito o pendiente). El encabezado es el documento del procedimiento,
  que ya es la fuente de verdad de la sustitución (decisión 3); no hay un
  «procedimiento anterior» aparte (L-002).
- En el documento: las rutinas y sus conteos guardados (L-017), y la regla
  «un procedimiento sustituido no tiene rutinas pendientes» (L-005).
- En la actividad: «Viene de», solo para Auditor, Jefe MAST y Dirección
  (L-018; decisión 11 de la tanda 2: la clave vieja no se muestra al
  personal).
- ``load_routines``: la carga por API (asistente y MCP), siempre con modo de
  prueba, idempotente y con transacción por procedimiento (L-008).

Sin datos con XML ID (decisión 4): instalar el módulo no carga ninguna rutina.
"""
import logging
import re
import unicodedata

import psycopg2

from odoo import api, fields, models
from odoo.exceptions import AccessError, UserError, ValidationError

_logger = logging.getLogger(__name__)

ROUTINE_STATES = [
    ('cubierta', "Cubierta por una actividad"),
    ('reemplazada', "La hace Odoo"),
    ('eliminada', "Eliminada a propósito"),
    ('pendiente', "Pendiente"),
]
DECISIONS = [
    ('actividad', "Crear actividad"),
    ('regla', "Volverla regla o automatización"),
    ('eliminar', "Eliminar con motivo"),
]
# Mismas reglas que docs/audit/12-transicion/validar_rutinas.py.
# Entrega 6 (57.0.0, decisión de Jose): «Rutina por rutina», «Procedimientos
# anteriores» y «Avance de la transición» solo para Auditor, Jefe MAST,
# Dirección y dueños de proceso (grupo sincronizado desde sgi.process). El
# buscador y «Formatos y documentos anteriores» son para todo Usuario SGI.
LEGACY_ROUTINE_GROUPS = ('quimibond_sgi.group_sgi_auditor', 'quimibond_sgi.group_sgi_manager',
                         'quimibond_sgi.group_sgi_director', 'quimibond_sgi.group_sgi_process_owner')
LEGACY_ROUTINE_GROUPS_ATTR = ','.join(LEGACY_ROUTINE_GROUPS)


def sgi_can_read_legacy_routines(env):
    """¿El usuario ve rutinas, procedimientos anteriores y avance?"""
    return env.su or any(env.user.has_group(group) for group in LEGACY_ROUTINE_GROUPS)


STATE_ALIASES = {'odoo': 'reemplazada', 'la hace odoo': 'reemplazada',
                 'omitida': 'eliminada', 'cubierto': 'cubierta'}
DECISION_ALIASES = {'actividad': 'actividad', 'crear actividad': 'actividad',
                    'regla': 'regla', 'automatizacion': 'regla',
                    'regla o automatizacion': 'regla',
                    'volverla regla o automatizacion': 'regla',
                    'eliminar': 'eliminar', 'eliminar con motivo': 'eliminar'}
RE_CLAVE = re.compile(r'^P-[A-Z]\d{2}$')
RE_NUMERAL = re.compile(r'^[CSE]\d\.\d{2}$')
RE_CREDENCIAL = re.compile(
    r'contrase[nñ]a|password|passwd|\bpwd\b|usuario\s*[:=]|user\s*[:=]|'
    r'\btoken\b|api[_ -]?key|eyJhbGci|sb_secret_', re.I)


def sgi_norm(text):
    """Minúsculas, sin acentos y con espacios sencillos (como el validador)."""
    text = unicodedata.normalize('NFKD', str(text or '')).encode('ascii', 'ignore').decode()
    return ' '.join(text.strip().lower().replace('_', ' ').split())


def sgi_cell(value):
    if value is None:
        return ''
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip()


class _Rollback(Exception):
    """Deshace el savepoint del modo de prueba."""


class SgiLegacyRoutine(models.Model):
    """Rutina de un procedimiento del Dropbox y su destino en Odoo (cubierta, reemplazada o
    pendiente de decisión). Se importa desde el libro «rutina por rutina»; vive en «Del Dropbox a
    Odoo»."""
    _name = 'sgi.legacy.routine'
    _description = "Rutina del procedimiento anterior"
    _inherit = ['mail.thread']
    _order = 'procedure_code, n, id'

    procedure_id = fields.Many2one(
        'documents.document', string="Procedimiento anterior", required=True, index=True,
        ondelete='restrict', tracking=True,
        domain=[('sgi_is_controlled', '=', True), ('sgi_doc_type', '=', 'procedimiento')],
        help="Procedimiento del Dropbox del que sale esta rutina (su PDF queda como histórico).")
    procedure_code = fields.Char(
        string="Clave anterior", compute='_compute_procedure_code', store=True, index=True,
        help="Clave del procedimiento en el Dropbox (P-A02). Solo aquí y en el buscador.")
    n = fields.Integer(
        string="N.º", required=True,
        help="Número de la rutina dentro del procedimiento anterior, tal como viene en el análisis.")
    name = fields.Char(string="Rutina", required=True, tracking=True,
                       help="Qué se hacía en el sistema anterior, en una línea.")
    frequency = fields.Char(
        string="Frecuencia anterior",
        help="Cada cuándo se hacía (texto del procedimiento; no es el vencimiento de Odoo).")
    previous_owner = fields.Char(
        string="Responsable anterior",
        help="Puesto que la hacía según el procedimiento del Dropbox.")
    state = fields.Selection(
        ROUTINE_STATES, string="Estado", required=True, default='pendiente', index=True,
        tracking=True,
        help="Cubierta: una actividad de Odoo la hace. La hace Odoo: Odoo la hace solo o era "
             "redundante. Eliminada: se dejó a propósito. Pendiente: nadie la cubre todavía.")
    # Con archivadas: una actividad que se archiva después no borra la
    # historia de la rutina (el tablero la cuenta como «cubierta por
    # actividad archivada» para que MAST la reasigne).
    activity_ids = fields.Many2many(
        'sgi.process.activity', 'sgi_legacy_routine_activity_rel', 'routine_id', 'activity_id',
        string="Actividades que la cubren", context={'active_test': False},
        domain=[('active', '=', True)], tracking=True,
        help="Actividades del proceso nuevo que hacen esta rutina.")
    activity_numbers = fields.Char(
        string="Numerales", compute='_compute_activity_numbers', store=True,
        help="Numerales de las actividades, para buscar y exportar.")
    has_archived_activity = fields.Boolean(
        string="Con actividad archivada", compute='_compute_activity_numbers', store=True)
    reason = fields.Text(
        string="Motivo", tracking=True,
        help="Cómo la cubre la actividad, por qué la hace Odoo solo, por qué se eliminó o por qué "
             "está pendiente.")
    process_id = fields.Many2one(
        'sgi.process', string="Proceso nuevo", compute='_compute_process_id', store=True,
        index=True,
        help="Proceso que sustituye al procedimiento, o su proceso actual si todavía no lo sustituye.")
    procedure_migration_state = fields.Selection(
        related='procedure_id.sgi_migration_state', string="Estado del procedimiento")
    decision = fields.Selection(DECISIONS, string="Decisión", tracking=True,
                                help="Qué se hará con la pendiente.")
    decision_owner_id = fields.Many2one(
        'res.users', string="Responsable de decidir", ondelete='set null', tracking=True,
        help="Quién cierra la pendiente (dueño del proceso o MAST).")
    decision_deadline = fields.Date(
        string="Fecha compromiso", tracking=True,
        help="Las 38 pendientes de hoy se deciden a más tardar el 16 de octubre de 2026.")
    review_state = fields.Selection(
        [('ok', "OK"), ('corregir', "Corregir")], string="Revisión", tracking=True,
        help="Revisión del dueño del proceso sobre el análisis.")
    review_note = fields.Text(string="Comentario de revisión")
    resolved_date = fields.Date(string="Resuelta el", readonly=True, copy=False,
                                help="Cuándo dejó de estar pendiente.")
    resolved_uid = fields.Many2one('res.users', string="Resuelta por", readonly=True, copy=False)
    company_id = fields.Many2one(related='procedure_id.company_id', string="Empresa", store=True)
    active = fields.Boolean(default=True, tracking=True)

    _procedure_n_uniq = models.Constraint(
        'unique(procedure_id, n)', "Ya existe la rutina con ese número en ese procedimiento.")
    _n_positive = models.Constraint('CHECK (n > 0)', "El número de la rutina es mayor que cero.")

    # ------------------------------------------------------------------
    @api.depends('procedure_id.sgi_previous_code', 'procedure_id.sgi_code')
    def _compute_procedure_code(self):
        for routine in self:
            doc = routine.procedure_id
            routine.procedure_code = doc.sgi_previous_code or doc.sgi_code or False

    @api.depends('activity_ids.number', 'activity_ids.active')
    def _compute_activity_numbers(self):
        for routine in self:
            activities = routine.with_context(active_test=False).activity_ids
            routine.activity_numbers = '; '.join(sorted(n for n in activities.mapped('number') if n)) or False
            routine.has_archived_activity = any(not a.active for a in activities)

    @api.depends('procedure_id.sgi_replaced_by_process_id', 'procedure_id.sgi_process_id')
    def _compute_process_id(self):
        for routine in self:
            doc = routine.procedure_id
            routine.process_id = doc.sgi_replaced_by_process_id or doc.sgi_process_id

    @api.depends('procedure_code', 'n', 'name')
    def _compute_display_name(self):
        for routine in self:
            head = "%s · %s" % (routine.procedure_code or '—', routine.n or '?')
            routine.display_name = "%s — %s" % (head, routine.name) if routine.name else head

    # ------------------------------------------------------------------
    @api.constrains('state', 'activity_ids', 'reason')
    def _check_state_rules(self):
        for routine in self:
            activities = routine.with_context(active_test=False).activity_ids
            label = routine.display_name
            if routine.state == 'cubierta' and not activities:
                raise ValidationError("%s: una rutina cubierta lleva al menos una actividad." % label)
            if routine.state != 'cubierta' and not (routine.reason or '').strip():
                raise ValidationError("%s: una rutina «%s» lleva motivo." % (
                    label, dict(ROUTINE_STATES)[routine.state]))
            if routine.state == 'pendiente' and activities:
                raise ValidationError(
                    "%s: una rutina pendiente no lleva actividades (o está cubierta o no las tiene)."
                    % label)

    @api.constrains('procedure_id')
    def _check_procedure(self):
        Doc = self.env['documents.document']
        excluded = Doc._sgi_dropbox_excluded_codes()
        for routine in self:
            doc = routine.procedure_id.sudo()
            if not doc.sgi_is_controlled or doc.sgi_doc_type != 'procedimiento':
                raise ValidationError("La rutina va sobre un procedimiento controlado.")
            if not doc.active:
                raise ValidationError("El procedimiento de la rutina está archivado.")
            if doc.sgi_legacy_family in excluded or (doc.sgi_previous_code or doc.sgi_code) in excluded:
                # L-001: no se repite nada del procedimiento excluido.
                raise ValidationError("Ese procedimiento va aparte: no lleva rutinas aquí.")

    @api.constrains('state', 'procedure_id', 'active')
    def _check_pending_vs_replaced(self):
        """L-005 / P-L2: un procedimiento que ya sustituye un proceso no tiene
        rutinas pendientes."""
        for routine in self.filtered(lambda r: r.active and r.state == 'pendiente'):
            process = routine.procedure_id.sudo().sgi_replaced_by_process_id
            if process:
                raise ValidationError(
                    "%s ya lo sustituye el proceso %s: no puede tener rutinas pendientes. Decida la "
                    "rutina (actividad, la hace Odoo o eliminada) o quite la sustitución." % (
                        routine.procedure_code, process.display_name))

    # ------------------------------------------------------------------
    @api.model
    def _sgi_default_decision_deadline(self):
        """Fecha límite para decidir una rutina pendiente (decisión de Jose,
        2026-09-29: 16 de octubre de 2026). Parámetro
        ``quimibond_sgi.legacy_decision_deadline`` (AAAA-MM-DD)."""
        value = self.env['ir.config_parameter'].sudo().get_param(
            'quimibond_sgi.legacy_decision_deadline', '2026-10-16')
        try:
            return fields.Date.to_date(value)
        except (TypeError, ValueError):
            return fields.Date.to_date('2026-10-16')

    @api.model_create_multi
    def create(self, vals_list):
        today = fields.Date.context_today(self)
        for vals in vals_list:
            if vals.get('state', 'pendiente') != 'pendiente':
                vals.setdefault('resolved_date', today)
                vals.setdefault('resolved_uid', self.env.uid)
            elif not vals.get('decision_deadline'):
                vals['decision_deadline'] = self._sgi_default_decision_deadline()
        return super().create(vals_list)

    def write(self, vals):
        if vals.get('state') and vals['state'] != 'pendiente':
            leaving = self.filtered(lambda r: r.state == 'pendiente')
            res = super().write(vals)
            if leaving:
                super(SgiLegacyRoutine, leaving).write({
                    'resolved_date': fields.Date.context_today(self),
                    'resolved_uid': self.env.uid})
            return res
        if vals.get('state') == 'pendiente':
            vals = dict(vals, resolved_date=False, resolved_uid=False)
            if 'decision_deadline' not in vals:
                without = self.filtered(lambda r: not r.decision_deadline)
                res = super().write(vals)
                if without:
                    super(SgiLegacyRoutine, without).write(
                        {'decision_deadline': self._sgi_default_decision_deadline()})
                return res
        return super().write(vals)

    def unlink(self):
        if not self.env.su:
            raise UserError("Las rutinas no se borran: se archivan (son la evidencia de la transición).")
        return super().unlink()

    # ------------------------------------------------------------------
    def action_open_activities(self):
        self.ensure_one()
        activities = self.with_context(active_test=False).activity_ids
        action = {
            'type': 'ir.actions.act_window',
            'name': "Actividades de %s" % self.display_name,
            'res_model': 'sgi.process.activity',
            'context': {'active_test': False},
        }
        if len(activities) == 1:
            action.update(view_mode='form', res_id=activities.id)
        else:
            action.update(view_mode='list,form', domain=[('id', 'in', activities.ids)])
        return action

    def action_open_procedure(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': self.procedure_code or "Procedimiento anterior",
            'res_model': 'documents.document',
            'res_id': self.procedure_id.id,
            'view_mode': 'form',
            'views': [(self.env.ref('quimibond_sgi.sgi_dropbox_procedure_view_form').id, 'form')],
        }

    # ------------------------------------------------------------------
    # L-008: carga (API del asistente y de MCP)
    # ------------------------------------------------------------------
    @api.model
    def load_routines(self, payload, dry_run=None):
        """Carga el análisis rutina por rutina. Formato en
        docs/audit/12-transicion.md §3.9::

            {"dry_run": True, "archive_missing": False,
             "routines": [{"clave", "n", "rutina", "frecuencia", "responsable_anterior",
                           "estado", "actividades": [...] | "C1.01; C1.02", "motivo",
                           "revision", "comentario", "procedimiento", "fila"}],
             "pending": [{"clave", "n" | "rutina", "decision", "responsable", "fecha"}],
             "summary": [{"clave", "proceso_nuevo", "rutinas", "cubiertas",
                          "reemplazadas", "pendientes"}]}

        Responde {ok, dry_run, summary, changes, errors, warnings}. Solo Jefe
        MAST (o superior). Por defecto en modo de prueba. Idempotente por
        (procedimiento, n); transacción por procedimiento: una fila con error
        deja sin cargar solo su procedimiento. Solo escribe rutinas."""
        if not (self.env.su or self.env.user.has_group('quimibond_sgi.group_sgi_manager')):
            raise AccessError("Solo el Jefe MAST carga las rutinas del Dropbox.")
        if not isinstance(payload, dict):
            raise UserError("La carga de rutinas es un objeto con «routines».")
        if dry_run is None:
            dry_run = payload.get('dry_run', True) is not False
        ctx = {'mail_create_nolog': True}
        if dry_run:
            ctx.update(tracking_disable=True, mail_notrack=True)
        loader = _RoutineLoader(self.with_context(**ctx), payload, dry_run)
        try:
            with self.env.cr.savepoint():
                loader.run()
                self.env.flush_all()
                if dry_run:
                    raise _Rollback()
        except _Rollback:
            pass
        self.env.invalidate_all(flush=False)
        result = loader.result()
        _logger.info("SGI load_routines%s: %s, %d error(es).", " (modo de prueba)" if dry_run else "",
                     result['summary'], len(result['errors']))
        return result


class _RoutineLoader:
    """Valida y escribe. Nunca guarda en el reporte el texto de una fila de un
    procedimiento excluido ni de una fila con forma de credencial (P-L5)."""

    FIELDS = ('name', 'frequency', 'previous_owner', 'state', 'activity_ids', 'reason',
              'review_state', 'review_note', 'active')

    def __init__(self, model, payload, dry_run):
        self.env = model.env
        self.Routine = model.with_context(active_test=False)
        self.payload = payload
        self.dry_run = dry_run
        self.errors, self.warnings, self.changes = [], [], []
        self.counts = {'created': 0, 'updated': 0, 'archived': 0, 'unchanged': 0}
        self.Doc = self.env['documents.document']
        self.excluded = set(self.Doc._sgi_dropbox_excluded_codes())

    # -- reporte --
    def error(self, rule, message, fila='', clave='', n=''):
        self.errors.append({'fila': fila, 'clave': clave, 'n': n, 'regla': rule, 'message': message})

    def warning(self, rule, message, fila='', clave='', n=''):
        self.warnings.append({'fila': fila, 'clave': clave, 'n': n, 'regla': rule, 'message': message})

    def result(self):
        return {'ok': not self.errors, 'dry_run': self.dry_run, 'summary': dict(self.counts),
                'changes': self.changes, 'errors': self.errors, 'warnings': self.warnings}

    # -- resolución --
    def _procedure(self, clave):
        """L-016: el único procedimiento controlado, activo y vigente o en
        piloto con esa clave anterior (o esa clave, si aún no se copió)."""
        docs = self.Doc.sudo().search([
            ('sgi_is_controlled', '=', True), ('sgi_doc_type', '=', 'procedimiento'),
            ('sgi_state', 'in', ('vigente', 'piloto')),
            '|', ('sgi_previous_code', '=', clave),
            '&', ('sgi_previous_code', '=', False), ('sgi_code', '=', clave)])
        return docs

    def _activities(self):
        activities = self.env['sgi.process.activity'].sudo().search([('active', '=', True)])
        by_number = {}
        for activity in activities:
            if activity.number:
                by_number.setdefault(activity.number, activity)
        return by_number

    # -- corrida --
    def run(self):
        payload = self.payload
        rows = payload.get('routines') or []
        if not isinstance(rows, list):
            self.error('formato', "«routines» es una lista de rutinas.")
            return
        activities = self._activities()
        by_clave, seen, bad_claves = {}, {}, set()
        for index, row in enumerate(rows, start=1):
            parsed = self._parse(row, index, activities, seen)
            if parsed is None:
                clave = sgi_cell((row or {}).get('clave')).upper() if isinstance(row, dict) else ''
                bad_claves.add(clave)
                continue
            by_clave.setdefault(parsed['clave'], []).append(parsed)
        procedures = {}
        for clave in sorted(set(by_clave) | bad_claves):
            if not clave or clave in self.excluded:
                continue
            docs = self._procedure(clave)
            if len(docs) != 1:
                self.error('procedimiento', "%s: %s" % (
                    clave, "no existe como procedimiento vigente o en piloto" if not docs else
                    "hay %d procedimientos vigentes con esa clave (ids %s): la llave es ambigua"
                    % (len(docs), ", ".join(str(i) for i in docs.ids))), clave=clave)
                bad_claves.add(clave)
                continue
            procedures[clave] = docs
            pending = sum(1 for r in by_clave.get(clave, []) if r['vals']['state'] == 'pendiente')
            if docs.sgi_replaced_by_process_id and pending:
                self.error('sustitucion', "%s ya tiene proceso que lo sustituye (%s) y %d rutina(s) "
                           "pendiente(s)" % (clave, docs.sgi_replaced_by_process_id.code, pending),
                           clave=clave)
                bad_claves.add(clave)
        self._check_summary(payload.get('summary') or [], by_clave, procedures, bad_claves)
        for clave in sorted(by_clave):
            if clave in bad_claves or clave not in procedures:
                if clave in by_clave and clave not in self.excluded:
                    self.warning('procedimiento', "%s no se carga: tiene errores (transacción por "
                                  "procedimiento)." % clave, clave=clave)
                continue
            self._load_procedure(procedures[clave], by_clave[clave])
        self._load_pending(payload.get('pending') or [], procedures, bad_claves)
        self._check_documents()

    def _check_documents(self):
        """Respuesta 8 de Jose a L (L-013): el modo de prueba enseña los
        documentos del Dropbox cuya clase y estado no cuadran, para que MAST
        los corrija antes de la carga real. No escribe en documentos."""
        from .sgi_document import SGI_DROPBOX_DOC_TYPES
        docs = self.Doc.sudo().search([
            ('sgi_is_controlled', '=', True), ('sgi_doc_type', 'in', SGI_DROPBOX_DOC_TYPES),
            '|', '&', ('sgi_migration_class', '=', 'd'), ('sgi_migration_state', '!=', 'na'),
            '&', ('sgi_migration_class', 'in', ('a', 'b', 'c')), ('sgi_migration_state', '=', 'na'),
        ] + self.Doc._sgi_dropbox_excluded_domain(), order='sgi_previous_code, id')
        for doc in docs:
            self.error('clase_estado', "Documento %s (id %d): clase %s con estado «%s». Corríjalo en "
                       "«Formatos y documentos anteriores» antes de cargar." % (
                           doc.sgi_previous_code or doc.sgi_code or '', doc.id,
                           (doc.sgi_migration_class or '').upper(),
                           dict(doc._fields['sgi_migration_state'].selection).get(doc.sgi_migration_state)),
                       clave=doc.sgi_previous_code or doc.sgi_code or '')

    def _parse(self, row, index, activities, seen):
        if not isinstance(row, dict):
            self.error('formato', "La fila no es un objeto.", fila=index)
            return None
        fila = row.get('fila') or index
        clave = sgi_cell(row.get('clave')).upper()
        if clave in self.excluded:
            # Nunca se repite el contenido de estas filas (ni el número).
            self.error('excluida', "Fila de un procedimiento que va aparte; quítela del archivo.",
                       fila=fila, clave=clave)
            return None
        n_txt = sgi_cell(row.get('n'))
        text = ' '.join(sgi_cell(row.get(k)) for k in ('rutina', 'motivo', 'responsable_anterior',
                                                          'comentario'))
        if RE_CREDENCIAL.search(text):
            self.error('credencial', "Texto con forma de credencial; revise la fila a mano (no se "
                       "muestra).", fila=fila, clave=clave, n=n_txt)
            return None
        ok = True
        if not RE_CLAVE.match(clave):
            self.error('clave', "Clave con formato inválido %r (se espera P-Xnn)." % clave,
                       fila, clave, n_txt)
            ok = False
        try:
            number = float(n_txt)
            n = int(number)
            if n <= 0 or number != n:
                raise ValueError
        except ValueError:
            self.error('n', "n debe ser entero positivo (llegó %r)." % n_txt, fila, clave, n_txt)
            n, ok = None, False
        if n is not None and (clave, n) in seen:
            self.error('duplicado', "(clave, n) repetido; primera vez en la fila %s." % seen[(clave, n)],
                       fila, clave, n)
            ok = False
        elif n is not None:
            seen[(clave, n)] = fila
        name = sgi_cell(row.get('rutina'))
        if not name:
            self.error('rutina', "Rutina vacía.", fila, clave, n)
            ok = False
        state = sgi_norm(row.get('estado'))
        state = STATE_ALIASES.get(state, state)
        if state not in dict(ROUTINE_STATES):
            self.error('estado', "Estado %r no válido (cubierta, reemplazada, eliminada, pendiente)."
                       % sgi_cell(row.get('estado')), fila, clave, n)
            ok = False
        tokens = row.get('actividades', row.get('actividades_odoo')) or []
        if isinstance(tokens, str):
            tokens = re.split(r'[;,]', tokens)
        tokens = [sgi_cell(t) for t in tokens if sgi_cell(t)]
        activity_ids = []
        for token in tokens:
            if not RE_NUMERAL.match(token):
                self.error('numeral', "Numeral %r con formato inválido (se espera C2.06)." % token,
                           fila, clave, n)
                ok = False
            elif token not in activities:
                self.error('numeral', "La actividad %s no está entre las activas." % token,
                           fila, clave, n)
                ok = False
            elif activities[token].id not in activity_ids:
                activity_ids.append(activities[token].id)
        if len(set(tokens)) != len(tokens):
            self.warning('numeral', "Actividad repetida en la misma fila.", fila, clave, n)
        reason = sgi_cell(row.get('motivo'))
        if state == 'cubierta' and not tokens:
            self.error('cubierta', "Rutina cubierta sin actividad de Odoo.", fila, clave, n)
            ok = False
        if state in ('reemplazada', 'eliminada', 'pendiente') and not reason:
            self.error('motivo', "Rutina %s sin motivo." % state, fila, clave, n)
            ok = False
        if state == 'pendiente' and tokens:
            self.error('pendiente', "Rutina pendiente con actividades: o está cubierta o no las "
                       "lleva.", fila, clave, n)
            ok = False
        review = sgi_norm(row.get('revision'))
        if review and review not in ('ok', 'corregir'):
            self.warning('revision', "Revisión %r: se espera OK o Corregir." % sgi_cell(row.get('revision')),
                         fila, clave, n)
            review = ''
        if review == 'corregir':
            self.warning('revision', "Marcada «Corregir» por el revisor: se importa como está.",
                         fila, clave, n)
        if not ok:
            return None
        return {'clave': clave, 'n': n, 'fila': fila, 'title': sgi_cell(row.get('procedimiento')),
                'vals': {'name': name, 'frequency': sgi_cell(row.get('frecuencia')) or False,
                         'previous_owner': sgi_cell(row.get('responsable_anterior')) or False,
                         'state': state, 'activity_ids': activity_ids, 'reason': reason or False,
                         'review_state': review or False,
                         'review_note': sgi_cell(row.get('comentario')) or False,
                         'active': True}}

    def _check_summary(self, summary, by_clave, procedures, bad_claves):
        """Hoja «Resumen por procedimiento»: conteos y proceso nuevo."""
        for index, row in enumerate(summary, start=2):
            clave = sgi_cell(row.get('clave')).upper()
            if not clave or clave == 'TOTAL' or clave in self.excluded:
                continue
            rows = by_clave.get(clave, [])
            states = [r['vals']['state'] for r in rows]
            for column, real in (('rutinas', len(rows)), ('cubiertas', states.count('cubierta')),
                                 ('reemplazadas', states.count('reemplazada')),
                                 ('pendientes', states.count('pendiente'))):
                if row.get(column) in (None, ''):
                    continue
                try:
                    said = int(float(row.get(column)))
                except (TypeError, ValueError):
                    said = None
                if said != real:
                    self.error('resumen', "Resumen dice %s=%s y la hoja de rutinas da %d." % (
                        column, row.get(column), real), fila=index, clave=clave)
                    bad_claves.add(clave)
            doc = procedures.get(clave)
            said_process = sgi_cell(row.get('proceso_nuevo')).split(' ')[0]
            if doc and said_process:
                expected = (doc.sgi_replaced_by_process_id or doc.sgi_process_id).code or ''
                if said_process != expected:
                    self.error('resumen', "Proceso nuevo %s en el libro; en Odoo el documento dice %s."
                               % (said_process, expected or '—'), fila=index, clave=clave)
                    bad_claves.add(clave)

    def _diff(self, routine, vals):
        changed = {}
        for name, value in vals.items():
            current = routine.with_context(active_test=False)[name]
            if name == 'activity_ids':
                if set(current.ids) != set(value):
                    changed[name] = [(6, 0, value)]
            elif (current or False) != (value or False):
                changed[name] = value
        return changed

    def _load_procedure(self, doc, rows):
        clave = rows[0]['clave']
        existing = {r.n: r for r in self.Routine.search([('procedure_id', '=', doc.id)])}
        title = rows[0]['title']
        if title and sgi_norm(title) not in sgi_norm(doc.sgi_title or doc.name):
            self.warning('procedimiento', "%s: el nombre del libro («%s») no se parece al del "
                         "documento." % (clave, title), clave=clave)
        planned, counts = [], dict.fromkeys(self.counts, 0)
        for row in rows:
            routine = existing.pop(row['n'], None)
            if routine is None:
                planned.append(('created', None, dict(row['vals'], procedure_id=doc.id, n=row['n']), row))
                continue
            changed = self._diff(routine, row['vals'])
            planned.append(('updated' if changed else 'unchanged', routine, changed, row))
        if self.payload.get('archive_missing'):
            for routine in existing.values():
                if routine.active:
                    planned.append(('archived', routine, {'active': False},
                                    {'clave': clave, 'n': routine.n, 'fila': ''}))
        try:
            with self.env.cr.savepoint():
                for action, routine, vals, row in planned:
                    if action == 'created':
                        self.Routine.create(vals)
                    elif action in ('updated', 'archived'):
                        routine.write(vals)
                self.env.flush_all()
        except (ValidationError, UserError, AccessError, psycopg2.Error) as exc:
            self.env.invalidate_all(flush=False)
            message = str(exc.args[0] if exc.args else exc)
            self.error('procedimiento', "%s no se cargó: %s" % (clave, message), clave=clave)
            return
        for action, routine, vals, row in planned:
            counts[action] += 1
            if action != 'unchanged':
                fields_changed = sorted(k for k in vals if k not in ('procedure_id', 'n'))
                self.changes.append({'clave': clave, 'n': row['n'], 'action': action,
                                     'fields': fields_changed})
        for key, value in counts.items():
            self.counts[key] += value

    def _load_pending(self, rows, procedures, bad_claves):
        """Hoja «Pendientes»: decisión, responsable y fecha de las pendientes."""
        Users = self.env['res.users'].sudo()
        for index, row in enumerate(rows, start=2):
            clave = sgi_cell(row.get('clave')).upper()
            if not clave or clave in self.excluded or clave in bad_claves:
                continue
            doc = procedures.get(clave) or self._procedure(clave)
            if len(doc) != 1:
                self.warning('pendientes', "%s no es un procedimiento vigente." % clave, fila=index,
                             clave=clave)
                continue
            domain = [('procedure_id', '=', doc.id), ('state', '=', 'pendiente')]
            n_txt = sgi_cell(row.get('n'))
            if n_txt:
                try:
                    domain.append(('n', '=', int(float(n_txt))))
                except ValueError:
                    pass
            elif sgi_cell(row.get('rutina')):
                domain.append(('name', '=', sgi_cell(row.get('rutina'))))
            routine = self.Routine.search(domain)
            if len(routine) != 1:
                self.warning('pendientes', "No se encontró una sola rutina pendiente de %s con esa "
                             "fila." % clave, fila=index, clave=clave)
                continue
            vals = {}
            decision = sgi_norm(row.get('decision'))
            if decision:
                if decision in DECISION_ALIASES:
                    vals['decision'] = DECISION_ALIASES[decision]
                else:
                    self.warning('pendientes', "Decisión %r no reconocida (crear actividad, regla o "
                                 "eliminar)." % sgi_cell(row.get('decision')), fila=index, clave=clave)
            else:
                self.warning('pendientes', "Pendiente sin decisión.", fila=index, clave=clave,
                             n=routine.n)
            who = sgi_cell(row.get('responsable'))
            if who:
                user = Users.search([('login', '=ilike', who)], limit=1) or \
                    Users.search([('name', '=ilike', who)], limit=2)
                if len(user) == 1:
                    vals['decision_owner_id'] = user.id
                else:
                    self.warning('pendientes', "Responsable %r no es un usuario (o hay varios)." % who,
                                 fila=index, clave=clave, n=routine.n)
            deadline = row.get('fecha')
            if deadline:
                try:
                    vals['decision_deadline'] = fields.Date.to_date(
                        deadline if not hasattr(deadline, 'date') else deadline.date())
                except (TypeError, ValueError):
                    self.warning('pendientes', "Fecha %r no válida." % sgi_cell(deadline), fila=index,
                                 clave=clave, n=routine.n)
            changed = self._diff(routine, vals)
            if changed:
                routine.write(changed)
                self.counts['updated'] += 1
                self.changes.append({'clave': clave, 'n': routine.n, 'action': 'updated',
                                     'fields': sorted(changed)})


class DocumentsDocumentLegacyRoutines(models.Model):
    """L-017: las rutinas y sus conteos en el procedimiento anterior."""
    _inherit = 'documents.document'

    # 57.0.0: con grupos para que un read() sin campos (exportar, copiar) de
    # un Usuario SGI no toque el modelo, que no puede leer.
    sgi_legacy_routine_ids = fields.One2many(
        'sgi.legacy.routine', 'procedure_id', string="Rutinas del procedimiento anterior",
        groups=LEGACY_ROUTINE_GROUPS_ATTR)
    sgi_routine_count = fields.Integer(string="Rutinas", compute='_compute_sgi_routine_counts',
                                       store=True)
    sgi_routine_covered_count = fields.Integer(string="Cubiertas", compute='_compute_sgi_routine_counts',
                                               store=True)
    sgi_routine_replaced_count = fields.Integer(string="La hace Odoo",
                                                compute='_compute_sgi_routine_counts', store=True)
    sgi_routine_eliminated_count = fields.Integer(string="Eliminadas",
                                                  compute='_compute_sgi_routine_counts', store=True)
    sgi_routine_pending_count = fields.Integer(string="Pendientes", compute='_compute_sgi_routine_counts',
                                               store=True)
    sgi_routine_resolved_pct = fields.Float(string="% resuelto", compute='_compute_sgi_routine_counts',
                                            store=True, aggregator='avg')

    @api.depends('sgi_legacy_routine_ids.state', 'sgi_legacy_routine_ids.active')
    def _compute_sgi_routine_counts(self):
        for doc in self:
            routines = doc.sgi_legacy_routine_ids.filtered('active')
            states = routines.mapped('state')
            total = len(routines)
            doc.sgi_routine_count = total
            doc.sgi_routine_covered_count = states.count('cubierta')
            doc.sgi_routine_replaced_count = states.count('reemplazada')
            doc.sgi_routine_eliminated_count = states.count('eliminada')
            doc.sgi_routine_pending_count = states.count('pendiente')
            doc.sgi_routine_resolved_pct = (
                100.0 * (total - states.count('pendiente')) / total) if total else 0.0

    @api.constrains('sgi_replaced_by_process_id')
    def _check_sgi_replaced_without_pending(self):
        """L-005 / P-L2: no se liga el proceso que sustituye a un procedimiento
        que todavía tiene rutinas pendientes."""
        for doc in self.filtered('sgi_replaced_by_process_id'):
            pending = self.env['sgi.legacy.routine'].sudo().search_count([
                ('procedure_id', '=', doc.id), ('state', '=', 'pendiente')])
            if pending:
                raise ValidationError(
                    "%s tiene %d rutina(s) pendiente(s): decídalas antes de ligar el proceso que lo "
                    "sustituye." % (doc.sgi_previous_code or doc.sgi_code or doc.name, pending))

    def action_sgi_open_routines(self):
        self.ensure_one()
        action = self.env['ir.actions.act_window']._for_xml_id('quimibond_sgi.sgi_dropbox_routine_action')
        action['domain'] = [('procedure_id', '=', self.id)]
        action['context'] = {'default_procedure_id': self.id}
        return action


class SgiProcessActivityLegacy(models.Model):
    """L-018 (+C-016): de qué rutinas del Dropbox viene la actividad. Solo
    Auditor, Jefe MAST y Dirección (la clave vieja no se muestra al
    personal, decisión 11 de la tanda 2)."""
    _inherit = 'sgi.process.activity'

    sgi_legacy_routine_ids = fields.Many2many(
        'sgi.legacy.routine', 'sgi_legacy_routine_activity_rel', 'activity_id', 'routine_id',
        string="Viene de (rutinas anteriores)", readonly=True,
        groups='quimibond_sgi.group_sgi_auditor,quimibond_sgi.group_sgi_manager,'
               'quimibond_sgi.group_sgi_director')

    def action_sgi_view_legacy_routines(self):
        self.ensure_one()
        user = self.env.user
        if not (self.env.su or any(user.has_group('quimibond_sgi.%s' % g) for g in (
                'group_sgi_auditor', 'group_sgi_manager', 'group_sgi_director'))):
            raise AccessError("«Viene de» es para Auditor, Jefe MAST y Dirección.")
        action = self.env['ir.actions.act_window']._for_xml_id('quimibond_sgi.sgi_dropbox_routine_action')
        action['domain'] = [('activity_ids', 'in', self.ids)]
        action['context'] = {'search_default_group_procedure': 0}
        action['name'] = "Viene de — %s" % (self.number or self.name or '')
        return action

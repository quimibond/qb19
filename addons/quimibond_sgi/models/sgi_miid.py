# -*- coding: utf-8 -*-
"""57.105.0: «MIID desde Odoo».

El Manual Integral de Información Documentada se genera desde Odoo: texto fijo
por sección (``sgi.miid.section``, lo edita el Jefe MAST) más los datos vivos
del SGI. La revisión aprobada pasa por el cambio documental de siempre (DOC-1,
Sign): la solicitud lleva el PDF generado y la huella de los datos; al
aprobarse se publica como revisión nueva (lo que se firmó es lo que se
publica) y el paso diario compara la huella viva con la de la revisión
vigente. Ninguna revisión se envía ni se aprueba con secciones «Por
confirmar» o procesos que no estén vigentes. Plan:
docs/superpowers/plans/2026-10-05-sgi-57-105-0-miid.md."""
import hashlib
import json
import re

from markupsafe import Markup

from odoo import api, fields, models
from odoo.exceptions import AccessError, ValidationError


try:  # C-1: texto sin formato (html2plaintext convierte <b> en *…*).
    from odoo.tools.mail import html_to_inner_content
except ImportError:  # pragma: no cover - versiones sin la función
    html_to_inner_content = None


MIID_CODE = 'MIID'
MIID_NOTICE_KIND = 'miid_desactualizado'
MIID_HELD_KIND = 'miid_retenido'
# Q5: la primera revisión generada desde Odoo continúa la numeración del
# Dropbox (Rev. 02 vigente, borrador Rev. 03). Solo aplica mientras la
# vigente no tenga huella (cargada del Dropbox).
MIID_FIRST_ODOO_REVISION = 3
MIID_LANG = 'es_MX'
MIID_DATA_MARK = "[[datos]]"
MIID_DATA_MARK_RE = re.compile(r'<p[^>]*>\s*\[\[datos\]\]\s*</p>|\[\[datos\]\]')
# Q16: estados de proceso con los que se puede aprobar el MIID.
MIID_READY_PROCESS_STATES = ('vigente',)
MIID_DIFF_LIMIT = 30
MIID_NOTICE_BUSINESS_DAYS = 3
MIID_APPROVER_PARAM = 'quimibond_sgi.miid_approver_user_id'
# Lo que no entra a la huella: cambia al publicar (identificación, control de
# cambios) o una revisión aprobada nunca lo tiene («Por confirmar»).
MIID_UNHASHED = ('identificacion', 'historial', 'pendientes')
MIID_BLOCKS = [
    ('identificacion', "Identificación del documento"),
    ('procesos', "Procesos del SGI, mapa e interacción"),
    ('politica', "Política integral vigente"),
    ('objetivos', "Objetivos integrales e indicadores"),
    ('tipos_documento', "Tipos de documento y su clave"),
    ('controles', "Controles operacionales vigentes"),
    ('plazos_nc', "Plazos de las NC"),
    ('correspondencia', "Correspondencia por cláusula"),
    ('procedimientos_anteriores', "Procedimientos anteriores por proceso"),
    ('anexos', "Anexos vigentes"),
    ('historial', "Control de cambios del MIID"),
]
# Plazos de NC: (llave de la foto, etiqueta, unidad).
MIID_NC_ROWS = (
    ('containment', "Contención", "días hábiles desde que se levanta la NC"),
    ('root_cause', "Causa raíz", "días hábiles desde que se levanta la NC"),
    ('plan', "Plan de acción", "días hábiles desde que se levanta la NC"),
    ('escalation', "Escalamiento de una NC interna sin atender", "días hábiles"),
    ('escalation_external', "Escalamiento de una NC de cliente o externa sin atender", "días hábiles"),
    ('effectiveness', "Verificación de eficacia", "días naturales desde la última acción correctiva"),
)
# Quien no es Jefe MAST solo puede poner o quitar «Por confirmar» (Dirección).
# (message_main_attachment_id: lo escribe el chatter al adjuntar.)
MIID_CONFIRM_FIELDS = frozenset(('to_confirm', 'to_confirm_note', 'message_main_attachment_id'))


def _miid_plain(html):
    """Texto comparable de un Html: sin etiquetas ni formato, espacios
    normalizados (las negritas y los espacios no son un cambio)."""
    if not html:
        return ''
    if html_to_inner_content:
        text = html_to_inner_content(html)
    else:  # pragma: no cover
        text = re.sub(r'<[^>]+>', ' ', str(html))
    return ' '.join(text.split())


def _miid_int(param, key, default):
    try:
        return int(param.get_param(key, default) or default)
    except (TypeError, ValueError):
        return default


class SgiMiidSection(models.Model):
    """Sección de texto fijo del MIID (una por título y subtítulo). La edita el
    Jefe MAST; los datos del sistema salen del bloque que declara. «Por
    confirmar» impide enviar o aprobar una revisión del MIID."""
    _name = 'sgi.miid.section'
    _description = "Sección del MIID"
    _inherit = ['mail.thread']
    _order = 'sequence, id'

    company_id = fields.Many2one(
        'res.company', string="Empresa", required=True, index=True,
        default=lambda self: self.env['sgi.config']._sgi_company())
    sequence = fields.Integer(string="Orden", default=10, tracking=True)
    clause = fields.Char(string="Numeral", tracking=True,
                         help="Numeral del manual (p. ej. 4.4). Vacío en la identificación del documento.")
    name = fields.Char(string="Título de la sección", required=True, tracking=True)
    heading_level = fields.Selection([('1', "Capítulo"), ('2', "Apartado")], string="Nivel del título",
                                     default='2', required=True,
                                     help="Capítulo («4 Contexto de la organización») o apartado («4.4 …»).")
    body = fields.Html(string="Texto de la sección",
                       help="Texto fijo que imprime el MIID. Escriba [[datos]] en un párrafo propio para "
                            "decidir dónde van los datos del sistema; si no, van al final de la sección.")
    live_block = fields.Selection(
        MIID_BLOCKS, string="Datos del sistema que lleva", tracking=True,
        help="Datos vivos de Odoo que imprime esta sección. Cada bloque va en una sola sección; si "
             "ninguna lo lleva, sale al final del manual.")
    body_fallback = fields.Boolean(
        string="Texto solo si no hay datos",
        help="Marque si el texto es el respaldo del bloque: solo se imprime cuando el sistema no trae datos.")
    to_confirm = fields.Boolean(
        string="Por confirmar", tracking=True,
        help="Mientras alguna sección esté por confirmar, el MIID no se puede enviar ni aprobar como "
             "revisión vigente. Quítelo cuando el texto esté confirmado (Jefe MAST, Dirección o "
             "Administrador SGI).")
    to_confirm_note = fields.Text(string="Qué falta confirmar",
                                  help="Se ve en la pantalla y en el PDF de borrador.")
    row_note_ids = fields.One2many('sgi.miid.row.note', 'section_id', string="Notas por renglón",
                                   help="Columna fija de un bloque de datos (12.1: la situación de cada anexo).")
    active = fields.Boolean(default=True, tracking=True)

    @api.constrains('live_block', 'company_id', 'active')
    def _check_live_block(self):
        for section in self.filtered(lambda s: s.live_block and s.active):
            other = self.search_count([('id', '!=', section.id), ('active', '=', True),
                                       ('company_id', '=', section.company_id.id),
                                       ('live_block', '=', section.live_block)], limit=1)
            if other:
                raise ValidationError(
                    "El bloque «%s» ya lo lleva otra sección del MIID. Quíteselo a esa sección "
                    "primero." % dict(MIID_BLOCKS)[section.live_block])

    @api.constrains('to_confirm', 'to_confirm_note')
    def _check_to_confirm_note(self):
        for section in self.filtered(lambda s: s.to_confirm and not (s.to_confirm_note or '').strip()):
            raise ValidationError("Escriba qué falta confirmar en la sección «%s»." % section.name)

    def write(self, vals):
        # Dirección quita «Por confirmar» (decisión de Jose, 2026-10-05); el
        # texto lo edita el Jefe MAST.
        if not self.env.su and set(vals) - MIID_CONFIRM_FIELDS \
                and not self.env.user.has_group('quimibond_sgi.group_sgi_manager'):
            raise AccessError("Solo el Jefe MAST edita los textos del MIID; Dirección solo pone o quita "
                              "«Por confirmar».")
        before = {section.id: section.to_confirm for section in self}
        res = super().write(vals)
        for section in self:
            if 'to_confirm' in vals and before[section.id] != section.to_confirm:
                section.message_post(body=(
                    "«Por confirmar» quitado por %s." % self.env.user.name if not section.to_confirm
                    else "Marcada «Por confirmar» por %s: %s" % (self.env.user.name, section.to_confirm_note or '')))
            if 'body' in vals:
                section.message_post(body="Texto de la sección editado.")
        return res

    def _sgi_heading(self):
        self.ensure_one()
        return " ".join(x for x in (self.clause, self.name) if x)

    def _sgi_body_parts(self):
        """(antes, después) del texto, partido en el párrafo [[datos]]. Sin la
        marca, todo es «antes» y el bloque va al final. La marca no se imprime."""
        self.ensure_one()
        body = str(self.body or '')
        match = MIID_DATA_MARK_RE.search(body)
        if not match:
            return Markup(body), Markup('')
        rest = MIID_DATA_MARK_RE.sub('', body[match.end():])
        return Markup(body[:match.start()]), Markup(rest)


class SgiMiidRowNote(models.Model):
    """Nota fija de un renglón de un bloque vivo del MIID (la «Situación» de cada
    anexo en 12.1). La edita el Jefe MAST."""
    _name = 'sgi.miid.row.note'
    _description = "Nota de renglón del MIID"
    _order = 'section_id, sequence, id'

    section_id = fields.Many2one('sgi.miid.section', string="Sección del MIID", required=True,
                                 index=True, ondelete='restrict')
    company_id = fields.Many2one(related='section_id.company_id', store=True, index=True)
    sequence = fields.Integer(string="Orden", default=10)
    key = fields.Char(string="Renglón", required=True,
                      help="Clave del renglón al que se pega la nota (p. ej. ANEXO 9).")
    text = fields.Char(string="Nota", required=True)


class SgiMiid(models.Model):
    """Manual del SGI (MIID) de una empresa: la vista del sistema, su
    comparación con la revisión vigente y el historial de revisiones. Uno por
    empresa; lo crea la primera apertura o el paso diario."""
    _name = 'sgi.miid'
    _description = "Manual del SGI (MIID)"
    _inherit = ['mail.thread', 'mail.activity.mixin']

    name = fields.Char(string="Nombre", compute='_compute_name')
    company_id = fields.Many2one('res.company', string="Empresa", required=True, index=True, readonly=True)
    document_id = fields.Many2one('documents.document', string="Revisión vigente del MIID",
                                  compute='_compute_document', compute_sudo=True)
    revision_label = fields.Char(string="Rev. vigente", compute='_compute_document', compute_sudo=True)
    issue_date = fields.Date(string="Emisión de la revisión vigente", compute='_compute_document',
                             compute_sudo=True)
    revision_ids = fields.Many2many('documents.document', string="Revisiones del MIID",
                                    compute='_compute_document', compute_sudo=True)
    pending_request_id = fields.Many2one('approval.request', string="Solicitud de cambio en curso",
                                         compute='_compute_document', compute_sudo=True)
    state = fields.Selection([
        ('al_dia', "Al día"), ('desactualizado', "Desactualizado"),
        ('sin_base', "Sin línea base"), ('sin_documento', "Sin MIID vigente"),
    ], string="Situación del MIID", compute='_compute_state', compute_sudo=True,
        help="Al día: la revisión vigente coincide con los datos del sistema. Sin línea base: la "
             "revisión vigente no se generó desde Odoo y no hay contra qué comparar.")
    diff_html = fields.Html(string="Diferencias con la revisión vigente", compute='_compute_state',
                            compute_sudo=True, sanitize=False)
    blocker_html = fields.Html(string="Lo que impide aprobar una revisión", compute='_compute_blockers',
                               compute_sudo=True, sanitize=False)
    to_confirm_count = fields.Integer(string="Secciones por confirmar", compute='_compute_blockers',
                                      compute_sudo=True)
    outdated_since = fields.Datetime(string="Desactualizado desde", readonly=True, copy=False)
    last_check = fields.Datetime(string="Última comparación", readonly=True, copy=False)
    _company_uniq = models.Constraint('UNIQUE(company_id)', "Ya existe el MIID de esta empresa.")

    # ------------------------------------------------------------------
    # Búsquedas
    # ------------------------------------------------------------------
    @api.model
    def _sgi_get(self, company=None):
        company = company or self.env['sgi.config']._sgi_company()
        miid = self.sudo().search([('company_id', '=', company.id)], limit=1)
        return miid or self.sudo().create({'company_id': company.id})

    def _sgi_documents(self):
        """Todas las revisiones del MIID de la empresa (por la clave, no por el
        tipo: en la copia de producción de las pruebas el real queda REAL~…)."""
        self.ensure_one()
        return self.env['documents.document'].sudo().with_context(active_test=False).search([
            ('sgi_code', '=', MIID_CODE), ('sgi_is_controlled', '=', True),
            ('company_id', '=', self.company_id.id)], order='sgi_revision desc, id desc')

    def _sgi_current_document(self):
        self.ensure_one()
        return self._sgi_documents().filtered(lambda d: d.active and d.sgi_state == 'vigente')[:1]

    def _sgi_open_request(self, doc):
        if not doc:
            return self.env['approval.request']
        return self.env['approval.request'].sudo().search([
            ('sgi_document_id', '=', doc.id), ('sgi_miid_hash', '!=', False),
            ('request_status', 'in', ('new', 'pending'))], order='id desc', limit=1)

    @staticmethod
    def _sgi_next_revision(doc):
        """Q5: vigente + 1; sin línea base, al menos la Rev. 03."""
        revision = (doc.sgi_revision or 0) + 1 if doc else MIID_FIRST_ODOO_REVISION
        if not (doc and doc.sgi_content_hash):
            revision = max(revision, MIID_FIRST_ODOO_REVISION)
        return revision

    @api.depends('company_id')
    def _compute_name(self):
        for miid in self:
            miid.name = "Manual del SGI (MIID) — %s" % (miid.company_id.name or '')

    @api.depends('company_id')
    def _compute_document(self):
        for miid in self:
            docs = miid._sgi_documents()
            vigente = docs.filtered(lambda d: d.active and d.sgi_state == 'vigente')[:1]
            miid.document_id = vigente
            miid.revision_label = vigente.sgi_revision_label if vigente else False
            miid.issue_date = vigente.sgi_issue_date if vigente else False
            miid.revision_ids = docs
            miid.pending_request_id = miid._sgi_open_request(vigente)

    @api.depends('company_id')
    def _compute_state(self):
        for miid in self:
            status = miid._sgi_status()
            miid.state = status['state']
            diffs = status['diffs']
            miid.diff_html = Markup("<ul>%s</ul>") % Markup('').join(
                Markup("<li>%s</li>") % d for d in diffs) if diffs else False

    @api.depends('company_id')
    def _compute_blockers(self):
        for miid in self:
            blockers = miid._sgi_blockers()
            miid.to_confirm_count = miid.env['sgi.miid.section'].sudo().search_count(
                [('company_id', '=', miid.company_id.id), ('to_confirm', '=', True)])
            miid.blocker_html = Markup("<ul class='mb-0'>%s</ul>") % Markup('').join(
                Markup("<li>%s</li>") % b for b in blockers) if blockers else False

    # ------------------------------------------------------------------
    # Foto de los datos y huella (1.5)
    # ------------------------------------------------------------------
    def _sgi_env(self):
        return self.sudo().with_context(lang=MIID_LANG, active_test=True).env

    def _sgi_snapshot(self):
        """Foto de lo que imprime el MIID, con claves (nunca etiquetas
        traducibles). Lo impreso sale de la misma foto."""
        self.ensure_one()
        env = self._sgi_env()
        company = self.company_id
        Doc = env['documents.document']
        excluded = Doc._sgi_dropbox_excluded_domain()
        Param = env['ir.config_parameter']
        raw = {}

        sections = env['sgi.miid.section'].search([('company_id', '=', company.id)])
        raw['sections'] = {str(s.id): [s.clause or '', s.name or '', s.heading_level or '',
                                       _miid_plain(s.body), s.live_block or '', bool(s.body_fallback)]
                           for s in sections}
        raw['row_notes'] = {str(s.id): {n.key: n.text for n in s.row_note_ids}
                            for s in sections if s.row_note_ids}
        raw['pendientes'] = sorted(sections.filtered('to_confirm').ids)

        processes = env['sgi.process'].search([('company_id', '=', company.id)])
        raw['processes'] = {p.code: [p.name or '', p.process_type or '', p.owner_id.name or '',
                                     p.state or '', p.parent_id.code or ''] for p in processes}
        flows = env['sgi.process.flow'].search([('from_process_id', 'in', processes.ids),
                                                ('to_process_id', 'in', processes.ids)])
        raw['flows'] = sorted([f.from_process_id.code, f.to_process_id.code, f.name or ''] for f in flows)

        policy = env['sgi.policy'].search([('state', '=', 'vigente')], limit=1)
        raw['policy'] = [policy.folio or '', policy.name or '', policy.issue_date,
                         _miid_plain(policy.policy_text)] if policy else None
        objectives = env['sgi.objective'].search([('policy_id', '=', policy.id)]) if policy \
            else env['sgi.objective']
        raw['objectives'] = {str(o.id): [o.name or '', o.target_year or 0,
                                         sorted(o.indicator_ids.filtered('active').mapped('code'))]
                             for o in objectives}

        types = env['sgi.document.type'].search([('company_id', 'in', [company.id, False])])
        raw['doc_types'] = {t.code: [t.name or '', t.prefix_pattern or '', t.legacy_code_regex or '',
                                     bool(t.code_required)] for t in types}

        base = [('sgi_is_controlled', '=', True), ('company_id', '=', company.id)] + excluded
        controls = Doc.search(base + [('sgi_doc_type', '=', 'control_operacional'),
                                      ('sgi_state', '=', 'vigente')])
        raw['controls'] = {d.sgi_code: [d.sgi_revision or 0, d.sgi_title or d.name or '',
                                        d.sgi_previous_code or ''] for d in controls if d.sgi_code}

        raw['nc'] = dict(env['quality.alert']._sgi_deadline_days(),
                         escalation=_miid_int(Param, 'quimibond_sgi.nc_escalation_days', 5),
                         escalation_external=_miid_int(Param, 'quimibond_sgi.nc_escalation_days_external', 3),
                         effectiveness=_miid_int(Param, 'quimibond_sgi.nc_effectiveness_days', 90))

        norms = env['sgi.norm'].search([]).filtered('clause_ids')
        raw['norms'] = {n.code: [n.name or '', sorted(n.clause_ids.mapped('code'))] for n in norms}

        # Solo los del Dropbox (clave P-X99): los procedimientos de proceso
        # nuevos (PR-…) no son «anteriores».
        legacy = base + [('sgi_doc_type', '=', 'procedimiento'), ('sgi_legacy_family', '!=', False)]
        previous = Doc.search(legacy + [('sgi_state', '=', 'vigente')])
        previous |= Doc.search(legacy + [('sgi_state', '=', 'obsoleto'),
                                         ('sgi_replaced_by_process_id', 'in', processes.ids)])
        raw['previous'] = {}
        for doc in previous:
            code = doc.sgi_previous_code or doc.sgi_code
            if not code:
                continue
            process = doc.sgi_replaced_by_process_id or doc.sgi_process_id
            raw['previous'][code] = [doc.sgi_revision or 0, doc.sgi_title or doc.name or '',
                                     doc.sgi_state or '', process.code or '']

        annexes = Doc.search(base + [('sgi_doc_type', '=', 'anexo'), ('sgi_state', '=', 'vigente')])
        raw['annexes'] = {d.sgi_code: [d.sgi_revision or 0, d.sgi_title or d.name or '']
                          for d in annexes if d.sgi_code}

        current = self._sgi_current_document()
        raw['identificacion'] = {
            'code': MIID_CODE,
            'revision': current.sgi_revision if current else None,
            'issue_date': current.sgi_issue_date if current else None,
            'baseline': bool(current.sgi_content_hash) if current else False,
            'processes_total': len(processes),
            'processes_ready': len(processes.filtered(lambda p: p.state in MIID_READY_PROCESS_STATES)),
        }
        raw['historial'] = [[d.sgi_revision or 0, d.sgi_issue_date, d.sgi_state or '',
                             d.sgi_doc_change_id.name or '', d.sgi_doc_change_id.sgi_reason or '']
                            for d in self._sgi_documents()]
        # C-2: claves en texto y tipos de JSON, igual que la foto guardada.
        return json.loads(json.dumps(raw, sort_keys=True, ensure_ascii=False, default=str))

    @api.model
    def _sgi_hash(self, snapshot):
        content = {k: v for k, v in (snapshot or {}).items() if k not in MIID_UNHASHED}
        raw = json.dumps(content, sort_keys=True, ensure_ascii=False, default=str)
        return hashlib.sha256(raw.encode('utf-8')).hexdigest()

    def _sgi_approved_snapshot(self):
        """Foto con que se generó la revisión vigente (la de su solicitud)."""
        self.ensure_one()
        raw = self._sgi_current_document().sgi_doc_change_id.sgi_miid_snapshot
        try:
            return json.loads(raw) if raw else None
        except ValueError:
            return None

    # ------------------------------------------------------------------
    # Diferencias legibles
    # ------------------------------------------------------------------
    @api.model
    def _sgi_diff(self, old, new):
        """Textos de lo que cambió entre dos fotos (máximo MIID_DIFF_LIMIT)."""
        old, new, out = old or {}, new or {}, []
        Process = self.env['sgi.process'].with_context(lang=MIID_LANG)
        state_labels = dict(Process._fields['state']._description_selection(Process.env))
        type_labels = dict(Process._fields['process_type']._description_selection(Process.env))

        def keyed(key, added, removed, changed):
            before, after = old.get(key) or {}, new.get(key) or {}
            out.extend(added(k, after[k]) for k in sorted(set(after) - set(before)))
            out.extend(removed(k, before[k]) for k in sorted(set(before) - set(after)))
            for k in sorted(set(before) & set(after)):
                if before[k] != after[k]:
                    out.extend(changed(k, before[k], after[k]))

        def heading(row):
            return " ".join(x for x in (row[0], row[1]) if x)

        def section_changed(_k, a, b):
            return ["Sección editada: %s" % heading(b)]
        keyed('sections', lambda k, v: "Sección nueva: %s" % heading(v),
              lambda k, v: "Sección archivada: %s" % heading(v), section_changed)

        def note_changed(_k, a, b):
            rows = []
            for key in sorted(set(a) | set(b)):
                if a.get(key) != b.get(key):
                    rows.append("Cambió la situación del %s" % key)
            return rows
        keyed('row_notes', lambda k, v: "Notas nuevas en una sección (%s)" % ", ".join(sorted(v)),
              lambda k, v: "Se quitaron notas de una sección (%s)" % ", ".join(sorted(v)), note_changed)

        def process_changed(code, a, b):
            rows = []
            if a[0] != b[0] or a[1] != b[1]:
                rows.append("Cambió el nombre o el tipo de %s: %s → %s (%s)" % (
                    code, a[0], b[0], type_labels.get(b[1], b[1])))
            if a[2] != b[2]:
                rows.append("Cambió el dueño de %s: %s → %s" % (code, a[2] or "sin dueño", b[2] or "sin dueño"))
            if a[3] != b[3]:
                rows.append("Cambió el estado de %s: %s → %s" % (
                    code, state_labels.get(a[3], a[3]), state_labels.get(b[3], b[3])))
            if a[4:] != b[4:]:
                rows.append("Cambió el macroproceso de %s" % code)
            return rows
        keyed('processes', lambda k, v: "Proceso nuevo: %s %s" % (k, v[0]),
              lambda k, v: "Proceso dado de baja: %s %s" % (k, v[0]), process_changed)

        old_flows = {tuple(f) for f in old.get('flows') or []}
        new_flows = {tuple(f) for f in new.get('flows') or []}
        if old_flows != new_flows:
            out.append("Cambió la interacción entre procesos (%d flujo(s) nuevo(s), %d quitado(s))" % (
                len(new_flows - old_flows), len(old_flows - new_flows)))

        old_policy, new_policy = old.get('policy'), new.get('policy')
        if (old_policy or [None])[0] != (new_policy or [None])[0]:
            out.append("Política nueva: %s" % (new_policy[0] or new_policy[1]) if new_policy
                       else "Ya no hay política vigente")
        elif old_policy != new_policy:
            out.append("Cambió el texto de la política")

        keyed('objectives', lambda k, v: "Objetivo nuevo: %s" % v[0],
              lambda k, v: "Objetivo retirado: %s" % v[0],
              lambda k, a, b: ["Cambiaron los indicadores del objetivo %s" % b[0]] if a[2] != b[2]
              else ["Cambió el objetivo %s" % b[0]])
        keyed('doc_types', lambda k, v: "Tipo de documento nuevo: %s" % v[0],
              lambda k, v: "Tipo de documento retirado: %s" % v[0],
              lambda k, a, b: ["Cambió el patrón de clave de %s" % b[0]])
        keyed('controls', lambda k, v: "Control operacional nuevo: %s" % k,
              lambda k, v: "Control operacional que ya no está vigente: %s" % k,
              lambda k, a, b: ["Nueva revisión de %s (Rev. %02d)" % (k, b[0] or 0)] if a[0] != b[0]
              else ["Cambió el título de %s" % k])

        old_nc, new_nc = old.get('nc') or {}, new.get('nc') or {}
        for key, label, unit in MIID_NC_ROWS:
            if old_nc.get(key) != new_nc.get(key):
                out.append("Cambiaron los plazos de NC: %s %s → %s %s" % (
                    label.lower(), old_nc.get(key), new_nc.get(key), unit))

        keyed('norms', lambda k, v: "Norma nueva: %s" % k, lambda k, v: "Norma retirada: %s" % k,
              lambda k, a, b: ["Cambiaron las cláusulas de %s" % k])

        def previous_changed(code, a, b):
            if a[3] != b[3]:
                return ["Procedimiento anterior: %s ahora lo sustituye %s" % (code, b[3] or "ningún proceso")]
            if a[2] != b[2]:
                return ["Procedimiento anterior %s: ahora %s" % (code, b[2])]
            return ["Cambió el procedimiento anterior %s" % code]
        keyed('previous', lambda k, v: "Procedimiento anterior nuevo: %s" % k,
              lambda k, v: "Procedimiento anterior que ya no aparece: %s" % k, previous_changed)
        keyed('annexes', lambda k, v: "Anexo nuevo: %s" % k, lambda k, v: "Anexo que ya no está vigente: %s" % k,
              lambda k, a, b: ["Nueva revisión del %s (Rev. %02d)" % (k, b[0] or 0)] if a[0] != b[0]
              else ["Cambió el título del %s" % k])

        if len(out) > MIID_DIFF_LIMIT:
            out = out[:MIID_DIFF_LIMIT] + ["… y %d diferencia(s) más." % (len(out) - MIID_DIFF_LIMIT)]
        return out

    # ------------------------------------------------------------------
    # Situación y candados (C-3: por método; los campos son para mostrar)
    # ------------------------------------------------------------------
    def _sgi_status(self):
        """{'state', 'diffs'} de la revisión vigente contra la vista del sistema."""
        self.ensure_one()
        doc = self._sgi_current_document()
        if not doc:
            return {'state': 'sin_documento', 'diffs': []}
        if not doc.sgi_content_hash:
            return {'state': 'sin_base', 'diffs': []}
        live = self._sgi_snapshot()
        if self._sgi_hash(live) == doc.sgi_content_hash:
            return {'state': 'al_dia', 'diffs': []}
        diffs = self._sgi_diff(self._sgi_approved_snapshot(), live) or [
            "La revisión vigente no tiene la foto de sus datos."]
        return {'state': 'desactualizado', 'diffs': diffs}

    def _sgi_blockers(self):
        """Lo que impide enviar o aprobar una revisión del MIID (Q16, Q17)."""
        self.ensure_one()
        env = self._sgi_env()
        out = []
        for section in env['sgi.miid.section'].search([('company_id', '=', self.company_id.id),
                                                       ('to_confirm', '=', True)]):
            out.append("La sección %s está por confirmar: %s" % (
                section._sgi_heading(), section.to_confirm_note or ''))
        processes = env['sgi.process'].search([('company_id', '=', self.company_id.id)])
        if not processes:
            out.append("No hay procesos activos en la empresa.")
        pending = processes.filtered(lambda p: p.state not in MIID_READY_PROCESS_STATES)
        if pending:
            Process = env['sgi.process']
            labels = dict(Process._fields['state']._description_selection(env))
            by_state = {}
            for process in pending:
                by_state.setdefault(process.state, []).append(process.code)
            out.append("%s: el MIID solo se aprueba cuando todos los procesos están vigentes." % "; ".join(
                "%d proceso(s) en %s (%s)" % (len(codes), (labels.get(state) or state or '').lower(),
                                              ", ".join(sorted(codes)))
                for state, codes in sorted(by_state.items())))
        return out

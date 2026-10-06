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
import base64
import hashlib
import json
import logging
import re

import psycopg2
from markupsafe import Markup, escape

from odoo import api, fields, models
from odoo.exceptions import AccessError, UserError, ValidationError
from odoo.tools import file_open, mute_logger

# Sin modelos (sgi_calendar, sgi_menu_paths) o ya cargados (sgi_report_print).
from .sgi_calendar import sgi_add_business_days, sgi_local_date, sgi_today
from .sgi_menu_paths import sgi_menu_path
from .sgi_report_print import DG_COLORS, DG_LEVEL_BG

try:  # C-1: texto sin formato (html2plaintext convierte <b> en *…*).
    from odoo.tools.mail import html_to_inner_content
except ImportError:  # pragma: no cover - versiones sin la función
    html_to_inner_content = None

_logger = logging.getLogger(__name__)

MIID_CODE = 'MIID'
MIID_NOTICE_KIND = 'miid_desactualizado'
MIID_HELD_KIND = 'miid_retenido'
# Q5: la primera revisión generada desde Odoo continúa la numeración del
# Dropbox (Rev. 02 vigente, borrador Rev. 03). Solo aplica mientras la
# vigente no tenga huella (cargada del Dropbox).
MIID_FIRST_ODOO_REVISION = 3
MIID_LANG = 'es_MX'
MIID_DATA_MARK = "[[datos]]"
MIID_SEED_FILE = 'quimibond_sgi/data/sgi_miid_sections.xml'
MIID_EDITED_MSG = "Texto de la sección editado."
# Por qué cambió la siembra (mensaje del chatter de _sgi_seed_update):
# 57.110.0 usa este; 57.113.0 pasa el suyo.
MIID_SEED_REASON = "lo que traía la revisión 02 del MIID"
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
            # 57.110.0: la marca de edición es la que respeta _sgi_seed_update;
            # solo una migración (superusuario) la omite.
            if 'body' in vals and not (self.env.su and self.env.context.get('sgi_miid_seed')):
                section.message_post(body=MIID_EDITED_MSG)
        return res

    @api.model
    def _sgi_seed_fields(self, xmlid):
        """{'body': html, 'to_confirm_note': texto} de la sección tal como está
        hoy en el archivo de datos (la siembra es noupdate)."""
        from lxml import etree
        with file_open(MIID_SEED_FILE, 'rb') as fh:
            tree = etree.parse(fh)
        out = {}
        for field in tree.xpath("//record[@id=$rid]/field", rid=xmlid):
            name = field.get('name')
            if name == 'body':
                out['body'] = (field.text or '') + ''.join(
                    etree.tostring(child, encoding='unicode') for child in field)
            elif name == 'to_confirm_note':
                out['to_confirm_note'] = field.text or ''
        return out

    @api.model
    def _sgi_seed_message(self, tag, keys, reason=MIID_SEED_REASON):
        """Mensaje del chatter cuando la siembra actualiza una sección, con
        el género y el número de lo que cambió (57.113.0): «texto
        actualizado», «nota de «Por confirmar» actualizada» o «texto y nota
        de «Por confirmar» actualizados»."""
        keys = set(keys)
        if keys == {'body'}:
            what = "texto actualizado"
        elif keys == {'to_confirm_note'}:
            what = "nota de «Por confirmar» actualizada"
        else:
            what = "texto y nota de «Por confirmar» actualizados"
        return "%s: %s con %s." % (tag, what, reason)

    @api.model
    def _sgi_seed_update(self, body_xmlids, old_notes, tag, reason=MIID_SEED_REASON):
        """Pone en producción el texto nuevo de la siembra sin pisar lo que
        corrigió el Jefe MAST (57.110.0). El texto se cambia solo en las
        secciones que nunca se editaron (sin el mensaje «Texto de la sección
        editado.»); la nota de «Por confirmar», solo si sigue idéntica a la
        sembrada (old_notes: {xmlid: nota anterior}). ``reason`` completa el
        mensaje del chatter («… actualizado con <reason>.»; 57.113.0 pasa las
        rutas nuevas del menú). Devuelve {xmlid: resultado}."""
        result = {}
        Message = self.env['mail.message'].sudo()
        for xmlid in list(body_xmlids) + [x for x in old_notes if x not in body_xmlids]:
            section = self.env.ref('quimibond_sgi.%s' % xmlid, raise_if_not_found=False)
            if not section or section._name != self._name:
                result[xmlid] = 'no existe'
                continue
            seed = self._sgi_seed_fields(xmlid)
            vals, skipped = {}, []
            if xmlid in body_xmlids and seed.get('body'):
                edited = Message.search_count([('model', '=', self._name), ('res_id', '=', section.id),
                                               ('body', 'ilike', MIID_EDITED_MSG)])
                if edited:
                    skipped.append("texto")
                elif _miid_plain(section.body) != _miid_plain(seed['body']):
                    vals['body'] = seed['body']
            if xmlid in old_notes and seed.get('to_confirm_note'):
                if (section.to_confirm_note or '').strip() == old_notes[xmlid].strip():
                    vals['to_confirm_note'] = seed['to_confirm_note']
                elif (section.to_confirm_note or '').strip() != seed['to_confirm_note'].strip():
                    skipped.append("nota")
            if vals:
                section.sudo().with_context(sgi_miid_seed=True).write(vals)
                section.sudo().message_post(body=self._sgi_seed_message(tag, vals, reason))
            if skipped:
                if len(skipped) > 1:
                    what = "texto y nota de «Por confirmar» nuevos para esta sección, pero no se aplicaron " \
                           "porque ya se editaron"
                elif skipped == ["texto"]:
                    what = "texto nuevo para esta sección, pero no se aplicó porque ya se editó"
                else:
                    what = "una nota de «Por confirmar» nueva para esta sección, pero no se aplicó porque " \
                           "ya se editó"
                section.sudo().message_post(body=(
                    "%s: la siembra trae %s a mano. Compárelo con "
                    "docs/sgi/transicion/miid-rev03-borrador.md.") % (tag, what))
                _logger.warning("%s: sección MIID %s editada a mano; no se toca (%s).", tag, xmlid,
                                ", ".join(skipped))
            result[xmlid] = ("actualizada" if vals else "sin cambio") + (
                " (editada a mano: %s)" % ", ".join(skipped) if skipped else "")
        return result

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
    live_html = fields.Html(string="Vista del sistema", compute='_compute_live_html',
                            compute_sudo=True, sanitize=False)
    # 1.12-4: el historial se pinta con sudo (un Usuario SGI no siempre lee
    # las revisiones obsoletas en Documentos).
    history_html = fields.Html(string="Historial de revisiones", compute='_compute_history_html',
                               compute_sudo=True, sanitize=False)

    _company_uniq = models.Constraint('UNIQUE(company_id)', "Ya existe el MIID de esta empresa.")

    # ------------------------------------------------------------------
    # Búsquedas
    # ------------------------------------------------------------------
    @api.model
    def _sgi_get(self, company=None):
        """El MIID de la empresa; lo crea si no existe. Si otra transacción lo
        crea al mismo tiempo (índice único), no truena ni deja ERROR en el
        log: devuelve lo que vea (puede venir vacío hasta la siguiente vez)."""
        company = company or self.env['sgi.config']._sgi_company()
        Miid = self.sudo()
        miid = Miid.search([('company_id', '=', company.id)], limit=1)
        if miid:
            return miid
        try:
            with self.env.cr.savepoint(), mute_logger('odoo.sql_db'):
                return Miid.create({'company_id': company.id})
        except psycopg2.IntegrityError:
            _logger.info("SGI: el MIID de %s lo creó otra transacción.", company.name)
            return Miid.search([('company_id', '=', company.id)], limit=1)

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

    @api.depends('company_id')
    def _compute_history_html(self):
        for miid in self:
            rows = Markup('').join(
                Markup("<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>") % (
                    doc.sgi_revision_label or '', doc.sgi_issue_date.strftime('%d/%m/%Y') if doc.sgi_issue_date
                    else '', dict(doc._fields['sgi_state']._description_selection(doc.env)).get(
                        doc.sgi_state, doc.sgi_state or ''),
                    doc.sgi_doc_change_id.name or "Carga inicial desde el Dropbox", doc.name or '')
                for doc in miid._sgi_documents())
            miid.history_html = Markup(
                "<table class='table table-sm table-bordered'><thead><tr><th>Rev.</th><th>Emisión</th>"
                "<th>Estado</th><th>Solicitud</th><th>Archivo</th></tr></thead><tbody>%s</tbody></table>") % rows \
                if rows else False

    def _compute_live_html(self):
        for miid in self:
            miid.live_html = self.env['ir.qweb'].sudo()._render('quimibond_sgi.report_miid_body', {
                'doc': miid, 'miid': miid._sgi_print_data('live'), 'miid_mode': 'screen',
                'dg_colors': DG_COLORS, 'dg_level_bg': DG_LEVEL_BG})

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
                                       _miid_plain(s.body), s.live_block or '', bool(s.body_fallback),
                                       s.sequence or 0]
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
            if a[:6] == b[:6]:
                return ["Sección movida de lugar: %s" % heading(b)]
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

    # ------------------------------------------------------------------
    # Lo que se imprime
    # ------------------------------------------------------------------
    def _sgi_filter_diagram(self, dg, process_ids):
        """M-1: solo los procesos de la empresa del MIID (sgi.diagram busca en
        todas las empresas)."""
        keep = {"sgi.process,%d" % pid for pid in process_ids}
        dg = dict(dg)
        lanes = []
        for lane in dg.get('lanes') or []:
            items = [i for i in lane.get('items') or [] if i.get('key') in keep]
            if items:
                lanes.append(dict(lane, items=items))
        dg['lanes'] = lanes
        dg['edges'] = [e for e in dg.get('edges') or [] if e.get('from') in keep and e.get('to') in keep]
        matrix = dg.get('matrix')
        if matrix:
            rows = [r for r in matrix.get('rows') or [] if r.get('key') in keep]
            cols = [c for c in matrix.get('cols') or [] if c.get('key') in keep]
            source, cells = matrix.get('cells') or {}, {}
            for row in rows:
                for col in cols:
                    cell = source.get(row['key'], {}).get(col['key'])
                    if cell:
                        cells.setdefault(row['key'], {})[col['key']] = cell
            dg['matrix'] = dict(matrix, rows=rows, cols=cols, cells=cells)
        return dg

    def _sgi_correspondence(self, env, process_ids):
        """Por norma con cláusulas: numeral → procesos de la empresa que lo
        cumplen (la matriz de cumplimiento, columnas de la empresa)."""
        result = []
        for norm in env['sgi.norm'].search([]).filtered('clause_ids'):
            matrix = norm._sgi_compliance_matrix()
            columns = [(index, p) for index, p in enumerate(matrix['processes']) if p.id in process_ids]
            rows, covered = [], False
            for row in matrix['rows']:
                codes = [p.code for index, p in columns if row['cells'][index]]
                covered = covered or bool(codes)
                rows.append([row['clause'].code or '', row['clause'].name or '', ", ".join(codes) or "—"])
            if covered:
                result.append({'name': "%s %s" % (norm.code or '', norm.name or ''), 'rows': rows})
        return result

    def _sgi_print_data(self, mode='live', request=None, revision=None, with_diagrams=False):
        """Lo que pinta la plantilla: la foto con etiquetas, las secciones
        partidas en [[datos]], los bloques sin sección y los candados."""
        self.ensure_one()
        env = self._sgi_env()
        snap = self._sgi_snapshot()
        company = self.company_id
        blockers = self._sgi_blockers()
        draft = mode == 'live' or bool(blockers)
        current = self._sgi_current_document()
        today = sgi_today(env)
        Process = env['sgi.process']
        state_labels = dict(Process._fields['state']._description_selection(env))
        type_labels = dict(Process._fields['process_type']._description_selection(env))
        processes = Process.search([('company_id', '=', company.id)])
        blocks = {}

        ident = snap['identificacion']
        next_revision = revision or (request.sgi_new_revision if request else None) \
            or self._sgi_next_revision(current)
        rows = [("Clave", MIID_CODE)]
        if current:
            rows.append(("Revisión vigente en Odoo", "%02d, emisión %s" % (
                ident['revision'] or 0, current.sgi_issue_date.strftime('%d/%m/%Y')
                if current.sgi_issue_date else "sin fecha")))
        if mode == 'live':
            rows.append(("Esta vista", "Revisión %02d en borrador%s" % (
                next_revision, "; sustituye a la revisión %02d vigente" % (ident['revision'] or 0)
                if current else "")))
        else:
            rows.append(("Revisión", "%02d%s" % (next_revision, "; sustituye a la revisión %02d" % (
                ident['revision'] or 0) if current else "")))
            if request:
                rows.append(("Solicitud de cambio", request.name or ''))
            rows.append(("Emisión", "La fecha de aprobación de la solicitud"))
        rows.append(("Estado", "Borrador — no vigente" if draft else "Para aprobación"))
        rows.append(("Procesos", "Al %s, %d de %d procesos vigentes" % (
            today.strftime('%d/%m/%Y'), ident['processes_ready'], ident['processes_total'])))
        blocks['identificacion'] = {'rows': rows}

        blocks['procesos'] = {'rows': [
            [code, values[0], type_labels.get(values[1], values[1]), values[2] or "—",
             state_labels.get(values[3], values[3])]
            for code, values in sorted(snap['processes'].items(),
                                       key=lambda kv: (['estrategico', 'cop', 'soporte'].index(kv[1][1])
                                                       if kv[1][1] in ('estrategico', 'cop', 'soporte') else 9,
                                                       kv[0]))]}
        diagrams = []
        if with_diagrams and processes:
            Diagram = env['sgi.diagram']
            for kind in ('process_map', 'interaction_matrix'):
                try:
                    dg = self._sgi_filter_diagram(Diagram.data(kind), processes.ids)
                except Exception:  # noqa: BLE001 - un diagrama roto no detiene el MIID
                    _logger.warning("SGI: no se pudo armar el diagrama %s del MIID.", kind, exc_info=True)
                    continue
                diagrams.append({'dg': dg, 'dg_edges': Diagram._sgi_print_edges(dg),
                                 'dg_subtitle': Diagram._sgi_print_subtitle(dg.get('subtitle'))})
        blocks['procesos']['diagrams'] = diagrams

        policy = env['sgi.policy'].search([('state', '=', 'vigente')], limit=1)
        blocks['politica'] = {'policy': policy and {
            'folio': policy.folio or '', 'name': policy.name or '',
            'issue_date': policy.issue_date.strftime('%d/%m/%Y') if policy.issue_date else '',
            'text': policy.policy_text or ''}}
        objectives = env['sgi.objective'].search([('policy_id', '=', policy.id)]) if policy \
            else env['sgi.objective']
        blocks['objetivos'] = {'rows': [
            [o.name or '', o.target_year or '', ", ".join(
                "%s %s" % (i.code or '', i.name or '') for i in o.indicator_ids.filtered('active').sorted('code'))
             or "—"] for o in objectives]}
        blocks['tipos_documento'] = {'rows': [
            [v[0], v[1] or "Conserva su clave"] for code, v in sorted(
                snap['doc_types'].items(), key=lambda kv: kv[1][0]) if v[3]]}
        blocks['controles'] = {'items': ["%s (%s)" % (v[1], code) for code, v in sorted(snap['controls'].items())]}
        blocks['plazos_nc'] = {'rows': [[label, snap['nc'].get(key), unit] for key, label, unit in MIID_NC_ROWS]}
        blocks['correspondencia'] = {'norms': self._sgi_correspondence(env, processes.ids)}

        names = {p.code: "%s %s" % (p.code, p.name or '') for p in processes}
        by_process = {}
        for code, values in sorted(snap['previous'].items()):
            by_process.setdefault(values[3] or '', []).append(code)
        order = [p.code for p in processes.sorted(lambda p: (
            ['estrategico', 'cop', 'soporte'].index(p.process_type)
            if p.process_type in ('estrategico', 'cop', 'soporte') else 9, p.code or ''))]
        prev_rows = [[names.get(code, code), ", ".join(by_process[code])] for code in order if code in by_process]
        prev_rows += [[code, ", ".join(codes)] for code, codes in sorted(by_process.items())
                      if code and code not in names]
        if by_process.get(''):
            prev_rows.append(["Sin proceso", ", ".join(by_process[''])])
        kept = ["%s (%s)" % (v[2], code) for code, v in sorted(snap['controls'].items()) if v[2]]
        blocks['procedimientos_anteriores'] = {'rows': prev_rows, 'kept': kept}

        notes_section = env['sgi.miid.section'].search([('company_id', '=', company.id),
                                                        ('live_block', '=', 'anexos')], limit=1)
        notes = {n.key: n.text for n in notes_section.row_note_ids}
        annex_rows = [[code, v[1], notes.get(code) or "—"] for code, v in sorted(
            snap['annexes'].items(), key=lambda kv: [int(x) if x.isdigit() else x
                                                     for x in re.split(r'(\d+)', kv[0])])]
        orphan_notes = [[key, text] for key, text in notes.items() if key not in snap['annexes']]
        blocks['anexos'] = {'rows': annex_rows, 'orphan_notes': orphan_notes}

        history = []
        if mode == 'live' or draft:
            history.append(["%02d" % next_revision, "Borrador del %s" % today.strftime('%d/%m/%Y'),
                            "En aprobación" if request else "Vista del sistema (no vigente)"])
        elif request:
            history.append(["%02d" % next_revision, "Al aprobarse %s" % (request.name or ''),
                            request.sgi_reason or ''])
        for rev, issue, state, req_name, reason in snap['historial']:
            history.append(["%02d" % (rev or 0), issue and fields.Date.from_string(issue).strftime('%d/%m/%Y') or '',
                            reason or ("Carga inicial desde el Dropbox" if not req_name else req_name)])
        blocks['historial'] = {'rows': history}

        has_data = {
            'identificacion': True,
            'procesos': bool(blocks['procesos']['rows']),
            'politica': bool(policy),
            'objetivos': bool(blocks['objetivos']['rows']),
            'tipos_documento': bool(blocks['tipos_documento']['rows']),
            'controles': bool(blocks['controles']['items']),
            'plazos_nc': True,
            'correspondencia': bool(blocks['correspondencia']['norms']),
            'procedimientos_anteriores': bool(prev_rows or kept),
            'anexos': bool(annex_rows or orphan_notes),
            'historial': bool(history),
        }

        sections, used = [], set()
        for section in env['sgi.miid.section'].search([('company_id', '=', company.id)]):
            before, after = section._sgi_body_parts()
            used.add(section.live_block)
            sections.append({
                'clause': section.clause or '', 'name': section.name or '', 'level': section.heading_level,
                'block': section.live_block or False, 'fallback': bool(section.body_fallback),
                'before': before, 'after': after, 'to_confirm': section.to_confirm,
                'note': section.to_confirm_note or ''})
        orphans = [(code, label) for code, label in MIID_BLOCKS if code not in used]
        return {'sections': sections, 'blocks': blocks, 'has_data': has_data, 'orphans': orphans,
                'blockers': blockers, 'draft': draft, 'company': company.name or '',
                'revision': next_revision, 'request': request or False,
                'signers': request._sgi_sign_rows() if request else []}

    def _sgi_footer_info(self, mode, revision=None, draft=True):
        """Pie de cada hoja (M-2: sin «Formato controlado del SGI:»)."""
        if mode == 'live':
            return {'label': "MIID · Borrador — no vigente", 'issue_date': False, 'plain': True}
        label = "MIID · Rev. %02d" % (revision or 0)
        if draft:
            label += " · Borrador — no vigente"
        return {'label': label, 'issue_date': False, 'plain': True}

    def _sgi_render_pdf(self, mode='live', revision=None, issue_date=None, request=None):
        """PDF del MIID (bytes). Las pruebas lo parchan."""
        self.ensure_one()
        pdf, _ = self.env['ir.actions.report'].with_context(
            sgi_miid_mode=mode, sgi_miid_revision=revision,
            sgi_miid_request_id=request.id if request else False,
        )._render_qweb_pdf('quimibond_sgi.action_report_miid', self.ids)
        return pdf

    # ------------------------------------------------------------------
    # Pantalla
    # ------------------------------------------------------------------
    @api.model
    def action_open(self):
        miid = self._sgi_get()
        if not miid:
            raise UserError("Se está creando el MIID de la empresa; intente de nuevo en un momento.")
        miid.invalidate_recordset()
        return {'type': 'ir.actions.act_window', 'name': "Manual del SGI (MIID)",
                'res_model': 'sgi.miid', 'res_id': miid.id, 'view_mode': 'form',
                'views': [(self.env.ref('quimibond_sgi.sgi_miid_view_form').id, 'form')],
                'target': 'current'}

    def action_print_live(self):
        self.ensure_one()
        return self.env.ref('quimibond_sgi.action_report_miid').report_action(self, config=False)

    def action_open_vigente_pdf(self):
        self.ensure_one()
        if not self.document_id:
            raise UserError("No hay revisión vigente del MIID.")
        return self.document_id.action_sgi_view_file()

    def action_open_sections(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'name': "Textos del MIID",
                'res_model': 'sgi.miid.section', 'view_mode': 'list,form',
                'views': [(self.env.ref('quimibond_sgi.sgi_miid_section_view_list').id, 'list'),
                          (self.env.ref('quimibond_sgi.sgi_miid_section_view_form').id, 'form')],
                'search_view_id': [self.env.ref('quimibond_sgi.sgi_miid_section_view_search').id],
                'domain': [('company_id', '=', self.company_id.id)],
                'context': {'default_company_id': self.company_id.id}}

    def action_open_request(self):
        self.ensure_one()
        if not self.pending_request_id:
            raise UserError("No hay solicitud de cambio del MIID en curso.")
        return {'type': 'ir.actions.act_window', 'res_model': 'approval.request',
                'res_id': self.pending_request_id.id, 'view_mode': 'form', 'target': 'current'}

    # ------------------------------------------------------------------
    # Solicitud de cambio (1.7)
    # ------------------------------------------------------------------
    def action_sgi_miid_request_change(self):
        """Arma (o abre) la solicitud de cambio documental del MIID con el PDF
        generado, la huella y las diferencias. Nunca la envía ni la aprueba."""
        self.ensure_one()
        if not (self.env.su or self.env.user.has_group('quimibond_sgi.group_sgi_manager')):
            raise UserError("Solo el Jefe MAST solicita el cambio del MIID.")
        doc = self._sgi_current_document()
        if not doc:
            raise UserError("No hay MIID vigente (clave MIID) en Documentos: cárguelo o publíquelo primero.")
        request = self._sgi_open_request(doc)
        if not request:
            snapshot = self._sgi_snapshot()
            baseline = bool(doc.sgi_content_hash)
            revision = self._sgi_next_revision(doc)
            diffs = self._sgi_diff(self._sgi_approved_snapshot(), snapshot) if baseline else []
            category = self.env.ref('quimibond_sgi.sgi_approval_category_doc_change')
            request = self.env['approval.request'].create({
                'name': "Cambio al MIID (Rev. %02d)" % revision,
                'category_id': category.id, 'request_owner_id': self.env.user.id, 'reference': MIID_CODE,
                'sgi_change_kind': 'modificacion', 'sgi_what_changes': 'contenido',
                'sgi_document_id': doc.id, 'sgi_new_revision': revision,
                'sgi_affected_process_ids': [(6, 0, doc.sgi_process_id.ids)],
                'sgi_reason': ("El MIID vigente ya no coincide con el sistema." if baseline
                               else "Primera revisión del MIID generada desde Odoo."),
                'sgi_changes': "\n".join(diffs) or (
                    "Revisión generada desde Odoo con los datos del sistema al %s."
                    % sgi_today(self.env).strftime('%d/%m/%Y')),
                'sgi_miid_hash': self._sgi_hash(snapshot),
                'sgi_miid_snapshot': json.dumps(snapshot, ensure_ascii=False, sort_keys=True),
                'sgi_miid_generated': fields.Datetime.now(),
            })
            request._sgi_miid_attach("MIID Rev. %02d (para aprobación).pdf" % revision)
            request.message_post(body="Solicitud del MIID creada desde %s." % sgi_menu_path('miid'))
        return {'type': 'ir.actions.act_window', 'res_model': 'approval.request',
                'res_id': request.id, 'view_mode': 'form', 'target': 'current'}

    # ------------------------------------------------------------------
    # Comparación diaria y aviso (1.8)
    # ------------------------------------------------------------------
    def _sgi_notice_note(self, since, diffs):
        self.ensure_one()
        doc = self._sgi_current_document()
        head = escape("Desde el %s el MIID vigente (Rev. %s%s) no coincide con el sistema:" % (
            since.strftime('%d/%m/%Y'), doc.sgi_revision_label or '00',
            ", emisión %s" % doc.sgi_issue_date.strftime('%d/%m/%Y') if doc.sgi_issue_date else ''))
        items = Markup('').join(Markup("<li>%s</li>") % d for d in diffs)
        tail = []
        pending = self._sgi_open_request(doc)
        if pending:
            tail.append("Ya está en curso la solicitud %s." % (pending.name or ''))
        tail.append("Abra %s y use «Solicitar cambio del MIID»." % sgi_menu_path('miid'))
        return Markup("<p>%s</p><ul>%s</ul><p>%s</p>") % (head, items, " ".join(tail))

    def _sgi_check(self):
        """Compara la huella viva con la revisión vigente; pone o quita
        «Desactualizado desde» y agenda o cierra el aviso al Jefe MAST (uno por
        empresa, clave miid_desactualizado:<empresa>). Nunca solicita ni aprueba."""
        Cron = self.env['sgi.cron']
        now = fields.Datetime.now()
        for miid in self.sudo():
            miid.invalidate_recordset()
            status = miid._sgi_status()
            key = '%s:%d' % (MIID_NOTICE_KIND, miid.company_id.id)
            open_notices = self.env['mail.activity'].sudo().with_context(active_test=False).search([
                ('sgi_cron_key', '=', key), ('sgi_episode_closed', '=', False)])
            vals = {'last_check': now}
            if status['state'] != 'desactualizado':
                vals['outdated_since'] = False
                miid.write(vals)
                reason = ("el MIID ya coincide con el sistema" if status['state'] == 'al_dia'
                          else "el MIID vigente no tiene contra qué comparar")
                Cron._sgi_close_activities(open_notices, reason)
                continue
            vals['outdated_since'] = miid.outdated_since or now
            miid.write(vals)
            manager_id = Cron._sgi_manager_user_id()
            if not manager_id:
                continue
            since = sgi_local_date(self.env, miid.outdated_since)
            deadline = sgi_add_business_days(self.env, since, MIID_NOTICE_BUSINESS_DAYS)
            Cron._sgi_schedule(miid, "El MIID vigente ya no coincide con el sistema",
                               miid._sgi_notice_note(since, status['diffs']), manager_id,
                               date_deadline=deadline, key=key)
        return True

    # ------------------------------------------------------------------
    # Diagnóstico (1.9)
    # ------------------------------------------------------------------
    @api.model
    def _sgi_diagnostic_lines(self):
        miid = self._sgi_get()
        if not miid:
            return []
        miid.invalidate_recordset()
        Diag = self.env['sgi.diagnostic']
        fix = sgi_menu_path('miid')
        status = miid._sgi_status()
        doc = miid._sgi_current_document()
        lines = []
        if status['state'] == 'al_dia':
            lines.append(Diag._sgi_line('ok', "MIID al día (Rev. %s, emisión %s)." % (
                doc.sgi_revision_label, doc.sgi_issue_date.strftime('%d/%m/%Y') if doc.sgi_issue_date
                else "sin fecha")))
        elif status['state'] == 'desactualizado':
            since = sgi_local_date(self.env, miid.outdated_since) if miid.outdated_since \
                else sgi_today(self.env)
            lines.append(Diag._sgi_line('warn', "MIID desactualizado desde %s: %d diferencia(s) con el sistema." % (
                since.strftime('%d/%m/%Y'), len(status['diffs'])), fix))
        elif status['state'] == 'sin_base':
            lines.append(Diag._sgi_line('warn', "El MIID vigente (Rev. %s) no se generó desde Odoo: no se "
                                                "puede comparar con el sistema." % doc.sgi_revision_label, fix))
        else:
            lines.append(Diag._sgi_line('bad', "No hay MIID vigente (clave MIID).", fix))
        if doc:
            env = miid._sgi_env()
            sections = env['sgi.miid.section'].search_count([('company_id', '=', miid.company_id.id),
                                                            ('to_confirm', '=', True)])
            processes = env['sgi.process'].search([('company_id', '=', miid.company_id.id)])
            not_ready = len(processes.filtered(lambda p: p.state not in MIID_READY_PROCESS_STATES))
            if sections or not_ready or not processes:
                lines.append(Diag._sgi_line(
                    'warn', "La siguiente revisión del MIID no se puede aprobar: %d sección(es) por confirmar "
                            "y %d proceso(s) sin publicar." % (sections, not_ready), fix))
        return lines


class ReportSgiMiid(models.AbstractModel):
    """57.105.0: valores del PDF del MIID. Modo por contexto: «live» (vista del
    sistema, copia no controlada) o «request» (el PDF de la solicitud, que es
    el que se firma y se publica)."""
    _name = 'report.quimibond_sgi.report_miid_document'
    _description = "MIID (PDF)"

    @api.model
    def _get_report_values(self, docids, data=None):
        docs = self.env['sgi.miid'].browse(docids)
        ctx = self.env.context
        mode = ctx.get('sgi_miid_mode') or 'live'
        request = self.env['approval.request'].sudo().browse(ctx.get('sgi_miid_request_id') or []).exists()
        now = fields.Datetime.context_timestamp(self, fields.Datetime.now())
        return {
            'doc_ids': docs.ids, 'doc_model': 'sgi.miid', 'docs': docs,
            'miid_mode': mode, 'miid_request': request or False,
            'miid_revision': ctx.get('sgi_miid_revision'),
            'miid_printed': now.strftime('%d/%m/%Y %H:%M'),
            'dg_colors': DG_COLORS, 'dg_level_bg': DG_LEVEL_BG,
        }


class ApprovalRequestMiid(models.Model):
    """La solicitud de cambio del MIID es una solicitud de cambio documental de
    siempre con la huella y la foto de los datos con que se generó su PDF."""
    _inherit = 'approval.request'

    sgi_miid_hash = fields.Char(string="Huella del MIID", readonly=True, copy=False,
                                help="Huella de los datos con que se generó el PDF del MIID de esta solicitud.")
    sgi_miid_snapshot = fields.Text(string="Datos del MIID", readonly=True, copy=False,
                                    help="Foto de los datos con que se generó el PDF del MIID.")
    sgi_miid_generated = fields.Datetime(string="MIID generado el", readonly=True, copy=False,
                                         help="Cuándo se generó el PDF del MIID que lleva esta solicitud.")
    sgi_miid_blocked_note = fields.Text(string="Último aviso de candados del MIID", readonly=True, copy=False,
                                        help="Lo que detiene la aprobación del MIID aunque las firmas estén "
                                             "completas.")

    def _sgi_miid(self):
        self.ensure_one()
        return self.env['sgi.miid']._sgi_get(self.sudo().sgi_document_id.company_id or None)

    def _sgi_miid_attach(self, name):
        """Genera el PDF del MIID para esta solicitud, lo adjunta y lo deja
        como el archivo que se manda a firmar (y que se publica)."""
        self.ensure_one()
        pdf = self._sgi_miid()._sgi_render_pdf('request', revision=self.sgi_new_revision, request=self)
        attachment = self.env['ir.attachment'].sudo().create({
            'name': name, 'res_model': 'approval.request', 'res_id': self.id,
            'datas': base64.b64encode(pdf), 'mimetype': 'application/pdf'})
        self.sudo().sgi_change_attachment_id = attachment
        return attachment

    def _sgi_miid_raise_blockers(self, verb):
        for req in self.filtered('sgi_miid_hash'):
            blockers = req._sgi_miid()._sgi_blockers()
            if blockers:
                raise UserError(
                    "No se puede %s el cambio del MIID todavía:\n%s\nQuite «Por confirmar» cuando el texto "
                    "esté confirmado y publique los procesos; después vuelva a intentarlo." % (
                        verb, "\n".join("• " + b for b in blockers)))

    def _sgi_miid_needs_refresh(self, digest):
        """Otra vez el PDF si cambiaron los datos o si se generó con secciones
        por confirmar (llevaba la marca de borrador)."""
        self.ensure_one()
        if digest != self.sgi_miid_hash:
            return True
        try:
            old = json.loads(self.sgi_miid_snapshot or '{}')
        except ValueError:
            return True
        return bool(old.get('pendientes')) or not old.get('identificacion', {}).get('processes_total') or \
            old['identificacion'].get('processes_ready') != old['identificacion'].get('processes_total')

    def _sgi_miid_refresh_before_send(self):
        self.ensure_one()
        miid = self._sgi_miid()
        snapshot = miid._sgi_snapshot()
        digest = miid._sgi_hash(snapshot)
        if not self._sgi_miid_needs_refresh(digest):
            return
        # I-4: el anterior es el que se iba a mandar a firmar.
        old = self.sgi_change_attachment_id or self._sgi_change_attachment()
        stamp = sgi_today(self.env).strftime('%d-%m-%Y')
        if old:
            base = old.name[:-4] if (old.name or '').lower().endswith('.pdf') else (old.name or 'MIID')
            old.sudo().write({'name': "%s (sustituido el %s).pdf" % (base, stamp)})
        try:
            previous = json.loads(self.sgi_miid_snapshot or '{}')
        except ValueError:
            previous = {}
        diffs = miid._sgi_diff(previous, snapshot)
        vals = {'sgi_miid_hash': digest,
                'sgi_miid_snapshot': json.dumps(snapshot, ensure_ascii=False, sort_keys=True),
                'sgi_miid_generated': fields.Datetime.now()}
        if diffs:
            vals['sgi_changes'] = "%s\nActualizado al enviar:\n%s" % (self.sgi_changes or '', "\n".join(diffs))
        self.sudo().write(vals)
        self._sgi_miid_attach("MIID Rev. %02d (para aprobación).pdf" % (self.sgi_new_revision or 0))
        self.message_post(body="Se generó otra vez el PDF del MIID antes de enviar (datos del sistema "
                               "actualizados); el anterior queda como «%s»." % (old.name if old else '—'))

    def action_confirm(self):
        self._sgi_miid_raise_blockers("enviar")
        for req in self.filtered(lambda r: r.sgi_miid_hash and r.request_status == 'new'):
            req._sgi_miid_refresh_before_send()
        return super().action_confirm()

    def action_sgi_send_to_sign(self):
        self._sgi_miid_raise_blockers("mandar a firmar")
        return super().action_sgi_send_to_sign()

    def _sgi_miid_held_head(self):
        """«Firmas completas» solo con la firma de Sign terminada; si no,
        cuántas van."""
        self.ensure_one()
        sign = self.sudo().sgi_sign_request_id
        if sign and sign.state == 'signed':
            return "Firmas completas"
        if sign and self.sgi_sign_progress:
            return "Firma en curso (%s)" % self.sgi_sign_progress
        return "Firma en curso"

    def _sgi_miid_held_key(self):
        return '%s:%d' % (MIID_HELD_KIND, self.id)

    def _sgi_miid_close_held(self, reason):
        for req in self:
            notices = self.env['mail.activity'].sudo().with_context(active_test=False).search([
                ('res_model', '=', 'approval.request'), ('res_id', '=', req.id),
                ('sgi_cron_key', '=', req._sgi_miid_held_key()), ('sgi_episode_closed', '=', False)])
            if notices:
                self.env['sgi.cron']._sgi_close_activities(notices, reason)
            if req.sgi_miid_blocked_note:
                req.sudo().sgi_miid_blocked_note = False

    def action_approve(self, approver=None):
        """Candados del MIID (Q16, Q17). Con el botón: error claro. Desde Sign
        (``sgi_sign_sync``; el cron diario no tiene savepoint por solicitud): no se
        levanta nada, la solicitud del MIID se salta, se anota una vez y se
        avisa al Jefe MAST."""
        if not self.env.context.get('sgi_sign_sync'):
            self._sgi_miid_raise_blockers("aprobar")
            return super().action_approve(approver=approver)
        held = self.env['approval.request']
        Cron = self.env['sgi.cron']
        for req in self.filtered('sgi_miid_hash'):
            blockers = req._sgi_miid()._sgi_blockers()
            if not blockers:
                continue
            held |= req
            text = "\n".join(blockers)
            if req.sgi_miid_blocked_note == text:
                continue
            req.sudo().sgi_miid_blocked_note = text
            head = req._sgi_miid_held_head()
            req.message_post(body=Markup("%s, pero el MIID no se aprueba hasta que:<br/>%s") % (
                head, Markup("<br/>").join(Markup("• %s") % b for b in blockers)))
            manager_id = Cron._sgi_manager_user_id()
            if manager_id:
                Cron._sgi_schedule(
                    req.sudo(), "MIID retenido: faltan los candados",
                    Markup("<p>%s</p><ul>%s</ul><p>%s</p>") % (
                        "Solicitud %s: %s, pero el MIID no se aprueba hasta que:" % (
                            req.name or '', head.lower()),
                        Markup('').join(Markup("<li>%s</li>") % b for b in blockers),
                        "Quite «Por confirmar» cuando el texto esté confirmado y publique los procesos; la "
                        "sincronización diaria con Sign la aprueba sola."),
                    manager_id, date_deadline=sgi_add_business_days(self.env, sgi_today(self.env),
                                                                    MIID_NOTICE_BUSINESS_DAYS),
                    key=req._sgi_miid_held_key())
        rest = self - held
        if not rest:
            return True
        rest.filtered('sgi_miid_hash')._sgi_miid_close_held("se levantaron los candados del MIID")
        return super(ApprovalRequestMiid, rest).action_approve(approver=approver)

    def _sgi_sign_mast_users(self):
        """Q4: el MIID lo aprueba Dirección (parámetro o primer miembro activo
        de «Dirección de Operaciones (SGI)»); si no hay, el de siempre."""
        if not self.sgi_miid_hash:
            return super()._sgi_sign_mast_users()
        param = self.env['ir.config_parameter'].sudo().get_param(MIID_APPROVER_PARAM)
        user = self.env['res.users'].sudo().browse(int(param)).exists() if param and param.isdigit() \
            else self.env['res.users']
        if not (user and user.active):
            user_id = self.env['sgi.cron']._sgi_first_user_id(
                self.env.ref('quimibond_sgi.group_sgi_director', raise_if_not_found=False))
            user = self.env['res.users'].sudo().browse(user_id) if user_id else self.env['res.users']
        return user[:1] or super()._sgi_sign_mast_users()

    def _sgi_archive_signed_pdf(self):
        """I-2: con la aprobación retenida por candados, el PDF firmado espera
        a la revisión nueva (si no, se archivaba en la revisión vieja). Una
        solicitud rechazada o cancelada sí archiva (deja de reintentarse)."""
        if self.sgi_miid_hash and self.request_status in ('new', 'pending'):
            return False
        return super()._sgi_archive_signed_pdf()

    def _sgi_apply_doc_change(self):
        """El MIID se publica con el PDF que se firmó (Q7, lo que se firma es lo
        que se publica) y la revisión nueva lleva la huella de la solicitud."""
        self.ensure_one()
        if not (self.sgi_miid_hash and self.sgi_change_kind == 'modificacion'):
            return super()._sgi_apply_doc_change()
        miid = self._sgi_miid()
        blockers = miid._sgi_blockers()
        if blockers:  # última red: action_approve ya los revisó
            if not self.env.context.get('sgi_sign_sync'):
                raise UserError("No se puede aprobar el cambio del MIID todavía:\n%s" % "\n".join(
                    "• " + b for b in blockers))
            self.message_post(body="El MIID no se publicó: hay candados (secciones por confirmar o "
                                   "procesos sin publicar).")
            return False
        if miid._sgi_hash(miid._sgi_snapshot()) != self.sgi_miid_hash:
            self.message_post(body="Los datos del sistema cambiaron después del envío: se publica el MIID "
                                   "que se firmó; la comparación diaria lo marcará desactualizado.")
        sent = self._sgi_change_attachment()
        if sent:
            sent.sudo().write({'name': "MIID Rev. %02d.pdf" % (self.sgi_new_revision or 0)})
        res = super()._sgi_apply_doc_change()
        target = self.sgi_new_document_id or self.sgi_document_id
        target.sudo().write({'sgi_content_hash': self.sgi_miid_hash})
        self._sgi_miid_close_held("el MIID se aprobó")
        miid._sgi_check()
        return res


class SgiCronMiid(models.AbstractModel):
    _inherit = 'sgi.cron'

    @api.model
    def cron_documents(self):
        res = super().cron_documents()
        self._sgi_step("MIID al día", self._sgi_miid_check)
        return res

    @api.model
    def _sgi_miid_check(self):
        """57.105.0: compara el MIID de la empresa del SGI con el sistema."""
        # Solo la empresa del SGI (D-03).
        self.env['sgi.miid']._sgi_get()._sgi_check()
        return True

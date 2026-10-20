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

MIID_CODE = 'MIID'  # clave del manual (las claves por documento están en MIID_KINDS)
MIID_HELD_KIND = 'miid_retenido'
# Q5: la primera revisión generada desde Odoo continúa la numeración del
# Dropbox (MIID: Rev. 02 vigente, borrador Rev. 03; P-G01: Rev. 11 → 12).
# Solo aplica mientras la vigente no tenga huella (cargada del Dropbox):
# MIID_KINDS[kind]['first_revision'].
MIID_LANG = 'es_MX'
MIID_DATA_MARK = "[[datos]]"
MIID_SEED_FILE = 'quimibond_sgi/data/sgi_miid_sections.xml'
MIID_EDITED_MSG = "Texto de la sección editado."
# 57.146.0: campos de estructura que la migración reacomoda desde la siembra.
# 57.148.0: también el documento al que pertenece la sección (kind).
MIID_SEED_LAYOUT_FIELDS = ('sequence', 'clause', 'name', 'heading_level', 'live_block', 'kind')
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
    ('normas', "Normas y cláusulas que rigen el SGI"),
    ('procesos', "Procesos del SGI, mapa e interacción"),
    ('politica', "Política integral vigente"),
    ('objetivos', "Objetivos integrales e indicadores"),
    ('roles', "Dueños de proceso, roles y autoridades"),
    ('tipos_documento', "Tipos de documento y su clave"),
    ('plazos_documentales', "Plazos documentales"),
    ('controles', "Controles operacionales vigentes"),
    ('plazos_nc', "Plazos de las NC"),
    ('correspondencia', "Correspondencia por cláusula"),
    ('procedimientos_anteriores', "Procedimientos anteriores por proceso"),
    ('anexos', "Anexos vigentes"),
    ('actividades', "Rutinas: actividades del proceso ligadas al documento"),
    ('historial', "Control de cambios"),
    # 57.162.0: lo que era texto del manual y ahora es dato de su registro.
    ('alcance', "Alcance por norma"),
    ('exclusiones', "Requisitos que no aplican"),
    ('reglas', "Reglas que el sistema hace cumplir"),
    ('elaboracion', "Quién elabora cada tipo de documento"),
    ('aprobacion', "Quién firma cada tipo de documento"),
]
# 57.148.0 (Dirección 2026-10-13, «prefiero otra cosa»: el P-G01 no va dentro
# del MIID): el mismo motor genera dos documentos controlados, cada uno con
# sus secciones de texto fijo (sgi.miid.section.kind), sus bloques vivos y su
# huella (solo las llaves de la foto que ese documento imprime). La clave es
# la del documento controlado en Documentos; la primera revisión desde Odoo
# continúa la numeración del Dropbox.
MIID_KINDS = {
    'miid': {
        'code': 'MIID', 'short': "MIID",
        'label': "Manual del SGI (MIID)",
        'title': "Manual Integral de Información Documentada (MIID)",
        'first_revision': 3, 'menu': 'miid', 'notice_kind': 'miid_desactualizado',
        'blocks': ('identificacion', 'alcance', 'exclusiones', 'normas', 'procesos', 'politica', 'objetivos',
                   'roles', 'reglas', 'tipos_documento', 'controles', 'plazos_nc', 'correspondencia',
                   'procedimientos_anteriores', 'anexos', 'historial'),
        'hash_keys': ('sections', 'items', 'row_notes', 'processes', 'flows', 'policy', 'objectives',
                      'doc_types', 'controls', 'nc', 'norms', 'roles', 'clause_activities',
                      'previous', 'annexes', 'scope', 'exclusions', 'rules'),
        'all_processes': True,
        # 57.161.0: pestaña de la pantalla por capítulo (primer número del
        # numeral; '' = sin numeral) y bloques que la pantalla muestra como
        # lista de Odoo (en esa pestaña; en otra, como tabla).
        'pages': {'': 'general', '1': 'objeto', '2': 'normas', '3': 'donde', '4': 'sistema',
                  '5': 'requisitos', '6': 'documentos', '7': 'transicion', '8': 'cambios'},
        'native': {'normas': 'normas', 'procesos': 'sistema', 'politica': 'sistema',
                   'objetivos': 'sistema', 'tipos_documento': 'documentos', 'reglas': 'requisitos'},
    },
    'pg01': {
        'code': 'P-G01', 'short': "P-G01",
        'label': "Control de información documentada (P-G01)",
        'title': "Elaboración y control de información documentada (P-G01)",
        'first_revision': 12, 'menu': 'pg01', 'notice_kind': 'pg01_desactualizado',
        'blocks': ('identificacion', 'tipos_documento', 'elaboracion', 'aprobacion', 'plazos_documentales',
                   'actividades', 'normas', 'historial'),
        'hash_keys': ('sections', 'items', 'row_notes', 'doc_types', 'doc_params', 'activities', 'norms',
                      'doc_roles'),
        'all_processes': False,
        'pages': {'': 'general', '1': 'proposito', '2': 'proposito', '3': 'responsabilidades',
                  '4': 'desarrollo', '5': 'rutinas', '6': 'marco', '7': 'cambios'},
        'native': {'tipos_documento': 'desarrollo', 'actividades': 'rutinas', 'normas': 'marco'},
    },
}
# 57.161.0: pestañas de la pantalla (la ficha las muestra según el documento;
# «otros» junta los capítulos que no están en 'pages').
MIID_PAGES = [
    ('general', "Identificación"), ('objeto', "Objeto y alcance"), ('proposito', "Propósito y alcance"),
    ('responsabilidades', "Responsabilidades"), ('normas', "Normas"), ('donde', "Dónde vive el sistema"),
    ('sistema', "El sistema"), ('requisitos', "Cómo se cumple"), ('desarrollo', "Desarrollo"),
    ('documentos', "Información documentada"), ('rutinas', "Rutinas"), ('marco', "Marco normativo"),
    ('transicion', "Transición"),
    ('cambios', "Control de cambios"), ('otros', "Otros capítulos"),
]
# Campos Html de la pantalla: uno por pestaña (el texto que sigue a la última
# lista nativa) y uno por lista nativa (el texto que va antes de ella).
MIID_SCREEN_SLOTS = (('normas', 'normas'), ('sistema', 'procesos'), ('sistema', 'politica'),
                     ('sistema', 'objetivos'), ('documentos', 'tipos_documento'),
                     ('desarrollo', 'tipos_documento'), ('rutinas', 'actividades'), ('marco', 'normas'),
                     ('requisitos', 'reglas'))
# Columnas de los renglones de una sección (campo del renglón, campo del título).
MIID_ITEM_COLUMNS = (('label', 'item_label_title'), ('clause', 'item_clause_title'),
                     ('text', 'item_text_title'), ('text_2', 'item_text2_title'),
                     ('text_3', 'item_text3_title'))
MIID_ITEM_FORMAT_FIELDS = ('menu_id', 'item_style') + tuple(t for _f, t in MIID_ITEM_COLUMNS)
MIID_ITEMS_SEED_FILE = 'quimibond_sgi/data/sgi_miid_items.xml'
# 57.162.0: alcance, no aplicabilidades, reglas y roles de los tipos de
# documento (datos de norma, cláusula, regla y tipo de documento).
MIID_FIELDS_SEED_FILE = 'quimibond_sgi/data/sgi_miid_fields.xml'
MIID_DOC_ROLE_FIELDS = ('author_text', 'location_text', 'control_text', 'reviewer_text', 'approver_text')
MIID_KIND_SELECTION = [(code, values['label']) for code, values in MIID_KINDS.items()]


def _miid_kind_for_code(code):
    """Documento generado al que pertenece la clave (None si no es generado)."""
    for kind, values in MIID_KINDS.items():
        if values['code'] == code:
            return kind
    return None


# 57.146.0: plazos documentales (parámetros del sistema; el P-G01 vive aquí).
# (llave, etiqueta, parámetro, valor por omisión, cómo se cuenta).
MIID_DOC_ROWS = (
    ('review_years', "Revisión de cada documento vigente", None, 2,
     "años desde su emisión; antes de esa fecha avisa al responsable"),
    ('review_notice', "Primer aviso de revisión", 'quimibond_sgi.doc_review_notice_days', 60,
     "días naturales antes de la próxima revisión"),
    ('review_notice_final', "Segundo aviso de revisión", 'quimibond_sgi.doc_review_notice_days_final', 30,
     "días naturales antes de la próxima revisión"),
    ('ack', "Acuse de lectura («Leído y entendido»)", 'quimibond_sgi.doc_ack_pending_days', 7,
     "días hábiles desde que se publica; después avisa a la persona y a su jefe"),
    ('pilot', "Prueba piloto de un documento", 'quimibond_sgi.pilot_days', 60,
     "días naturales por omisión"),
    ('pilot_notice', "Aviso de piloto por vencer", 'quimibond_sgi.doc_pilot_notice_days', 7,
     "días naturales antes del fin del piloto"),
    ('external', "Implantar un documento externo", 'quimibond_sgi.external_doc_days', 10,
     "días hábiles desde su recepción; avisa al dueño 2 días hábiles antes y al vencer"),
)
# Roles de actividad que imprime el bloque «roles» (clave, etiqueta).
MIID_ROLE_KINDS = (('ejecuta', "Ejecuta"), ('aprueba', "Aprueba"), ('escala', "Escala"))
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


def _miid_menu_link(menu):
    """(ruta, url) de un menú de Odoo (57.161.0): la ruta se imprime («SGI →
    Sistema → Mapa de procesos») y la url abre su acción en la pantalla. Sin
    menú, o sin acción, la url es vacía."""
    menu = menu.sudo() if menu else menu
    if not menu:
        return '', ''
    path = " → ".join(part.strip() for part in (menu.complete_name or menu.name or '').split('/'))
    action = menu.action
    action = action.exists() if action else action
    return path, ("/odoo/action-%d" % action.id if action else '')


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
    kind = fields.Selection(
        MIID_KIND_SELECTION, string="Documento", required=True, default='miid', index=True,
        tracking=True,
        help="Documento generado al que pertenece la sección: el Manual del SGI (MIID) o el "
             "procedimiento de control de información documentada (P-G01).")
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
    # 57.161.0: lo que antes eran listas y tablas escritas en el texto son
    # renglones; las rutas escritas son menús que la pantalla abre.
    page = fields.Selection(MIID_PAGES, string="Pestaña", compute='_compute_page', store=True,
                            help="Pestaña de la pantalla donde se ve la sección; sale del numeral (el "
                                 "capítulo).")
    menu_id = fields.Many2one('ir.ui.menu', string="Pantalla en Odoo", tracking=True, ondelete='set null',
                              help="Menú donde se consulta en Odoo lo que dice la sección. La pantalla lo "
                                   "abre con un botón; el PDF imprime su ruta.")
    item_ids = fields.One2many('sgi.miid.item', 'section_id', string="Renglones", copy=True,
                               help="Lista o tabla de la sección: un renglón por concepto.")
    item_count = fields.Integer(string="# Renglones", compute='_compute_item_count')
    item_style = fields.Selection([('table', "Tabla"), ('list', "Lista")], string="Los renglones van en",
                                  default='table', required=True,
                                  help="Tabla: una columna por cada título que tenga nombre. Lista: el "
                                       "concepto en negritas y su descripción.")
    item_label_title = fields.Char(string="Columna del concepto", default="Concepto")
    item_clause_title = fields.Char(string="Columna del numeral",
                                    help="Vacío: la tabla no muestra el numeral.")
    item_text_title = fields.Char(string="Columna de la descripción", default="Descripción")
    item_text2_title = fields.Char(string="Segunda columna de texto",
                                   help="Vacío: la tabla no la muestra.")
    item_text3_title = fields.Char(string="Tercera columna de texto",
                                   help="Vacío: la tabla no la muestra.")

    @api.depends('kind', 'clause')
    def _compute_page(self):
        for section in self:
            chapter = (section.clause or '').strip().split('.')[0]
            section.page = MIID_KINDS.get(section.kind, MIID_KINDS['miid'])['pages'].get(chapter, 'otros')

    @api.depends('item_ids')
    def _compute_item_count(self):
        for section in self:
            section.item_count = len(section.item_ids)

    @api.constrains('live_block', 'company_id', 'active', 'kind')
    def _check_live_block(self):
        for section in self.filtered(lambda s: s.live_block and s.active):
            other = self.search_count([('id', '!=', section.id), ('active', '=', True),
                                       ('company_id', '=', section.company_id.id),
                                       ('kind', '=', section.kind),
                                       ('live_block', '=', section.live_block)], limit=1)
            if other:
                raise ValidationError(
                    "El bloque «%s» ya lo lleva otra sección de este documento. Quíteselo a esa "
                    "sección primero." % dict(MIID_BLOCKS)[section.live_block])

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
            elif name in MIID_SEED_LAYOUT_FIELDS:
                # 57.146.0: la estructura (orden, numeral, título, nivel,
                # bloque) también se lee de la siembra, para reacomodar el
                # manual en una base existente.
                text = (field.text or '').strip()
                out[name] = int(text) if name == 'sequence' else text
        return out

    @api.model
    def _sgi_seed_layout_update(self, xmlids, tag):
        """57.146.0: pone en una base existente el orden, numeral, título,
        nivel y bloque de la siembra en las secciones dadas (noupdate no lo
        hace). No toca el texto ni la nota de «Por confirmar» (eso es
        ``_sgi_seed_update``) y no deja marca de edición: cambiar la
        estructura no es editar el texto. Devuelve {xmlid: resultado}."""
        result = {}
        for xmlid in xmlids:
            section = self.env.ref('quimibond_sgi.%s' % xmlid, raise_if_not_found=False)
            if not section or section._name != self._name:
                result[xmlid] = 'no existe'
                continue
            seed = self._sgi_seed_fields(xmlid)
            vals = {}
            for name in MIID_SEED_LAYOUT_FIELDS:
                new = seed.get(name, 0 if name == 'sequence' else '') or False
                if name == 'sequence':
                    new = seed.get('sequence') or 10
                elif name == 'kind':
                    new = seed.get('kind') or 'miid'
                current = section[name] or False
                if (current or False) != (new or False):
                    vals[name] = new
            if not section.active:
                vals['active'] = True
            if vals:
                section.sudo().with_context(sgi_miid_seed=True, tracking_disable=True).write(vals)
                section.sudo().message_post(body="%s: sección reacomodada en el manual generado (%s)." % (
                    tag, ", ".join(sorted(vals))))
            result[xmlid] = 'reacomodada' if vals else 'sin cambio'
        return result

    @api.model
    def _sgi_seed_archive(self, xmlids, tag, reason):
        """57.146.0: archiva las secciones que el manual generado ya no lleva
        (su texto queda en la sección archivada y en Conocimiento). Devuelve
        las archivadas."""
        archived = self.browse()
        for xmlid in xmlids:
            section = self.env.ref('quimibond_sgi.%s' % xmlid, raise_if_not_found=False)
            if not section or section._name != self._name or not section.active:
                continue
            section.sudo().with_context(sgi_miid_seed=True, tracking_disable=True).write({'active': False})
            section.sudo().message_post(body="%s: sección archivada: %s." % (tag, reason))
            archived |= section
        return archived

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

    @api.model
    def _sgi_seed_format_fields(self, xmlid):
        """57.161.0: formato de la sección (menú, renglones como tabla o lista y
        títulos de columna) tal como está en ``MIID_ITEMS_SEED_FILE``. Solo los
        campos que el archivo declara."""
        from lxml import etree
        with file_open(MIID_ITEMS_SEED_FILE, 'rb') as fh:
            tree = etree.parse(fh)
        out = {}
        for field in tree.xpath("//record[@id=$rid]/field", rid=xmlid):
            name = field.get('name')
            if name not in MIID_ITEM_FORMAT_FIELDS:
                continue
            if field.get('ref'):
                ref = field.get('ref')
                record = self.env.ref(ref if '.' in ref else 'quimibond_sgi.%s' % ref,
                                      raise_if_not_found=False)
                out[name] = record.id if record else False
            else:
                out[name] = (field.text or '').strip() or False
        return out

    @api.model
    def _sgi_seed_format_update(self, xmlids, tag):
        """57.161.0: pone en una base existente el formato de la siembra en las
        secciones dadas (la siembra es noupdate). No deja marca de edición.
        Devuelve {xmlid: resultado}."""
        result = {}
        for xmlid in xmlids:
            section = self.env.ref('quimibond_sgi.%s' % xmlid, raise_if_not_found=False)
            if not section or section._name != self._name:
                result[xmlid] = 'no existe'
                continue
            seed = self._sgi_seed_format_fields(xmlid)
            vals = {}
            for name, new in seed.items():
                current = section[name].id if name == 'menu_id' else section[name]
                if (current or False) != (new or False):
                    vals[name] = new
            if vals:
                section.sudo().with_context(sgi_miid_seed=True, tracking_disable=True).write(vals)
            result[xmlid] = 'con formato' if vals else 'sin cambio'
        return result

    @api.model
    def _sgi_seed_items(self):
        """57.162.0: {xmlid de sección: {xmlid de renglón: valores}} de
        ``MIID_ITEMS_SEED_FILE``. Los campos que el renglón no declara van
        vacíos (la siembra es la versión completa del renglón)."""
        from lxml import etree
        with file_open(MIID_ITEMS_SEED_FILE, 'rb') as fh:
            tree = etree.parse(fh)
        seed = {}
        for record in tree.xpath('//record[@model="sgi.miid.item"]'):
            vals = {'clause': False, 'text': False, 'text_2': False, 'text_3': False, 'menu_id': False}
            section = None
            for field in record.findall('field'):
                name = field.get('name')
                if name == 'section_id':
                    section = field.get('ref')
                elif name == 'menu_id':
                    menu = self.env.ref('quimibond_sgi.%s' % field.get('ref'), raise_if_not_found=False)
                    vals['menu_id'] = menu.id if menu else False
                elif name == 'sequence':
                    vals['sequence'] = int((field.text or '10').strip())
                else:
                    vals[name] = (field.text or '').strip() or False
            seed.setdefault(section, {})[record.get('id')] = vals
        return seed

    @api.model
    def _sgi_seed_items_update(self, xmlids, tag):
        """57.162.0: pone los renglones de la siembra en una base existente
        (son noupdate): actualiza los sembrados y quita los que la siembra ya
        no trae, solo en las secciones que nadie editó a mano (renglones o
        texto). Los renglones nuevos los crea la actualización. Devuelve
        {xmlid: resultado}."""
        seed = self._sgi_seed_items()
        Data = self.env['ir.model.data'].sudo()
        Message = self.env['mail.message'].sudo()
        Item = self.env['sgi.miid.item'].sudo()
        result = {}
        for xmlid in xmlids:
            section = self.env.ref('quimibond_sgi.%s' % xmlid, raise_if_not_found=False)
            if not section or section._name != self._name:
                result[xmlid] = 'no existe'
                continue
            if Message.search_count([('model', '=', self._name), ('res_id', '=', section.id),
                                     ('body', 'ilike', MIID_EDITED_MSG)]):
                section.sudo().message_post(body=(
                    "%s: la siembra trae renglones nuevos para esta sección, pero no se aplicaron porque ya se "
                    "editó a mano. Compárelo con docs/sgi/transicion/miid-rev03-borrador.md." % tag))
                result[xmlid] = 'editada a mano'
                continue
            wanted = seed.get(xmlid, {})
            seeded = {d.name: d.res_id for d in Data.search([
                ('module', '=', 'quimibond_sgi'), ('model', '=', Item._name),
                ('res_id', 'in', section.item_ids.ids)])}
            changed = 0
            for name, res_id in seeded.items():
                item = Item.browse(res_id)
                if name not in wanted:
                    item.unlink()
                    changed += 1
                    continue
                vals = {}
                for field, new in wanted[name].items():
                    current = item[field].id if field == 'menu_id' else item[field]
                    if (current or False) != (new or False):
                        vals[field] = new
                if vals:
                    item.write(vals)
                    changed += 1
            result[xmlid] = '%d renglón(es) al día' % changed if changed else 'sin cambio'
        return result

    @api.model
    def _sgi_seed_fill_fields(self, tag):
        """57.162.0: llena en una base existente los campos que siembra
        ``MIID_FIELDS_SEED_FILE`` en normas, cláusulas y tipos de documento
        (son noupdate), solo donde están vacíos: lo que MAST ya capturó no se
        pisa. Las reglas del sistema son registros nuevos (los crea la
        actualización). Devuelve {xmlid: campos llenados}."""
        from lxml import etree
        with file_open(MIID_FIELDS_SEED_FILE, 'rb') as fh:
            tree = etree.parse(fh)
        result = {}
        models_ = ('sgi.norm', 'sgi.norm.clause', 'sgi.document.type')
        for record in tree.xpath('//record'):
            if record.get('model') not in models_:
                continue
            xmlid = record.get('id')
            target = self.env.ref('quimibond_sgi.%s' % xmlid, raise_if_not_found=False)
            if not target:
                result[xmlid] = []
                continue
            vals = {}
            for field in record.findall('field'):
                name = field.get('name')
                if target[name]:
                    continue
                if field.get('ref'):
                    ref = self.env.ref('quimibond_sgi.%s' % field.get('ref'), raise_if_not_found=False)
                    if ref:
                        vals[name] = ref.id
                elif field.get('eval'):
                    vals[name] = field.get('eval') == 'True'
                else:
                    vals[name] = (field.text or '').strip()
            if vals:
                target.sudo().write(vals)
            result[xmlid] = sorted(vals)
        return result

    def _sgi_open_menu(self, menu):
        """Abre el menú (su acción de ventana; si no, el menú mismo). Si el
        usuario no ve el menú, lo dice en vez de abrir una pantalla que Odoo le
        negaría."""
        if not menu:
            raise UserError("Esta sección no tiene pantalla en Odoo.")
        if menu.id not in self.env['ir.ui.menu']._visible_menu_ids():
            raise UserError("Su usuario no tiene acceso a «%s». Pida el acceso al Jefe MAST." % (
                _miid_menu_link(menu)[0]))
        action = menu.sudo().action
        action = action.exists() if action else action
        if action and action._name == 'ir.actions.act_window':
            return action.read()[0]
        return {'type': 'ir.actions.client', 'tag': 'reload', 'params': {'menu_id': menu.id}}

    def action_open_menu(self):
        self.ensure_one()
        return self._sgi_open_menu(self.menu_id)

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


class SgiMiidItem(models.Model):
    """Renglón de una sección del MIID o del P-G01 (57.161.0): un concepto con
    su descripción y, si aplica, su numeral, columnas extra y el menú donde se
    ve en Odoo. Sustituye las listas y tablas que se escribían en el texto. Lo
    edita el Jefe MAST; editarlo cuenta como editar la sección."""
    _name = 'sgi.miid.item'
    _description = "Renglón de una sección del MIID"
    _order = 'section_id, sequence, id'

    section_id = fields.Many2one('sgi.miid.section', string="Sección", required=True, index=True,
                                 ondelete='cascade')
    company_id = fields.Many2one(related='section_id.company_id', store=True, index=True)
    kind = fields.Selection(related='section_id.kind')
    sequence = fields.Integer(string="Orden", default=10)
    label = fields.Char(string="Concepto", required=True,
                        help="Lo que se define o se regula (p. ej. «Riesgos y oportunidades», «Jefe de MAST "
                             "y SGI»).")
    clause = fields.Char(string="Numeral", help="Cláusulas de la norma que cubre (p. ej. 6.1, 7.5).")
    text = fields.Text(string="Descripción")
    text_2 = fields.Text(string="Texto 2", help="Segunda columna de la tabla, si la sección le pone título.")
    text_3 = fields.Text(string="Texto 3", help="Tercera columna de la tabla, si la sección le pone título.")
    menu_id = fields.Many2one('ir.ui.menu', string="Pantalla en Odoo", ondelete='set null',
                              help="Menú donde se hace o se consulta. La pantalla lo abre; el PDF imprime su "
                                   "ruta.")

    def _sgi_mark_edited(self, sections):
        # Como el texto de la sección (57.110.0): la siembra respeta lo que se
        # editó a mano. La carga de datos y las migraciones (superusuario) no
        # cuentan.
        if self.env.su:
            return
        for section in sections:
            section.message_post(body=MIID_EDITED_MSG)

    def _sgi_check_editor(self):
        if not self.env.su and not self.env.user.has_group('quimibond_sgi.group_sgi_manager'):
            raise AccessError("Solo el Jefe MAST edita los textos del MIID y del P-G01.")

    @api.model_create_multi
    def create(self, vals_list):
        self._sgi_check_editor()
        items = super().create(vals_list)
        self._sgi_mark_edited(items.section_id)
        return items

    def write(self, vals):
        self._sgi_check_editor()
        sections = self.section_id
        res = super().write(vals)
        self._sgi_mark_edited(sections | self.section_id)
        return res

    def unlink(self):
        self._sgi_check_editor()
        sections = self.section_id
        res = super().unlink()
        self._sgi_mark_edited(sections.exists())
        return res

    def action_open_menu(self):
        self.ensure_one()
        return self.section_id._sgi_open_menu(self.menu_id)


class SgiMiid(models.Model):
    """Manual del SGI (MIID) de una empresa: la vista del sistema, su
    comparación con la revisión vigente y el historial de revisiones. Uno por
    empresa; lo crea la primera apertura o el paso diario."""
    _name = 'sgi.miid'
    _description = "Manual del SGI (MIID)"
    _inherit = ['mail.thread', 'mail.activity.mixin']

    name = fields.Char(string="Nombre", compute='_compute_name')
    company_id = fields.Many2one('res.company', string="Empresa", required=True, index=True, readonly=True)
    kind = fields.Selection(MIID_KIND_SELECTION, string="Documento", required=True, default='miid',
                            index=True, readonly=True)
    document_id = fields.Many2one('documents.document', string="Revisión vigente",
                                  compute='_compute_document', compute_sudo=True)
    revision_label = fields.Char(string="Rev. vigente", compute='_compute_document', compute_sudo=True)
    issue_date = fields.Date(string="Emisión de la revisión vigente", compute='_compute_document',
                             compute_sudo=True)
    revision_ids = fields.Many2many('documents.document', string="Revisiones",
                                    compute='_compute_document', compute_sudo=True)
    pending_request_id = fields.Many2one('approval.request', string="Solicitud de cambio en curso",
                                         compute='_compute_document', compute_sudo=True)
    state = fields.Selection([
        ('al_dia', "Al día"), ('desactualizado', "Desactualizado"),
        ('sin_base', "Sin línea base"), ('sin_documento', "Sin revisión vigente"),
    ], string="Situación", compute='_compute_state', compute_sudo=True,
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

    # 57.161.0: pantalla por capítulos. Lo que es registro de Odoo se ve como
    # lista nativa (se abre); el texto, por pestaña, sin la letra del PDF.
    norm_ids = fields.Many2many('sgi.norm', relation='sgi_miid_screen_norm_rel', string="Normas",
                                compute='_compute_screen_records', compute_sudo=True)
    pg01_norm_ids = fields.Many2many('sgi.norm', relation='sgi_miid_screen_pg01_norm_rel',
                                     string="Normas del P-G01", compute='_compute_screen_records',
                                     compute_sudo=True)
    process_ids = fields.Many2many('sgi.process', relation='sgi_miid_screen_process_rel', string="Procesos",
                                   compute='_compute_screen_records', compute_sudo=True)
    policy_id = fields.Many2one('sgi.policy', string="Política vigente", compute='_compute_screen_records',
                                compute_sudo=True)
    policy_text = fields.Html(related='policy_id.policy_text', string="Texto de la política")
    objective_ids = fields.Many2many('sgi.objective', relation='sgi_miid_screen_objective_rel',
                                     string="Objetivos integrales", compute='_compute_screen_records',
                                     compute_sudo=True)
    doc_type_ids = fields.Many2many('sgi.document.type', relation='sgi_miid_screen_doc_type_rel',
                                    string="Tipos de documento", compute='_compute_screen_records',
                                    compute_sudo=True)
    pg01_doc_type_ids = fields.Many2many('sgi.document.type', relation='sgi_miid_screen_pg01_doc_type_rel',
                                         string="Tipos de documento del P-G01",
                                         compute='_compute_screen_records', compute_sudo=True)
    # 57.161.1: no «activity_ids», que es de mail.activity.mixin (pisarlo
    # tumbó el registro en el build de main).
    routine_ids = fields.Many2many('sgi.process.activity', relation='sgi_miid_screen_routine_rel',
                                   string="Rutinas", compute='_compute_screen_records', compute_sudo=True)
    rule_ids = fields.Many2many('sgi.system.rule', relation='sgi_miid_screen_rule_rel',
                                string="Reglas del sistema", compute='_compute_screen_records', compute_sudo=True)
    scr_requisitos_reglas_html = fields.Html(compute='_compute_screen', compute_sudo=True, sanitize=False)
    scr_general_html = fields.Html(compute='_compute_screen', compute_sudo=True, sanitize=False)
    scr_objeto_html = fields.Html(compute='_compute_screen', compute_sudo=True, sanitize=False)
    scr_proposito_html = fields.Html(compute='_compute_screen', compute_sudo=True, sanitize=False)
    scr_responsabilidades_html = fields.Html(compute='_compute_screen', compute_sudo=True, sanitize=False)
    scr_normas_html = fields.Html(compute='_compute_screen', compute_sudo=True, sanitize=False)
    scr_donde_html = fields.Html(compute='_compute_screen', compute_sudo=True, sanitize=False)
    scr_sistema_html = fields.Html(compute='_compute_screen', compute_sudo=True, sanitize=False)
    scr_requisitos_html = fields.Html(compute='_compute_screen', compute_sudo=True, sanitize=False)
    scr_desarrollo_html = fields.Html(compute='_compute_screen', compute_sudo=True, sanitize=False)
    scr_documentos_html = fields.Html(compute='_compute_screen', compute_sudo=True, sanitize=False)
    scr_rutinas_html = fields.Html(compute='_compute_screen', compute_sudo=True, sanitize=False)
    scr_marco_html = fields.Html(compute='_compute_screen', compute_sudo=True, sanitize=False)
    scr_transicion_html = fields.Html(compute='_compute_screen', compute_sudo=True, sanitize=False)
    scr_cambios_html = fields.Html(compute='_compute_screen', compute_sudo=True, sanitize=False)
    scr_otros_html = fields.Html(compute='_compute_screen', compute_sudo=True, sanitize=False)
    scr_normas_normas_html = fields.Html(compute='_compute_screen', compute_sudo=True, sanitize=False)
    scr_sistema_procesos_html = fields.Html(compute='_compute_screen', compute_sudo=True, sanitize=False)
    scr_sistema_politica_html = fields.Html(compute='_compute_screen', compute_sudo=True, sanitize=False)
    scr_sistema_objetivos_html = fields.Html(compute='_compute_screen', compute_sudo=True, sanitize=False)
    scr_documentos_tipos_documento_html = fields.Html(compute='_compute_screen', compute_sudo=True, sanitize=False)
    scr_desarrollo_tipos_documento_html = fields.Html(compute='_compute_screen', compute_sudo=True, sanitize=False)
    scr_rutinas_actividades_html = fields.Html(compute='_compute_screen', compute_sudo=True, sanitize=False)
    scr_marco_normas_html = fields.Html(compute='_compute_screen', compute_sudo=True, sanitize=False)

    # 57.148.0: uno por empresa y documento (la migración quita el único viejo).
    _company_uniq = models.Constraint('UNIQUE(company_id, kind)',
                                      "Ya existe este documento generado para la empresa.")

    # ------------------------------------------------------------------
    # Búsquedas
    # ------------------------------------------------------------------
    @api.model
    def _sgi_get(self, company=None, kind='miid'):
        """El documento generado (MIID o P-G01) de la empresa; lo crea si no
        existe. Si otra transacción lo crea al mismo tiempo (índice único), no
        truena ni deja ERROR en el log: devuelve lo que vea (puede venir vacío
        hasta la siguiente vez)."""
        company = company or self.env['sgi.config']._sgi_company()
        kind = kind if kind in MIID_KINDS else 'miid'
        Miid = self.sudo()
        miid = Miid.search([('company_id', '=', company.id), ('kind', '=', kind)], limit=1)
        if miid:
            return miid
        try:
            with self.env.cr.savepoint(), mute_logger('odoo.sql_db'):
                return Miid.create({'company_id': company.id, 'kind': kind})
        except psycopg2.IntegrityError:
            _logger.info("SGI: el %s de %s lo creó otra transacción.", kind, company.name)
            return Miid.search([('company_id', '=', company.id), ('kind', '=', kind)], limit=1)

    @api.model
    def _sgi_kind_for_code(self, code):
        return _miid_kind_for_code(code)

    def _sgi_kind(self):
        """Los datos fijos del documento generado (MIID_KINDS)."""
        self.ensure_one()
        return MIID_KINDS[self.kind or 'miid']

    def _sgi_documents(self):
        """Todas las revisiones del documento en la empresa (por la clave, no por
        el tipo: en la copia de producción de las pruebas el real queda REAL~…)."""
        self.ensure_one()
        return self.env['documents.document'].sudo().with_context(active_test=False).search([
            ('sgi_code', '=', self._sgi_kind()['code']), ('sgi_is_controlled', '=', True),
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

    def _sgi_next_revision(self, doc):
        """Q5: vigente + 1; sin línea base, al menos la primera revisión desde
        Odoo del documento (MIID 03, P-G01 12: continúan al Dropbox)."""
        first = self._sgi_kind()['first_revision']
        revision = (doc.sgi_revision or 0) + 1 if doc else first
        if not (doc and doc.sgi_content_hash):
            revision = max(revision, first)
        return revision

    @api.depends('company_id', 'kind')
    def _compute_name(self):
        for miid in self:
            miid.name = "%s — %s" % (miid._sgi_kind()['label'], miid.company_id.name or '')

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
                [('company_id', '=', miid.company_id.id), ('kind', '=', miid.kind),
                 ('to_confirm', '=', True)])
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

    def _compute_screen_records(self):
        for miid in self:
            env = miid._sgi_env()
            company = miid.company_id
            kind = miid._sgi_kind()
            processes = env['sgi.process'].search([('company_id', '=', company.id)]).sorted(lambda p: (
                ['estrategico', 'cop', 'soporte'].index(p.process_type)
                if p.process_type in ('estrategico', 'cop', 'soporte') else 9, p.code or ''))
            policy = env['sgi.policy'].search([('state', '=', 'vigente')], limit=1)
            types = env['sgi.document.type'].search([('company_id', 'in', [company.id, False])]).filtered(
                'code_required').sorted(lambda t: t.name or '')
            norms = env['sgi.norm'].search([]).filtered('clause_ids')
            miid.norm_ids = norms if miid.kind == 'miid' else False
            miid.pg01_norm_ids = norms if miid.kind == 'pg01' else False
            miid.process_ids = processes if kind['all_processes'] else False
            miid.policy_id = policy if kind['all_processes'] else False
            miid.objective_ids = env['sgi.objective'].search([('policy_id', '=', policy.id)]) \
                if policy and kind['all_processes'] else False
            miid.rule_ids = env['sgi.system.rule'].search([]) if miid.kind == 'miid' else False
            miid.doc_type_ids = types if miid.kind == 'miid' else False
            miid.pg01_doc_type_ids = types if miid.kind == 'pg01' else False
            miid.routine_ids = env['sgi.process.activity'].search(
                [('related_procedure_id', 'in', miid._sgi_documents().ids)], order='process_id, step, id') \
                if miid.kind == 'pg01' else False

    @api.model
    def _sgi_screen_fields(self):
        return ['scr_%s_html' % page for page, _label in MIID_PAGES] + [
            'scr_%s_%s_html' % slot for slot in MIID_SCREEN_SLOTS]

    def _compute_screen(self):
        """57.161.0: el texto de cada pestaña. Las secciones van en orden; la
        que lleva un bloque que la pantalla muestra como lista nativa parte el
        texto: lo de antes va al campo de esa lista (scr_<pestaña>_<bloque>) y
        lo de después sigue en la pestaña (scr_<pestaña>)."""
        names = self._sgi_screen_fields()
        QWeb = self.env['ir.qweb'].sudo()
        for miid in self:
            values = dict.fromkeys(names, False)
            data = miid._sgi_print_data('live')
            native = miid._sgi_kind()['native']
            context = {'doc': miid, 'miid': data, 'miid_mode': 'screen',
                       'dg_colors': DG_COLORS, 'dg_level_bg': DG_LEVEL_BG}

            def render(section, part):
                return QWeb._render('quimibond_sgi.report_miid_section',
                                    dict(context, section=section, section_part=part))

            def put(field, parts):
                html = Markup('').join(parts).strip()
                if html:
                    values[field] = Markup('%s%s') % (values[field] or Markup(''), html)

            page, pending = None, []
            for section in data['sections']:
                if section['page'] != page:
                    if page:
                        put('scr_%s_html' % page, pending)
                    page, pending = section['page'], []
                block = section['block']
                slot = 'scr_%s_%s_html' % (page, block)
                if block and native.get(block) == page and slot in values and not values[slot]:
                    pending.append(render(section, 'head'))
                    put(slot, pending)
                    pending = [render(section, 'tail')]
                else:
                    pending.append(render(section, 'all'))
            if page:
                put('scr_%s_html' % page, pending)
            if data['orphans']:
                put('scr_otros_html', [QWeb._render('quimibond_sgi.report_miid_orphans', context)])
            for name, value in values.items():
                miid[name] = value

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
        raw = {'kind': self.kind}

        sections = env['sgi.miid.section'].search([('company_id', '=', company.id), ('kind', '=', self.kind)])
        # 57.161.0: el formato de la sección (pantalla en Odoo, estilo y
        # títulos de los renglones) va después del orden (índice 6).
        raw['sections'] = {str(s.id): [s.clause or '', s.name or '', s.heading_level or '',
                                       _miid_plain(s.body), s.live_block or '', bool(s.body_fallback),
                                       s.sequence or 0, _miid_menu_link(s.menu_id)[0], s.item_style or '']
                           + [s[title] or '' for _f, title in MIID_ITEM_COLUMNS]
                           for s in sections}
        raw['items'] = {str(s.id): [[i.label or '', i.clause or '', i.text or '', i.text_2 or '',
                                     i.text_3 or '', _miid_menu_link(i.menu_id)[0]] for i in s.item_ids]
                        for s in sections if s.item_ids}
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
        # 57.162.0: datos de norma, cláusula, regla y tipo de documento.
        raw['scope'] = {n.code: n.scope for n in env['sgi.norm'].search([]) if (n.scope or '').strip()}
        raw['exclusions'] = {'%s %s' % (c.norm_id.code, c.code): [c.exclusion_detail or '', c.name or '',
                                                                  c.exclusion_reason or '']
                             for c in env['sgi.norm.clause'].search([('exclusion', '=', True)])}
        raw['rules'] = {str(r.id): [r.sequence or 0, r.name or '', r.clause_label or '', r.text or '',
                                    _miid_menu_link(r.menu_id)[0]]
                        for r in env['sgi.system.rule'].search([])}
        raw['doc_roles'] = {t.code: [t.name or ''] + [t[f] or '' for f in MIID_DOC_ROLE_FIELDS]
                            + [_miid_menu_link(t.odoo_menu_id)[0]]
                            for t in types if any(t[f] for f in MIID_DOC_ROLE_FIELDS)}

        # 57.146.0: plazos documentales (el P-G01 vive en el MIID), dueños y
        # roles por proceso, y qué actividades cumplen cada cláusula. Todo
        # entra en la huella: es lo que imprime el manual.
        raw['doc_params'] = {key: (_miid_int(Param, param, default) if param else default)
                             for key, _label, param, default, _unit in MIID_DOC_ROWS}
        activities = env['sgi.process.activity'].search([('process_id', 'in', processes.ids)])
        roles = env['sgi.activity.role'].search([('activity_id', 'in', activities.ids),
                                                 ('target_type', '=', 'job'), ('job_id', '!=', False)])
        raw['roles'] = {}
        for process in processes:
            mine = activities.filtered(lambda a: a.process_id == process)
            owner = process.owner_id
            row = [owner.name or '', owner.job_id.name or '', len(mine)]
            for kind, _label in MIID_ROLE_KINDS:
                row.append(sorted({r.job_id.name for r in roles
                                   if r.activity_id.process_id == process and r.role == kind}))
            raw['roles'][process.code] = row
        raw['clause_activities'] = {}
        for activity in activities.filtered('norm_clause_ids'):
            number = activity.number or activity.legacy_number or activity.name or ''
            for clause in activity.norm_clause_ids:
                raw['clause_activities'].setdefault(clause.norm_id.code or '', {}).setdefault(
                    clause.code or '', []).append(number)
        for by_clause in raw['clause_activities'].values():
            for code in by_clause:
                by_clause[code] = sorted(set(by_clause[code]))
        # 57.148.0: las rutinas del documento son las actividades ligadas a
        # cualquiera de sus revisiones («Procedimiento relacionado»).
        raw['activities'] = {}
        own_docs = self._sgi_documents()
        for activity in env['sgi.process.activity'].search(
                [('related_procedure_id', 'in', own_docs.ids)], order='process_id, step, id'):
            number = activity.number or activity.legacy_number or activity.name or ''
            by_role = {kind: sorted({r.job_id.name for r in activity.role_ids
                                     if r.role == kind and r.job_id})
                       for kind, _label in MIID_ROLE_KINDS}
            raw['activities'][number] = [
                activity.name or '', by_role['ejecuta'], by_role['aprueba'],
                activity.measure_cadence or '', activity.done_criteria or '', activity.process_id.code or '']

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
            'code': self._sgi_kind()['code'],
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
    def _sgi_hash_keys(self, snapshot):
        """Llaves de la foto que entran a la huella del documento al que
        pertenece (57.148.0): solo lo que ese documento imprime."""
        kind = (snapshot or {}).get('kind') or 'miid'
        return MIID_KINDS.get(kind, MIID_KINDS['miid'])['hash_keys']

    @api.model
    def _sgi_hash(self, snapshot):
        keys = self._sgi_hash_keys(snapshot)
        content = {k: v for k, v in (snapshot or {}).items() if k in keys and k not in MIID_UNHASHED}
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
        """Textos de lo que cambió entre dos fotos (máximo MIID_DIFF_LIMIT). Solo
        las llaves que el documento imprime (57.148.0)."""
        keys = self._sgi_hash_keys(new or old)
        old = {k: v for k, v in (old or {}).items() if k in keys}
        new = {k: v for k, v in (new or {}).items() if k in keys}
        out = []
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
            # El índice 6 es el orden; lo demás es texto o formato.
            if a[:6] + a[7:] == b[:6] + b[7:]:
                return ["Sección movida de lugar: %s" % heading(b)]
            return ["Sección editada: %s" % heading(b)]
        keyed('sections', lambda k, v: "Sección nueva: %s" % heading(v),
              lambda k, v: "Sección archivada: %s" % heading(v), section_changed)

        # 57.161.0: renglones de las secciones.
        def section_name(key):
            row = (new.get('sections') or {}).get(key) or (old.get('sections') or {}).get(key)
            return heading(row) if row else "una sección"
        keyed('items', lambda k, v: "Renglones nuevos en la sección %s" % section_name(k),
              lambda k, v: "Se quitaron los renglones de la sección %s" % section_name(k),
              lambda k, a, b: ["Cambiaron los renglones de la sección %s (%d → %d)" % (
                  section_name(k), len(a), len(b))])

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

        keyed('scope', lambda k, v: "Alcance nuevo: %s" % k, lambda k, v: "Sin alcance: %s" % k,
              lambda k, a, b: ["Cambió el alcance de %s" % k])
        keyed('exclusions', lambda k, v: "Requisito que ya no aplica: %s" % k,
              lambda k, v: "Requisito que vuelve a aplicar: %s" % k,
              lambda k, a, b: ["Cambió la justificación de %s" % k])
        keyed('rules', lambda k, v: "Regla del sistema nueva: %s" % v[1],
              lambda k, v: "Regla del sistema retirada: %s" % v[1],
              lambda k, a, b: ["Regla del sistema movida de lugar: %s" % b[1]] if a[1:] == b[1:]
              else ["Cambió la regla del sistema %s" % b[1]])
        keyed('doc_roles', lambda k, v: "Quién elabora y firma, nuevo: %s" % v[0],
              lambda k, v: "Quién elabora y firma, retirado: %s" % v[0],
              lambda k, a, b: ["Cambió quién elabora o firma %s" % b[0]])

        old_doc, new_doc = old.get('doc_params') or {}, new.get('doc_params') or {}
        for key, label, _param, _default, unit in MIID_DOC_ROWS:
            if old_doc.get(key) != new_doc.get(key):
                out.append("Cambiaron los plazos documentales: %s %s → %s %s" % (
                    label.lower(), old_doc.get(key), new_doc.get(key), unit.split(';')[0]))

        def roles_changed(code, a, b):
            rows = []
            if a[:2] != b[:2]:
                rows.append("Cambió el dueño de %s en el bloque de roles: %s → %s" % (
                    code, a[0] or "sin dueño", b[0] or "sin dueño"))
            if a[2] != b[2]:
                rows.append("Cambió el número de actividades de %s: %d → %d" % (code, a[2], b[2]))
            if a[3:] != b[3:]:
                rows.append("Cambiaron los puestos que ejecutan, aprueban o escalan en %s" % code)
            return rows
        keyed('roles', lambda k, v: "Roles nuevos: proceso %s" % k,
              lambda k, v: "Roles retirados: proceso %s" % k, roles_changed)

        def activity_changed(number, a, b):
            if a[0] != b[0]:
                return ["Cambió el nombre de la actividad %s" % number]
            if a[1:3] != b[1:3]:
                return ["Cambiaron los puestos que ejecutan o aprueban %s" % number]
            return ["Cambió la actividad %s (cadencia o criterio de terminado)" % number]
        keyed('activities', lambda k, v: "Actividad nueva ligada al documento: %s %s" % (k, v[0]),
              lambda k, v: "Actividad que ya no está ligada al documento: %s" % k, activity_changed)

        old_ca, new_ca = old.get('clause_activities') or {}, new.get('clause_activities') or {}
        for norm_code in sorted(set(old_ca) | set(new_ca)):
            before, after = old_ca.get(norm_code) or {}, new_ca.get(norm_code) or {}
            for clause in sorted(set(before) | set(after)):
                if before.get(clause) != after.get(clause):
                    out.append("Cambiaron las actividades que cumplen %s %s" % (norm_code, clause))

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
                                                       ('kind', '=', self.kind), ('to_confirm', '=', True)]):
            out.append("La sección %s está por confirmar: %s" % (
                section._sgi_heading(), section.to_confirm_note or ''))
        if not self._sgi_kind()['all_processes']:
            # 57.148.0: un procedimiento se aprueba con su proceso publicado.
            doc = self._sgi_current_document()
            process = doc.sgi_process_id if doc else env['sgi.process']
            if process and process.state not in MIID_READY_PROCESS_STATES:
                out.append("El proceso %s no está vigente: el %s solo se aprueba con su proceso publicado." % (
                    process.code or process.name, self._sgi_kind()['short']))
            return out
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
                numbers = [n for index, _p in columns for n in row['cells'][index]]
                covered = covered or bool(codes)
                # 57.146.0: la cuarta columna son las actividades (numerales).
                clause = row['clause']
                # 57.162.0: lo que no aplica y las reglas del sistema también cumplen.
                numbers_text = "No aplica" if clause.exclusion else (", ".join(numbers) or "—")
                rows.append([clause.code or '', clause.name or '', ", ".join(codes) or "—", numbers_text,
                             ", ".join(clause.system_rule_ids.mapped('name')) or "—"])
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

        kind = self._sgi_kind()
        ident = snap['identificacion']
        next_revision = revision or (request.sgi_new_revision if request else None) \
            or self._sgi_next_revision(current)
        rows = [("Clave", kind['code'])]
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
        if kind['all_processes']:
            rows.append(("Procesos", "Al %s, %d de %d procesos vigentes" % (
                today.strftime('%d/%m/%Y'), ident['processes_ready'], ident['processes_total'])))
        elif current and current.sgi_process_id:
            rows.append(("Proceso", "%s %s (%s)" % (
                current.sgi_process_id.code or '', current.sgi_process_id.name or '',
                (state_labels.get(current.sgi_process_id.state) or '').lower())))
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
        names = {p.code: "%s %s" % (p.code, p.name or '') for p in processes}
        order = [p.code for p in processes.sorted(lambda p: (
            ['estrategico', 'cop', 'soporte'].index(p.process_type)
            if p.process_type in ('estrategico', 'cop', 'soporte') else 9, p.code or ''))]

        def order_codes(mapping):
            """Claves de ``mapping`` en el orden del mapa (estratégicos, cadena,
            soporte) y después las que no son procesos de la empresa."""
            return [c for c in order if c in mapping] + sorted(c for c in mapping if c not in order)

        blocks['controles'] = {'items': ["%s (%s)" % (v[1], code) for code, v in sorted(snap['controls'].items())]}
        # 57.162.0: alcance, no aplicabilidades, reglas y roles por tipo.
        blocks['alcance'] = {'rows': [[code, text] for code, text in sorted(snap['scope'].items())]}
        blocks['exclusiones'] = {'rows': [[v[0] or key, v[1], v[2]] for key, v in sorted(snap['exclusions'].items())]}
        rule_rows = []
        for rule in env['sgi.system.rule'].search([]):
            path, url = _miid_menu_link(rule.menu_id)
            rule_rows.append([rule.name or '', rule.clause_label or '', rule.text or '', path, url])
        blocks['reglas'] = {'rows': rule_rows}
        role_types = env['sgi.document.type'].search([('company_id', 'in', [company.id, False])]).filtered(
            lambda t: any(t[f] for f in MIID_DOC_ROLE_FIELDS))
        elab_rows, sign_rows = [], []
        for dtype in role_types:
            path, url = _miid_menu_link(dtype.odoo_menu_id)
            elab_rows.append([dtype.name or '', dtype.author_text or '—', dtype.location_text or '—',
                              dtype.control_text or '—', path, url])
            if dtype.reviewer_text or dtype.approver_text:
                sign_rows.append([dtype.name or '', dtype.author_text or '—', dtype.reviewer_text or '—',
                                  dtype.approver_text or '—'])
        blocks['elaboracion'] = {'rows': elab_rows}
        blocks['aprobacion'] = {'rows': sign_rows}
        blocks['plazos_nc'] = {'rows': [[label, snap['nc'].get(key), unit] for key, label, unit in MIID_NC_ROWS]}
        blocks['correspondencia'] = {'norms': self._sgi_correspondence(env, processes.ids)}
        # 57.146.0: normas, roles y plazos documentales.
        blocks['normas'] = {'rows': [[code, v[0], len(v[1])] for code, v in sorted(snap['norms'].items())]}
        blocks['plazos_documentales'] = {'rows': [
            [label, snap['doc_params'].get(key), unit] for key, label, _param, _default, unit in MIID_DOC_ROWS]}
        role_rows = []
        for code in order_codes(snap['roles']):
            values = snap['roles'][code]
            role_rows.append([names.get(code, code), values[0] or "—", values[1] or "—", values[2]]
                             + [", ".join(v) or "—" for v in values[3:]])
        blocks['roles'] = {'rows': role_rows, 'kinds': [label for _k, label in MIID_ROLE_KINDS]}
        cadence_labels = dict(env['sgi.process.activity']._fields['measure_cadence']._description_selection(env))
        blocks['actividades'] = {'rows': [
            [number, v[0], ", ".join(v[1]) or "—", ", ".join(v[2]) or "—",
             cadence_labels.get(v[3], v[3]) or "—", v[4] or "—"]
            for number, v in sorted(snap['activities'].items())]}

        by_process = {}
        for code, values in sorted(snap['previous'].items()):
            by_process.setdefault(values[3] or '', []).append(code)
        prev_rows = [[names.get(code, code), ", ".join(by_process[code])] for code in order if code in by_process]
        prev_rows += [[code, ", ".join(codes)] for code, codes in sorted(by_process.items())
                      if code and code not in names]
        if by_process.get(''):
            prev_rows.append(["Sin proceso", ", ".join(by_process[''])])
        kept = ["%s (%s)" % (v[2], code) for code, v in sorted(snap['controls'].items()) if v[2]]
        blocks['procedimientos_anteriores'] = {'rows': prev_rows, 'kept': kept}

        notes_section = env['sgi.miid.section'].search([('company_id', '=', company.id),
                                                        ('kind', '=', self.kind),
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
            'normas': bool(blocks['normas']['rows']),
            'roles': bool(role_rows),
            'plazos_documentales': True,
            'procesos': bool(blocks['procesos']['rows']),
            'politica': bool(policy),
            'objetivos': bool(blocks['objetivos']['rows']),
            'tipos_documento': bool(blocks['tipos_documento']['rows']),
            'controles': bool(blocks['controles']['items']),
            'plazos_nc': True,
            'correspondencia': bool(blocks['correspondencia']['norms']),
            'procedimientos_anteriores': bool(prev_rows or kept),
            'anexos': bool(annex_rows or orphan_notes),
            'actividades': bool(blocks['actividades']['rows']),
            'historial': bool(history),
            'alcance': bool(blocks['alcance']['rows']),
            'exclusiones': bool(blocks['exclusiones']['rows']),
            'reglas': bool(rule_rows),
            'elaboracion': bool(elab_rows),
            'aprobacion': bool(sign_rows),
        }

        sections, used = [], set()
        for section in env['sgi.miid.section'].search([('company_id', '=', company.id), ('kind', '=', self.kind)]):
            before, after = section._sgi_body_parts()
            used.add(section.live_block)
            menu_path, menu_url = _miid_menu_link(section.menu_id)
            items = []
            for item in section.item_ids:
                path, url = _miid_menu_link(item.menu_id)
                items.append({'label': item.label or '', 'clause': item.clause or '', 'text': item.text or '',
                              'text_2': item.text_2 or '', 'text_3': item.text_3 or '',
                              'menu_path': path, 'menu_url': url})
            # 57.161.0: columnas de la tabla = los títulos con nombre (el
            # concepto siempre); el menú es una columna más si algún renglón lo
            # tiene.
            columns = [(name, section[title] or '') for name, title in MIID_ITEM_COLUMNS
                       if name == 'label' or section[title]]
            sections.append({
                'clause': section.clause or '', 'name': section.name or '', 'level': section.heading_level,
                'page': section.page or 'otros',
                'block': section.live_block or False, 'fallback': bool(section.body_fallback),
                'before': before, 'after': after, 'to_confirm': section.to_confirm,
                'note': section.to_confirm_note or '', 'menu_path': menu_path, 'menu_url': menu_url,
                'items': items, 'item_style': section.item_style or 'table', 'columns': columns,
                'items_menu': any(i['menu_path'] for i in items)})
        orphans = [(code, label) for code, label in MIID_BLOCKS if code in kind['blocks'] and code not in used]
        return {'sections': sections, 'blocks': blocks, 'has_data': has_data, 'orphans': orphans,
                'blockers': blockers, 'draft': draft, 'company': company.name or '',
                'revision': next_revision, 'request': request or False,
                'signers': request._sgi_sign_rows() if request else [],
                'kind': self.kind, 'code': kind['code'], 'short': kind['short'], 'title': kind['title']}

    def _sgi_footer_info(self, mode, revision=None, draft=True):
        """Pie de cada hoja (M-2: sin «Formato controlado del SGI:»)."""
        code = self._sgi_kind()['code']
        if mode == 'live':
            return {'label': "%s · Borrador — no vigente" % code, 'issue_date': False, 'plain': True}
        label = "%s · Rev. %02d" % (code, revision or 0)
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
        """La pantalla del documento generado; ``sgi_miid_kind`` en el contexto
        elige cuál (por omisión el MIID)."""
        kind = self.env.context.get('sgi_miid_kind') or 'miid'
        miid = self._sgi_get(kind=kind)
        if not miid:
            raise UserError("Se está creando el documento generado de la empresa; intente de nuevo en un momento.")
        miid.invalidate_recordset()
        return {'type': 'ir.actions.act_window', 'name': miid._sgi_kind()['label'],
                'res_model': 'sgi.miid', 'res_id': miid.id, 'view_mode': 'form',
                'views': [(self.env.ref('quimibond_sgi.sgi_miid_view_form').id, 'form')],
                'target': 'current'}

    def action_print_live(self):
        self.ensure_one()
        return self.env.ref('quimibond_sgi.action_report_miid').report_action(self, config=False)

    def action_open_vigente_pdf(self):
        self.ensure_one()
        if not self.document_id:
            raise UserError("No hay revisión vigente del %s." % self._sgi_kind()['short'])
        return self.document_id.action_sgi_view_file()

    def action_open_sections(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'name': "Textos del %s" % self._sgi_kind()['short'],
                'res_model': 'sgi.miid.section', 'view_mode': 'list,form',
                'views': [(self.env.ref('quimibond_sgi.sgi_miid_section_view_list').id, 'list'),
                          (self.env.ref('quimibond_sgi.sgi_miid_section_view_form').id, 'form')],
                'search_view_id': [self.env.ref('quimibond_sgi.sgi_miid_section_view_search').id],
                'domain': [('company_id', '=', self.company_id.id), ('kind', '=', self.kind)],
                'context': {'default_company_id': self.company_id.id, 'default_kind': self.kind,
                            'search_default_group_page': 1,
                            'search_default_to_confirm': self.env.context.get('search_default_to_confirm')}}

    def action_open_revisions(self):
        """57.161.0: las revisiones de este documento (vigente y obsoletas)."""
        self.ensure_one()
        action = self.env['ir.actions.act_window']._for_xml_id('quimibond_sgi.sgi_miid_revisions_action')
        action['domain'] = [('sgi_is_controlled', '=', True), ('sgi_code', '=', self._sgi_kind()['code'])]
        action['name'] = "Revisiones del %s" % self._sgi_kind()['short']
        return action

    def action_open_request(self):
        self.ensure_one()
        if not self.pending_request_id:
            raise UserError("No hay solicitud de cambio del %s en curso." % self._sgi_kind()['short'])
        return {'type': 'ir.actions.act_window', 'res_model': 'approval.request',
                'res_id': self.pending_request_id.id, 'view_mode': 'form', 'target': 'current'}

    # ------------------------------------------------------------------
    # Solicitud de cambio (1.7)
    # ------------------------------------------------------------------
    def action_sgi_miid_request_change(self):
        """Arma (o abre) la solicitud de cambio documental del MIID con el PDF
        generado, la huella y las diferencias. Nunca la envía ni la aprueba."""
        self.ensure_one()
        kind = self._sgi_kind()
        if not (self.env.su or self.env.user.has_group('quimibond_sgi.group_sgi_manager')):
            raise UserError("Solo el Jefe MAST solicita el cambio del %s." % kind['short'])
        doc = self._sgi_current_document()
        if not doc:
            raise UserError("No hay %s vigente (clave %s) en Documentos: cárguelo o publíquelo primero." % (
                kind['short'], kind['code']))
        request = self._sgi_open_request(doc)
        if not request:
            snapshot = self._sgi_snapshot()
            baseline = bool(doc.sgi_content_hash)
            revision = self._sgi_next_revision(doc)
            diffs = self._sgi_diff(self._sgi_approved_snapshot(), snapshot) if baseline else []
            category = self.env.ref('quimibond_sgi.sgi_approval_category_doc_change')
            request = self.env['approval.request'].create({
                'name': "Cambio al %s (Rev. %02d)" % (kind['short'], revision),
                'category_id': category.id, 'request_owner_id': self.env.user.id, 'reference': kind['code'],
                'sgi_change_kind': 'modificacion', 'sgi_what_changes': 'contenido',
                'sgi_document_id': doc.id, 'sgi_new_revision': revision,
                'sgi_affected_process_ids': [(6, 0, doc.sgi_process_id.ids)],
                'sgi_reason': ("El %s vigente ya no coincide con el sistema." % kind['short'] if baseline
                               else "Primera revisión del %s generada desde Odoo." % kind['short']),
                'sgi_changes': "\n".join(diffs) or (
                    "Revisión generada desde Odoo con los datos del sistema al %s."
                    % sgi_today(self.env).strftime('%d/%m/%Y')),
                'sgi_miid_hash': self._sgi_hash(snapshot),
                'sgi_miid_snapshot': json.dumps(snapshot, ensure_ascii=False, sort_keys=True),
                'sgi_miid_generated': fields.Datetime.now(),
            })
            request._sgi_miid_attach("%s Rev. %02d (para aprobación).pdf" % (kind['short'], revision))
            request.message_post(body="Solicitud del %s creada desde %s." % (
                kind['short'], sgi_menu_path(kind['menu'])))
        return {'type': 'ir.actions.act_window', 'res_model': 'approval.request',
                'res_id': request.id, 'view_mode': 'form', 'target': 'current'}

    # ------------------------------------------------------------------
    # Comparación diaria y aviso (1.8)
    # ------------------------------------------------------------------
    def _sgi_notice_note(self, since, diffs):
        self.ensure_one()
        doc = self._sgi_current_document()
        kind = self._sgi_kind()
        head = escape("Desde el %s el %s vigente (Rev. %s%s) no coincide con el sistema:" % (
            since.strftime('%d/%m/%Y'), kind['short'], doc.sgi_revision_label or '00',
            ", emisión %s" % doc.sgi_issue_date.strftime('%d/%m/%Y') if doc.sgi_issue_date else ''))
        items = Markup('').join(Markup("<li>%s</li>") % d for d in diffs)
        tail = []
        pending = self._sgi_open_request(doc)
        if pending:
            tail.append("Ya está en curso la solicitud %s." % (pending.name or ''))
        tail.append("Abra %s y use «Solicitar cambio de revisión»." % sgi_menu_path(kind['menu']))
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
            kind = miid._sgi_kind()
            key = '%s:%d' % (kind['notice_kind'], miid.company_id.id)
            open_notices = self.env['mail.activity'].sudo().with_context(active_test=False).search([
                ('sgi_cron_key', '=', key), ('sgi_episode_closed', '=', False)])
            vals = {'last_check': now}
            if status['state'] != 'desactualizado':
                vals['outdated_since'] = False
                miid.write(vals)
                reason = ("el %s ya coincide con el sistema" % kind['short'] if status['state'] == 'al_dia'
                          else "el %s vigente no tiene contra qué comparar" % kind['short'])
                Cron._sgi_close_activities(open_notices, reason)
                continue
            vals['outdated_since'] = miid.outdated_since or now
            miid.write(vals)
            manager_id = Cron._sgi_manager_user_id()
            if not manager_id:
                continue
            since = sgi_local_date(self.env, miid.outdated_since)
            deadline = sgi_add_business_days(self.env, since, MIID_NOTICE_BUSINESS_DAYS)
            Cron._sgi_schedule(miid, "El %s vigente ya no coincide con el sistema" % kind['short'],
                               miid._sgi_notice_note(since, status['diffs']), manager_id,
                               date_deadline=deadline, key=key)
        return True

    # ------------------------------------------------------------------
    # Diagnóstico (1.9)
    # ------------------------------------------------------------------
    @api.model
    def _sgi_diagnostic_lines(self):
        lines = []
        for kind in MIID_KINDS:
            miid = self._sgi_get(kind=kind)
            if miid:
                lines += miid._sgi_diagnostic_lines_one()
        return lines

    def _sgi_diagnostic_lines_one(self):
        self.ensure_one()
        miid = self
        miid.invalidate_recordset()
        kind = miid._sgi_kind()
        short = kind['short']
        Diag = self.env['sgi.diagnostic']
        fix = sgi_menu_path(kind['menu'])
        status = miid._sgi_status()
        doc = miid._sgi_current_document()
        lines = []
        if status['state'] == 'al_dia':
            lines.append(Diag._sgi_line('ok', "%s al día (Rev. %s, emisión %s)." % (
                short, doc.sgi_revision_label, doc.sgi_issue_date.strftime('%d/%m/%Y') if doc.sgi_issue_date
                else "sin fecha")))
        elif status['state'] == 'desactualizado':
            since = sgi_local_date(self.env, miid.outdated_since) if miid.outdated_since \
                else sgi_today(self.env)
            lines.append(Diag._sgi_line('warn', "%s desactualizado desde %s: %d diferencia(s) con el sistema." % (
                short, since.strftime('%d/%m/%Y'), len(status['diffs'])), fix))
        elif status['state'] == 'sin_base':
            lines.append(Diag._sgi_line('warn', "El %s vigente (Rev. %s) no se generó desde Odoo: no se "
                                                "puede comparar con el sistema." % (short, doc.sgi_revision_label),
                                        fix))
        else:
            lines.append(Diag._sgi_line('bad', "No hay %s vigente (clave %s)." % (short, kind['code']), fix))
        if doc:
            blockers = miid._sgi_blockers()
            if blockers:
                lines.append(Diag._sgi_line(
                    'warn', "La siguiente revisión del %s no se puede aprobar: %s" % (
                        short, "; ".join(blockers)), fix))
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
        """El documento generado de la solicitud (por la clave del documento)."""
        self.ensure_one()
        doc = self.sudo().sgi_document_id
        kind = self.env['sgi.miid']._sgi_kind_for_code(doc.sgi_code) or 'miid'
        return self.env['sgi.miid']._sgi_get(doc.company_id or None, kind=kind)

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
                    "No se puede %s el cambio del %s todavía:\n%s\nQuite «Por confirmar» cuando el texto "
                    "esté confirmado y publique los procesos; después vuelva a intentarlo." % (
                        verb, req._sgi_miid()._sgi_kind()['short'], "\n".join("• " + b for b in blockers)))

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
        short = miid._sgi_kind()['short']
        if old:
            base = old.name[:-4] if (old.name or '').lower().endswith('.pdf') else (old.name or short)
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
        self._sgi_miid_attach("%s Rev. %02d (para aprobación).pdf" % (short, self.sgi_new_revision or 0))
        self.message_post(body="Se generó otra vez el PDF del %s antes de enviar (datos del sistema "
                               "actualizados); el anterior queda como «%s»." % (short, old.name if old else '—'))

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
        short = miid._sgi_kind()['short']
        blockers = miid._sgi_blockers()
        if blockers:  # última red: action_approve ya los revisó
            if not self.env.context.get('sgi_sign_sync'):
                raise UserError("No se puede aprobar el cambio del %s todavía:\n%s" % (short, "\n".join(
                    "• " + b for b in blockers)))
            self.message_post(body="El %s no se publicó: hay candados (secciones por confirmar o "
                                   "procesos sin publicar)." % short)
            return False
        if miid._sgi_hash(miid._sgi_snapshot()) != self.sgi_miid_hash:
            self.message_post(body="Los datos del sistema cambiaron después del envío: se publica el %s "
                                   "que se firmó; la comparación diaria lo marcará desactualizado." % short)
        sent = self._sgi_change_attachment()
        if sent:
            sent.sudo().write({'name': "%s Rev. %02d.pdf" % (short, self.sgi_new_revision or 0)})
        previous = self.sgi_document_id
        res = super()._sgi_apply_doc_change()
        target = self.sgi_new_document_id or self.sgi_document_id
        target.sudo().write({'sgi_content_hash': self.sgi_miid_hash})
        if miid.kind != 'miid' and previous and previous != target and previous.sgi_legacy_family:
            # 57.148.0: la revisión del Dropbox queda «Migrado a Odoo» hacia
            # el documento generado; la nueva no es «anterior».
            previous.sudo().write({'sgi_migration_state': 'migrado',
                                   'sgi_migration_target': sgi_menu_path(miid._sgi_kind()['menu'])})
        self._sgi_miid_close_held("el %s se aprobó" % short)
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
        """57.105.0: compara el MIID de la empresa del SGI con el sistema;
        57.148.0: también el P-G01."""
        # Solo la empresa del SGI (D-03).
        for kind in MIID_KINDS:
            miid = self.env['sgi.miid']._sgi_get(kind=kind)
            if miid:
                miid._sgi_check()
        return True

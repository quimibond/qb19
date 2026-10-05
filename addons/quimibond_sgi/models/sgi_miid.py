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

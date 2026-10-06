# -*- coding: utf-8 -*-
"""1.2.0: la documentación de texto del SGI en Conocimiento.

- Espacio «SGI» sembrado desde el código (``_sgi_kb_seed``): raíz, «Cómo usar
  el sistema» con los manuales del repo, un artículo por proceso y
  «Reglamentos». Fuente de verdad = código (decisión 2, opción A): un
  artículo sembrado se refresca solo si nadie lo editó (huella del texto
  plano guardada después de escribir).
- Borradores importados de los documentos controlados (``sgi_kind =
  'documento'``, ver ``sgi_knowledge_import.py``) y su estado de publicación.
- «Ayuda» por rol y el aviso diario de artículos publicados que cambiaron.

Nada de aquí escribe en ``sgi.process.activity.instruction_id`` ni en los
campos que forman la huella de Mi procedimiento o del MIID.
"""
import hashlib
import logging
import re

from markupsafe import Markup

from odoo import api, fields, models
from odoo.exceptions import AccessError, UserError
from odoo.tools import file_open, html2plaintext

from odoo.addons.quimibond_sgi.models.sgi_base import sgi_bypass_allowed

_logger = logging.getLogger(__name__)

# Versión del satélite que se cita en los mensajes de la siembra (la prueba
# test_kb_seed revisa que coincida con el manifest).
SGI_KB_VERSION = '1.2.0'

SGI_KB_DOC_TYPES = [
    ('instructivo', "Instructivo (IT)"),
    ('control_operacional', "Control operacional (CO)"),
    ('protocolo', "Protocolo (PROT)"),
    ('reglamento', "Reglamento (R)"),
]
SGI_KB_DOC_TYPE_CODES = tuple(code for code, _label in SGI_KB_DOC_TYPES)

# (clave de siembra, título, archivo en data/manuales, fuente en docs/sgi).
# Misma tabla que MANUALS en tools/sgi_knowledge_html.py.
SGI_KB_MANUALS = (
    ('manual:primeros-pasos', "Primeros pasos", 'primeros-pasos.html', 'primeros-pasos.md'),
    ('manual:operador-o-supervisor', "Manual del operador o supervisor",
     'operador-o-supervisor.html', 'usuarios/operador-o-supervisor.md'),
    ('manual:jefe-de-area', "Manual del jefe de área o dueño de proceso",
     'jefe-de-area.html', 'usuarios/jefe-de-area.md'),
    ('manual:mast', "Manual del Jefe MAST (día a día)", 'mast.html', 'usuarios/mast.md'),
    ('manual:manual-jefe-mast', "Manual del Jefe MAST (configuración)",
     'manual-jefe-mast.html', 'administracion/manual-jefe-mast.md'),
    ('manual:direccion', "Manual de Dirección", 'direccion.html', 'usuarios/direccion.md'),
    ('manual:rh', "Manual de RH", 'rh.html', 'usuarios/rh.md'),
    ('manual:auditor', "Manual del auditor", 'auditor.html', 'usuarios/auditor.md'),
    ('manual:glosario', "Glosario", 'glosario.html', 'glosario.md'),
)
SGI_KB_ROOT_NAME = "SGI"
SGI_KB_OLD_ROOT_NAME = "SGI (estructura 2025, sin uso)"
SGI_KB_PROCESS_ORDER = {'E': 0, 'C': 1, 'S': 2}
# Campos que solo escribe el sistema o el Jefe MAST (sin contexto de
# excepción: ``sgi_bypass_allowed`` = superusuario o Jefe MAST).
SGI_KB_PROTECTED_FIELDS = frozenset((
    'sgi_kind', 'sgi_seed_key', 'sgi_seed_hash', 'sgi_process_id', 'sgi_document_code',
    'sgi_doc_type', 'sgi_import_note'))
SGI_KB_LINK_RE = re.compile(r'<a\s+href="sgi-kb:([^"]+)"[^>]*>(.*?)</a>', re.S)
SGI_KB_MAST_GROUP = 'quimibond_sgi.group_sgi_manager'


def sgi_kb_plain(html):
    """Texto plano normalizado (como ``_miid_plain``): un guardado del editor
    que solo normaliza etiquetas no cuenta como edición."""
    return ' '.join(html2plaintext(html or '').split())


def sgi_kb_hash(html):
    return hashlib.sha256(sgi_kb_plain(html).encode('utf-8')).hexdigest()


def sgi_kb_url(article):
    """Liga relativa al artículo (sirve igual en producción y en un build)."""
    return '/knowledge/article/%d' % article.id


class KnowledgeArticleSgi(models.Model):
    _inherit = 'knowledge.article'

    sgi_kind = fields.Selection([
        ('raiz', "Raíz del espacio SGI"),
        ('carpeta', "Carpeta"),
        ('manual', "Manual"),
        ('proceso', "Proceso"),
        ('documento', "Documento controlado"),
    ], string="Tipo en el SGI", readonly=True, copy=False, index=True,
        help="Qué es este artículo para el SGI. Vacío: artículo ajeno al SGI.")
    sgi_seed_key = fields.Char(
        string="Clave de siembra", readonly=True, copy=False, index=True,
        help="Clave con que el sistema siembra y refresca este artículo (raiz, manual:mast, proceso:C4…).")
    sgi_seed_hash = fields.Char(
        string="Huella de la siembra", readonly=True, copy=False,
        help="Huella del texto que dejó la última siembra. Si el texto actual ya no coincide, alguien lo "
             "editó en Conocimiento y la siembra no lo vuelve a escribir.")
    sgi_process_id = fields.Many2one(
        'sgi.process', string="Proceso SGI", readonly=True, copy=False, index=True,
        help="Proceso al que pertenece el artículo.")
    sgi_document_code = fields.Char(
        string="Clave del documento", readonly=True, copy=False, index=True,
        help="Clave SGI del documento que explica este artículo (IT-C4-02). Sobrevive a las revisiones.")
    sgi_document_id = fields.Many2one(
        'documents.document', string="Documento vigente", compute='_compute_sgi_document_id',
        help="La revisión vigente del documento controlado con esta clave.")
    sgi_doc_type = fields.Selection(
        SGI_KB_DOC_TYPES, string="Tipo de documento", readonly=True, copy=False,
        help="Tipo del documento controlado que explica este artículo.")
    sgi_publish_state = fields.Selection([
        ('borrador', "Borrador"),
        ('pdf', "Borrador del PDF vigente"),
        ('publicado', "Publicado"),
        ('cambio', "Cambió desde la publicación"),
    ], string="Estado de publicación", compute='_compute_sgi_publish_state',
        search='_search_sgi_publish_state',
        help="Borrador: nunca publicado. Borrador del PDF vigente: la revisión vigente es el PDF y este "
             "artículo es su borrador importado. Publicado: la revisión vigente se congeló de este "
             "artículo. Cambió desde la publicación: el artículo cambió después de publicarse.")
    sgi_import_note = fields.Text(
        string="Nota de importación", readonly=True, copy=False,
        help="Qué pasó al importar el PDF: texto extraído, páginas, actividades ligadas o sugeridas.")
    sgi_activity_label = fields.Char(
        string="Actividades", compute='_compute_sgi_activity_label',
        help="Actividades del SGI que usan este artículo como instructivo.")

    # ------------------------------------------------------------------
    # Candado de los campos técnicos
    # ------------------------------------------------------------------
    @api.model_create_multi
    def create(self, vals_list):
        if not sgi_bypass_allowed(self.env):
            for vals in vals_list:
                if SGI_KB_PROTECTED_FIELDS & {k for k, v in vals.items() if v}:
                    raise AccessError("Solo el Jefe MAST o el sistema marcan artículos del SGI.")
        return super().create(vals_list)

    def write(self, vals):
        if SGI_KB_PROTECTED_FIELDS & set(vals) and not sgi_bypass_allowed(self.env):
            raise AccessError("Solo el Jefe MAST o el sistema cambian los datos del SGI de un artículo.")
        return super().write(vals)

    # ------------------------------------------------------------------
    # Huella y estado de publicación
    # ------------------------------------------------------------------
    def _sgi_article_hash(self):
        """Huella del artículo con que se congela una revisión (DOC-5): la
        misma fórmula de siempre, ``nombre + "\\n" + body``."""
        self.ensure_one()
        return hashlib.sha256((self.name or '').encode() + b'\n' + str(self.body or '').encode('utf-8')).hexdigest()

    def _sgi_published_documents(self):
        """Documentos (todas las revisiones) congelados de este artículo."""
        self.ensure_one()
        return self.env['documents.document'].sudo().with_context(active_test=False).search(
            [('sgi_article_id', '=', self.id), ('sgi_content_hash', '!=', False)])

    @api.depends('sgi_document_code')
    def _compute_sgi_document_id(self):
        Doc = self.env['documents.document'].sudo()
        for article in self:
            doc = Doc.browse()
            if article.sgi_document_code:
                doc = Doc.search([('sgi_code', '=', article.sgi_document_code), ('sgi_state', '=', 'vigente'),
                                  ('sgi_is_controlled', '=', True)], limit=1)
            elif article.id:
                doc = Doc.search([('sgi_article_id', '=', article.id), ('sgi_state', '=', 'vigente')], limit=1)
            article.sgi_document_id = doc

    @api.depends('sgi_document_code', 'body', 'name')
    def _compute_sgi_publish_state(self):
        for article in self:
            article.sgi_publish_state = article._sgi_publish_state_value()

    def _sgi_publish_state_value(self):
        self.ensure_one()
        if not self.id:
            return 'borrador'
        current = self.sgi_document_id.sudo()
        if current and current.sgi_article_id.id == self.id and current.sgi_content_hash:
            return 'publicado' if current.sgi_content_hash == self._sgi_article_hash() else 'cambio'
        if current and self.sgi_kind == 'documento':
            return 'pdf'
        return 'borrador'

    def _search_sgi_publish_state(self, operator, value):
        if operator not in ('=', '!=', 'in', 'not in'):
            raise UserError("Búsqueda no soportada en «Estado de publicación».")
        values = {value} if isinstance(value, str) else set(value or ())
        articles = self.sudo().search(['|', ('sgi_kind', '=', 'documento'), ('sgi_document_code', '!=', False)])
        matching = articles.filtered(lambda a: a._sgi_publish_state_value() in values)
        if operator in ('=', 'in'):
            return [('id', 'in', matching.ids)]
        return [('id', 'not in', matching.ids)]

    def _compute_sgi_activity_label(self):
        Activity = self.env['sgi.process.activity'].sudo()
        for article in self:
            activities = Activity.search([('instruction_article_id', '=', article.id)]) if article.id else Activity
            article.sgi_activity_label = ", ".join(
                a.number or a.legacy_number or a.name or '' for a in activities) or False

    # ------------------------------------------------------------------
    # Abrir
    # ------------------------------------------------------------------
    def _sgi_kb_open_action(self):
        """Abre el artículo en Conocimiento (ruta /knowledge/article/<id>)."""
        self.ensure_one()
        return {'type': 'ir.actions.act_url', 'url': sgi_kb_url(self), 'target': 'self'}

    def action_sgi_open(self):
        self.ensure_one()
        return self._sgi_kb_open_action()

    @api.model
    def _sgi_kb_find(self, key):
        """Artículo sembrado con esa clave (activo o archivado)."""
        return self.sudo().with_context(active_test=False).search([('sgi_seed_key', '=', key)], order='id', limit=1)

    @api.model
    def _sgi_kb_help_action(self):
        """«Ayuda» (SGI → Inicio): el manual del rol de quien entra."""
        user = self.env.user
        if user.has_group(SGI_KB_MAST_GROUP):
            key = 'manual:mast'
        elif user.has_group('quimibond_sgi.group_sgi_director'):
            key = 'manual:direccion'
        elif user.has_group('quimibond_sgi.group_sgi_auditor'):
            key = 'manual:auditor'
        elif user.has_group('hr.group_hr_user'):
            key = 'manual:rh'
        elif user.has_group('quimibond_sgi.group_sgi_process_owner') \
                or user.has_group('quimibond_sgi.group_sgi_efficiency_capture'):
            key = 'manual:jefe-de-area'
        else:
            key = 'manual:operador-o-supervisor'
        for candidate in (key, 'carpeta:ayuda', 'raiz'):
            article = self._sgi_kb_find(candidate)
            if article and article.active:
                return article._sgi_kb_open_action()
        raise UserError("Todavía no están los manuales del SGI en Conocimiento. Avise al Jefe MAST.")

    # ------------------------------------------------------------------
    # Siembra (decisión 2, opción A)
    # ------------------------------------------------------------------
    @api.model
    def _sgi_kb_manual_html(self, filename):
        """HTML de un manual generado por tools/sgi_knowledge_html.py, sin el
        comentario de cabecera."""
        with file_open('quimibond_sgi_knowledge/data/manuales/%s' % filename, 'r') as handle:
            text = handle.read()
        return re.sub(r'^\s*<!--.*?-->\s*', '', text, count=1, flags=re.S)

    @api.model
    def _sgi_kb_resolve_links(self, html):
        """Cambia los marcadores ``sgi-kb:<clave>`` por la liga del artículo;
        si el artículo no existe, deja solo el texto."""
        def repl(match):
            article = self._sgi_kb_find(match.group(1))
            if article and article.active:
                return '<a href="%s">%s</a>' % (sgi_kb_url(article), match.group(2))
            return match.group(2)
        return SGI_KB_LINK_RE.sub(repl, html or '')

    @api.model
    def _sgi_kb_mast_partners(self):
        group = self.env.ref(SGI_KB_MAST_GROUP, raise_if_not_found=False)
        users = group.sudo().all_user_ids.filtered(lambda u: u.active and not u.share) if group else self.env['res.users']
        partners = users.partner_id
        if not partners:
            partners = self.env.ref('base.partner_root')
        return partners

    @api.model
    def _sgi_kb_note_once(self, article, marker, body):
        """Mensaje en el chatter del artículo solo si no hay otro con ``marker``."""
        found = self.env['mail.message'].sudo().search_count([
            ('model', '=', 'knowledge.article'), ('res_id', '=', article.id),
            ('body', 'ilike', marker)])
        if not found:
            article.sudo().message_post(body=body)
            return True
        return False

    @api.model
    def _sgi_kb_seed_one(self, key, name, parent, html, kind, sequence=10, vals=None, source=None):
        """Crea o refresca un artículo sembrado. Devuelve (artículo, qué pasó):
        ``creado``, ``actualizado``, ``igual``, ``editado`` o ``archivado``."""
        Article = self.sudo().with_context(active_test=False)
        article = self._sgi_kb_find(key)
        if not article:
            create_vals = dict(vals or {}, name=name, body=html, sgi_kind=kind, sgi_seed_key=key,
                               sequence=sequence)
            if parent:
                create_vals['parent_id'] = parent.id
            article = Article.create(create_vals)
            article.sgi_seed_hash = sgi_kb_hash(article.body)
            return article, 'creado'
        if not article.active:
            return article, 'archivado'
        current = sgi_kb_plain(article.body)
        new = sgi_kb_plain(html)
        edited = bool(article.sgi_seed_hash) and \
            hashlib.sha256(current.encode('utf-8')).hexdigest() != article.sgi_seed_hash
        if edited:
            if hashlib.sha256(new.encode('utf-8')).hexdigest() != article.sgi_seed_hash:
                marker = "La versión %s del sistema" % SGI_KB_VERSION
                where = " Compárelo con docs/sgi/%s en el repositorio." % source if source else ""
                if self._sgi_kb_note_once(article, marker, Markup(
                        "%s trae texto nuevo para este artículo, pero no se aplicó porque se editó en "
                        "Conocimiento.%s") % (marker, where)):
                    _logger.warning("SGI Conocimiento: «%s» (%s) se editó a mano; la siembra %s no lo pisó.",
                                    article.name, key, SGI_KB_VERSION)
            return article, 'editado'
        if new == current:
            if not article.sgi_seed_hash:
                article.sgi_seed_hash = sgi_kb_hash(article.body)
            return article, 'igual'
        article.write({'body': html})
        article.sgi_seed_hash = sgi_kb_hash(article.body)
        article.message_post(body="Actualizado con la versión del sistema (quimibond_sgi_knowledge %s)."
                             % SGI_KB_VERSION)
        return article, 'actualizado'

    @api.model
    def _sgi_kb_rename_old_space(self):
        """N1: el espacio «SGI» de 2025 (raíz de espacio de trabajo sin clave)
        se renombra; no se mueve, no se archiva, no cambian sus permisos."""
        old = self.sudo().search([('parent_id', '=', False), ('name', '=', SGI_KB_ROOT_NAME),
                                  ('sgi_seed_key', '=', False), ('category', '=', 'workspace')])
        for article in old:
            article.name = SGI_KB_OLD_ROOT_NAME
            article.message_post(body="Se renombró al crear el espacio «SGI» del sistema "
                                      "(quimibond_sgi_knowledge %s). Su contenido no cambió." % SGI_KB_VERSION)
        return old

    @api.model
    def _sgi_kb_processes(self):
        company = self.env['sgi.config']._sgi_company()
        processes = self.env['sgi.process'].sudo().search([('company_id', 'in', (company.id, False))])
        return processes.sorted(lambda p: (SGI_KB_PROCESS_ORDER.get((p.code or 'Z')[:1].upper(), 3),
                                           p.code or '', p.id))

    @api.model
    def _sgi_kb_process_html(self, process):
        selection = process._fields['process_type'].selection
        types = dict(selection) if isinstance(selection, (list, tuple)) else {}
        parts = [
            Markup("<p><strong>%s</strong></p>") % (process.display_name or process.code or ''),
            Markup("<ul><li>Tipo de proceso: %s</li><li>Dueño: %s</li></ul>") % (
                types.get(process.process_type, "sin capturar"),
                process.owner_id.name or "sin capturar"),
            Markup("<p>Debajo están los instructivos, controles operacionales y protocolos del proceso. "
                   "Las actividades del proceso están en SGI → Sistema → Actividades.</p>"),
        ]
        return str(Markup('').join(parts))

    @api.model
    def _sgi_kb_seed(self):
        """Siembra el espacio «SGI» en Conocimiento (cada instalación y cada
        actualización, por ``data/sgi_knowledge_data.xml``). Idempotente; nunca
        borra ni archiva. Cada artículo en su savepoint: uno que falla deja un
        WARNING y no tumba la carga del módulo."""
        self = self.sudo()
        result = {'renamed': 0, 'created': 0, 'updated': 0, 'edited': 0}
        try:
            with self.env.cr.savepoint():
                result['renamed'] = len(self._sgi_kb_rename_old_space())
        except Exception:
            _logger.warning("SGI Conocimiento: no se pudo renombrar el espacio «SGI» de 2025.", exc_info=True)

        def seed(*args, **kwargs):
            try:
                with self.env.cr.savepoint():
                    article, what = self._sgi_kb_seed_one(*args, **kwargs)
            except Exception:
                _logger.warning("SGI Conocimiento: no se pudo sembrar %s.", args[0], exc_info=True)
                return self.browse(), 'error'
            if what == 'creado':
                result['created'] += 1
            elif what == 'actualizado':
                result['updated'] += 1
            elif what == 'editado':
                result['edited'] += 1
            return article, what

        root = self._sgi_kb_find('raiz')
        if not root:
            mast = self._sgi_kb_mast_partners()
            root, _what = seed('raiz', SGI_KB_ROOT_NAME, None, str(Markup(
                "<p>Sistema de Gestión Integral de Quimibond: cómo usar el sistema, los instructivos, "
                "controles operacionales y protocolos de cada proceso, y los reglamentos.</p>")),
                'raiz', 10, vals={
                    'icon': '📘', 'internal_permission': 'read',
                    'article_member_ids': [(0, 0, {'partner_id': p.id, 'permission': 'write'}) for p in mast]})
            if not root:
                return result
        if not root.active:
            # Alguien archivó el espacio: no se recrea ni se le cuelga nada.
            return result
        help_folder, _what = seed('carpeta:ayuda', "Cómo usar el sistema", root, str(Markup(
            "<p>Manuales del SGI por perfil. En el menú SGI → Inicio → Ayuda se abre el de usted.</p>")),
            'carpeta', 10, vals={'icon': '❓'})
        seed('carpeta:reglamentos', "Reglamentos", root, str(Markup(
            "<p>Reglamentos vigentes del SGI.</p>")), 'carpeta', 90, vals={'icon': '📜'})

        for index, process in enumerate(self._sgi_kb_processes(), start=1):
            article, _what = seed('proceso:%s' % (process.code or process.id),
                                  "%s — %s" % (process.code or '', process.name or ''),
                                  root, self._sgi_kb_process_html(process), 'proceso', 20 + index,
                                  vals={'sgi_process_id': process.id, 'icon': '⚙️'})
            if article and process.sgi_article_id != article:
                try:
                    with self.env.cr.savepoint():
                        process.sgi_article_id = article
                except Exception:
                    _logger.warning("SGI Conocimiento: no se pudo ligar el proceso %s.", process.code, exc_info=True)

        if help_folder:
            # Primero existen todos los manuales; después se escriben con las
            # ligas entre ellos ya resueltas (sin mensaje de «actualizado»).
            fresh = set()
            for index, (key, name, filename, source) in enumerate(SGI_KB_MANUALS):
                if not self._sgi_kb_find(key):
                    article, what = seed(key, name, help_folder, '<p></p>', 'manual', 10 + index,
                                         vals={'icon': '📗'})
                    if what == 'creado':
                        fresh.add(key)
            for index, (key, name, filename, source) in enumerate(SGI_KB_MANUALS):
                try:
                    with self.env.cr.savepoint():
                        html = self._sgi_kb_resolve_links(self._sgi_kb_manual_html(filename))
                        if key in fresh:
                            article = self._sgi_kb_find(key)
                            article.write({'body': html})
                            article.sgi_seed_hash = sgi_kb_hash(article.body)
                            continue
                except Exception:
                    _logger.warning("SGI Conocimiento: no se pudo sembrar %s.", key, exc_info=True)
                    continue
                seed(key, name, help_folder, html, 'manual', 10 + index, source=source)
        self._sgi_kb_sync_members()
        _logger.info("SGI Conocimiento: siembra %s: %s.", SGI_KB_VERSION, result)
        return result

    # ------------------------------------------------------------------
    # Miembros
    # ------------------------------------------------------------------
    @api.model
    def _sgi_kb_add_writers(self, article, partners):
        """Agrega ``partners`` como miembros con escritura (o sube a escritura
        a quien ya lee). Nunca quita miembros. Devuelve cuántos cambió."""
        Member = self.env['knowledge.article.member'].sudo()
        changed = 0
        for partner in partners:
            member = article.sudo().article_member_ids.filtered(lambda m, p=partner: m.partner_id == p)
            if not member:
                Member.create({'article_id': article.id, 'partner_id': partner.id, 'permission': 'write'})
                changed += 1
            elif member[0].permission != 'write':
                member[0].permission = 'write'
                changed += 1
        return changed

    @api.model
    def _sgi_kb_sync_members(self):
        """Raíz: escritura para el Jefe MAST (y quien lo implica). Proceso:
        escritura para el usuario del dueño. Borradores importados (sin
        publicar, desincronizados): Jefe MAST y dueño. Solo agrega; quitar
        a quien dejó de serlo lo hace a mano el administrador de Conocimiento."""
        added = 0
        root = self._sgi_kb_find('raiz')
        if not root or not root.active:
            return added
        mast = self._sgi_kb_mast_partners()
        try:
            with self.env.cr.savepoint():
                added += self._sgi_kb_add_writers(root, mast)
        except Exception:
            _logger.warning("SGI Conocimiento: no se pudieron poner los miembros de la raíz.", exc_info=True)
        root_writers = root.sudo().article_member_ids.filtered(lambda m: m.permission == 'write').partner_id
        Article = self.sudo()
        for article in Article.search([('sgi_kind', 'in', ('proceso', 'documento')), ('sgi_process_id', '!=', False)]):
            owner = article.sgi_process_id.owner_id.user_id.partner_id
            if article.sgi_kind == 'proceso':
                partners = owner - root_writers
            elif article.is_desynchronized:
                partners = mast | owner
            else:
                continue
            if not partners:
                continue
            try:
                with self.env.cr.savepoint():
                    added += self._sgi_kb_add_writers(article, partners)
            except Exception:
                _logger.warning("SGI Conocimiento: no se pudieron poner los miembros de «%s».",
                                article.name, exc_info=True)
        return added

    # ------------------------------------------------------------------
    # Publicación desde el artículo y cron
    # ------------------------------------------------------------------
    def _sgi_kb_publish_context(self):
        self.ensure_one()
        activity = self.env['sgi.process.activity'].sudo().search(
            [('instruction_article_id', '=', self.id)], order='id', limit=1)
        code = self.sgi_document_code or self.sgi_document_id.sgi_code or activity.instruction_id.sgi_code
        doc_type = self.sgi_doc_type or self.sgi_document_id.sgi_doc_type
        return {
            'default_article_id': self.id,
            'default_activity_id': activity.id or False,
            'default_code': code or False,
            'default_doc_type': doc_type if doc_type in SGI_KB_DOC_TYPE_CODES else 'instructivo',
        }

    def action_sgi_publish(self):
        """«Publicar» (Jefe MAST): abre el asistente de DOC-5 generalizado."""
        self.ensure_one()
        if not self.env.user.has_group(SGI_KB_MAST_GROUP):
            raise UserError("Solo el Jefe MAST publica documentos desde Conocimiento.")
        return {
            'type': 'ir.actions.act_window', 'name': "Publicar desde Conocimiento",
            'res_model': 'sgi.instruction.publish', 'view_mode': 'form', 'target': 'new',
            'context': self._sgi_kb_publish_context(),
        }

    def action_sgi_request_publish(self):
        """«Pedir publicación» (dueño del proceso o quien puede escribir el
        borrador): una actividad para el Jefe MAST, sin duplicar."""
        self.ensure_one()
        try:
            self.check_access('write')
        except AccessError:
            raise UserError("Solo quien puede editar este artículo pide su publicación.")
        mast_id = self.env['sgi.cron'].sudo()._sgi_manager_user_id()
        if not mast_id:
            raise UserError("No hay Jefe MAST a quien pedir la publicación.")
        code = self.sgi_document_code or self.name or ''
        self.env['sgi.cron'].sudo()._sgi_schedule(
            self.sudo(), "Publicar %s desde Conocimiento" % code,
            "%s pide publicar este artículo como revisión nueva de %s. Revíselo contra el PDF adjunto y "
            "pulse «Publicar» en SGI → Sistema → Conocimiento del SGI." % (self.env.user.name, code),
            mast_id, key='kb_publicar')
        self.sudo().message_post(body="%s pidió la publicación al Jefe MAST." % self.env.user.name)
        return {'type': 'ir.actions.client', 'tag': 'display_notification', 'params': {
            'type': 'success', 'message': "Se pidió la publicación al Jefe MAST.", 'sticky': False}}

    @api.model
    def _cron_sgi_kb_daily(self):
        """Diario: miembros del espacio y aviso al Jefe MAST de cada artículo
        publicado que cambió después de publicarse (una actividad por artículo)."""
        added = self._sgi_kb_sync_members()
        mast_id = self.env['sgi.cron'].sudo()._sgi_manager_user_id()
        changed = self.sudo().search([('sgi_publish_state', '=', 'cambio')])
        if mast_id:
            for article in changed:
                try:
                    with self.env.cr.savepoint():
                        self.env['sgi.cron'].sudo()._sgi_schedule(
                            article, "Artículo publicado cambió: %s" % (article.sgi_document_code or article.name),
                            "El artículo cambió después de publicarse como revisión del documento. Si el cambio "
                            "vale, publíquelo otra vez; si no, regrese el texto.", mast_id, key='kb_cambio')
                except Exception:
                    _logger.warning("SGI Conocimiento: no se pudo avisar del cambio de «%s».",
                                    article.name, exc_info=True)
        _logger.info("SGI Conocimiento: cron diario: %d miembro(s) agregados, %d artículo(s) cambiados.",
                     added, len(changed))
        return {'added': added, 'changed': len(changed)}

# -*- coding: utf-8 -*-
"""1.2.0 (decisión 4): importador por lotes de los documentos controlados
vigentes que viven en Conocimiento (instructivo, control operacional,
protocolo y reglamento).

Por documento: un borrador bajo el artículo de su proceso (o «Reglamentos»)
con el texto extraído del PDF, el PDF vigente adjunto, ligado al documento
(``sgi_article_id``) y a las actividades que ya usan esa clave
(``instruction_article_id``). Nunca escribe ``instruction_id``, ni la
revisión, el estado, el archivo o el nombre del documento. Idempotente por
clave. Lo que no pudo ligar va al resumen y al chatter de la raíz «SGI».
"""
import io
import logging
import re

from markupsafe import Markup, escape

from odoo import api, fields, models
from odoo.exceptions import UserError

from .sgi_knowledge_article import SGI_KB_DOC_TYPE_CODES, SGI_KB_MAST_GROUP

_logger = logging.getLogger(__name__)

SGI_KB_IMPORT_MAX_PAGES = 40
SGI_KB_IMPORT_MAX_BYTES = 8 * 1024 * 1024
SGI_KB_IMPORT_LABELS = {
    'importado': "importado",
    'ya_importado': "ya importado",
    'excluido': "excluido por L-001",
    'restringido': "restringido: no se copia a Conocimiento",
    'sin_proceso': "sin proceso: no se importó",
    'pendiente': "pendiente para el siguiente lote",
    'error': "error: no se importó",
}


def sgi_kb_pdf_reader():
    """Lector de PDF que trae Odoo (pypdf o PyPDF2, según la versión)."""
    try:
        from odoo.tools.pdf import PdfReader
        return PdfReader
    except ImportError:
        try:
            from odoo.tools.pdf import PdfFileReader
            return PdfFileReader
        except ImportError:
            return None


class SgiKnowledgeImport(models.TransientModel):
    """Importa a Conocimiento los instructivos, controles operacionales, protocolos y reglamentos
    vigentes, como borradores para que el dueño del proceso los corrija y el Jefe MAST los publique."""
    _name = 'sgi.knowledge.import'
    _description = "Importar documentos del SGI a Conocimiento"

    preset = fields.Selection([
        ('co_c4', "CO y C4: controles operacionales e instructivos de C4"),
        ('todos', "Todos los vigentes de los cuatro tipos"),
        ('manual', "Los que elija"),
    ], string="Qué importar", default='co_c4', required=True,
        help="Por dónde empezar: los cinco controles operacionales y los instructivos de C4, todos, o la "
             "selección de abajo.")
    document_ids = fields.Many2many(
        'documents.document', string="Documentos",
        domain="[('sgi_is_controlled', '=', True), ('sgi_state', '=', 'vigente'), "
               "('sgi_doc_type', 'in', ['instructivo', 'control_operacional', 'protocolo', 'reglamento'])]",
        help="Documentos controlados vigentes que se importan cuando elige «Los que elija».")
    batch_size = fields.Integer(
        string="Documentos por lote", default=10,
        help="Cuántos documentos nuevos se importan en cada corrida. Vuelva a pulsar «Importar» para el "
             "siguiente lote.")
    summary = fields.Text(string="Resultado", readonly=True,
                          help="Qué pasó con cada documento en la última corrida.")

    @api.model
    def default_get(self, fields_list):
        values = super().default_get(fields_list)
        if self.env.context.get('active_model') == 'documents.document' and self.env.context.get('active_ids'):
            values['preset'] = 'manual'
            values['document_ids'] = [(6, 0, self.env.context['active_ids'])]
        return values

    @api.model
    def _sgi_check_manager(self):
        if not self.env.user.has_group(SGI_KB_MAST_GROUP):
            raise UserError("Solo el Jefe MAST importa documentos a Conocimiento.")

    def _sgi_preset_documents(self):
        """Documentos de la corrida, incluidos los excluidos por L-001 (salen en
        el resumen como excluidos)."""
        self.ensure_one()
        Doc = self.env['documents.document'].sudo()
        if self.preset == 'manual':
            docs = self.document_ids.sudo()
        else:
            company = self.env['sgi.config']._sgi_company()
            docs = Doc.search([
                ('sgi_is_controlled', '=', True), ('sgi_state', '=', 'vigente'),
                ('sgi_doc_type', 'in', list(SGI_KB_DOC_TYPE_CODES)),
                ('company_id', 'in', (company.id, False))], order='sgi_code, id')
            if self.preset == 'co_c4':
                docs = docs.filtered(lambda d: d.sgi_doc_type == 'control_operacional' or (
                    d.sgi_doc_type == 'instructivo' and (d.sgi_process_id.code or '').upper() == 'C4'))
        return docs.filtered(lambda d: d.sgi_doc_type in SGI_KB_DOC_TYPE_CODES)

    # ------------------------------------------------------------------
    # Texto del PDF
    # ------------------------------------------------------------------
    @api.model
    def _sgi_pdf_text(self, attachment):
        """(texto, nota). Primero el texto que ya extrajo attachment_indexation
        (``index_content``); si viene vacío, el lector de PDF de Odoo."""
        attachment = attachment.sudo()
        if not attachment:
            return '', "sin archivo"
        size = attachment.file_size or 0
        if size > SGI_KB_IMPORT_MAX_BYTES:
            return '', "demasiado grande para extraer en línea (%.1f MB): solo el PDF adjunto" % (size / 1048576.0)
        text = (attachment.index_content or '').strip() if 'index_content' in attachment._fields else ''
        if text and not text.startswith('application/'):
            return text, "texto extraído al subir el archivo"
        Reader = sgi_kb_pdf_reader()
        if not Reader or (attachment.mimetype or '') != 'application/pdf':
            return '', "sin texto: posible PDF escaneado"
        try:
            reader = Reader(io.BytesIO(attachment.raw or b''), strict=False)
            pages = list(getattr(reader, 'pages', []))
            if len(pages) > SGI_KB_IMPORT_MAX_PAGES:
                return '', "demasiado grande para extraer en línea (%d páginas): solo el PDF adjunto" % len(pages)
            chunks = []
            for page in pages:
                extract = getattr(page, 'extract_text', None) or getattr(page, 'extractText', None)
                chunks.append((extract() if extract else '') or '')
            text = '\n\n'.join(chunk.strip() for chunk in chunks if chunk.strip())
        except Exception as exc:  # pypdf lanza muchos tipos distintos con PDF dañados
            _logger.info("SGI Conocimiento: no se pudo leer el PDF %s: %s", attachment.id, exc)
            return '', "sin texto: el PDF no se pudo leer"
        if not text:
            return '', "sin texto: posible PDF escaneado"
        return text, "texto extraído del PDF (%d páginas)" % len(pages)

    @api.model
    def _sgi_text_html(self, text):
        """Un ``<p>`` por párrafo (línea en blanco), saltos simples como ``<br>``, escapado."""
        paragraphs = [p.strip() for p in re.split(r'\n\s*\n', (text or '').replace('\r\n', '\n')) if p.strip()]
        return Markup('').join(
            Markup('<p>%s</p>') % Markup('<br>').join(escape(line.strip()) for line in p.split('\n'))
            for p in paragraphs)

    # ------------------------------------------------------------------
    # Un documento
    # ------------------------------------------------------------------
    @api.model
    def _sgi_suggest_activities(self, doc):
        """Actividades del mismo proceso que mencionan la clave (o la anterior)
        en su texto: solo sugerencias, no se ligan."""
        codes = [c for c in (doc.sgi_code, doc.sgi_previous_code) if c]
        if not codes or not doc.sgi_process_id:
            return self.env['sgi.process.activity']
        Activity = self.env['sgi.process.activity'].sudo()
        found = Activity.browse()
        for code in codes:
            domain = [('process_id', '=', doc.sgi_process_id.id)]
            text_fields = [name for name in ('name', 'how_steps', 'check_against', 'done_criteria', 'place_note')
                           if name in Activity._fields]
            domain += ['|'] * (len(text_fields) - 1) + [(name, 'ilike', code) for name in text_fields]
            found |= Activity.search(domain)
        return found

    @api.model
    def _sgi_kb_import_one(self, doc):
        """Importa un documento. Devuelve (estado, artículo, detalle)."""
        Article = self.env['knowledge.article'].sudo()
        doc = doc.sudo()
        if doc._sgi_is_dropbox_excluded():
            return 'excluido', Article, ""
        if (doc.access_internal or 'none') != 'view':
            return 'restringido', Article, ""
        existing = Article.with_context(active_test=False).search(
            [('sgi_document_code', '=', doc.sgi_code)], order='id', limit=1)
        if existing:
            if not doc.sgi_article_id:
                doc.write({'sgi_article_id': existing.id})
                return 'ya_importado', existing, "se completó la liga del documento"
            return 'ya_importado', existing, ""
        if doc.sgi_doc_type == 'reglamento':
            parent = Article._sgi_kb_find('carpeta:reglamentos')
        else:
            parent = doc.sgi_process_id.sgi_article_id
        if not parent:
            return 'sin_proceso', Article, (
                "el proceso %s no tiene artículo en Conocimiento" % doc.sgi_process_id.code
                if doc.sgi_process_id else "el documento no tiene proceso")

        text, note = self._sgi_pdf_text(doc.attachment_id)
        date = doc.sgi_issue_date and fields.Date.to_string(doc.sgi_issue_date) or "sin fecha"
        body = Markup(
            '<div class="alert alert-info" role="status">Borrador importado del PDF de la revisión %s (%s). '
            'El texto se extrajo sin formato ni imágenes y puede tener errores: compárelo con el PDF adjunto '
            'antes de publicar.</div>') % (doc.sgi_revision_label or '00', date)
        if not text:
            body += Markup('<p><em>%s</em></p>') % note
        body += self._sgi_text_html(text)

        mast = Article._sgi_kb_mast_partners()
        owner = doc.sgi_process_id.owner_id.user_id.partner_id
        members = mast | owner
        article = Article.create({
            'name': "%s %s" % (doc.sgi_code or '', doc.sgi_title or doc.name or ''),
            'parent_id': parent.id,
            'body': body,
            'icon': '📄',
            'sgi_kind': 'documento',
            'sgi_document_code': doc.sgi_code,
            'sgi_doc_type': doc.sgi_doc_type,
            'sgi_process_id': doc.sgi_process_id.id or False,
            # N4: solo el dueño del proceso y el Jefe MAST hasta publicar.
            'is_desynchronized': True,
            'internal_permission': 'none',
            'article_member_ids': [(0, 0, {'partner_id': p.id, 'permission': 'write'}) for p in members],
        })
        attachment = doc.attachment_id.sudo()
        copy = self.env['ir.attachment']
        if attachment:
            copy = self.env['ir.attachment'].sudo().create({
                'name': attachment.name or ('%s.pdf' % doc.sgi_code),
                'raw': attachment.raw,
                'mimetype': attachment.mimetype,
                'res_model': 'knowledge.article',
                'res_id': article.id,
            })
            article.body = article.body + Markup('<p><a href="/web/content/%d?download=false" target="_blank">'
                                                 'Abrir el PDF vigente (revisión %s)</a></p>') % (
                copy.id, doc.sgi_revision_label or '00')
        # N3: liga del documento (no es campo de transición: no dispara nada).
        doc.write({'sgi_article_id': article.id})
        # Actividades que ya usan esta clave (cualquier revisión): solo el
        # artículo, nunca instruction_id.
        family = doc.with_context(active_test=False).search([('sgi_code', '=', doc.sgi_code)])
        activities = self.env['sgi.process.activity'].sudo().search(
            [('instruction_id', 'in', family.ids), ('instruction_article_id', '=', False)])
        if activities:
            activities.write({'instruction_article_id': article.id})
        detail = [note]
        if activities:
            detail.append("actividad ligada: %s" % ", ".join(
                a.number or a.legacy_number or a.name or '' for a in activities))
        else:
            suggested = self._sgi_suggest_activities(doc)
            if suggested:
                detail.append("sin actividad; sugeridas: %s" % ", ".join(
                    a.number or a.legacy_number or a.name or '' for a in suggested))
            else:
                detail.append("sin actividad conocida")
        article.sgi_import_note = "; ".join(detail)
        return 'importado', article, "; ".join(detail)

    # ------------------------------------------------------------------
    # La corrida
    # ------------------------------------------------------------------
    def action_import(self):
        self.ensure_one()
        self._sgi_check_manager()
        Article = self.env['knowledge.article'].sudo()
        if not Article._sgi_kb_find('raiz'):
            Article._sgi_kb_seed()
        docs = self._sgi_preset_documents()
        if not docs:
            raise UserError("No hay documentos vigentes que importar con esta opción.")
        limit = max(self.batch_size or 10, 1)
        rows = []
        new = 0
        for doc in docs:
            label = doc.sgi_code or doc.name or str(doc.id)
            excluded = doc._sgi_is_dropbox_excluded()
            restricted = (doc.access_internal or 'none') != 'view'
            already = Article.with_context(active_test=False).search_count(
                [('sgi_document_code', '=', doc.sgi_code)]) if doc.sgi_code else 0
            if new >= limit and not (excluded or restricted or already):
                rows.append(('pendiente', label, ""))
                continue
            try:
                with self.env.cr.savepoint():
                    state, _article, detail = self._sgi_kb_import_one(doc)
            except Exception as exc:
                _logger.info("SGI Conocimiento: no se importó %s: %s", label, exc, exc_info=True)
                state, detail = 'error', str(exc)[:200]
            if state == 'importado':
                new += 1
            rows.append((state, label, detail))
        counts = {}
        for state, _label, _detail in rows:
            counts[state] = counts.get(state, 0) + 1
        header = ", ".join("%s: %d" % (SGI_KB_IMPORT_LABELS[s], counts[s])
                           for s in SGI_KB_IMPORT_LABELS if counts.get(s))
        lines = [header] + ["%s — %s%s" % (label, SGI_KB_IMPORT_LABELS[state], (": " + detail) if detail else "")
                            for state, label, detail in rows]
        self.summary = "\n".join(lines)
        root = Article._sgi_kb_find('raiz')
        if root:
            table = Markup('').join(
                Markup('<tr><td>%s</td><td>%s</td><td>%s</td></tr>') % (label, SGI_KB_IMPORT_LABELS[state], detail)
                for state, label, detail in rows)
            root.message_post(body=Markup(
                '<p>Importación de documentos a Conocimiento (%s): %s.</p>'
                '<table class="table table-sm"><tr><th>Documento</th><th>Resultado</th><th>Detalle</th></tr>%s'
                '</table>') % (self.env.user.name, header, table))
        _logger.info("SGI Conocimiento: importación: %s.", header)
        return {
            'type': 'ir.actions.act_window', 'res_model': self._name, 'res_id': self.id,
            'view_mode': 'form', 'target': 'new', 'name': "Importar documentos a Conocimiento",
        }

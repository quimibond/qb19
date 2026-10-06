# -*- coding: utf-8 -*-
"""DOC-5 (53.0.0): el instructivo de una actividad puede ser un artículo de
Knowledge. Se escribe ahí (con fotos) y «Publicar como instructivo» lo
congela como revisión del documento controlado con su clave IT: PDF del
artículo archivado en Documentos, ligado a la actividad (`instruction_id`)
y con la huella del contenido para saber si el artículo cambió después.

1.2.0: la publicación sirve para los cuatro tipos que viven en Conocimiento
(instructivo, control operacional, protocolo y reglamento), se abre desde la
actividad, el documento o la lista «Conocimiento del SGI», copia del vigente
los datos de la revisión (clave anterior, documento padre, carpeta, área,
título) y re-apunta todas las actividades que usaban la revisión anterior.
"""
import base64
import hashlib

from markupsafe import Markup

from odoo import api, fields, models
from odoo.exceptions import UserError

from odoo.addons.quimibond_sgi.models.sgi_measure_manual_reason import SgiActivityManualReason

from .sgi_knowledge_article import SGI_KB_DOC_TYPES, SGI_KB_DOC_TYPE_CODES, SGI_KB_MAST_GROUP

# Lo que se copia del vigente a la revisión nueva: los mismos campos que el
# flujo formal de cambios (approval.request._SGI_REVISION_FIELDS,
# quimibond_sgi/models/sgi_doc_change.py) más los de la transición y el
# padre, para que el MIID (controls: revisión, título, clave anterior) y la
# exclusión L-001 sigan viendo el mismo documento.
SGI_KB_REVISION_FIELDS = (
    'sgi_doc_type_id', 'sgi_doc_type', 'sgi_code', 'sgi_owner_id', 'sgi_job_ids',
    'sgi_process_id', 'sgi_area_id', 'sgi_odoo_menu_id', 'sgi_retention_years',
    'folder_id', 'company_id',
)
SGI_KB_PUBLISH_EXTRA_FIELDS = ('sgi_previous_code', 'sgi_previous_code_date', 'sgi_parent_document_id')


class SgiProcessActivityKnowledge(models.Model):
    _inherit = 'sgi.process.activity'

    # 1.2.0: ligar el artículo no es un cambio al cuerpo del procedimiento: no
    # marca el procedimiento vivo como modificado (G14) ni las cifras de Mi
    # procedimiento. Tampoco entra a su huella (no está en
    # _sgi_my_procedure_hash). Unión con el último conjunto del núcleo.
    _SGI_MEASURE_FIELDS = set(SgiActivityManualReason._SGI_MEASURE_FIELDS) | {'instruction_article_id'}

    instruction_article_id = fields.Many2one(
        'knowledge.article', string="Instructivo en Knowledge",
        help="Artículo donde se escribe el instructivo. «Publicar como instructivo» lo "
             "congela como revisión del IT.")
    instruction_article_stale = fields.Boolean(
        string="Artículo cambió desde la última revisión", compute='_compute_instruction_article_stale',
        help="El artículo de Conocimiento del instructivo cambió después de publicarse como revisión.")

    def _sgi_article_hash(self):
        self.ensure_one()
        if self.instruction_article_id:
            return self.instruction_article_id._sgi_article_hash()
        return hashlib.sha256(b'\n').hexdigest()

    @api.depends('instruction_article_id.body', 'instruction_article_id.name', 'instruction_id.sgi_content_hash')
    def _compute_instruction_article_stale(self):
        for activity in self:
            doc = activity.instruction_id
            # N3: solo cuenta si la revisión se congeló del artículo (huella
            # guardada). Un borrador importado del PDF no prende el aviso.
            activity.instruction_article_stale = bool(
                activity.instruction_article_id and doc and doc.sgi_article_id == activity.instruction_article_id
                and doc.sgi_content_hash and doc.sgi_content_hash != activity._sgi_article_hash())

    def action_publish_instruction(self):
        self.ensure_one()
        if not self.instruction_article_id:
            raise UserError("Ligue primero el artículo de Conocimiento del instructivo.")
        return {
            'type': 'ir.actions.act_window', 'name': "Publicar instructivo",
            'res_model': 'sgi.instruction.publish', 'view_mode': 'form', 'target': 'new',
            'context': {'default_activity_id': self.id,
                        'default_article_id': self.instruction_article_id.id,
                        'default_code': self.instruction_id.sgi_code or False},
        }


class DocumentsDocumentArticle(models.Model):
    _inherit = 'documents.document'

    sgi_article_id = fields.Many2one(
        'knowledge.article', string="Artículo de Knowledge", readonly=True, copy=False,
        help="Artículo de Conocimiento del documento: borrador importado de su PDF o artículo del que se "
             "congeló esta revisión.")

    def action_sgi_kb_open(self):
        """«Abrir en Conocimiento»."""
        self.ensure_one()
        if not self.sgi_article_id:
            raise UserError("Este documento no tiene artículo en Conocimiento.")
        return self.sgi_article_id._sgi_kb_open_action()

    def action_sgi_kb_publish(self):
        """«Publicar desde Conocimiento» (Jefe MAST)."""
        self.ensure_one()
        if not self.sgi_article_id:
            raise UserError("Este documento no tiene artículo en Conocimiento: impórtelo primero.")
        return self.sgi_article_id.action_sgi_publish()


class SgiInstructionPublish(models.TransientModel):
    """Asistente que publica un artículo de Conocimiento como revisión nueva de un instructivo,
    control operacional, protocolo o reglamento, con acuses para los puestos."""
    _name = 'sgi.instruction.publish'
    _description = "Publicar artículo de Knowledge como documento controlado"

    activity_id = fields.Many2one('sgi.process.activity',
                                  help="Actividad cuyo instructivo se publica (opcional).")
    article_id = fields.Many2one(
        'knowledge.article', string="Artículo", compute='_compute_article_id', store=True, readonly=False,
        help="Artículo de Conocimiento que se publica como revisión del documento.")
    doc_type = fields.Selection(
        SGI_KB_DOC_TYPES, string="Tipo de documento", compute='_compute_doc_type', store=True, readonly=False,
        help="Tipo del documento controlado que se publica.")
    code = fields.Char(string="Clave", compute='_compute_code', store=True, readonly=False,
                       help="Clave del documento, por ejemplo IT-C4-02 o CO-E2-01.")
    current_id = fields.Many2one('documents.document', string="Revisión vigente",
                                 compute='_compute_current_id',
                                 help="La revisión vigente con esta clave, que quedará obsoleta.")
    job_ids = fields.Many2many('hr.job', string="Puestos que aplican",
                               compute='_compute_job_ids', store=True, readonly=False,
                               help="Por omisión, los que ejecutan las actividades que usan el documento; si no "
                                    "hay, los del documento vigente; y en controles operacionales, protocolos y "
                                    "reglamentos, los de las actividades del proceso.")

    @api.depends('activity_id')
    def _compute_article_id(self):
        for wiz in self:
            if not wiz.article_id and wiz.activity_id:
                wiz.article_id = wiz.activity_id.sudo().instruction_article_id

    @api.depends('article_id')
    def _compute_code(self):
        for wiz in self:
            if not wiz.code:
                article = wiz.article_id.sudo()
                wiz.code = article.sgi_document_code or wiz.activity_id.sudo().instruction_id.sgi_code or False

    @api.depends('article_id', 'code')
    def _compute_doc_type(self):
        for wiz in self:
            if wiz.doc_type:
                continue
            current = wiz._sgi_current()
            kind = wiz.article_id.sudo().sgi_doc_type or current.sgi_doc_type
            wiz.doc_type = kind if kind in SGI_KB_DOC_TYPE_CODES else 'instructivo'

    def _sgi_code(self):
        return (self.code or '').strip().upper()

    def _sgi_family(self):
        """Todas las revisiones (activas o archivadas) de la clave."""
        code = self._sgi_code()
        Doc = self.env['documents.document'].sudo().with_context(active_test=False)
        return Doc.search([('sgi_code', '=', code)]) if code else Doc

    def _sgi_current(self):
        return self._sgi_family().filtered(lambda d: d.sgi_state == 'vigente' and d.active)[:1]

    @api.depends('code')
    def _compute_current_id(self):
        for wiz in self:
            wiz.current_id = wiz._sgi_current()

    def _sgi_family_activities(self):
        family = self._sgi_family()
        Activity = self.env['sgi.process.activity'].sudo()
        return Activity.search([('instruction_id', 'in', family.ids)]) if family else Activity

    @api.depends('activity_id', 'article_id', 'code', 'doc_type')
    def _compute_job_ids(self):
        for wiz in self:
            if wiz.job_ids:
                continue
            activities = wiz._sgi_family_activities() | wiz.activity_id.sudo()
            jobs = activities.filtered('active').responsible_job_ids
            current = wiz._sgi_current()
            if not jobs and current:
                jobs = current.sgi_job_ids
            if not jobs and wiz.doc_type in ('control_operacional', 'protocolo', 'reglamento'):
                process = current.sgi_process_id or wiz.article_id.sudo().sgi_process_id \
                    or wiz.activity_id.sudo().process_id
                if process:
                    # Misma regla que el flujo formal de cambios (_sgi_notify_process).
                    jobs = process.sudo().procedure_activity_ids.filtered('active').responsible_job_ids
            wiz.job_ids = jobs

    def _sgi_revision_values(self, source):
        """Valores que la revisión nueva copia del vigente (o de la última revisión)."""
        vals = {}
        for name in SGI_KB_REVISION_FIELDS + SGI_KB_PUBLISH_EXTRA_FIELDS:
            if name not in source._fields:
                continue
            field = source._fields[name]
            value = source[name]
            if field.type == 'many2one':
                vals[name] = value.id
            elif field.type in ('many2many', 'one2many'):
                vals[name] = [(6, 0, value.ids)]
            else:
                vals[name] = value
        return vals

    def action_publish(self):
        self.ensure_one()
        if not self.env.user.has_group(SGI_KB_MAST_GROUP):
            raise UserError("Solo el Jefe MAST publica documentos desde Conocimiento.")
        activity = self.activity_id.sudo()
        article = (self.article_id or activity.instruction_article_id).sudo()
        if not article:
            raise UserError("Falta el artículo de Conocimiento que se publica.")
        code = self._sgi_code()
        if not code:
            raise UserError("Falta la clave del documento.")
        doc_type = self.doc_type or 'instructivo'
        Doc = self.env['documents.document'].sudo()
        previous = self._sgi_family()
        current = previous.filtered(lambda d: d.sgi_state == 'vigente' and d.active)[:1]
        source = current or previous.sorted(lambda d: (d.sgi_revision, d.id))[-1:]
        content_hash = article._sgi_article_hash()
        if current and current.sgi_content_hash == content_hash:
            raise UserError("El artículo no cambió desde la revisión vigente %s." % current.sgi_revision_label)
        revision = (max(previous.mapped('sgi_revision')) + 1) if previous else 0
        report = self.env.ref('quimibond_sgi_knowledge.action_report_knowledge_instruction')
        pdf, _ = self.env['ir.actions.report'].sudo()._render_qweb_pdf(report.report_name, article.ids)
        today = fields.Date.context_today(self)
        if source:
            vals = self._sgi_revision_values(source)
            if source.sgi_doc_type != doc_type:
                vals.pop('sgi_doc_type_id', None)
            # El título no cambia (el MIID compara título; la revisión va en el chatter).
            name = "%s %s.pdf" % (code, source.sgi_title or article.name or '')
        else:
            vals = {}
            # Sin el número de revisión en el nombre: el título lo copian las
            # revisiones siguientes y la revisión va en su campo.
            name = "%s %s.pdf" % (code, article.name or '')
        process = source.sgi_process_id or article.sgi_process_id or activity.process_id
        vals.update({
            'name': name,
            'type': 'binary',
            'datas': base64.b64encode(pdf),
            'mimetype': 'application/pdf',
            'sgi_is_controlled': True,
            'sgi_doc_type': doc_type,
            'sgi_code': code,
            'sgi_state': 'vigente',
            'sgi_revision': revision,
            'sgi_issue_date': today,
            'sgi_process_id': process.id or False,
            'sgi_job_ids': [(6, 0, self.job_ids.ids)],
            'sgi_owner_id': source.sgi_owner_id.id or self.env.user.id,
            'sgi_content_hash': content_hash,
            'sgi_article_id': article.id,
            'company_id': source.company_id.id or activity.company_id.id or self.env.company.id,
        })
        vals = {k: v for k, v in vals.items() if v is not None}
        # Superusuario: los campos de transición (clave anterior) solo los
        # escribe el sistema o el Jefe MAST (sgi_bypass_allowed acepta env.su).
        doc = Doc.create(vals)
        doc.action_generate_acks()
        # Todas las actividades que usaban una revisión anterior (DOC-5 solo
        # movía una) y la del asistente.
        activities = self.env['sgi.process.activity'].sudo().search(
            [('instruction_id', 'in', (previous - doc).ids)]) if previous else self.env['sgi.process.activity']
        activities |= activity
        if activities:
            activities.write({'instruction_id': doc.id})
            activities.filtered(lambda a: not a.instruction_article_id).write({'instruction_article_id': article.id})
        # N4: publicado, lo leen todos (vuelve a heredar del padre) y se bloquea.
        article_vals = {'is_locked': True}
        if article.sgi_kind == 'documento' and article.is_desynchronized and article.parent_id:
            article_vals.update({'is_desynchronized': False, 'internal_permission': False})
        if not article.sgi_document_code and article.sgi_kind in (False, 'documento'):
            article_vals['sgi_document_code'] = code
        if article.sgi_kind in ('documento', 'manual', 'proceso', 'carpeta', 'raiz'):
            article.write(article_vals)
        else:
            # Artículo de DOC-5 fuera del espacio SGI: solo se liga la clave.
            article.write({k: v for k, v in article_vals.items() if k == 'sgi_document_code'})
        article.invalidate_recordset(['sgi_document_id', 'sgi_publish_state'])
        type_label = dict(SGI_KB_DOC_TYPES).get(doc_type, doc_type)
        writer = article.last_edition_uid or article.write_uid
        doc.message_post(body=Markup(
            "%s publicado desde el artículo de Conocimiento «%s», revisión %02d. Redactó: %s; publicó: %s.") % (
            type_label, article.name or '', revision, writer.name or '', self.env.user.name or ''))
        article.message_post(body="Publicado como revisión %02d de %s." % (revision, code))
        return {
            'type': 'ir.actions.act_window', 'res_model': 'documents.document', 'res_id': doc.id,
            'view_mode': 'form', 'views': [(self.env.ref('quimibond_sgi.sgi_document_view_form').id, 'form')],
        }

# -*- coding: utf-8 -*-
"""DOC-5 (53.0.0): el instructivo de una actividad puede ser un artículo de
Knowledge. Se escribe ahí (con fotos) y «Publicar como instructivo» lo
congela como revisión del documento controlado con su clave IT: PDF del
artículo archivado en Documentos, ligado a la actividad (`instruction_id`)
y con la huella del contenido para saber si el artículo cambió después.
"""
import base64
import hashlib

from odoo import api, fields, models
from odoo.exceptions import UserError


class SgiProcessActivityKnowledge(models.Model):
    _inherit = 'sgi.process.activity'

    instruction_article_id = fields.Many2one(
        'knowledge.article', string="Instructivo en Knowledge",
        help="Artículo donde se escribe el instructivo. «Publicar como instructivo» lo "
             "congela como revisión del IT.")
    instruction_article_stale = fields.Boolean(
        string="Artículo cambió desde la última revisión", compute='_compute_instruction_article_stale')

    def _sgi_article_hash(self):
        self.ensure_one()
        body = self.instruction_article_id.body or ''
        return hashlib.sha256((self.instruction_article_id.name or '').encode() + b'\n'
                              + str(body).encode('utf-8')).hexdigest()

    @api.depends('instruction_article_id.body', 'instruction_id.sgi_content_hash')
    def _compute_instruction_article_stale(self):
        for activity in self:
            doc = activity.instruction_id
            activity.instruction_article_stale = bool(
                activity.instruction_article_id and doc and doc.sgi_article_id == activity.instruction_article_id
                and doc.sgi_content_hash != activity._sgi_article_hash())

    def action_publish_instruction(self):
        self.ensure_one()
        if not self.instruction_article_id:
            raise UserError("Liga primero el artículo de Knowledge del instructivo.")
        return {
            'type': 'ir.actions.act_window', 'name': "Publicar instructivo",
            'res_model': 'sgi.instruction.publish', 'view_mode': 'form', 'target': 'new',
            'context': {'default_activity_id': self.id,
                        'default_code': self.instruction_id.sgi_code or False},
        }


class DocumentsDocumentArticle(models.Model):
    _inherit = 'documents.document'

    sgi_article_id = fields.Many2one(
        'knowledge.article', string="Artículo de Knowledge", readonly=True, copy=False,
        help="Artículo del que se congeló esta revisión (DOC-5).")


class SgiInstructionPublish(models.TransientModel):
    _name = 'sgi.instruction.publish'
    _description = "Publicar artículo de Knowledge como instructivo (IT)"

    activity_id = fields.Many2one('sgi.process.activity', required=True)
    article_id = fields.Many2one(related='activity_id.instruction_article_id')
    code = fields.Char(string="Clave IT", required=True, help="Ej. IT-P-C11-05.")
    job_ids = fields.Many2many('hr.job', string="Puestos que aplican",
                               compute='_compute_job_ids', store=True, readonly=False,
                               help="Por omisión, los que ejecutan la actividad.")

    @api.depends('activity_id')
    def _compute_job_ids(self):
        for wiz in self:
            if wiz.activity_id and not wiz.job_ids:
                wiz.job_ids = wiz.activity_id.sudo().responsible_job_ids

    def action_publish(self):
        self.ensure_one()
        if not self.env.user.has_group('quimibond_sgi.group_sgi_manager'):
            raise UserError("Solo el Jefe MAST publica instructivos.")
        activity = self.activity_id
        article = activity.instruction_article_id.sudo()
        if not article:
            raise UserError("La actividad no tiene artículo de Knowledge ligado.")
        code = (self.code or '').strip().upper()
        Doc = self.env['documents.document'].sudo()
        previous = Doc.with_context(active_test=False).search([('sgi_code', '=', code)])
        current = previous.filtered(lambda d: d.sgi_state == 'vigente')[:1]
        content_hash = activity._sgi_article_hash()
        if current and current.sgi_content_hash == content_hash:
            raise UserError("El artículo no cambió desde la revisión vigente %s." % current.sgi_revision_label)
        revision = (max(previous.mapped('sgi_revision')) + 1) if previous else 0
        report = self.env.ref('quimibond_sgi.action_report_knowledge_instruction')
        pdf, _ = self.env['ir.actions.report'].sudo()._render_qweb_pdf(report.report_name, article.ids)
        today = fields.Date.context_today(self)
        doc = Doc.create({
            'name': "%s %s (Rev. %02d).pdf" % (code, article.name or '', revision),
            'type': 'binary',
            'datas': base64.b64encode(pdf),
            'mimetype': 'application/pdf',
            'sgi_is_controlled': True,
            'sgi_doc_type': 'instructivo',
            'sgi_code': code,
            'sgi_state': 'vigente',
            'sgi_revision': revision,
            'sgi_issue_date': today,
            'sgi_process_id': activity.process_id.id,
            'sgi_job_ids': [(6, 0, self.job_ids.ids)],
            'sgi_owner_id': self.env.user.id,
            'sgi_content_hash': content_hash,
            'sgi_article_id': article.id,
            'company_id': activity.company_id.id or self.env.company.id,
        })
        doc.action_generate_acks()
        activity.write({'instruction_id': doc.id})
        doc.message_post(body="Instructivo publicado desde el artículo de Knowledge «%s», revisión %02d." % (
            article.name or '', revision))
        return {
            'type': 'ir.actions.act_window', 'res_model': 'documents.document', 'res_id': doc.id,
            'view_mode': 'form', 'views': [(self.env.ref('quimibond_sgi.sgi_document_view_form').id, 'form')],
        }

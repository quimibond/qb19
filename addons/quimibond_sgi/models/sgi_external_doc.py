# -*- coding: utf-8 -*-
"""Documentos externos (E2.36, 56.21.0).

Normas, especificaciones de cliente, NOMs, manuales de proveedor: no los
emite Quimibond pero se controlan. Se registra quién lo emite, su revisión,
cuándo llegó y qué proceso lo aplica; desde la recepción hay 10 días hábiles
para implantarlo (parámetro ``quimibond_sgi.external_doc_days``) y el cron de
vencimientos documentales avisa al dueño del proceso dos días hábiles antes y
cuando ya venció.
"""
from odoo import api, fields, models

from .sgi_calendar import sgi_add_business_days

from .sgi_guard import sgi_require_system


class DocumentsDocumentExternal(models.Model):
    _inherit = 'documents.document'

    sgi_ext_issuer = fields.Char(
        string="Emisor", help="Quién emite el documento (cliente, norma, autoridad, proveedor).")
    sgi_ext_issuer_revision = fields.Char(
        string="Revisión del emisor", help="Revisión o edición como la trae el emisor.")
    sgi_ext_received_date = fields.Date(string="Fecha de recepción")
    sgi_ext_deadline = fields.Date(
        string="Implantar a más tardar", compute='_compute_sgi_ext_deadline', store=True,
        help="Recepción + 10 días hábiles (parámetro quimibond_sgi.external_doc_days).")
    sgi_ext_implemented_date = fields.Date(
        string="Implantado el",
        help="Fecha en que el proceso dueño ya lo aplica (y difundió lo que cambia).")
    sgi_ext_state = fields.Selection([
        ('por_implantar', "Por implantar"),
        ('vencido', "Vencido"),
        ('implantado', "Implantado"),
    ], string="Implantación", compute='_compute_sgi_ext_state', store=True)

    @api.depends('sgi_ext_received_date', 'sgi_doc_type_id.code', 'company_id')
    def _compute_sgi_ext_deadline(self):
        days = int(self.env['ir.config_parameter'].sudo().get_param(
            'quimibond_sgi.external_doc_days', 10) or 10)
        for doc in self:
            doc.sgi_ext_deadline = sgi_add_business_days(
                self.env, doc.sgi_ext_received_date, days, doc.company_id) \
                if doc.sgi_doc_type_id.code == 'externo' and doc.sgi_ext_received_date else False

    @api.depends('sgi_ext_deadline', 'sgi_ext_implemented_date', 'sgi_doc_type_id.code')
    def _compute_sgi_ext_state(self):
        today = fields.Date.context_today(self)
        for doc in self:
            if doc.sgi_doc_type_id.code != 'externo' or not doc.sgi_ext_received_date:
                doc.sgi_ext_state = False
            elif doc.sgi_ext_implemented_date:
                doc.sgi_ext_state = 'implantado'
            else:
                doc.sgi_ext_state = 'vencido' if doc.sgi_ext_deadline and doc.sgi_ext_deadline < today \
                    else 'por_implantar'

    def _sgi_ext_owner_user_id(self):
        self.ensure_one()
        owner = self.sudo().sgi_process_id.owner_id.user_id
        return owner.id or self.env['sgi.cron']._sgi_manager_user_id()


class SgiCronExternalDoc(models.AbstractModel):
    _inherit = 'sgi.cron'

    @api.model
    def cron_documents(self):
        sgi_require_system(self.env)  # F-008
        res = super().cron_documents()
        self._sgi_step("implantación de documentos externos", self._sgi_external_doc_notices)
        return res

    @api.model
    def _sgi_external_doc_notices(self):
        """Aviso al dueño del proceso: 2 días hábiles antes del plazo de
        implantación y al vencer. Una actividad por documento y aviso."""
        Doc = self.env['documents.document'].sudo()
        today = fields.Date.context_today(self)
        soon = sgi_add_business_days(self.env, today, 2)
        pending = Doc.search([('sgi_doc_type_id.code', '=', 'externo'), ('sgi_ext_implemented_date', '=', False),
                              ('sgi_ext_deadline', '!=', False)])
        pending._compute_sgi_ext_state()  # «Vencido» depende del día
        docs = pending.filtered(lambda d: d.sgi_ext_deadline <= soon)
        for doc in docs:
            late = doc.sgi_ext_deadline < today
            summary = "%s documento externo %s" % (
                "Vencido: implantar" if late else "Implantar", doc.sgi_code or doc.name)
            self._sgi_schedule(
                doc, summary,
                "Llegó el %s de %s (rev. %s). Plazo de implantación: %s." % (
                    doc.sgi_ext_received_date, doc.sgi_ext_issuer or "emisor sin capturar",
                    doc.sgi_ext_issuer_revision or "—", doc.sgi_ext_deadline),
                doc._sgi_ext_owner_user_id())
        return len(docs)

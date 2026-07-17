# -*- coding: utf-8 -*-
from dateutil.relativedelta import relativedelta

from odoo import models, fields, api
from odoo.exceptions import ValidationError


class ApprovalCategory(models.Model):
    _inherit = 'approval.category'

    sgi_is_doc_change = fields.Boolean(string="Cambio documental SGI")


class ApprovalRequest(models.Model):
    _inherit = 'approval.request'

    sgi_is_doc_change = fields.Boolean(related='category_id.sgi_is_doc_change', store=True)
    sgi_document_id = fields.Many2one('documents.document', string="Documento afectado")
    sgi_change_kind = fields.Selection([
        ('alta', "Alta"),
        ('modificacion', "Modificación"),
        ('baja', "Baja"),
    ], string="Tipo de cambio")
    sgi_what_changes = fields.Selection([
        ('formato', "Formato"),
        ('contenido', "Contenido"),
    ], string="¿Qué se modifica?")
    sgi_current_revision = fields.Char(related='sgi_document_id.sgi_revision',
                                       string="Revisión vigente", readonly=True)
    sgi_new_revision = fields.Char(string="Nueva revisión")
    sgi_pilot = fields.Boolean(string="Prueba piloto")
    sgi_pilot_start = fields.Date(string="Inicio de piloto")
    sgi_pilot_end = fields.Date(string="Fin de piloto")
    sgi_reason = fields.Text(string="Motivo del cambio")
    sgi_changes = fields.Text(string="Descripción de cambios")
    sgi_affected_process_ids = fields.Many2many('sgi.process', string="Procesos afectados")
    sgi_applied = fields.Boolean(string="Cambio aplicado al documento", copy=False)

    @api.constrains('sgi_is_doc_change', 'sgi_change_kind', 'sgi_document_id')
    def _check_document_required(self):
        for req in self:
            if not req.sgi_is_doc_change:
                continue
            if req.sgi_change_kind in ('modificacion', 'baja') and not req.sgi_document_id:
                raise ValidationError(
                    "Una modificación o baja requiere seleccionar el documento afectado.")

    @api.constrains('sgi_is_doc_change', 'sgi_pilot', 'sgi_pilot_start', 'sgi_pilot_end')
    def _check_pilot(self):
        for req in self:
            if not req.sgi_is_doc_change or not req.sgi_pilot:
                continue
            if req.sgi_pilot_start and req.sgi_pilot_end:
                if req.sgi_pilot_end < req.sgi_pilot_start:
                    raise ValidationError("El fin del piloto no puede ser anterior al inicio.")
                if (req.sgi_pilot_end - req.sgi_pilot_start).days > 90:
                    raise ValidationError("La prueba piloto no puede exceder 90 días naturales.")
            if req.sgi_pilot_start:
                # Máximo 15 días hábiles previos (~21 días naturales) antes de hoy
                min_start = fields.Date.context_today(req) - relativedelta(days=21)
                if req.sgi_pilot_start < min_start:
                    raise ValidationError(
                        "El inicio del piloto no puede ser anterior a 15 días hábiles.")

    def _sgi_apply_doc_change(self):
        """Aplica el efecto del cambio aprobado sobre el documento controlado."""
        self.ensure_one()
        doc = self.sgi_document_id
        today = fields.Date.context_today(self)
        if self.sgi_change_kind == 'modificacion' and doc:
            vals = {
                'sgi_issue_date': today,
                'sgi_next_review_date': today + relativedelta(years=2),
            }
            if self.sgi_new_revision:
                vals['sgi_revision'] = self.sgi_new_revision
            if self.sgi_pilot:
                vals['sgi_state'] = 'piloto'
                vals['sgi_pilot_end_date'] = self.sgi_pilot_end
            else:
                vals['sgi_state'] = 'vigente'
            doc.write(vals)
            doc.message_post(
                body="Cambio documental aprobado (%s): revisión %s, estado %s." % (
                    self.name, vals.get('sgi_revision', doc.sgi_revision), vals['sgi_state']))
            doc.action_generate_acks()
        elif self.sgi_change_kind == 'baja' and doc:
            doc.write({'sgi_state': 'obsoleto'})
            doc.message_post(body="Documento dado de baja por solicitud aprobada %s." % self.name)
        elif self.sgi_change_kind == 'alta':
            manager = self.env.ref('quimibond_sgi.group_sgi_manager', raise_if_not_found=False)
            manager_user = manager and manager.all_user_ids[:1]
            self.activity_schedule(
                'mail.mail_activity_data_todo',
                summary="Crear documento SGI dado de alta (%s)" % (self.name or ''),
                note=self.sgi_changes or self.sgi_reason or '',
                user_id=(manager_user.id if manager_user else self.env.user.id),
            )
        self.sgi_applied = True

    def action_approve(self, approver=None):
        res = super().action_approve(approver=approver)
        for req in self:
            if req.sgi_is_doc_change and req.request_status == 'approved' and not req.sgi_applied:
                req._sgi_apply_doc_change()
        return res

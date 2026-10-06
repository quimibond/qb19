# -*- coding: utf-8 -*-
"""57.116.0: menos categorías en Aprobaciones.

Cada rol «Aprueba» como solicitud creaba su propia categoría (24 activas el
2026-10-06, 18 del SGI y ninguna con solicitudes). Ahora varias aprobaciones
comparten una categoría por área («Autorizaciones de Contabilidad»,
«Autorizaciones de Finanzas»…) y quien pide elige el **asunto**. Cada asunto
apunta a su rol «Aprueba»: pone como aprobadores a las personas de ese puesto
y deja medir cada actividad por separado (``sgi_subject_id.role_id``).
"""
from odoo import Command, api, fields, models
from odoo.exceptions import UserError


class SgiApprovalSubject(models.Model):
    _name = 'sgi.approval.subject'
    _description = "Asunto de una categoría de Aprobaciones"
    _order = 'category_id, sequence, name'

    name = fields.Char(string="Asunto", required=True,
                       help="Lo que se pide, en pocas palabras: «Propuesta de pago semanal».")
    sequence = fields.Integer(default=10)
    category_id = fields.Many2one('approval.category', string="Categoría", required=True,
                                  index=True, ondelete='cascade')
    role_id = fields.Many2one(
        'sgi.activity.role', string="Aprobación del SGI", index=True, ondelete='set null',
        domain="[('role', '=', 'aprueba')]",
        help="Rol «Aprueba» de la actividad: sus personas aprueban la solicitud y la "
             "medición de la actividad cuenta las solicitudes de este asunto.")
    activity_id = fields.Many2one(related='role_id.activity_id', string="Actividad")
    active = fields.Boolean(default=True)


class ApprovalCategorySubjects(models.Model):
    _inherit = 'approval.category'

    sgi_subject_ids = fields.One2many('sgi.approval.subject', 'category_id', string="Asuntos")


class ApprovalRequestSubject(models.Model):
    _inherit = 'approval.request'

    sgi_subject_id = fields.Many2one(
        'sgi.approval.subject', string="Asunto", index=True, ondelete='restrict', copy=True,
        domain="[('category_id', '=', category_id)]",
        help="Qué se pide. Define quién lo aprueba.")
    sgi_has_subjects = fields.Boolean(compute='_compute_sgi_has_subjects')

    @api.depends('category_id')
    def _compute_sgi_has_subjects(self):
        for request in self:
            request.sgi_has_subjects = bool(request.category_id.sudo().sgi_subject_ids)

    def _sgi_apply_subject_approvers(self):
        """Los aprobadores salen del asunto: las personas del puesto del rol."""
        for request in self.filtered(lambda r: r.sgi_subject_id.role_id and r.request_status == 'new'):
            users = request.sgi_subject_id.role_id.sudo()._sgi_approver_users()
            if users:
                request.approver_ids = [Command.clear()] + [
                    Command.create({'user_id': user.id, 'required': False}) for user in users]

    @api.onchange('sgi_subject_id')
    def _onchange_sgi_subject_id(self):
        self._sgi_apply_subject_approvers()

    @api.model_create_multi
    def create(self, vals_list):
        requests = super().create(vals_list)
        requests.filtered('sgi_subject_id')._sgi_apply_subject_approvers()
        return requests

    def write(self, vals):
        res = super().write(vals)
        if 'sgi_subject_id' in vals:
            self._sgi_apply_subject_approvers()
        return res

    def action_confirm(self):
        missing = self.filtered(lambda r: r.sgi_has_subjects and not r.sgi_subject_id)
        if missing:
            raise UserError("Elija el asunto de la solicitud: dice qué se pide y quién lo aprueba.")
        return super().action_confirm()

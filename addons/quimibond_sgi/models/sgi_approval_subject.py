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

    def _sgi_approval_role(self):
        """El rol «Aprueba» que manda en esta solicitud: el del asunto o, sin
        asunto, el de la categoría propia del rol (57.143.0)."""
        self.ensure_one()
        return self.sgi_subject_id.role_id or self.category_id.sudo().sgi_role_id

    def _sgi_apply_subject_approvers(self):
        """Los aprobadores salen del rol: las personas del puesto y de su
        suplente, sin quien pide la solicitud (57.143.0: «nadie aprueba lo que
        él mismo pidió»; si el titular la pide, queda el suplente y al revés).
        Si no queda nadie, la lista se vacía y «Confirmar» dice por qué."""
        for request in self.filtered(lambda r: r.request_status == 'new'):
            role = request._sgi_approval_role()
            if not role:
                continue
            users = role.sudo()._sgi_approver_users(record=request)
            if request.sgi_subject_id:
                request.approver_ids = [Command.clear()] + [
                    Command.create({'user_id': user.id, 'required': False}) for user in users]
                continue
            # Categoría propia de un rol, sin asunto: solo se quita a quien pide y se
            # suma el suplente; los aprobadores que otros flujos agregan se respetan.
            owner = request.request_owner_id
            request.approver_ids = request.approver_ids.filtered(lambda a: a.user_id != owner)
            missing = users - request.approver_ids.user_id
            if missing:
                request.approver_ids = [
                    Command.create({'user_id': user.id, 'required': False}) for user in missing]

    def _sgi_check_requester_not_approver(self):
        """UserError si la solicitud se quedó sin aprobadores porque quien la
        pide es el único que aprueba (o el rol no tiene personas)."""
        for request in self:
            role = request._sgi_approval_role()
            if not role or request.approver_ids:
                continue
            conflict = role.sudo()._sgi_requester_conflict(request, request.request_owner_id)
            if conflict:
                raise UserError(conflict)
            raise UserError("La solicitud «%s» no tiene quién la apruebe: el rol «Aprueba» de %s no resuelve "
                            "a nadie (sin personas en el puesto ni suplente, o el jefe del que pide no está "
                            "en Empleados)." % (request.name, role.activity_id.sudo().display_name))

    @api.onchange('sgi_subject_id', 'request_owner_id')
    def _onchange_sgi_subject_id(self):
        self._sgi_apply_subject_approvers()

    @api.model_create_multi
    def create(self, vals_list):
        requests = super().create(vals_list)
        requests.filtered(lambda r: r._sgi_approval_role())._sgi_apply_subject_approvers()
        return requests

    def write(self, vals):
        res = super().write(vals)
        if {'sgi_subject_id', 'request_owner_id', 'category_id'} & set(vals):
            self._sgi_apply_subject_approvers()
        return res

    def action_confirm(self):
        missing = self.filtered(lambda r: r.sgi_has_subjects and not r.sgi_subject_id)
        if missing:
            raise UserError("Elija el asunto de la solicitud: dice qué se pide y quién lo aprueba.")
        self._sgi_check_requester_not_approver()
        return super().action_confirm()

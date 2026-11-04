# -*- coding: utf-8 -*-
"""57.197.0 (Dirección General 2026-11-04): cada botón de aprobación del desarrollo lo ve quien lo aprueba.

El 3-nov Diseño presionó «Aprobar solicitud de desarrollo» (lo aprueba Dirección de Operaciones) y Studio pidió la
aprobación a sus aprobadores; el 4-nov presionó «Asignar folio FT» en dos proyectos en Cotización y se gastaron dos
folios. Ahora:

- «Aprobar solicitud de desarrollo» (C1.07) y «Aprobar análisis y factibilidad» (C1.02 / C1.03) solo se ven para
  las personas que hoy aprueban ese botón según el SGI (renglón «Aprueba» de la actividad: puesto, familia o
  suplente, ``sgi.activity.role.approval_user_ids``). Sin renglón configurado, los ve cualquiera, como antes.
- «Asignar folio FT» solo se ve para quien aprueba alguno de esos dos botones (Administración de Ventas y
  Dirección de Operaciones), Dirección del SGI y administradores; y en la vista solo de Muestra en adelante, con
  confirmación. El folio de Cotización sale solo con la «Aprobación para iniciar».

Es una barrera de pantalla para evitar el clic equivocado: los métodos no cambian, porque el sistema también los
llama solo (el folio al pasar a Muestra o al generar la aprobación para iniciar).
"""
from odoo import api, fields, models

# Botón del proyecto → método que aprueba el renglón «Aprueba» del SGI.
METHOD_APPROVE_REQUEST = 'action_sgi_dev_approve_request'   # C1.07, Dirección de Operaciones
METHOD_REVIEW_APPROVE = 'action_sgi_dev_review_approve'     # C1.02 / C1.03, Administración de Ventas


class ProjectProjectDevButtons(models.Model):
    _inherit = 'project.project'

    sgi_dev_can_approve_request = fields.Boolean(
        string="Puede aprobar la solicitud de desarrollo", compute='_compute_sgi_dev_button_access',
        help="Quien aprueba C1.07 según el SGI (o cualquiera si el SGI no lo tiene configurado).")
    sgi_dev_can_review_approve = fields.Boolean(
        string="Puede aprobar análisis y factibilidad", compute='_compute_sgi_dev_button_access',
        help="Quien aprueba C1.02 / C1.03 según el SGI (o cualquiera si el SGI no lo tiene configurado).")
    sgi_dev_can_assign_folio = fields.Boolean(
        string="Puede asignar folio FT", compute='_compute_sgi_dev_button_access',
        help="Administración de Ventas y Dirección de Operaciones (quienes aprueban los botones del desarrollo), "
             "Dirección del SGI y administradores.")

    @api.model
    def _sgi_dev_button_approvers(self, method):
        """Usuarios que hoy aprueban el botón ``method`` del proyecto según el SGI; ``None`` si ningún renglón
        «Aprueba» activo lo tiene (entonces no se restringe)."""
        roles = self.env['sgi.activity.role'].sudo().search([
            ('role', '=', 'aprueba'), ('approval_kind', '=', 'boton'), ('approval_method', '=', method),
            ('approval_model_id.model', '=', 'project.project')])
        roles = roles.filtered(lambda r: not r.activity_id or 'active' not in r.activity_id._fields
                               or r.activity_id.active)
        if not roles:
            return None
        return roles.approval_user_ids

    @api.depends_context('uid')
    def _compute_sgi_dev_button_access(self):
        user = self.env.user
        privileged = user.has_group('base.group_system') or user.has_group('quimibond_sgi.group_sgi_director')
        request_users = self._sgi_dev_button_approvers(METHOD_APPROVE_REQUEST)
        review_users = self._sgi_dev_button_approvers(METHOD_REVIEW_APPROVE)
        can_request = request_users is None or user in request_users
        can_review = review_users is None or user in review_users
        if request_users is None and review_users is None:
            can_folio = True
        else:
            can_folio = privileged or user in (request_users or user.browse()) | (review_users or user.browse())
        for project in self:
            project.sgi_dev_can_approve_request = can_request
            project.sgi_dev_can_review_approve = can_review
            project.sgi_dev_can_assign_folio = can_folio

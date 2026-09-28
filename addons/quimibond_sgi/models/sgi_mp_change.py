# -*- coding: utf-8 -*-
"""56.2.0: proponer cambios a «Mi procedimiento» desde la pantalla.

Cada tarjeta de actividad lleva «Proponer cambio» y el encabezado «Proponer
nueva actividad». El asistente arma una solicitud de Aprobaciones en la
categoría «Proponer cambio a mi procedimiento (SGI)» con el texto vigente ya
escrito, la propuesta, el motivo y un adjunto opcional. Al aprobarse, el Jefe
MAST y SGI recibe una actividad «Aplicar cambio aprobado y republicar» en la
actividad del procedimiento (o en el proceso, si es una actividad nueva).
"""
from markupsafe import Markup, escape

from odoo import api, fields, models
from odoo.exceptions import UserError

MP_CHANGE_TYPES = [
    ('agregar', "Agregar actividad"),
    ('cambiar', "Cambiar esta actividad"),
    ('quitar', "Quitar esta actividad"),
]
MP_APPLY_SUMMARY = "Aplicar cambio aprobado y republicar"


class ApprovalCategoryMpChange(models.Model):
    _inherit = 'approval.category'

    sgi_is_mp_change = fields.Boolean(
        string="Cambio a «Mi procedimiento» (SGI)",
        help="Categoría que usa el botón «Proponer cambio» de Mi procedimiento.")

    @api.model
    def _sgi_mp_change_category(self):
        category = self.sudo().search([('sgi_is_mp_change', '=', True)], limit=1)
        if not category:
            category = self.sudo().search(
                [('name', 'ilike', 'Proponer cambio a mi procedimiento')], limit=1)
        return category


class ApprovalRequestMpChange(models.Model):
    _inherit = 'approval.request'

    sgi_activity_id = fields.Many2one(
        'sgi.process.activity', string="Actividad del procedimiento", index='btree_not_null',
        help="Actividad de «Mi procedimiento» a la que se propone el cambio.")
    sgi_mp_change_type = fields.Selection(MP_CHANGE_TYPES, string="Tipo de propuesta")
    sgi_mp_apply_scheduled = fields.Boolean(
        string="Aplicación agendada a MAST", readonly=True, copy=False)

    def action_approve(self, approver=None):
        res = super().action_approve(approver=approver)
        self.filtered(lambda r: r.request_status == 'approved' and r.sgi_mp_change_type
                      and not r.sgi_mp_apply_scheduled)._sgi_mp_schedule_apply()
        return res

    def _sgi_mp_apply_users(self):
        group = self.env.ref('quimibond_sgi.group_sgi_manager')
        users = group.sudo().user_ids.filtered(lambda u: u.active and not u.share)
        return users or group.sudo().all_user_ids.filtered(lambda u: u.active and not u.share)

    def _sgi_mp_schedule_apply(self):
        """Una actividad «Aplicar cambio aprobado y republicar» por cada Jefe
        MAST y SGI, en la actividad del procedimiento (o en el proceso)."""
        users = self._sgi_mp_apply_users()
        for req in self:
            target = req.sgi_activity_id or req.sgi_affected_process_ids[:1]
            if target and users:
                note = Markup("%s <b>%s</b> (%s): %s") % (
                    "Solicitud aprobada", req.name or '', req.reference or '',
                    dict(MP_CHANGE_TYPES).get(req.sgi_mp_change_type, ''))
                for user in users:
                    target.sudo().activity_schedule(
                        'mail.mail_activity_data_todo', user_id=user.id,
                        summary=MP_APPLY_SUMMARY, note=note)
            req.sudo().sgi_mp_apply_scheduled = True
            req.sudo().message_post(author_id=self.env.user.partner_id.id, body="Aprobada: se agendó «%s» a %s." % (
                MP_APPLY_SUMMARY, ", ".join(users.mapped('name')) or "nadie (sin Jefe MAST y SGI)"))


class SgiProcessActivityMpChange(models.Model):
    """La actividad del procedimiento recibe actividades de seguimiento (la de
    aplicar el cambio aprobado) y las muestra en su chatter."""
    _name = 'sgi.process.activity'
    _inherit = ['sgi.process.activity', 'mail.thread', 'mail.activity.mixin']

    sgi_mp_change_ids = fields.One2many(
        'approval.request', 'sgi_activity_id', string="Propuestas de cambio")

    def action_sgi_mp_propose_change(self):
        self.ensure_one()
        return self.env['sgi.mp.change.wizard']._sgi_open(activity=self)


class SgiActivityRoleMpChange(models.Model):
    _inherit = 'sgi.activity.role'

    def action_mp_propose_change(self):
        self.ensure_one()
        return self.env['sgi.mp.change.wizard']._sgi_open(activity=self.activity_id, role=self)


class SgiMyProcedureMpChange(models.TransientModel):
    _inherit = 'sgi.my.procedure'

    no_employee = fields.Boolean(
        string="Usuario sin empleado", compute='_compute_no_employee',
        help="El usuario no está ligado a un empleado y no eligió a nadie: la "
             "pantalla muestra el aviso en vez de quedar vacía.")

    @api.depends('employee_id', 'job_id')
    @api.depends_context('uid')
    def _compute_no_employee(self):
        linked = bool(self.env.user.employee_id)
        for wiz in self:
            wiz.no_employee = not linked and not wiz.employee_id and not wiz.job_id

    def action_propose_new_activity(self):
        self.ensure_one()
        return self.env['sgi.mp.change.wizard']._sgi_open(
            processes=self.process_ids, job=self._sgi_mp_job())


class SgiMpChangeWizard(models.TransientModel):
    _name = 'sgi.mp.change.wizard'
    _description = "Proponer cambio a mi procedimiento"

    change_type = fields.Selection(MP_CHANGE_TYPES, string="Qué propones", required=True, default='cambiar')
    activity_id = fields.Many2one('sgi.process.activity', string="Actividad", readonly=True)
    role_id = fields.Many2one('sgi.activity.role', string="Mi rol", readonly=True)
    process_id = fields.Many2one('sgi.process', string="Proceso")
    allowed_process_ids = fields.Many2many('sgi.process', string="Procesos del puesto")
    job_id = fields.Many2one('hr.job', string="Puesto", readonly=True)
    current_text = fields.Text(string="Cómo está hoy", readonly=True)
    proposal = fields.Text(string="Cómo propones que quede", required=True)
    reason = fields.Text(string="Por qué", required=True)
    attachment = fields.Binary(string="Adjunto", attachment=False)
    attachment_name = fields.Char(string="Nombre del adjunto")

    @api.model
    def _sgi_current_text(self, activity, role=None):
        role = role or activity.role_ids[:1]
        if not role:
            return activity.name or ''
        role = role.sudo()
        pieces = [
            ("Actividad", "%s %s" % (role.mp_number or '', role.mp_name or '')),
            ("Cuándo", role.activity_when),
            ("Cómo", role.mp_how), ("Dónde", role.mp_where),
            ("Contra qué se revisa", role.mp_check_against), ("Terminada cuando", role.mp_done),
            ("Si no se puede", role.mp_on_fail), ("Recibe", role.mp_inputs),
            ("Entrega", role.mp_outputs), ("Conforme a", role.mp_related),
            ("Si se atora, escala a", role.mp_escalates),
        ]
        return "\n".join("%s: %s" % (label, str(value).strip()) for label, value in pieces
                         if value and str(value).strip())

    @api.model
    def _sgi_open(self, activity=None, role=None, processes=None, job=None):
        vals = {'change_type': 'cambiar' if activity else 'agregar'}
        if activity:
            activity = activity.sudo()
            vals.update({
                'activity_id': activity.id,
                'role_id': role.id if role else False,
                'process_id': activity.process_id.id,
                'current_text': self._sgi_current_text(activity, role),
            })
        else:
            processes = (processes or self.env['sgi.process']).sudo()
            vals.update({
                'allowed_process_ids': [(6, 0, processes.ids)],
                'process_id': processes[:1].id if len(processes) == 1 else False,
                'job_id': job.id if job else False,
            })
        wizard = self.create(vals)
        return {
            'type': 'ir.actions.act_window',
            'name': "Proponer cambio a mi procedimiento" if activity else "Proponer nueva actividad",
            'res_model': self._name, 'res_id': wizard.id,
            'view_mode': 'form', 'target': 'new',
        }

    def _sgi_reference(self):
        process = self.process_id.sudo()
        head = process.code or process.name or ''
        if self.activity_id:
            activity = self.activity_id.sudo()
            number = activity.number or activity.legacy_number or ''
            return ("%s / %s %s" % (head, number, activity.name or '')).replace('  ', ' ').strip()
        return "%s / Nueva actividad" % head

    def _sgi_reason_html(self):
        rows = [
            ("Tipo", dict(MP_CHANGE_TYPES).get(self.change_type, '')),
            ("Puesto", self.job_id.sudo().name or ''),
            ("Cómo está hoy", self.current_text or ''),
            ("Cómo propongo que quede", self.proposal or ''),
            ("Por qué", self.reason or ''),
        ]
        body = Markup('').join(
            Markup("<p><b>%s</b><br/>%s</p>") % (label, Markup('<br/>').join(
                escape(line) for line in value.splitlines()))
            for label, value in rows if value)
        return body

    def action_submit(self):
        self.ensure_one()
        if self.change_type in ('cambiar', 'quitar') and not self.activity_id:
            raise UserError("Para cambiar o quitar, abre la propuesta desde la tarjeta de la actividad.")
        if self.change_type == 'agregar' and not self.process_id:
            raise UserError("Elige el proceso donde va la actividad nueva.")
        category = self.env['approval.category']._sgi_mp_change_category()
        if not category:
            raise UserError("No existe la categoría de Aprobaciones «Proponer cambio a mi "
                            "procedimiento (SGI)». Pide al Jefe MAST y SGI que la cree.")
        title = {
            'agregar': "Agregar actividad",
            'cambiar': "Cambiar actividad",
            'quitar': "Quitar actividad",
        }[self.change_type]
        request = self.env['approval.request'].create({
            'name': "%s — %s" % (title, self._sgi_reference()),
            'category_id': category.id,
            'request_owner_id': self.env.user.id,
            'reference': self._sgi_reference(),
            'reason': self._sgi_reason_html(),
            'sgi_activity_id': self.activity_id.id,
            'sgi_mp_change_type': self.change_type,
            'sgi_affected_process_ids': [(6, 0, self.process_id.ids)],
        })
        if self.attachment:
            self.env['ir.attachment'].create({
                'name': self.attachment_name or "Adjunto de la propuesta",
                'datas': self.attachment,
                'res_model': 'approval.request', 'res_id': request.id,
            })
        try:
            with self.env.cr.savepoint():
                request.action_confirm()
        except UserError as error:
            request.message_post(body="La propuesta quedó en borrador: %s" % error)
        return {
            'type': 'ir.actions.act_window', 'name': "Mi propuesta",
            'res_model': 'approval.request', 'res_id': request.id,
            'view_mode': 'form', 'target': 'current',
        }

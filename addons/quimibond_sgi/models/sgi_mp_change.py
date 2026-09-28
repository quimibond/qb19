# -*- coding: utf-8 -*-
"""Proponer cambios a «Mi procedimiento» editando la actividad de verdad.

56.2.0 abrió la propuesta como texto libre; 56.4.0 la vuelve una propuesta
ESTRUCTURADA (`sgi.activity.change`): la persona edita los mismos campos de
la actividad (qué, cómo, instructivo y formatos, cuándo, dónde en Odoo,
terminado cuando, si no se puede, y quién la ejecuta, aprueba, participa o
escala), la solicitud de Aprobaciones lleva el antes → después campo por
campo, y al quedar aprobada el cambio se APLICA solo a la actividad (o se
crea la actividad nueva, o se archiva la que se quita). El Jefe MAST y SGI
recibe «Revisar cambio aplicado y republicar» para publicar la revisión del
procedimiento.
"""
from markupsafe import Markup, escape

from odoo import Command, api, fields, models
from odoo.exceptions import UserError

MP_CHANGE_TYPES = [
    ('agregar', "Agregar actividad"),
    ('cambiar', "Cambiar esta actividad"),
    ('quitar', "Quitar esta actividad"),
]
MP_APPLY_SUMMARY = "Revisar cambio aplicado y republicar"
# Campos de la actividad que se pueden proponer, en el orden de la ficha.
MP_FIELDS = (
    'name', 'description', 'how_steps', 'instruction_id', 'format_document_ids',
    'related_procedure_id', 'measure_cadence', 'due_weekday', 'due_business_day',
    'due_month', 'due_day',
    'exec_channel', 'odoo_menu_id', 'external_system', 'place_note',
    'check_against', 'done_criteria', 'on_fail',
)
ROLE_KEYS = ('role', 'target_type', 'job_id', 'family_id', 'relative_role', 'after_days', 'condition')


def _activity_selection(field_name):
    def selection(self):
        return self.env['sgi.process.activity']._fields[field_name].selection
    return selection


def _role_selection(field_name):
    def selection(self):
        return self.env['sgi.activity.role']._fields[field_name].selection
    return selection


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
    sgi_mp_proposal_id = fields.Many2one(
        'sgi.activity.change', string="Propuesta", readonly=True, copy=False, index='btree_not_null')
    sgi_mp_diff_html = fields.Html(
        related='sgi_mp_proposal_id.diff_snapshot', string="Qué cambia", sanitize=False)
    sgi_mp_apply_scheduled = fields.Boolean(
        string="Cambio aplicado", readonly=True, copy=False)

    def action_approve(self, approver=None):
        res = super().action_approve(approver=approver)
        self.filtered(lambda r: r.request_status == 'approved' and r.sgi_mp_change_type
                      and not r.sgi_mp_apply_scheduled)._sgi_mp_apply()
        return res

    def _sgi_mp_apply_users(self):
        group = self.env.ref('quimibond_sgi.group_sgi_manager')
        users = group.sudo().user_ids.filtered(lambda u: u.active and not u.share)
        return users or group.sudo().all_user_ids.filtered(lambda u: u.active and not u.share)

    def _sgi_mp_apply(self):
        """Aplica la propuesta aprobada a la actividad y agenda a cada Jefe
        MAST y SGI revisar y republicar el procedimiento."""
        users = self._sgi_mp_apply_users()
        for req in self:
            proposal = req.sgi_mp_proposal_id.sudo()
            target = proposal._sgi_apply() if proposal else (
                req.sgi_activity_id or req.sgi_affected_process_ids[:1])
            if proposal and target and target._name == 'sgi.process.activity':
                req.sudo().sgi_activity_id = target
            if target and users:
                note = Markup("%s <b>%s</b> (%s)") % (
                    "Cambio aprobado y aplicado", req.name or '', req.reference or '')
                for user in users:
                    target.sudo().activity_schedule(
                        'mail.mail_activity_data_todo', user_id=user.id,
                        summary=MP_APPLY_SUMMARY, note=note)
            req.sudo().sgi_mp_apply_scheduled = True
            req.sudo().message_post(
                author_id=self.env.user.partner_id.id,
                body=Markup("Aprobada y aplicada a la actividad. Se agendó «%s» a %s.") % (
                    MP_APPLY_SUMMARY, ", ".join(users.mapped('name')) or "nadie (sin Jefe MAST y SGI)"))


class SgiProcessActivityMpChange(models.Model):
    """La actividad del procedimiento recibe actividades de seguimiento y
    guarda en su chatter los cambios aprobados."""
    _name = 'sgi.process.activity'
    _inherit = ['sgi.process.activity', 'mail.thread', 'mail.activity.mixin']

    sgi_mp_change_ids = fields.One2many(
        'approval.request', 'sgi_activity_id', string="Propuestas de cambio")

    def action_sgi_mp_propose_change(self):
        self.ensure_one()
        return self.env['sgi.activity.change']._sgi_open(activity=self)


class SgiActivityRoleMpChange(models.Model):
    _inherit = 'sgi.activity.role'

    def action_mp_propose_change(self):
        self.ensure_one()
        return self.env['sgi.activity.change']._sgi_open(activity=self.activity_id)


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
        return self.env['sgi.activity.change']._sgi_open(
            processes=self.process_ids, job=self._sgi_mp_job())


class SgiActivityChange(models.Model):
    """Propuesta de cambio a una actividad: los mismos campos de la actividad
    con los valores propuestos, más quién la hace. Nace con los valores de hoy;
    lo que la persona cambie es lo que se aprueba y se aplica."""
    _name = 'sgi.activity.change'
    _description = "Propuesta de cambio a una actividad (Mi procedimiento)"
    _order = 'id desc'
    _rec_name = 'display_title'

    state = fields.Selection([
        ('borrador', "Borrador"),
        ('enviada', "Enviada"),
        ('aplicada', "Aplicada"),
    ], string="Estado", default='borrador', required=True, readonly=True)
    change_type = fields.Selection(MP_CHANGE_TYPES, string="Qué propones", required=True, default='cambiar')
    activity_id = fields.Many2one('sgi.process.activity', string="Actividad", readonly=True, index=True)

    # 56.7.0: lo aprobado es lo que se aplica. Enviada la propuesta, solo MAST
    # (o el sistema, al aprobar) la toca; en borrador, solo quien la hizo.
    def _sgi_check_editable(self):
        if self.env.su or self.env.user.has_group('quimibond_sgi.group_sgi_manager'):
            return
        for change in self:
            if change.state != 'borrador':
                raise UserError("La propuesta «%s» ya se envió y no se puede cambiar; si hace falta, "
                                "haz otra." % (change.display_title or change.name or ''))
            if change.create_uid and change.create_uid != self.env.user:
                raise UserError("Solo quien hizo la propuesta puede cambiarla.")

    def write(self, vals):
        self._sgi_check_editable()
        return super().write(vals)

    def unlink(self):
        self._sgi_check_editable()
        return super().unlink()
    process_id = fields.Many2one('sgi.process', string="Proceso")
    allowed_process_ids = fields.Many2many(
        'sgi.process', 'sgi_activity_change_allowed_process_rel', 'change_id', 'process_id',
        string="Procesos del puesto")
    job_id = fields.Many2one('hr.job', string="Puesto de quien propone", readonly=True)
    request_id = fields.Many2one('approval.request', string="Solicitud", readonly=True, copy=False)
    reason = fields.Text(string="Por qué")
    attachment = fields.Binary(string="Adjunto", attachment=True)
    attachment_name = fields.Char(string="Nombre del adjunto")
    display_title = fields.Char(compute='_compute_display_title')
    # --- Los campos de la actividad, con los valores propuestos ---
    name = fields.Char(string="Resumen")
    description = fields.Text(string="Descripción")
    how_steps = fields.Text(string="Cómo (pasos)")
    instruction_id = fields.Many2one(
        'documents.document', string="Instructivo", domain=[('sgi_doc_type', '=', 'instructivo')])
    format_document_ids = fields.Many2many(
        'documents.document', 'sgi_activity_change_format_rel', 'change_id', 'document_id',
        string="Formatos referenciados", domain=[('sgi_is_controlled', '=', True)])
    related_procedure_id = fields.Many2one(
        'documents.document', string="Procedimiento relacionado",
        domain=[('sgi_doc_type', '=', 'procedimiento')])
    measure_cadence = fields.Selection(_activity_selection('measure_cadence'), string="Cadencia esperada")
    due_weekday = fields.Selection(_activity_selection('due_weekday'), string="Vence el (semanal)")
    due_business_day = fields.Integer(string="Vence el día hábil (mensual)")
    due_month = fields.Selection(_activity_selection('due_month'), string="Vence en el mes")
    due_day = fields.Integer(string="Vence el día")
    exec_channel = fields.Selection(_activity_selection('exec_channel'), string="Dónde se hace")
    odoo_menu_id = fields.Many2one('ir.ui.menu', string="Menú de Odoo")
    external_system = fields.Char(string="Sistema externo")
    place_note = fields.Char(string="Lugar")
    check_against = fields.Char(string="Contra qué se compara")
    done_criteria = fields.Text(string="Criterio de terminado")
    on_fail = fields.Text(string="Si no se puede cumplir")
    role_line_ids = fields.One2many('sgi.activity.change.role', 'change_id', string="Quién la hace")
    diff_html = fields.Html(string="Qué cambia", compute='_compute_diff_html', sanitize=False)
    # Lo que se aprobó, congelado al enviar: después de aplicarse, la actividad
    # ya es igual a la propuesta y el diferencial en vivo saldría vacío.
    diff_snapshot = fields.Html(string="Cambio enviado", readonly=True, sanitize=False, copy=False)

    @api.depends('change_type', 'activity_id', 'name', 'process_id')
    def _compute_display_title(self):
        for rec in self:
            label = dict(MP_CHANGE_TYPES).get(rec.change_type, '')
            rec.display_title = "%s — %s" % (label, rec._sgi_reference())

    # ------------------------------------------------------------------
    @api.model
    def _sgi_activity_vals(self, activity):
        vals = {}
        for fname in MP_FIELDS:
            value = activity[fname]
            field = activity._fields[fname]
            if field.type == 'many2one':
                vals[fname] = value.id
            elif field.type == 'many2many':
                vals[fname] = [(6, 0, value.ids)]
            else:
                vals[fname] = value
        vals['role_line_ids'] = [(0, 0, {
            'role': r.role, 'target_type': r.target_type, 'job_id': r.job_id.id,
            'family_id': r.family_id.id, 'relative_role': r.relative_role,
            'after_days': r.after_days, 'condition': r.condition, 'sequence': r.sequence,
        }) for r in activity.role_ids]
        return vals

    @api.model
    def _sgi_open(self, activity=None, processes=None, job=None):
        if activity:
            activity = activity.sudo()
            vals = dict(self._sgi_activity_vals(activity), change_type='cambiar',
                        activity_id=activity.id, process_id=activity.process_id.id)
        else:
            processes = (processes or self.env['sgi.process']).sudo()
            vals = {
                'change_type': 'agregar',
                'allowed_process_ids': [(6, 0, processes.ids)],
                'process_id': processes[:1].id if len(processes) == 1 else False,
                'measure_cadence': 'evento',
                'role_line_ids': [(0, 0, {'role': 'ejecuta', 'target_type': 'job', 'job_id': job.id})]
                if job else [],
            }
        if job:
            vals['job_id'] = job.id
        proposal = self.create(vals)
        return {
            'type': 'ir.actions.act_window',
            'name': "Proponer cambio a mi procedimiento" if activity else "Proponer nueva actividad",
            'res_model': self._name, 'res_id': proposal.id,
            'view_mode': 'form', 'target': 'new',
        }

    @api.model
    def action_sgi_new_from_context(self):
        """«Nuevo» del kanban de Mis actividades: la propuesta de actividad
        nueva con el puesto y los procesos que trae el contexto."""
        ctx = self.env.context
        job = self.env['hr.job'].browse(ctx.get('sgi_mp_job_id') or []).exists()
        processes = self.env['sgi.process'].browse(ctx.get('sgi_mp_process_ids') or []).exists()
        if not job:
            job = self.env.user.employee_id.sudo().job_id
        if job and not processes:
            processes = job.sudo()._sgi_mp_role_lists()['detail'].activity_id.process_id
        return self._sgi_open(processes=processes, job=job or None)

    def _sgi_reference(self):
        self.ensure_one()
        process = self.process_id.sudo()
        head = process.code or process.name or ''
        if self.activity_id:
            activity = self.activity_id.sudo()
            number = activity.number or activity.legacy_number or ''
            return " ".join(("%s / %s %s" % (head, number, activity.name or '')).split())
        return "%s / Nueva actividad%s" % (head, (": %s" % self.name) if self.name else '')

    # ------------------------------------------------------------------
    # Antes → después
    # ------------------------------------------------------------------
    @api.model
    def _sgi_display(self, record, fname):
        field = record._fields[fname]
        value = record[fname]
        if field.type == 'many2one':
            return value.display_name or ''
        if field.type == 'many2many':
            return ", ".join(value.mapped('display_name'))
        if field.type == 'selection':
            return dict(field._description_selection(record.env)).get(value, '') if value else ''
        if field.type == 'integer':
            return str(value) if value else ''
        return value or ''

    def _sgi_role_labels(self, roles):
        labels = []
        role_names = dict(self.env['sgi.activity.role']._fields['role']._description_selection(self.env))
        for r in roles:
            target = r.job_id.name or r.family_id.name or dict(
                self.env['sgi.activity.role']._fields['relative_role']._description_selection(
                    self.env)).get(r.relative_role, '') or ''
            extra = " (a los %d días hábiles)" % r.after_days if r.role == 'escala' and r.after_days else ''
            cond = " si %s" % r.condition if r.condition else ''
            labels.append("%s: %s%s%s" % (role_names.get(r.role, r.role), target, extra, cond))
        return labels

    def _sgi_changes(self):
        """[(etiqueta, antes, después)] de lo que cambia."""
        self.ensure_one()
        activity = self.activity_id.sudo()
        rows = []
        for fname in MP_FIELDS:
            label = self._fields[fname].string
            after = self._sgi_display(self.sudo(), fname)
            before = self._sgi_display(activity, fname) if activity else ''
            if before != after:
                rows.append((label, before, after))
        before_roles = sorted(self._sgi_role_labels(activity.role_ids)) if activity else []
        after_roles = sorted(self._sgi_role_labels(self.sudo().role_line_ids))
        if before_roles != after_roles:
            rows.append(("Quién la hace", "\n".join(before_roles), "\n".join(after_roles)))
        return rows

    @api.depends(*MP_FIELDS, 'role_line_ids', 'role_line_ids.role', 'role_line_ids.job_id',
                 'role_line_ids.family_id', 'role_line_ids.relative_role',
                 'role_line_ids.after_days', 'role_line_ids.condition', 'change_type', 'reason')
    def _compute_diff_html(self):
        def cell(text):
            return Markup('<br/>').join(escape(line) for line in (text or '').splitlines()) or Markup(
                '<span class="text-muted">—</span>')
        for rec in self:
            if rec.change_type == 'quitar':
                body = Markup("<p><b>Quitar la actividad</b> %s (se archiva, no se borra).</p>") % (
                    rec._sgi_reference())
            else:
                rows = rec._sgi_changes()
                if not rows:
                    body = Markup('<p class="text-muted">Sin cambios todavía.</p>')
                else:
                    body = Markup(
                        '<table class="table table-sm table-bordered"><thead><tr>'
                        '<th>Campo</th><th>Antes</th><th>Propuesto</th></tr></thead><tbody>%s</tbody></table>'
                    ) % Markup('').join(
                        Markup('<tr><td><b>%s</b></td><td>%s</td><td>%s</td></tr>') % (
                            label, cell(before), cell(after))
                        for label, before, after in rows)
            if rec.reason:
                body += Markup("<p><b>Por qué:</b> %s</p>") % cell(rec.reason)
            rec.diff_html = body

    # ------------------------------------------------------------------
    def action_submit(self):
        self.ensure_one()
        if self.state != 'borrador':
            raise UserError("Esta propuesta ya se envió.")
        if not (self.reason or '').strip():
            raise UserError("Escribe por qué propones el cambio.")
        if self.change_type in ('cambiar', 'quitar') and not self.activity_id:
            raise UserError("Para cambiar o quitar, abre la propuesta desde la tarjeta de la actividad.")
        if self.change_type == 'agregar':
            if not self.process_id:
                raise UserError("Elige el proceso donde va la actividad nueva.")
            if not (self.name or '').strip():
                raise UserError("Escribe el resumen de la actividad nueva.")
        if self.change_type == 'cambiar' and not self._sgi_changes():
            raise UserError("No cambiaste ningún campo de la actividad.")
        category = self.env['approval.category']._sgi_mp_change_category()
        if not category:
            raise UserError("No existe la categoría de Aprobaciones «Proponer cambio a mi "
                            "procedimiento (SGI)». Pide al Jefe MAST y SGI que la cree.")
        snapshot = self.diff_html
        request = self.env['approval.request'].create({
            'name': self.display_title,
            'category_id': category.id,
            'request_owner_id': self.env.user.id,
            'reference': self._sgi_reference(),
            'reason': snapshot,
            'sgi_activity_id': self.activity_id.id,
            'sgi_mp_change_type': self.change_type,
            'sgi_mp_proposal_id': self.id,
            'sgi_affected_process_ids': [(6, 0, self.process_id.ids)],
        })
        if self.attachment:
            self.env['ir.attachment'].create({
                'name': self.attachment_name or "Adjunto de la propuesta",
                'datas': self.attachment,
                'res_model': 'approval.request', 'res_id': request.id,
            })
        self._sgi_add_process_owner_approver(request)
        self.write({'request_id': request.id, 'state': 'enviada', 'diff_snapshot': snapshot})
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

    def _sgi_add_process_owner_approver(self, request):
        """56.7.0: el dueño del proceso aprueba los cambios a SU proceso (la
        categoría solo manda al jefe directo y a MAST). No se agrega si es
        quien propone o si ya está en la lista."""
        owner = self.process_id.owner_id.user_id
        if not owner or not owner.active or owner == self.env.user \
                or owner in request.sudo().approver_ids.user_id:
            return
        request.sudo().write({'approver_ids': [(0, 0, {'user_id': owner.id, 'required': True})]})

    def _sgi_values_for_activity(self):
        vals = {}
        for fname in MP_FIELDS:
            field = self._fields[fname]
            value = self[fname]
            if field.type == 'many2one':
                vals[fname] = value.id
            elif field.type == 'many2many':
                vals[fname] = [(6, 0, value.ids)]
            else:
                vals[fname] = value
        return vals

    def _sgi_role_commands(self, activity=None):
        """Comandos del one2many role_ids para dejar exactamente los roles
        propuestos. Van por la actividad (create/write) para que la regla
        «un solo ejecutor» se revise una vez al final y no a medio camino
        (antes: alta sin roles y quitar uno por uno reventaban).

        Un rol que solo cambia de puesto se actualiza en su lugar: conserva su
        configuración de aprobación nativa (documento, botón, condición) en
        vez de borrarse y nacer vacío."""
        def key(r):
            return (r.role, r.target_type, r.job_id.id, r.family_id.id, r.relative_role or False)

        def line_vals(line):
            return {'role': line.role, 'target_type': line.target_type, 'job_id': line.job_id.id,
                    'family_id': line.family_id.id, 'relative_role': line.relative_role,
                    'after_days': line.after_days, 'condition': line.condition, 'sequence': line.sequence}
        remaining = list(self.role_line_ids)
        commands, unmatched = [], []
        for role in (activity.role_ids if activity else []):
            line = next((line for line in remaining if key(line) == key(role)), None)
            if line is None:
                unmatched.append(role)
                continue
            remaining.remove(line)
            commands.append(Command.update(role.id, {
                'after_days': line.after_days, 'condition': line.condition, 'sequence': line.sequence}))
        for role in unmatched:
            line = next((line for line in remaining if line.role == role.role), None)
            if line is None:
                commands.append(Command.delete(role.id))
            else:
                remaining.remove(line)
                commands.append(Command.update(role.id, line_vals(line)))
        commands += [Command.create(line_vals(line)) for line in remaining]
        return commands

    def _sgi_apply(self):
        """Aplica la propuesta (sudo) y devuelve la actividad afectada."""
        self.ensure_one()
        Activity = self.env['sgi.process.activity'].sudo()
        diff = self.diff_snapshot or self.diff_html
        if self.change_type == 'quitar':
            activity = self.activity_id.sudo()
            activity.active = False
        elif self.change_type == 'agregar':
            activity = Activity.create(dict(self._sgi_values_for_activity(), process_id=self.process_id.id,
                                            role_ids=self._sgi_role_commands()))
        else:
            activity = self.activity_id.sudo()
            activity.write(dict(self._sgi_values_for_activity(),
                                role_ids=self._sgi_role_commands(activity)))
        activity.message_post(body=Markup("<p>Cambio aprobado (%s):</p>%s") % (
            self.request_id.name or '', diff))
        self.write({'state': 'aplicada', 'activity_id': activity.id})
        return activity


class SgiActivityChangeRole(models.Model):
    _name = 'sgi.activity.change.role'
    _description = "Quién hace la actividad (propuesta)"
    _order = 'change_id, sequence, id'

    change_id = fields.Many2one('sgi.activity.change', required=True, ondelete='cascade', index=True)
    sequence = fields.Integer(default=10)

    @api.model_create_multi
    def create(self, vals_list):
        lines = super().create(vals_list)
        lines.change_id._sgi_check_editable()
        return lines

    def write(self, vals):
        self.change_id._sgi_check_editable()
        return super().write(vals)

    def unlink(self):
        self.change_id._sgi_check_editable()
        return super().unlink()
    role = fields.Selection(_role_selection('role'), string="Rol", required=True, default='ejecuta')
    target_type = fields.Selection(_role_selection('target_type'), string="Asignado a",
                                   required=True, default='job')
    job_id = fields.Many2one('hr.job', string="Puesto")
    family_id = fields.Many2one('sgi.job.family', string="Familia de puestos")
    relative_role = fields.Selection(_role_selection('relative_role'), string="Rol relativo")
    after_days = fields.Integer(string="Escala a los (días hábiles)")
    condition = fields.Char(string="Condición")

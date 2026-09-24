# -*- coding: utf-8 -*-
"""Pantalla «Mi procedimiento» (Inicio → Mi procedimiento).

Una sola pantalla con lo que le toca al puesto del usuario: secciones por
cadencia real, cada actividad como tarjeta expandible (cerrada: estado,
cuándo, nombre, rol; abierta: cómo, contra qué, criterio de terminado, si
falla, entradas con plazo, salidas, a quién escala) con «Ir a hacerlo» y
«Ver instructivo»; participa y se entera en lista corta; escalamientos que
recibe; y la firma «leído y entendido» contra la revisión vigente del PDF
archivado (camino A).

Quién ve a quién: cada quien su puesto; los jefes (responsable directo o del
departamento) y los dueños de proceso eligen un empleado o puesto de su
equipo; el Jefe MAST, el administrador del SGI y la Dirección de Operaciones,
cualquiera.

Es un modelo transitorio con el contenido como HTML calculado en el servidor
(sin JS propio): las tarjetas son ``<details>`` nativos y los botones son
enlaces a ``/odoo/action-<id>`` y al archivo del instructivo.
"""
from markupsafe import Markup, escape

from odoo import api, fields, models
from odoo.exceptions import UserError

_STATUS_STYLE = {
    'al_dia': ('#1e7e34', '#e6f4ea'),
    'atrasada': ('#b02a37', '#fdecee'),
    'sin_medir': ('#6c757d', '#f1f3f5'),
}
_ROLE_STYLE = {
    'Ejecuta': '#e6f4ea',
    'Aprueba': '#fff4e5',
}


class HrEmployeeMyProcedureTeam(models.Model):
    _inherit = 'hr.employee'

    def _sgi_mp_team_employees(self):
        """Empleados cuyo «Mi procedimiento» puede ver este empleado: él
        mismo, sus reportes (directos e indirectos), los de los departamentos
        que dirige y los de los puestos con rol en los procesos que posee."""
        self.ensure_one()
        Employee = self.env['hr.employee'].sudo()
        team = self
        team |= Employee.search([('parent_id', 'child_of', self.id)])
        departments = self.env['hr.department'].sudo().search([('manager_id', '=', self.id)])
        if departments:
            team |= Employee.search([('department_id', 'child_of', departments.ids)])
        processes = self.env['sgi.process'].sudo().search([('owner_id', '=', self.id)])
        if processes:
            roles = self.env['sgi.activity.role'].sudo().search(
                [('process_id', 'in', processes.ids)])
            jobs = roles.job_id | roles.family_id.job_ids
            if jobs:
                team |= Employee.search([('job_id', 'in', jobs.ids)])
        return team


class SgiMyProcedure(models.TransientModel):
    _name = 'sgi.my.procedure'
    _description = "Mi procedimiento (pantalla)"

    # hr.employee.public: mismo id que hr.employee y legible por cualquier
    # usuario interno (hr.employee no lo es en Odoo 19). Las lecturas de
    # fondo van con sudo sobre hr.employee.
    employee_id = fields.Many2one('hr.employee.public', string="Empleado")
    job_id = fields.Many2one(
        'hr.job', string="Puesto", compute='_compute_job_id', store=True, readonly=False)
    can_pick = fields.Boolean(compute='_compute_scope')
    can_publish = fields.Boolean(compute='_compute_scope')
    allowed_employee_ids = fields.Many2many(
        'hr.employee.public', compute='_compute_scope', string="Empleados visibles")
    allowed_job_ids = fields.Many2many(
        'hr.job', compute='_compute_scope', string="Puestos visibles")
    is_me = fields.Boolean(compute='_compute_ack')
    ack_state = fields.Selection([
        ('sin_publicar', "Sin revisión publicada"),
        ('no_aplica', "No aplica"),
        ('pendiente', "Pendiente de firma"),
        ('leido', "Leído y entendido"),
    ], compute='_compute_ack')
    ack_label = fields.Char(compute='_compute_ack')
    doc_id = fields.Many2one('documents.document', compute='_compute_ack')
    content = fields.Html(string="Contenido", compute='_compute_content', sanitize=False)

    # ------------------------------------------------------------------
    # Ven a cualquiera: Jefe MAST, administrador del SGI y Dirección de
    # Operaciones (grupo quimibond_sgi.group_sgi_director).
    _SGI_MP_SEE_ALL_GROUPS = (
        'quimibond_sgi.group_sgi_manager',
        'quimibond_sgi.group_sgi_admin',
        'quimibond_sgi.group_sgi_director',
    )

    @api.model
    def _sgi_mp_is_admin(self):
        user = self.env.user
        return any(user.has_group(group) for group in self._SGI_MP_SEE_ALL_GROUPS)

    @api.model
    def _sgi_mp_my_employee(self):
        return self.env.user.employee_id.sudo()

    def _sgi_mp_employee(self):
        """El empleado elegido como hr.employee (sudo)."""
        self.ensure_one()
        return self.env['hr.employee'].sudo().browse(self.employee_id.id) \
            if self.employee_id else self.env['hr.employee'].sudo()

    @api.depends('employee_id')
    def _compute_job_id(self):
        for wiz in self:
            if wiz.employee_id:
                wiz.job_id = wiz.employee_id.job_id

    @api.depends_context('uid')
    def _compute_scope(self):
        admin = self._sgi_mp_is_admin()
        me = self._sgi_mp_my_employee()
        Employee = self.env['hr.employee'].sudo()
        Job = self.env['hr.job'].sudo()
        for wiz in self:
            wiz.can_publish = self.env.user.has_group('quimibond_sgi.group_sgi_manager')
            if admin:
                wiz.can_pick = True
                wiz.allowed_employee_ids = Employee.search([]).ids
                wiz.allowed_job_ids = Job.search([]).ids
                continue
            team = me._sgi_mp_team_employees() if me else Employee
            wiz.allowed_employee_ids = team.ids
            wiz.allowed_job_ids = team.job_id.ids
            wiz.can_pick = len(team) > 1

    @api.depends('employee_id', 'job_id')
    @api.depends_context('uid')
    def _compute_ack(self):
        Ack = self.env['sgi.document.ack'].sudo()
        me = self._sgi_mp_my_employee()
        for wiz in self:
            job = wiz.job_id.sudo()
            doc = job._sgi_my_procedure_current_doc() if job else False
            wiz.doc_id = doc or False
            wiz.is_me = bool(me and wiz.employee_id.id == me.id)
            if not doc:
                wiz.ack_state = 'sin_publicar'
                wiz.ack_label = "Aún no hay revisión publicada de este puesto: la firma se habilita cuando MAST la publique."
                continue
            emp = wiz._sgi_mp_employee()
            if not emp or emp.job_id != job:
                wiz.ack_state = 'no_aplica'
                wiz.ack_label = "Revisión %s vigente desde %s." % (
                    doc.sgi_revision_label, doc.sgi_issue_date or '')
                continue
            ack = Ack.search([('document_id', '=', doc.id), ('employee_id', '=', emp.id)], limit=1)
            if ack and ack.state == 'leido':
                wiz.ack_state = 'leido'
                wiz.ack_label = "%s firmó «leído y entendido» de la revisión %s el %s." % (
                    emp.name, doc.sgi_revision_label,
                    fields.Datetime.context_timestamp(self, ack.ack_date).strftime('%d/%m/%Y %H:%M')
                    if ack.ack_date else '')
            else:
                wiz.ack_state = 'pendiente'
                wiz.ack_label = "Revisión %s (%s) pendiente de firma de %s." % (
                    doc.sgi_revision_label, doc.sgi_issue_date or '', emp.name)

    @api.depends('job_id', 'employee_id')
    def _compute_content(self):
        for wiz in self:
            if not wiz.job_id:
                wiz.content = Markup(
                    "<div class='alert alert-warning'>Sin puesto asignado: pide a RH que lo "
                    "capture en tu ficha de empleado.</div>")
                continue
            job = wiz.job_id.sudo()
            data = job._sgi_my_procedure_data()
            wiz.content = self._sgi_mp_render(job, wiz._sgi_mp_employee(), data)

    # ------------------------------------------------------------------
    # Render
    # ------------------------------------------------------------------
    @api.model
    def _sgi_mp_badge(self, text, color, background):
        return Markup(
            '<span style="display:inline-block; padding:1px 8px; border-radius:10px; '
            'font-size:12px; font-weight:600; color:%s; background:%s; margin-right:6px;">%s</span>'
        ) % (color, background, text)

    @api.model
    def _sgi_mp_button(self, label, url, icon, primary=False, title=''):
        cls = 'btn btn-sm %s me-2' % ('btn-primary' if primary else 'btn-secondary')
        return Markup('<a class="%s" href="%s" target="_self" title="%s"><i class="fa %s me-1"></i>%s</a>') % (
            cls, url, title, icon, label)

    @api.model
    def _sgi_mp_row(self, label, value):
        if not value:
            return Markup('')
        return Markup('<div style="margin:3px 0;"><b>%s:</b> %s</div>') % (label, value)

    @api.model
    def _sgi_mp_card(self, entry):
        color, background = _STATUS_STYLE[entry['status']]
        head = Markup('')
        head += self._sgi_mp_badge(entry['status_label'], color, background)
        if entry['when']:
            head += Markup('<span style="font-weight:600; margin-right:8px;">%s</span>') % entry['when']
        head += Markup('<span style="font-size:15px;">%s</span>') % entry['name']
        for role in entry['roles']:
            head += Markup(' ') + self._sgi_mp_badge(role, '#333', _ROLE_STYLE.get(role, '#eee'))
        head += Markup('<span class="text-muted" style="font-size:12px;"> %s · %s</span>') % (
            entry['process'].code or '', entry['number'])

        body = Markup('')
        if entry['status_detail']:
            body += Markup('<div class="text-muted" style="font-size:12px; margin-bottom:4px;">%s</div>') % entry['status_detail']
        body += self._sgi_mp_row("Cómo", entry['how'])
        body += self._sgi_mp_row("Dónde", entry['where'])
        body += self._sgi_mp_row("Contra qué se revisa", entry['check_against'])
        body += self._sgi_mp_row("Terminada cuando", entry['done'])
        body += self._sgi_mp_row("Si no se puede", entry['on_fail'])
        if entry['inputs']:
            body += self._sgi_mp_row("Recibe", ", ".join(
                "%s (%d días hábiles)" % (name, days) if days else name
                for name, days in entry['inputs']))
        if entry['outputs']:
            body += self._sgi_mp_row("Entrega", ", ".join(entry['outputs']))
        if entry['related']:
            body += self._sgi_mp_row("Conforme a", entry['related'])
        if entry['escalates_to']:
            body += self._sgi_mp_row("Si se atora, escala a", "; ".join(entry['escalates_to']))

        buttons = Markup('')
        if entry['action_url']:
            buttons += self._sgi_mp_button("Ir a hacerlo", entry['action_url'], 'fa-play',
                                           primary=True, title=entry['where'])
        elif entry['external']:
            buttons += Markup('<span class="btn btn-sm btn-outline-secondary disabled me-2">'
                              '<i class="fa fa-external-link me-1"></i>Se hace en %s</span>') % entry['external']
        if entry['instruction_url']:
            buttons += self._sgi_mp_button("Ver instructivo %s" % entry['instruction'],
                                           entry['instruction_url'], 'fa-book')
        elif entry['instruction']:
            buttons += Markup('<span class="text-muted me-2"><i class="fa fa-book me-1"></i>%s (sin archivo)</span>') % entry['instruction']
        buttons += self._sgi_mp_button("Ver actividad", entry['activity_url'], 'fa-file-text-o')

        return Markup(
            '<details style="border:1px solid #dee2e6; border-left:4px solid %s; border-radius:6px; '
            'padding:8px 12px; margin:6px 0; background:#fff;">'
            '<summary style="cursor:pointer; list-style:revert;">%s</summary>'
            '<div style="padding:8px 4px 4px 4px; font-size:13px;">%s'
            '<div style="margin-top:8px;">%s</div></div></details>'
        ) % (color, head, body, buttons)

    @api.model
    def _sgi_mp_render(self, job, employee, data):
        cover = data['cover']
        html = Markup('<div style="font-size:14px;">')
        # Encabezado
        html += Markup('<div style="margin-bottom:10px;"><span style="font-size:18px; font-weight:700;">%s</span>') % job.name
        if cover['family']:
            html += Markup(' <span class="text-muted">· familia %s</span>') % cover['family'].name
        html += Markup('</div>')
        meta = []
        if cover['department']:
            meta.append("Área: %s" % (cover['department'].complete_name or cover['department'].name))
        if cover['manager']:
            meta.append("Jefe inmediato: %s" % cover['manager'].name)
        if cover['employees']:
            meta.append("Personas en el puesto: %s" % ", ".join(cover['employees'].mapped('name')))
        if cover['processes']:
            meta.append("Procesos: %s" % ", ".join(
                "%s %s" % (p.code, p.name) for p in cover['processes']))
        if cover['counts']:
            meta.append(" · ".join("%s %d" % (label, n) for label, n in cover['counts']))
        html += Markup('<div class="text-muted" style="font-size:12px; margin-bottom:12px;">%s</div>') % Markup('<br/>').join(escape(m) for m in meta)

        counts = {'al_dia': 0, 'atrasada': 0, 'sin_medir': 0}
        for section in data['sections']:
            for entry in section['entries']:
                counts[entry['status']] += 1
        summary = Markup('')
        for code, label in (('atrasada', "Atrasadas"), ('al_dia', "Al día"), ('sin_medir', "Sin medir")):
            if counts[code]:
                color, background = _STATUS_STYLE[code]
                summary += self._sgi_mp_badge("%s: %d" % (label, counts[code]), color, background)
        if summary:
            html += Markup('<div style="margin-bottom:10px;">%s</div>') % summary

        if not data['sections']:
            html += Markup('<div class="alert alert-info">Este puesto no ejecuta ni aprueba '
                           'actividades en ningún procedimiento.</div>')
        for section in data['sections']:
            html += Markup('<h4 style="margin:16px 0 4px 0; padding:4px 8px; background:#e9ecef; '
                           'border-radius:4px;">%s <span class="text-muted" style="font-size:12px;">(%d)</span></h4>') % (
                section['label'], len(section['entries']))
            for entry in section['entries']:
                html += self._sgi_mp_card(entry)

        if data['received']:
            html += Markup('<h4 style="margin:16px 0 4px 0; padding:4px 8px; background:#e9ecef; border-radius:4px;">Escalamientos que recibe</h4>')
            html += Markup('<ul style="font-size:13px;">')
            for rc in data['received']:
                html += Markup('<li><b>%s %s</b> %s — la ejecuta %s; escala %s</li>') % (
                    rc['process'].code or '', rc['number'], rc['name'], rc['from'] or '—',
                    "a los %d días hábiles" % rc['after_days'] if rc['after_days'] else "al atorarse")
            html += Markup('</ul>')

        if data['short']:
            html += Markup('<h4 style="margin:16px 0 4px 0; padding:4px 8px; background:#e9ecef; border-radius:4px;">Participa o se entera</h4>')
            html += Markup('<ul style="font-size:13px;">')
            for sh in data['short']:
                html += Markup('<li><span class="text-muted">%s</span> · <b>%s %s</b> %s '
                               '<a href="/odoo/sgi.process.activity/%d" target="_self">ver</a></li>') % (
                    ", ".join(sh['roles']), sh['process'].code or '', sh['number'], sh['name'],
                    sh['activity'].id)
            html += Markup('</ul>')
        html += Markup('</div>')
        return html

    # ------------------------------------------------------------------
    # Acciones
    # ------------------------------------------------------------------
    @api.model
    def action_open_mine(self):
        """Inicio → Mi procedimiento: la pantalla del puesto del usuario."""
        me = self._sgi_mp_my_employee()
        if not me and not self._sgi_mp_is_admin():
            raise UserError(
                "Tu usuario no tiene empleado ligado. Pide a RH que lo capture en tu ficha.")
        wiz = self.create({'employee_id': me.id if me else False})
        return {
            'type': 'ir.actions.act_window',
            'name': "Mi procedimiento",
            'res_model': 'sgi.my.procedure',
            'res_id': wiz.id,
            'view_mode': 'form',
            'target': 'current',
        }

    def action_sign(self):
        """Firma «leído y entendido» del propio empleado contra la revisión
        vigente. El candado de identidad vive en sgi.document.ack.write()."""
        self.ensure_one()
        me = self._sgi_mp_my_employee()
        if not me or self.employee_id.id != me.id:
            raise UserError("Solo puedes firmar tu propio «Mi procedimiento».")
        doc = self.job_id.sudo()._sgi_my_procedure_current_doc() if self.job_id else False
        if not doc:
            raise UserError("Aún no hay una revisión publicada de este puesto para firmar.")
        if me.job_id.id != self.job_id.id:
            raise UserError("Tu puesto ya no es %s: no hay acuse que firmar." % self.job_id.name)
        Ack = self.env['sgi.document.ack']
        ack = Ack.sudo().search([('document_id', '=', doc.id), ('employee_id', '=', me.id)], limit=1)
        if not ack:
            ack = Ack.sudo().create({'document_id': doc.id, 'employee_id': me.id})
        ack.with_user(self.env.user).action_mark_read()
        return self._reload()

    def action_print(self):
        self.ensure_one()
        if not self.job_id:
            raise UserError("Elige un puesto.")
        return self.job_id.sudo().with_context(
            sgi_mp_employee_id=self.employee_id.id).action_sgi_print_my_procedure()

    def action_publish(self):
        self.ensure_one()
        if not self.job_id:
            raise UserError("Elige un puesto.")
        self.job_id.action_sgi_publish_my_procedure()
        return self._reload()

    def action_open_doc(self):
        self.ensure_one()
        return self.job_id.action_sgi_open_my_procedure_doc()

    def _reload(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window', 'res_model': 'sgi.my.procedure',
            'res_id': self.id, 'view_mode': 'form', 'target': 'current',
            'name': "Mi procedimiento",
        }

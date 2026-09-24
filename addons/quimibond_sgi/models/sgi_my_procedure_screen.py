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
    # Neutro a propósito: sin medición automática no es una alarma.
    'sin_medir': ('#6c757d', '#f8f9fa'),
}
_CARD_BORDER = {'al_dia': '#1e7e34', 'atrasada': '#b02a37', 'sin_medir': '#dee2e6'}
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
        if entry['status'] == 'sin_medir':
            head += Markup('<span class="text-muted" style="font-size:12px; margin-right:6px;">%s</span>') % entry['status_label']
        else:
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
        ) % (_CARD_BORDER[entry['status']], head, body, buttons)

    @api.model
    def _sgi_mp_render(self, job, employee, data):
        """Orden de la estructura del SGI (nivel 4): arriba quién soy, mi puesto,
        mi jefe y el estado (atrasadas, al día, firmas pendientes); en medio
        Mis pendientes, Mis actividades por cadencia y Mis documentos."""
        cover = data['cover']
        html = Markup('<div style="font-size:14px;">')
        # Quién soy
        title = employee.name if employee else job.name
        html += Markup('<div style="margin-bottom:4px;"><span style="font-size:18px; font-weight:700;">%s</span>') % title
        if employee:
            html += Markup(' <span class="text-muted">· %s</span>') % job.name
        if cover['family']:
            html += Markup(' <span class="text-muted">· familia %s</span>') % cover['family'].name
        html += Markup('</div>')
        meta = []
        boss = employee.parent_id if employee and employee.parent_id else cover['manager']
        if boss:
            meta.append("Mi jefe: %s" % boss.name)
        if cover['department']:
            meta.append("Área: %s" % (cover['department'].complete_name or cover['department'].name))
        if not employee and cover['employees']:
            meta.append("Personas en el puesto: %s" % ", ".join(cover['employees'].mapped('name')))
        if cover['processes']:
            meta.append("Procesos: %s" % ", ".join(
                "%s %s" % (p.code, p.name) for p in cover['processes']))
        html += Markup('<div class="text-muted" style="font-size:12px; margin-bottom:8px;">%s</div>') % Markup(' · ').join(escape(m) for m in meta)

        # Estado en una línea
        counts = {'al_dia': 0, 'atrasada': 0, 'sin_medir': 0}
        for section in data['sections']:
            for entry in section['entries']:
                counts[entry['status']] += 1
        pending_acks = self.env['sgi.document.ack'].sudo().search_count(
            [('employee_id', '=', employee.id), ('state', '=', 'pendiente')]) if employee else 0
        summary = Markup('')
        if counts['atrasada']:
            summary += self._sgi_mp_badge("Atrasadas: %d" % counts['atrasada'], *_STATUS_STYLE['atrasada'])
        if counts['al_dia']:
            summary += self._sgi_mp_badge("Al día: %d" % counts['al_dia'], *_STATUS_STYLE['al_dia'])
        if pending_acks:
            summary += self._sgi_mp_badge("Firmas pendientes: %d" % pending_acks, '#8a6d00', '#fff4e5')
        if counts['sin_medir']:
            summary += Markup('<span class="text-muted" style="font-size:12px;">Sin medición automática: %d</span>') % counts['sin_medir']
        if not summary:
            summary = Markup('<span class="text-muted" style="font-size:12px;">Sin atrasos ni firmas pendientes.</span>')
        html += Markup('<div style="margin-bottom:12px;">%s</div>') % summary

        # Mis pendientes (lo atrasado arriba)
        html += self._sgi_mp_render_pending(employee)

        # Mis actividades por cadencia
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
            html += self._sgi_mp_section_title("Escalamientos que recibe")
            html += Markup('<ul style="font-size:13px;">')
            for rc in data['received']:
                html += Markup('<li><b>%s %s</b> %s — la ejecuta %s; escala %s</li>') % (
                    rc['process'].code or '', rc['number'], rc['name'], rc['from'] or '—',
                    "a los %d días hábiles" % rc['after_days'] if rc['after_days'] else "al atorarse")
            html += Markup('</ul>')

        if data['short']:
            html += self._sgi_mp_section_title("Participa o se entera")
            html += Markup('<ul style="font-size:13px;">')
            for sh in data['short']:
                html += Markup('<li><span class="text-muted">%s</span> · <b>%s %s</b> %s '
                               '<a href="/odoo/sgi.process.activity/%d" target="_self">ver</a></li>') % (
                    ", ".join(sh['roles']), sh['process'].code or '', sh['number'], sh['name'],
                    sh['activity'].id)
            html += Markup('</ul>')

        # Mis documentos
        html += self._sgi_mp_render_documents(job, employee)
        html += Markup('</div>')
        return html

    @api.model
    def _sgi_mp_section_title(self, title):
        return Markup('<h4 style="margin:16px 0 4px 0; padding:4px 8px; background:#e9ecef; '
                      'border-radius:4px;">%s</h4>') % title

    @api.model
    def _sgi_mp_documents(self, job, employee):
        """Documentos vigentes que aplican al puesto, con el acuse del empleado."""
        Doc = self.env['documents.document'].sudo()
        docs = Doc.search([('sgi_state', '=', 'vigente'), ('sgi_job_ids', 'in', job.ids),
                           ('sgi_doc_type', '!=', 'mi_procedimiento')],
                          order='sgi_doc_type, sgi_code, name')
        acks = {}
        if employee and docs:
            for ack in self.env['sgi.document.ack'].sudo().search(
                    [('document_id', 'in', docs.ids), ('employee_id', '=', employee.id)]):
                acks[ack.document_id.id] = ack
        rows = []
        for doc in docs:
            ack = acks.get(doc.id)
            if not employee:
                state, label = 'na', ''
            elif ack and ack.state == 'leido':
                state, label = 'leido', "Leído el %s" % (
                    fields.Datetime.context_timestamp(self, ack.ack_date).strftime('%d/%m/%Y')
                    if ack.ack_date else '')
            elif ack:
                state, label = 'pendiente', "Acuse pendiente"
            else:
                state, label = 'sin_acuse', "Sin acuse generado"
            rows.append({
                'doc': doc, 'code': doc.sgi_code or '', 'name': doc.name or '',
                'type': dict(doc._fields['sgi_doc_type'].selection).get(doc.sgi_doc_type, ''),
                'revision': doc.sgi_revision_label, 'ack': ack, 'state': state, 'label': label,
                'file_url': '/web/content/%d?filename=%s' % (doc.attachment_id.id, doc.name or '')
                if doc.attachment_id else (doc.url or ''),
            })
        return rows

    @api.model
    def _sgi_mp_render_documents(self, job, employee):
        rows = self._sgi_mp_documents(job, employee)
        if not rows:
            return Markup('')
        html = self._sgi_mp_section_title("Mis documentos")
        html += Markup('<table class="table table-sm" style="font-size:13px;"><thead><tr>'
                       '<th>Clave</th><th>Documento</th><th>Rev.</th><th>Acuse</th><th></th></tr></thead><tbody>')
        for row in rows:
            if row['state'] == 'leido':
                ack_html = self._sgi_mp_badge(row['label'], '#1e7e34', '#e6f4ea')
            elif row['state'] == 'pendiente':
                ack_html = self._sgi_mp_badge(row['label'], '#8a6d00', '#fff4e5')
                if row['ack']:
                    ack_html += Markup(' <a href="/odoo/sgi.document.ack/%d" target="_self">firmar</a>') % row['ack'].id
            elif row['state'] == 'sin_acuse':
                ack_html = Markup('<span class="text-muted">%s</span>') % row['label']
            else:
                ack_html = Markup('')
            link = Markup('<a href="%s" target="_blank">Ver archivo</a>') % row['file_url'] if row['file_url'] else Markup('')
            html += Markup('<tr><td><b>%s</b></td><td>%s <span class="text-muted">· %s</span></td>'
                           '<td>%s</td><td>%s</td><td>%s</td></tr>') % (
                row['code'], row['name'], row['type'], row['revision'], ack_html, link)
        html += Markup('</tbody></table>')
        return html

    @api.model
    def _sgi_mp_pending(self, employee):
        """«Mis pendientes» del usuario del empleado: acciones abiertas o
        vencidas, NC a contestar, obligaciones confirmadas con vencimiento e
        indicadores oficiales a su cargo. Sin usuario, no hay pendientes que
        mostrar (viven en Odoo, no en el puesto)."""
        user = employee.user_id if employee else False
        if not user:
            return None
        env = self.env
        today = fields.Date.context_today(self)
        actions = env['sgi.action.line'].sudo().search(
            [('responsible_id', '=', user.id), ('state', 'in', ('abierta', 'vencida'))],
            order='date_commit, id')
        ncs = env['quality.alert'].sudo().search(
            [('sgi_responsible_ids', 'in', user.id), ('sgi_stage_is_closing', '=', False),
             ('sgi_stage_is_cancel', '=', False)], order='create_date') \
            if 'sgi_responsible_ids' in env['quality.alert']._fields else env['quality.alert']
        obligations = env['qb.obligation'].sudo().search(
            [('user_id', '=', user.id), ('state', '=', 'confirmed')], order='date_deadline') \
            if 'qb.obligation' in env else []
        indicators = env['sgi.indicator'].sudo().search(
            [('responsible_id', '=', user.id), ('status', '=', 'oficial')], order='code')
        return {
            'actions': [{'name': a.name, 'origin': a.origin_display, 'date': a.date_commit,
                         'late': a.state == 'vencida' or (a.date_commit and a.date_commit < today),
                         'url': '/odoo/sgi.action.line/%d' % a.id} for a in actions],
            'ncs': [{'name': "%s %s" % (n.sgi_folio or '', n.name or ''), 'stage': n.stage_id.name or '',
                     'url': '/odoo/quality.alert/%d' % n.id} for n in ncs],
            'obligations': [{'name': o.name, 'date': o.date_deadline,
                             'late': o.date_deadline and o.date_deadline < today,
                             'url': '/odoo/qb.obligation/%d' % o.id} for o in obligations],
            'indicators': [{'code': i.code, 'name': i.name, 'semaphore': i.last_semaphore or '',
                            'value': i.last_value, 'uom': i.uom or '',
                            'url': '/odoo/sgi.indicator/%d' % i.id} for i in indicators],
        }

    @api.model
    def _sgi_mp_render_pending(self, employee):
        pending = self._sgi_mp_pending(employee)
        if pending is None:
            return Markup('')
        total = sum(len(v) for v in pending.values())
        html = self._sgi_mp_section_title("Mis pendientes")
        if not total:
            return html + Markup('<div class="text-muted" style="font-size:13px;">Sin acciones, '
                                 'no conformidades, obligaciones ni indicadores oficiales a tu cargo.</div>')
        html += Markup('<ul style="font-size:13px;">')
        for a in pending['actions']:
            html += Markup('<li>%s<b>Acción:</b> <a href="%s" target="_self">%s</a>'
                           '<span class="text-muted"> · %s · compromiso %s</span></li>') % (
                self._sgi_mp_badge("Vencida", '#b02a37', '#fdecee') if a['late'] else Markup(''),
                a['url'], a['name'], a['origin'] or '', a['date'] or '')
        for n in pending['ncs']:
            html += Markup('<li><b>NC a contestar:</b> <a href="%s" target="_self">%s</a>'
                           '<span class="text-muted"> · %s</span></li>') % (n['url'], n['name'], n['stage'])
        for o in pending['obligations']:
            html += Markup('<li>%s<b>Obligación:</b> <a href="%s" target="_self">%s</a>'
                           '<span class="text-muted"> · vence %s</span></li>') % (
                self._sgi_mp_badge("Vencida", '#b02a37', '#fdecee') if o['late'] else Markup(''),
                o['url'], o['name'], o['date'] or '')
        for i in pending['indicators']:
            sem = {'verde': ('#1e7e34', '#e6f4ea'), 'amarillo': ('#8a6d00', '#fff4e5'),
                   'rojo': ('#b02a37', '#fdecee')}.get(i['semaphore'])
            html += Markup('<li>%s<b>Indicador:</b> <a href="%s" target="_self">%s %s</a>'
                           '<span class="text-muted"> · último %s %s</span></li>') % (
                self._sgi_mp_badge(i['semaphore'].capitalize(), *sem) if sem else Markup(''),
                i['url'], i['code'], i['name'], i['value'], i['uom'])
        html += Markup('</ul>')
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

    def action_publish_all(self):
        return self.env['hr.job'].action_sgi_publish_all_my_procedures()

    def action_precheck(self):
        """Lo que hay que limpiar antes de publicar para toda la planta."""
        check = self.env['sgi.my.procedure.check'].create({})
        return {
            'type': 'ir.actions.act_window', 'res_model': 'sgi.my.procedure.check',
            'res_id': check.id, 'view_mode': 'form', 'target': 'new',
            'name': "Revisión previa a publicar",
        }

    def _reload(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window', 'res_model': 'sgi.my.procedure',
            'res_id': self.id, 'view_mode': 'form', 'target': 'current',
            'name': "Mi procedimiento",
        }


class SgiMyProcedureCheck(models.TransientModel):
    _name = 'sgi.my.procedure.check'
    _description = "Mi procedimiento: revisión previa a publicar"

    result = fields.Html(string="Resultado", compute='_compute_result', sanitize=False)

    def _compute_result(self):
        Job = self.env['hr.job']
        for wiz in self:
            data = Job._sgi_my_procedure_precheck()
            html = Markup('<div style="font-size:13px;">')
            html += Markup('<p><b>%d</b> puesto(s) con roles y personas listos para publicar.</p>') % data['ready']

            html += Markup('<h5>Puestos duplicados (%d grupos)</h5>') % len(data['duplicates'])
            if data['duplicates']:
                html += Markup('<ul>')
                for group in data['duplicates']:
                    html += Markup('<li>') + Markup(' · ').join(
                        Markup('<a href="/odoo/hr.job/%d" target="_self">%s</a> (%d personas, %d roles)') % (
                            j.id, j.name, j.no_of_employee, j.sgi_role_count) for j in group) + Markup('</li>')
                html += Markup('</ul>')
            else:
                html += Markup('<p class="text-muted">Ninguno.</p>')

            html += Markup('<h5>Empleados sin puesto (%d)</h5>') % len(data['no_job'])
            if data['no_job']:
                html += Markup('<p>%s</p>') % ", ".join(data['no_job'].mapped('name'))
            else:
                html += Markup('<p class="text-muted">Ninguno.</p>')

            html += Markup('<h5>Empleados en un puesto sin roles (%d)</h5>') % len(data['job_without_roles'])
            if data['job_without_roles']:
                by_job = {}
                for emp in data['job_without_roles']:
                    by_job.setdefault(emp.job_id, []).append(emp.name)
                html += Markup('<ul>')
                for job, names in sorted(by_job.items(), key=lambda kv: kv[0].name):
                    html += Markup('<li><a href="/odoo/hr.job/%d" target="_self">%s</a>: %s</li>') % (
                        job.id, job.name, ", ".join(names))
                html += Markup('</ul>')
            else:
                html += Markup('<p class="text-muted">Ninguno.</p>')

            html += Markup('<h5>Puestos con roles pero sin personas (%d)</h5>') % len(data['roles_without_people'])
            if data['roles_without_people']:
                html += Markup('<p>%s</p>') % ", ".join(data['roles_without_people'].mapped('name'))
            else:
                html += Markup('<p class="text-muted">Ninguno.</p>')
            html += Markup('</div>')
            wiz.result = html


class SgiMyTeam(models.TransientModel):
    """Inicio → Mi equipo (nivel 4, jefes): una fila por persona del equipo con
    sus atrasos, firmas pendientes y brechas de capacitación, y el botón para
    abrir su procedimiento."""
    _name = 'sgi.my.team'
    _description = "Mi equipo (pantalla)"

    content = fields.Html(string="Contenido", compute='_compute_content', sanitize=False)

    @api.model
    def _sgi_team(self):
        Wiz = self.env['sgi.my.procedure']
        me = Wiz._sgi_mp_my_employee()
        if Wiz._sgi_mp_is_admin():
            return self.env['hr.employee'].sudo().search([('job_id', '!=', False)], order='name')
        if not me:
            return self.env['hr.employee'].sudo()
        return (me._sgi_mp_team_employees() - me).sorted('name')

    @api.model
    def _sgi_team_rows(self, team):
        """Una fila por persona; los datos del puesto se arman una vez por
        puesto, no por persona."""
        Ack = self.env['sgi.document.ack'].sudo()
        pending = {}
        if team:
            for emp, count in Ack._read_group(
                    [('employee_id', 'in', team.ids), ('state', '=', 'pendiente')],
                    ['employee_id'], ['__count']):
                pending[emp.id] = count
        by_job = {}
        rows = []
        for emp in team:
            job = emp.job_id.sudo()
            if job not in by_job:
                data = job._sgi_my_procedure_data() if job else None
                late = sum(1 for sct in data['sections'] for e in sct['entries']
                           if e['status'] == 'atrasada') if data else 0
                total = sum(len(sct['entries']) for sct in data['sections']) if data else 0
                by_job[job] = (late, total)
            late, total = by_job[job]
            rows.append({
                'employee': emp, 'job': job, 'late': late, 'total': total,
                'acks': pending.get(emp.id, 0),
                'gaps': emp.sgi_skill_gap_count if 'sgi_skill_gap_count' in emp._fields else 0,
            })
        rows.sort(key=lambda r: (-r['late'], -r['acks'], r['employee'].name or ''))
        return rows

    def _compute_content(self):
        Wiz = self.env['sgi.my.procedure']
        for wiz in self:
            team = self._sgi_team()
            if not team:
                wiz.content = Markup('<div class="alert alert-info">No tienes personas a tu cargo '
                                     'en Odoo (reportes directos, tu departamento o los puestos de '
                                     'tus procesos).</div>')
                continue
            rows = self._sgi_team_rows(team)
            with_late = sum(1 for r in rows if r['late'])
            with_acks = sum(1 for r in rows if r['acks'])
            html = Markup('<div style="font-size:14px;">')
            html += Markup('<div style="margin-bottom:10px;"><span style="font-size:18px; font-weight:700;">Mi equipo</span>'
                           ' <span class="text-muted">· %d personas</span></div>') % len(rows)
            summary = Markup('')
            if with_late:
                summary += Wiz._sgi_mp_badge("Con atrasos: %d" % with_late, *_STATUS_STYLE['atrasada'])
            if with_acks:
                summary += Wiz._sgi_mp_badge("Con firmas pendientes: %d" % with_acks, '#8a6d00', '#fff4e5')
            if not summary:
                summary = Markup('<span class="text-muted" style="font-size:12px;">Sin atrasos ni firmas pendientes.</span>')
            html += Markup('<div style="margin-bottom:12px;">%s</div>') % summary
            html += Markup('<table class="table table-sm" style="font-size:13px;"><thead><tr>'
                           '<th>Persona</th><th>Puesto</th><th>Atrasadas</th><th>Firmas pendientes</th>'
                           '<th>Brechas de capacitación</th><th></th></tr></thead><tbody>')
            for r in rows:
                person_wiz = Wiz.create({'employee_id': r['employee'].id})
                late_html = Wiz._sgi_mp_badge(str(r['late']), *_STATUS_STYLE['atrasada']) if r['late'] \
                    else Markup('<span class="text-muted">0 de %d</span>') % r['total']
                acks_html = Wiz._sgi_mp_badge(str(r['acks']), '#8a6d00', '#fff4e5') if r['acks'] \
                    else Markup('<span class="text-muted">0</span>')
                gaps_html = Markup('<span class="text-muted">%d</span>') % r['gaps']
                html += Markup('<tr><td><b>%s</b></td><td>%s</td><td>%s</td><td>%s</td><td>%s</td>'
                               '<td><a class="btn btn-sm btn-secondary" href="/odoo/sgi.my.procedure/%d" '
                               'target="_self">Abrir su procedimiento</a></td></tr>') % (
                    r['employee'].name, r['job'].name or '—', late_html, acks_html, gaps_html, person_wiz.id)
            html += Markup('</tbody></table></div>')
            wiz.content = html

    @api.model
    def action_open_mine(self):
        wiz = self.create({})
        return {
            'type': 'ir.actions.act_window', 'name': "Mi equipo",
            'res_model': 'sgi.my.team', 'res_id': wiz.id,
            'view_mode': 'form', 'target': 'current',
        }

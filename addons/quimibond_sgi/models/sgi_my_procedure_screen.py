# -*- coding: utf-8 -*-
"""Pantalla «Mi procedimiento» (Inicio) con vistas NATIVAS de Odoo.

Regla del CEO (2026-09-25): nada de HTML servido ni enlaces armados a mano;
todo con campos, vistas y acciones de Odoo, para que la actualización a la
siguiente versión no rompa nada y la navegación (migas, regresar) funcione
sola. Por eso:

- El transitorio ``sgi.my.procedure`` solo tiene campos: quién soy, mi jefe,
  mi área, conteos de estado y varias relaciones calculadas (mis actividades,
  escalamientos, participa/se entera, pendientes, acuses y documentos) que el
  formulario muestra con kanban y listas nativas.
- Cada actividad es una fila de ``sgi.activity.role`` con las piezas que la
  persona necesita (estado, cuándo, cómo, dónde, contra qué, terminada
  cuando, si no se puede, recibe, entrega, instructivo, escala) como campos
  calculados, y botones de objeto: «Ir a hacerlo», «Ver instructivo», «Ver
  actividad». Al ser acciones de ventana, las migas de pan regresan solas.
- El PDF (QWeb) y la huella siguen en ``hr.job._sgi_my_procedure_data``.
"""
from odoo import api, fields, models
from odoo.exceptions import UserError

from .sgi_my_procedure import _CADENCE_RANK, _DETAIL_ROLES, _SHORT_ROLES

_MP_STATUS = [
    ('al_dia', "Al día"),
    ('atrasada', "Atrasada"),
    ('sin_medir', "Sin medición automática"),
]


class HrEmployeeTeamScope(models.Model):
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


class HrJobMyProcedureLists(models.Model):
    _inherit = 'hr.job'

    def _sgi_mp_role_lists(self):
        """Las tres listas de «Mi procedimiento» del puesto, ya ordenadas:
        ``detail`` (ejecuta / aprueba, por cadencia y fecha), ``received``
        (escalamientos que recibe) y ``short`` (participa o se entera). Las
        usan la pantalla de Inicio, la ficha del empleado y la del puesto."""
        self.ensure_one()
        Role = self.env['sgi.activity.role'].sudo()
        roles = Role.search(self._sgi_roles_domain()).filtered(lambda r: r.activity_id.active)
        detail = roles.filtered(lambda r: r.role in _DETAIL_ROLES)
        short = roles.filtered(lambda r: r.role in _SHORT_ROLES) - detail.filtered(
            lambda r: r.activity_id in detail.activity_id)
        received = roles.filtered(lambda r: r.role == 'escala')
        Job = self.env['hr.job']

        def sort_key(role):
            when_key, _when = Job._sgi_mp_when(role.activity_id)
            return (_CADENCE_RANK.get(role.activity_id.measure_cadence or 'evento', 99),
                    when_key, role.activity_id.process_id.code or '',
                    role.activity_id.number or '', role.activity_id.name or '')

        def by_process(role):
            return (role.activity_id.process_id.code or '', role.activity_id.number or '')

        return {
            'detail': detail.sorted(key=sort_key),
            'received': received.sorted(key=by_process),
            'short': short.sorted(key=by_process),
        }


class SgiMyProcedureMixin(models.AbstractModel):
    """«Mi procedimiento» dentro de la ficha (empleado, empleado público y
    puesto): las mismas listas que la pantalla de Inicio, como campos
    calculados, para que el procedimiento se vea donde vive la persona
    (app Empleados) sin pantalla aparte. CEO, 2026-09-25."""
    _name = 'sgi.my.procedure.mixin'
    _description = "Mi procedimiento en la ficha"

    sgi_mp_role_ids = fields.Many2many(
        'sgi.activity.role', string="Mis actividades", compute='_compute_sgi_mp_lists')
    sgi_mp_received_role_ids = fields.Many2many(
        'sgi.activity.role', string="Escalamientos que recibe", compute='_compute_sgi_mp_lists')
    sgi_mp_short_role_ids = fields.Many2many(
        'sgi.activity.role', string="Participa o se entera", compute='_compute_sgi_mp_lists')
    sgi_mp_ack_ids = fields.Many2many(
        'sgi.document.ack', string="Acuses de lectura", compute='_compute_sgi_mp_lists')
    sgi_mp_document_ids = fields.Many2many(
        'documents.document', string="Documentos que aplican al puesto", compute='_compute_sgi_mp_lists')
    sgi_mp_epp_ids = fields.Many2many(
        'sgi.epp.delivery', string="Responsivas de EPP", compute='_compute_sgi_mp_lists')
    sgi_mp_epp_text = fields.Text(string="EPP requerido por el puesto", compute='_compute_sgi_mp_lists')
    sgi_mp_can_sign = fields.Boolean(string="Puede firmar", compute='_compute_sgi_mp_lists')
    sgi_mp_process_ids = fields.Many2many(
        'sgi.process', string="Procesos donde participa", compute='_compute_sgi_mp_lists')

    def _sgi_mp_job(self):
        """El puesto cuyo procedimiento se muestra (el del empleado; el
        propio registro en hr.job)."""
        return self.job_id.sudo()

    def _sgi_mp_employee_rec(self):
        """El empleado (sudo) detrás del registro; vacío en hr.job."""
        return self.env['hr.employee'].sudo().browse(self.id) if self._name != 'hr.job' else \
            self.env['hr.employee'].sudo()

    # Sin @api.depends: no se almacena y hr.job no tiene job_id; se
    # recalcula en cada lectura, como la pantalla de Inicio.
    @api.depends_context('uid')
    def _compute_sgi_mp_lists(self):
        Role = self.env['sgi.activity.role'].sudo()
        Ack = self.env['sgi.document.ack'].sudo()
        Doc = self.env['documents.document'].sudo()
        Epp = self.env['sgi.epp.delivery'].sudo()
        me = self.env.user.employee_id.sudo()
        for rec in self:
            job = rec._sgi_mp_job()
            emp = rec._sgi_mp_employee_rec().exists()
            lists = job._sgi_mp_role_lists() if job else {'detail': Role, 'received': Role, 'short': Role}
            rec.sgi_mp_role_ids = lists['detail'].ids
            rec.sgi_mp_received_role_ids = lists['received'].ids
            rec.sgi_mp_short_role_ids = lists['short'].ids
            rec.sgi_mp_process_ids = lists['detail'].activity_id.process_id.ids
            rec.sgi_mp_document_ids = Doc.search(
                [('sgi_state', '=', 'vigente'), ('sgi_job_ids', 'in', job.ids),
                 ('sgi_doc_type', '!=', 'mi_procedimiento')],
                order='sgi_doc_type, sgi_code, name').ids if job else False
            rec.sgi_mp_epp_text = (job.sgi_epp_required or False) if job else False
            if emp:
                rec.sgi_mp_ack_ids = Ack.search([('employee_id', '=', emp.id)], order='state, sgi_code').ids
                rec.sgi_mp_epp_ids = Epp.search([('employee_id', '=', emp.id)]).ids
                doc = job._sgi_my_procedure_current_doc() if job else False
                ack = Ack.search([('document_id', '=', doc.id), ('employee_id', '=', emp.id)], limit=1) \
                    if doc else Ack
                rec.sgi_mp_can_sign = bool(me and emp.id == me.id and doc and emp.job_id == job
                                           and (not ack or ack.state == 'pendiente'))
            else:
                rec.sgi_mp_ack_ids = False
                rec.sgi_mp_epp_ids = False
                rec.sgi_mp_can_sign = False

    def action_sgi_mp_sign(self):
        """Firmar «leído y entendido» desde la ficha: mismo candado que la
        pantalla (solo el propio empleado, contra la revisión vigente)."""
        self.ensure_one()
        emp = self._sgi_mp_employee_rec()
        if not emp:
            raise UserError("Un puesto no firma; firma cada empleado desde su ficha.")
        wiz = self.env['sgi.my.procedure'].create({'employee_id': emp.id})
        wiz.action_sign()
        return True


class HrEmployeeMyProcedureTab(models.Model):
    _name = 'hr.employee'
    _inherit = ['hr.employee', 'sgi.my.procedure.mixin']


class HrEmployeePublicMyProcedureTab(models.Model):
    _name = 'hr.employee.public'
    _inherit = ['hr.employee.public', 'sgi.my.procedure.mixin']


class HrJobMyProcedureTab(models.Model):
    _name = 'hr.job'
    _inherit = ['hr.job', 'sgi.my.procedure.mixin']

    def _sgi_mp_job(self):
        return self.sudo()


class SgiActivityRoleMyProcedureScreen(models.Model):
    """Las piezas de una actividad como campos, para la tarjeta nativa."""
    _inherit = 'sgi.activity.role'

    mp_status = fields.Selection(_MP_STATUS, string="Estado", compute='_compute_mp_pieces')
    mp_status_detail = fields.Char(string="Detalle del estado", compute='_compute_mp_pieces')
    mp_number = fields.Char(string="Numeral", compute='_compute_mp_pieces')
    mp_name = fields.Char(related='activity_id.name', string="Actividad")
    mp_how = fields.Text(string="Cómo", compute='_compute_mp_pieces')
    mp_where = fields.Char(string="Dónde", compute='_compute_mp_pieces')
    mp_check_against = fields.Char(string="Contra qué se revisa", compute='_compute_mp_pieces')
    mp_done = fields.Text(string="Terminada cuando", compute='_compute_mp_pieces')
    mp_on_fail = fields.Text(string="Si no se puede", compute='_compute_mp_pieces')
    mp_inputs = fields.Char(string="Recibe", compute='_compute_mp_pieces')
    mp_outputs = fields.Char(string="Entrega", compute='_compute_mp_pieces')
    mp_related = fields.Char(string="Conforme a", compute='_compute_mp_pieces')
    mp_escalates = fields.Char(string="Si se atora, escala a", compute='_compute_mp_pieces')
    mp_external = fields.Char(related='activity_id.external_system', string="Se hace en")
    mp_instruction_id = fields.Many2one(
        related='activity_id.instruction_id', string="Instructivo")
    mp_can_go = fields.Boolean(string="Se puede ir a hacer", compute='_compute_mp_pieces')

    @api.depends('activity_id', 'activity_id.measure_state', 'activity_id.measure_last_date',
                 'activity_id.how_steps', 'activity_id.done_criteria', 'activity_id.on_fail',
                 'activity_id.check_against', 'activity_id.role_ids', 'activity_id.input_ids',
                 'activity_id.output_deliverable_ids')
    def _compute_mp_pieces(self):
        Job = self.env['hr.job']
        activities = self.activity_id.sudo()
        status = Job._sgi_mp_status_map(activities) if activities else {}
        for role in self:
            activity = role.activity_id.sudo()
            if not activity:
                role.update({f: False for f in (
                    'mp_status', 'mp_status_detail', 'mp_number', 'mp_how', 'mp_where',
                    'mp_check_against', 'mp_done', 'mp_on_fail', 'mp_inputs', 'mp_outputs',
                    'mp_related', 'mp_escalates', 'mp_can_go')})
                continue
            extra = Job._sgi_mp_entry_extra(activity, status.get(activity))
            role.mp_status = extra['status']
            role.mp_status_detail = extra['status_detail']
            role.mp_number = activity.number or activity.legacy_number or ''
            role.mp_how = extra['how']
            role.mp_where = extra['where']
            role.mp_check_against = extra['check_against']
            role.mp_done = extra['done']
            role.mp_on_fail = extra['on_fail']
            role.mp_inputs = ", ".join(
                "%s (%d días hábiles)" % (name, days) if days else name
                for name, days in extra['inputs'])
            role.mp_outputs = ", ".join(extra['outputs'])
            role.mp_related = extra['related']
            role.mp_escalates = "; ".join(
                "%s%s" % (r._sgi_target_label(),
                          " (a los %d días hábiles)" % r.after_days if r.after_days else "")
                for r in activity.role_ids.filtered(lambda r: r.role == 'escala'))
            role.mp_can_go = bool(activity.odoo_menu_id or activity.odoo_ref
                                  or activity.measure_model_id)

    def action_mp_go(self):
        """«Ir a hacerlo»: el menú real de Odoo donde se ejecuta el paso."""
        self.ensure_one()
        return self.activity_id.sudo().action_open_odoo()

    def action_mp_instruction(self):
        """«Ver instructivo»: el archivo del IT de la actividad."""
        self.ensure_one()
        return self.activity_id.action_open_instruction()


class SgiMyProcedure(models.TransientModel):
    _name = 'sgi.my.procedure'
    _description = "Mi procedimiento (pantalla)"

    # hr.employee.public: mismo id que hr.employee y legible por cualquier
    # usuario interno (hr.employee no lo es en Odoo 19). Las lecturas de
    # fondo van con sudo sobre hr.employee.
    employee_id = fields.Many2one('hr.employee.public', string="Ver como: empleado")
    job_id = fields.Many2one(
        'hr.job', string="Ver como: puesto", compute='_compute_job_id', store=True, readonly=False)
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

    # Quién soy
    boss_id = fields.Many2one('hr.employee.public', string="Mi jefe", compute='_compute_who')
    department_id = fields.Many2one('hr.department', string="Área", compute='_compute_who')
    family_id = fields.Many2one('sgi.job.family', string="Familia de puestos", compute='_compute_who')
    process_ids = fields.Many2many('sgi.process', string="Procesos donde participa", compute='_compute_who')
    job_employee_ids = fields.Many2many(
        'hr.employee.public', string="Personas en el puesto", compute='_compute_who')

    # Estado en una línea
    late_count = fields.Integer(string="Atrasadas", compute='_compute_lists')
    ok_count = fields.Integer(string="Al día", compute='_compute_lists')
    unmeasured_count = fields.Integer(string="Sin medición automática", compute='_compute_lists')
    pending_ack_count = fields.Integer(string="Firmas pendientes", compute='_compute_lists')

    # Mis actividades (ejecuta / aprueba), escalamientos que recibe y lista corta
    role_ids = fields.Many2many(
        'sgi.activity.role', string="Mis actividades", compute='_compute_lists')
    received_role_ids = fields.Many2many(
        'sgi.activity.role', string="Escalamientos que recibe", compute='_compute_lists')
    short_role_ids = fields.Many2many(
        'sgi.activity.role', string="Participa o se entera", compute='_compute_lists')

    # Mis pendientes (viven en el usuario, no en el puesto)
    has_user = fields.Boolean(compute='_compute_lists')
    pending_action_ids = fields.Many2many(
        'sgi.action.line', string="Acciones abiertas o vencidas", compute='_compute_lists')
    pending_nc_ids = fields.Many2many(
        'quality.alert', string="NC a contestar", compute='_compute_lists')
    pending_measure_ids = fields.Many2many(
        'sgi.indicator.measure', string="Mediciones por capturar o validar", compute='_compute_lists')
    official_indicator_ids = fields.Many2many(
        'sgi.indicator', string="Indicadores oficiales a mi cargo", compute='_compute_lists')
    has_obligations = fields.Boolean(compute='_compute_lists')
    pending_legal_ids = fields.Many2many(
        'sgi.legal.requirement', string="Requisitos legales por evaluar", compute='_compute_lists')
    pending_doc_review_ids = fields.Many2many(
        'documents.document', string="Documentos por revisar (60 días)", compute='_compute_lists')

    # Mis documentos
    ack_ids = fields.Many2many(
        'sgi.document.ack', string="Mis acuses de lectura", compute='_compute_lists')
    document_ids = fields.Many2many(
        'documents.document', string="Documentos que aplican al puesto", compute='_compute_lists')

    # EPP del puesto y responsivas de entrega del empleado (PER-2)
    epp_required = fields.Text(string="EPP requerido por el puesto", compute='_compute_lists')
    epp_delivery_ids = fields.Many2many(
        'sgi.epp.delivery', string="Responsivas de EPP", compute='_compute_lists')
    epp_pending_sign = fields.Boolean(string="Responsiva de EPP por firmar", compute='_compute_lists')
    epp_label = fields.Char(string="EPP", compute='_compute_lists')

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

    @api.depends('employee_id', 'job_id')
    def _compute_who(self):
        Employee = self.env['hr.employee'].sudo()
        for wiz in self:
            emp = wiz._sgi_mp_employee()
            job = wiz.job_id.sudo()
            boss = emp.parent_id if emp and emp.parent_id else (job.department_id.manager_id if job else False)
            wiz.boss_id = boss.id if boss else False
            wiz.department_id = (emp.department_id if emp and emp.department_id else job.department_id) or False
            wiz.family_id = job.sgi_family_id if job else False
            if job:
                roles = self.env['sgi.activity.role'].sudo().search(job._sgi_roles_domain())
                wiz.process_ids = roles.filtered(lambda r: r.activity_id.active).mapped(
                    'activity_id.process_id').ids
                wiz.job_employee_ids = Employee.search([('job_id', '=', job.id)]).ids
            else:
                wiz.process_ids = False
                wiz.job_employee_ids = False

    @api.depends('employee_id', 'job_id')
    @api.depends_context('uid')
    def _compute_lists(self):
        Role = self.env['sgi.activity.role'].sudo()
        Ack = self.env['sgi.document.ack'].sudo()
        Doc = self.env['documents.document'].sudo()
        env = self.env
        today = fields.Date.context_today(self)
        for wiz in self:
            job = wiz.job_id.sudo()
            emp = wiz._sgi_mp_employee()
            user = emp.user_id if emp else False
            # --- Actividades del puesto y de su familia, solo activas.
            lists = job._sgi_mp_role_lists() if job else {
                'detail': Role, 'received': Role, 'short': Role}
            detail = lists['detail']
            wiz.role_ids = detail.ids
            wiz.received_role_ids = lists['received'].ids
            wiz.short_role_ids = lists['short'].ids
            counts = {'al_dia': 0, 'atrasada': 0, 'sin_medir': 0}
            for role in detail:
                counts[role.mp_status or 'sin_medir'] += 1
            wiz.late_count = counts['atrasada']
            wiz.ok_count = counts['al_dia']
            wiz.unmeasured_count = counts['sin_medir']
            # --- Acuses y documentos del puesto
            acks = Ack.search([('employee_id', '=', emp.id)], order='state, sgi_code') if emp else Ack
            wiz.ack_ids = acks.ids
            wiz.pending_ack_count = len(acks.filtered(lambda a: a.state == 'pendiente'))
            wiz.document_ids = Doc.search(
                [('sgi_state', '=', 'vigente'), ('sgi_job_ids', 'in', job.ids),
                 ('sgi_doc_type', '!=', 'mi_procedimiento')],
                order='sgi_doc_type, sgi_code, name').ids if job else False
            # --- EPP del puesto y responsivas del empleado
            wiz.epp_required = job.sgi_epp_required or False
            deliveries = env['sgi.epp.delivery'].sudo().search(
                [('employee_id', '=', emp.id)]) if emp else env['sgi.epp.delivery']
            wiz.epp_delivery_ids = deliveries.ids
            pending = deliveries.filtered(lambda d: d.state == 'entregada')
            wiz.epp_pending_sign = bool(pending)
            if pending:
                wiz.epp_label = "Responsiva %s por firmar" % ", ".join(pending.mapped('name'))
            elif deliveries:
                wiz.epp_label = "Responsiva %s firmada" % deliveries[0].name
            else:
                wiz.epp_label = "Sin responsiva de entrega" if job.sgi_epp_required else False
            # --- Mis pendientes: solo con usuario (viven en Odoo, no en el puesto)
            wiz.has_user = bool(user)
            if not user:
                wiz.pending_action_ids = False
                wiz.pending_nc_ids = False
                wiz.pending_measure_ids = False
                wiz.official_indicator_ids = False
                wiz.has_obligations = False
                wiz.pending_legal_ids = False
                wiz.pending_doc_review_ids = False
                continue
            wiz.pending_action_ids = env['sgi.action.line'].sudo().search(
                [('responsible_id', '=', user.id), ('state', 'in', ('abierta', 'vencida'))],
                order='date_commit, id').ids
            Alert = env['quality.alert'].sudo()
            wiz.pending_nc_ids = Alert.search(
                [('sgi_responsible_ids', 'in', user.id), ('sgi_stage_is_closing', '=', False),
                 ('sgi_stage_is_cancel', '=', False)], order='create_date').ids \
                if 'sgi_responsible_ids' in Alert._fields else False
            wiz.pending_measure_ids = env['sgi.indicator.measure'].sudo().search(
                [('indicator_id.responsible_id', '=', user.id),
                 ('state', 'in', ('pendiente', 'capturado')), ('period_date', '<=', today)],
                order='period_date', limit=50).ids
            wiz.official_indicator_ids = env['sgi.indicator'].sudo().search(
                [('responsible_id', '=', user.id), ('status', '=', 'oficial')], order='code').ids
            wiz.has_obligations = bool('qb.obligation' in env and env['qb.obligation'].sudo().search_count(
                [('user_id', '=', user.id), ('state', '=', 'confirmed')]))
            # DIR-1: requisitos legales del usuario que vencen en 60 días o ya vencieron.
            soon = fields.Date.add(today, days=60)
            wiz.pending_legal_ids = env['sgi.legal.requirement'].sudo().search(
                [('responsible_id', '=', user.id),
                 '|', ('next_eval_date', '<=', soon), ('expiry_date', '<=', soon)],
                order='next_eval_date, id').ids
            # DOC-4: documentos del usuario cuya próxima revisión vence en 60 días.
            wiz.pending_doc_review_ids = Doc.search(
                [('sgi_owner_id', '=', user.id), ('sgi_is_controlled', '=', True),
                 ('sgi_state', 'in', ('vigente', 'piloto')),
                 ('sgi_next_review_date', '!=', False), ('sgi_next_review_date', '<=', soon)],
                order='sgi_next_review_date, sgi_code').ids

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

    @api.model
    def action_open_for(self, employee_id=False, job_id=False):
        """«Ver su procedimiento» desde la ficha del empleado, la del puesto o
        una fila de Mi equipo, respetando el alcance del usuario."""
        wiz = self.create({'employee_id': employee_id or False, 'job_id': job_id or False})
        if wiz.employee_id and wiz.employee_id.id not in wiz.allowed_employee_ids.ids:
            raise UserError("Esa persona no está en tu equipo; solo ves a tu gente, tus "
                            "departamentos y los puestos de tus procesos.")
        if not wiz.employee_id and wiz.job_id and wiz.job_id.id not in wiz.allowed_job_ids.ids:
            raise UserError("Ese puesto no está en tu equipo.")
        return {
            'type': 'ir.actions.act_window',
            'name': "Mi procedimiento — %s" % (wiz.employee_id.name or wiz.job_id.name or ''),
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
            'res_id': check.id, 'view_mode': 'form', 'target': 'current',
            'name': "Revisión previa a publicar",
        }

    def action_open_obligations(self):
        """Mis obligaciones (módulo qb_obligation, si está instalado): la
        lista nativa de ese módulo, acotada al usuario."""
        self.ensure_one()
        if 'qb.obligation' not in self.env:
            raise UserError("El módulo de obligaciones no está instalado.")
        user = self._sgi_mp_employee().user_id
        return {
            'type': 'ir.actions.act_window', 'name': "Mis obligaciones",
            'res_model': 'qb.obligation', 'view_mode': 'list,form',
            'domain': [('user_id', '=', user.id), ('state', '=', 'confirmed')],
        }

    def _reload(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window', 'res_model': 'sgi.my.procedure',
            'res_id': self.id, 'view_mode': 'form', 'target': 'current',
            'name': "Mi procedimiento",
        }


class HrEmployeeMyProcedureOpen(models.Model):
    _inherit = 'hr.employee'

    def action_sgi_open_my_procedure(self):
        self.ensure_one()
        return self.env['sgi.my.procedure'].action_open_for(employee_id=self.id)


class HrJobMyProcedureOpen(models.Model):
    _inherit = 'hr.job'

    def action_sgi_open_my_procedure(self):
        self.ensure_one()
        return self.env['sgi.my.procedure'].action_open_for(job_id=self.id)


class SgiMyProcedureCheck(models.TransientModel):
    """Revisión previa a publicar, con listas nativas (antes HTML)."""
    _name = 'sgi.my.procedure.check'
    _description = "Mi procedimiento: revisión previa a publicar"

    ready_count = fields.Integer(
        string="Puestos listos para publicar", compute='_compute_result',
        help="Puestos con roles y con personas.")
    duplicate_job_ids = fields.Many2many(
        'hr.job', 'sgi_mp_check_dup_rel', string="Puestos duplicados", compute='_compute_result',
        help="Puestos con el mismo nombre normalizado. Un empleado en el duplicado sin roles "
             "abre su procedimiento y lo ve vacío.")
    no_job_employee_ids = fields.Many2many(
        'hr.employee.public', 'sgi_mp_check_nojob_rel', string="Empleados sin puesto",
        compute='_compute_result')
    job_without_roles_employee_ids = fields.Many2many(
        'hr.employee.public', 'sgi_mp_check_noroles_rel', string="Empleados en un puesto sin roles",
        compute='_compute_result')
    roles_without_people_job_ids = fields.Many2many(
        'hr.job', 'sgi_mp_check_nopeople_rel', string="Puestos con roles pero sin personas",
        compute='_compute_result')

    def _compute_result(self):
        Job = self.env['hr.job']
        for wiz in self:
            data = Job._sgi_my_procedure_precheck()
            wiz.ready_count = data['ready']
            wiz.duplicate_job_ids = [j.id for group in data['duplicates'] for j in group]
            wiz.no_job_employee_ids = data['no_job'].ids
            wiz.job_without_roles_employee_ids = data['job_without_roles'].ids
            wiz.roles_without_people_job_ids = data['roles_without_people'].ids


class HrEmployeePublicMyTeam(models.Model):
    """Inicio → Mi equipo: lista NATIVA de Odoo sobre hr.employee.public
    (legible por cualquier usuario interno): buscar, filtrar, agrupar y
    exportar. Las cifras de «Mi procedimiento» se calculan por puesto una
    sola vez por lote y los filtros las buscan con métodos propios."""
    _inherit = 'hr.employee.public'

    sgi_mp_late = fields.Integer(
        string="Atrasadas", compute='_compute_sgi_mp_stats', search='_search_sgi_mp_late',
        help="Actividades del puesto (ejecuta o aprueba) que hoy están atrasadas.")
    sgi_mp_ok = fields.Integer(string="Al día", compute='_compute_sgi_mp_stats')
    sgi_mp_unmeasured = fields.Integer(string="Sin medición automática", compute='_compute_sgi_mp_stats')
    sgi_mp_total = fields.Integer(string="Actividades", compute='_compute_sgi_mp_stats')
    sgi_mp_acks_pending = fields.Integer(
        string="Firmas pendientes", compute='_compute_sgi_mp_stats',
        search='_search_sgi_mp_acks_pending',
        help="Acuses de lectura pendientes de la persona (Mi procedimiento y demás documentos).")
    sgi_mp_ack_state = fields.Selection([
        ('sin_publicar', "Sin publicar"),
        ('pendiente', "Acuse pendiente"),
        ('leido', "Leído y entendido"),
    ], string="Mi procedimiento", compute='_compute_sgi_mp_stats', search='_search_sgi_mp_ack_state')

    @api.model
    def _sgi_mp_job_stats(self, jobs):
        """{puesto: (atrasadas, al día, sin medir, total)} una vez por puesto."""
        stats = {}
        for job in jobs.sudo():
            if not job:
                continue
            data = job._sgi_my_procedure_data()
            counts = {'atrasada': 0, 'al_dia': 0, 'sin_medir': 0}
            for section in data['sections']:
                for entry in section['entries']:
                    counts[entry['status']] += 1
            stats[job.id] = (counts['atrasada'], counts['al_dia'], counts['sin_medir'],
                             sum(counts.values()))
        return stats

    def _compute_sgi_mp_stats(self):
        Employee = self.env['hr.employee'].sudo()
        employees = Employee.browse(self.ids)
        stats = self._sgi_mp_job_stats(employees.job_id)
        pending = {}
        if employees:
            for emp, count in self.env['sgi.document.ack'].sudo()._read_group(
                    [('employee_id', 'in', employees.ids), ('state', '=', 'pendiente')],
                    ['employee_id'], ['__count']):
                pending[emp.id] = count
        ack_state = {emp.id: emp.sgi_my_procedure_ack_state for emp in employees}
        for rec in self:
            late, ok, unmeasured, total = stats.get(rec.job_id.id, (0, 0, 0, 0))
            rec.sgi_mp_late = late
            rec.sgi_mp_ok = ok
            rec.sgi_mp_unmeasured = unmeasured
            rec.sgi_mp_total = total
            rec.sgi_mp_acks_pending = pending.get(rec.id, 0)
            rec.sgi_mp_ack_state = ack_state.get(rec.id, 'sin_publicar')

    def _sgi_mp_ids_where(self, predicate):
        """Ids de empleados activos con puesto que cumplen el predicado sobre
        sus cifras (calculadas por lote)."""
        records = self.sudo().search([('job_id', '!=', False)])
        return [rec.id for rec in records if predicate(rec)]

    _NUMERIC_OPS = {
        '>': lambda a, b: a > b, '>=': lambda a, b: a >= b, '=': lambda a, b: a == b,
        '!=': lambda a, b: a != b, '<': lambda a, b: a < b, '<=': lambda a, b: a <= b,
    }

    @api.model
    def _search_sgi_mp_late(self, operator, value):
        if operator not in self._NUMERIC_OPS:
            raise UserError("Filtro no soportado sobre «Atrasadas».")
        op = self._NUMERIC_OPS[operator]
        return [('id', 'in', self._sgi_mp_ids_where(lambda r: op(r.sgi_mp_late, value)))]

    @api.model
    def _search_sgi_mp_acks_pending(self, operator, value):
        if operator not in self._NUMERIC_OPS:
            raise UserError("Filtro no soportado sobre «Firmas pendientes».")
        op = self._NUMERIC_OPS[operator]
        return [('id', 'in', self._sgi_mp_ids_where(lambda r: op(r.sgi_mp_acks_pending, value)))]

    @api.model
    def _search_sgi_mp_ack_state(self, operator, value):
        values = value if isinstance(value, (list, tuple)) else [value]
        if operator in ('=', 'in'):
            return [('id', 'in', self._sgi_mp_ids_where(lambda r: r.sgi_mp_ack_state in values))]
        if operator in ('!=', 'not in'):
            return [('id', 'in', self._sgi_mp_ids_where(lambda r: r.sgi_mp_ack_state not in values))]
        raise UserError("Filtro no soportado sobre «Mi procedimiento».")

    def action_sgi_open_my_procedure(self):
        self.ensure_one()
        return self.env['sgi.my.procedure'].action_open_for(employee_id=self.id)

    def action_sgi_print_my_procedure(self):
        """Imprimir desde la ficha pública: el PDF del puesto para esta persona."""
        self.ensure_one()
        return self.env['hr.employee'].sudo().browse(self.id).action_sgi_print_my_procedure()

    @api.model
    def _sgi_team(self):
        """Las personas que el usuario puede ver: su gente, sus departamentos
        y los puestos de sus procesos; MAST, administrador y Dirección de
        Operaciones ven a todos los que tienen puesto."""
        Wiz = self.env['sgi.my.procedure']
        me = Wiz._sgi_mp_my_employee()
        Employee = self.env['hr.employee'].sudo()
        if Wiz._sgi_mp_is_admin():
            return Employee.search([('job_id', '!=', False)])
        if not me:
            return Employee
        return me._sgi_mp_team_employees() - me

    @api.model
    def action_open_my_team(self):
        """Inicio → Mi equipo: la lista nativa acotada al equipo."""
        team = self._sgi_team()
        return {
            'type': 'ir.actions.act_window',
            'name': "Mi equipo",
            'res_model': 'hr.employee.public',
            'view_mode': 'list,kanban,form',
            'views': [(self.env.ref('quimibond_sgi.sgi_my_team_view_list').id, 'list'),
                      (False, 'kanban'), (False, 'form')],
            'search_view_id': [self.env.ref('quimibond_sgi.sgi_my_team_view_search').id, 'search'],
            'domain': [('id', 'in', team.ids)],
            'context': {'search_default_group_job': 1},
            'help': "<p class='o_view_nocontent_smiling_face'>No tienes personas a tu cargo en Odoo</p>"
                    "<p>Reportes directos, tu departamento o los puestos de tus procesos.</p>",
        }

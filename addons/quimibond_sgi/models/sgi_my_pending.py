# -*- coding: utf-8 -*-
"""56.3.0: «Mis pendientes» en una sola lista con semáforo.

Acciones, NC a contestar, mediciones por capturar o validar, requisitos
legales por evaluar y documentos por revisar, de una o varias personas, en
renglones de ``sgi.my.pending`` con Tipo, Qué, Proceso, Vence y Estado
(atrasada: vence antes de hoy; por vencer: en 7 días o menos; al día). Lo usan
la pantalla «Mi procedimiento» (un solo botón) y Mi equipo (semáforo por
persona y lista de pendientes del equipo).
"""
from datetime import timedelta

from dateutil.relativedelta import relativedelta

from odoo import api, fields, models

PENDING_KINDS = [
    ('accion', "Acción"),
    ('nc', "No conformidad"),
    ('medicion', "Medición"),
    ('legal', "Requisito legal"),
    ('documento', "Revisión de documento"),
]
PENDING_STATES = [
    ('atrasada', "Atrasada"),
    ('por_vencer', "Por vencer"),
    ('al_dia', "Al día"),
]
STATE_RANK = {'atrasada': 0, 'por_vencer': 1, 'al_dia': 2}
SOON_DAYS = 7
HORIZON_DAYS = 60


def pending_state(due, today):
    if not due:
        return 'al_dia'
    if due < today:
        return 'atrasada'
    if due <= today + timedelta(days=SOON_DAYS):
        return 'por_vencer'
    return 'al_dia'


class SgiMyPending(models.TransientModel):
    _name = 'sgi.my.pending'
    _description = "Mis pendientes (SGI)"
    _order = 'state_rank, date_due, id'

    user_id = fields.Many2one('res.users', string="Usuario", readonly=True)
    employee_id = fields.Many2one('hr.employee.public', string="Persona", readonly=True)
    kind = fields.Selection(PENDING_KINDS, string="Tipo", required=True, readonly=True)
    name = fields.Char(string="Qué", readonly=True)
    process_id = fields.Many2one('sgi.process', string="Proceso", readonly=True)
    date_due = fields.Date(string="Vence", readonly=True)
    state = fields.Selection(PENDING_STATES, string="Estado", readonly=True)
    state_rank = fields.Integer(readonly=True)
    res_model = fields.Char(readonly=True)
    res_id = fields.Integer(readonly=True)

    # ------------------------------------------------------------------
    # Fuentes: una búsqueda por tipo para todos los usuarios a la vez.
    # ------------------------------------------------------------------
    @api.model
    def _sgi_pending_records(self, users):
        """{tipo: registros (sudo)} de los usuarios dados; mismos criterios
        que tenía cada botón de la pantalla."""
        env = self.env
        today = fields.Date.context_today(self)
        soon = today + timedelta(days=HORIZON_DAYS)
        ids = users.ids
        Alert = env['quality.alert'].sudo()
        records = {
            'accion': env['sgi.action.line'].sudo().search(
                [('responsible_id', 'in', ids), ('state', 'in', ('abierta', 'vencida'))],
                order='date_commit, id'),
            'nc': Alert.search(
                [('sgi_responsible_ids', 'in', ids), ('sgi_stage_is_closing', '=', False),
                 ('sgi_stage_is_cancel', '=', False)], order='create_date')
            if 'sgi_responsible_ids' in Alert._fields else Alert,
            'medicion': env['sgi.indicator.measure'].sudo().search(
                [('indicator_id.responsible_id', 'in', ids),
                 ('state', 'in', ('pendiente', 'capturado')), ('period_date', '<=', today)],
                order='period_date'),
            'legal': env['sgi.legal.requirement'].sudo().search(
                [('responsible_id', 'in', ids),
                 '|', ('next_eval_date', '<=', soon), ('expiry_date', '<=', soon)],
                order='next_eval_date, id'),
            'documento': env['documents.document'].sudo().search(
                [('sgi_owner_id', 'in', ids), ('sgi_is_controlled', '=', True),
                 ('sgi_state', 'in', ('vigente', 'piloto')),
                 ('sgi_next_review_date', '!=', False), ('sgi_next_review_date', '<=', soon)],
                order='sgi_next_review_date, sgi_code'),
        }
        return records

    @api.model
    def _sgi_row(self, kind, rec):
        """Valores de un renglón: qué, proceso y fecha de vencimiento."""
        if kind == 'accion':
            return {'name': rec.name, 'date_due': rec.date_commit,
                    'process_id': rec.alert_id.sgi_process_id.id if rec.alert_id else False}
        if kind == 'nc':
            dues = [due for due, st in ((rec.sgi_due_containment, rec.sgi_containment_state),
                                        (rec.sgi_due_root_cause, rec.sgi_root_cause_state),
                                        (rec.sgi_due_plan, rec.sgi_plan_state))
                    if due and st != 'hecha']
            due = min(dues) if dues else rec.sgi_effectiveness_due
            label = " ".join(p for p in (rec.name, rec.title if 'title' in rec._fields else '') if p)
            return {'name': "Contestar NC %s" % label, 'date_due': due,
                    'process_id': rec.sgi_process_id.id}
        if kind == 'medicion':
            indicator = rec.indicator_id
            if indicator.frequency == 'weekly':
                due = rec.period_date + timedelta(days=6)
            else:
                due = rec.period_date + relativedelta(day=31)
            return {'name': "Medir %s %s (%s)" % (indicator.code or '', indicator.name or '',
                                                  rec.period_date.strftime('%m/%Y')),
                    'date_due': due, 'process_id': indicator.process_id.id}
        if kind == 'legal':
            dates = [d for d in (rec.next_eval_date, rec.expiry_date) if d]
            return {'name': "Evaluar %s" % (rec.display_name or rec.name),
                    'date_due': min(dates) if dates else False,
                    'process_id': rec.process_ids[:1].id}
        return {'name': "Revisar %s %s" % (rec.sgi_code or '', rec.name or ''),
                'date_due': rec.sgi_next_review_date, 'process_id': rec.sgi_process_id.id}

    @api.model
    def _sgi_row_users(self, kind, rec):
        if kind == 'accion':
            return rec.responsible_id
        if kind == 'nc':
            return rec.sgi_responsible_ids
        if kind == 'medicion':
            return rec.indicator_id.responsible_id
        if kind == 'legal':
            return rec.responsible_id
        return rec.sgi_owner_id

    @api.model
    def _sgi_pending_values(self, users):
        """{usuario.id: [valores de renglón]} ya con estado y orden."""
        today = fields.Date.context_today(self)
        result = {uid: [] for uid in users.ids}
        for kind, records in self._sgi_pending_records(users).items():
            for rec in records:
                row = dict(self._sgi_row(kind, rec), kind=kind,
                           res_model=rec._name, res_id=rec.id)
                row['state'] = pending_state(row['date_due'], today)
                row['state_rank'] = STATE_RANK[row['state']]
                for user in self._sgi_row_users(kind, rec):
                    if user.id in result:
                        result[user.id].append(dict(row, user_id=user.id))
        for rows in result.values():
            rows.sort(key=lambda r: (r['state_rank'], r['date_due'] or fields.Date.to_date('9999-12-31')))
        return result

    @api.model
    def _sgi_summary(self, users):
        """{usuario.id: (total, atrasadas, peor estado)} para botones y Mi equipo."""
        summary = {}
        for uid, rows in self._sgi_pending_values(users).items():
            late = sum(1 for r in rows if r['state'] == 'atrasada')
            worst = rows[0]['state'] if rows else False
            summary[uid] = (len(rows), late, worst)
        return summary

    @api.model
    def _sgi_build(self, employees):
        """Renglones de los empleados dados (hr.employee, sudo) para abrirlos
        en la lista; los anteriores del mismo usuario y personas se reemplazan."""
        employees = employees.sudo().filtered('user_id')
        self.search([('create_uid', '=', self.env.uid),
                     ('employee_id', 'in', employees.ids)]).unlink()
        values = self._sgi_pending_values(employees.user_id)
        by_user = {emp.user_id.id: emp for emp in employees}
        vals_list = []
        for uid, rows in values.items():
            for row in rows:
                vals_list.append(dict(row, employee_id=by_user[uid].id))
        return self.create(vals_list)

    @api.model
    def _sgi_action(self, rows, name, group_by_person=False):
        context = {'create': False}
        if group_by_person:
            context['search_default_group_employee'] = 1
        return {
            'type': 'ir.actions.act_window', 'name': name, 'res_model': self._name,
            'view_mode': 'list', 'domain': [('id', 'in', rows.ids)],
            'views': [(self.env.ref('quimibond_sgi.sgi_my_pending_view_list').id, 'list')],
            'search_view_id': [self.env.ref('quimibond_sgi.sgi_my_pending_view_search').id, 'search'],
            'context': context,
        }

    def action_open(self):
        """Abre el registro de origen (la acción, la NC, la medición…)."""
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window', 'res_model': self.res_model, 'res_id': self.res_id,
            'view_mode': 'form', 'target': 'current',
        }


class SgiMyProcedurePending(models.TransientModel):
    _inherit = 'sgi.my.procedure'

    pending_total = fields.Integer(string="Mis pendientes", compute='_compute_pending_summary')
    pending_late = fields.Integer(string="Atrasadas", compute='_compute_pending_summary')

    @api.depends('employee_id')
    @api.depends_context('uid')
    def _compute_pending_summary(self):
        Pending = self.env['sgi.my.pending']
        for wiz in self:
            user = wiz._sgi_mp_employee().user_id
            total, late, _worst = Pending._sgi_summary(user).get(user.id, (0, 0, False)) \
                if user else (0, 0, False)
            wiz.pending_total = total
            wiz.pending_late = late

    def action_show_pending(self):
        self.ensure_one()
        rows = self.env['sgi.my.pending']._sgi_build(self._sgi_mp_employee())
        name = "Mis pendientes" if self.is_me else "Pendientes — %s" % (self.employee_id.name or '')
        return self.env['sgi.my.pending']._sgi_action(rows, name)


class HrEmployeePublicPending(models.Model):
    """Mi equipo: el mismo semáforo por persona."""
    _inherit = 'hr.employee.public'

    sgi_mp_pending_total = fields.Integer(
        string="Pendientes", compute='_compute_sgi_mp_pending',
        search='_search_sgi_mp_pending_total')
    sgi_mp_pending_late = fields.Integer(
        string="Pendientes atrasados", compute='_compute_sgi_mp_pending',
        search='_search_sgi_mp_pending_late')
    sgi_mp_pending_state = fields.Selection(
        PENDING_STATES, string="Semáforo", compute='_compute_sgi_mp_pending',
        search='_search_sgi_mp_pending_state',
        help="El peor estado de sus pendientes: atrasada, por vencer o al día.")

    @api.model
    def _sgi_pending_by_employee(self, employees):
        employees = employees.sudo()
        summary = self.env['sgi.my.pending']._sgi_summary(employees.user_id)
        return {emp.id: summary.get(emp.user_id.id, (0, 0, False)) for emp in employees}

    def _compute_sgi_mp_pending(self):
        values = self._sgi_pending_by_employee(self.env['hr.employee'].sudo().browse(self.ids))
        for rec in self:
            total, late, worst = values.get(rec.id, (0, 0, False))
            rec.sgi_mp_pending_total = total
            rec.sgi_mp_pending_late = late
            rec.sgi_mp_pending_state = worst or ('al_dia' if total else False)

    def _sgi_pending_ids_where(self, predicate):
        employees = self.env['hr.employee'].sudo().search([('user_id', '!=', False)])
        values = self._sgi_pending_by_employee(employees)
        return [emp_id for emp_id, vals in values.items() if predicate(vals)]

    @api.model
    def _search_sgi_mp_pending_total(self, operator, value):
        op = self._NUMERIC_OPS[operator]
        return [('id', 'in', self._sgi_pending_ids_where(lambda v: op(v[0], value)))]

    @api.model
    def _search_sgi_mp_pending_late(self, operator, value):
        op = self._NUMERIC_OPS[operator]
        return [('id', 'in', self._sgi_pending_ids_where(lambda v: op(v[1], value)))]

    @api.model
    def _search_sgi_mp_pending_state(self, operator, value):
        # Import local: cargar el módulo de la pantalla desde aquí a nivel de
        # módulo cambiaría el orden del registro (regla en CLAUDE.md).
        from .sgi_my_procedure_screen import _sgi_as_list
        values = _sgi_as_list(value)
        if operator in ('=', 'in'):
            return [('id', 'in', self._sgi_pending_ids_where(lambda v: v[2] in values))]
        return [('id', 'in', self._sgi_pending_ids_where(lambda v: v[2] not in values))]

    def action_sgi_open_pending(self):
        self.ensure_one()
        employee = self.env['hr.employee'].sudo().browse(self.id)
        rows = self.env['sgi.my.pending']._sgi_build(employee)
        return self.env['sgi.my.pending']._sgi_action(rows, "Pendientes — %s" % self.name)

    def action_sgi_team_pending(self):
        """«Pendientes del equipo»: una lista, agrupada por persona."""
        team = self.env['hr.employee'].sudo().browse(self.ids) if self else self._sgi_team()
        rows = self.env['sgi.my.pending']._sgi_build(team)
        return self.env['sgi.my.pending']._sgi_action(rows, "Pendientes del equipo", group_by_person=True)

# -*- coding: utf-8 -*-
"""56.3.0: «Mis pendientes» en una sola lista con semáforo.

Acciones, NC a contestar, mediciones por capturar o validar, requisitos
legales por evaluar y documentos por revisar, de una o varias personas, en
renglones de ``sgi.my.pending`` con Tipo, Qué, Proceso, Vence y Estado
(atrasada: vence antes de hoy; por vencer: en 7 días o menos; al día). Lo usan
la pantalla «Mi procedimiento» (un solo botón) y Mi equipo (semáforo por
persona y lista de pendientes del equipo).

56.36.0 (entrega 8a, auditoría 2026-09, D-04: «Mis pendientes con todo
adentro»):

- G-001 / I-006: la medición ``pendiente`` es «Capturar» solo si alguien
  tiene que capturarla (indicador manual, o automático que no pudo calcular);
  la ``capturado`` es «Validar» y le toca al dueño del indicador en 3 días
  hábiles desde la captura (decisión 3 de la tanda 2). El vencimiento de la
  captura es el día hábil en que se mide (el N-ésimo del mes siguiente, o el
  primero después de la semana) más el plazo de captura en días hábiles: ya
  no nace atrasada.
- I-001: dos fuentes por persona (con o sin usuario): las actividades de su
  procedimiento que van atrasadas (ejecuta; desde 56.38.1 ya no aprueba) y los acuses de
  «leído y entendido» pendientes. Además, las firmas de Firma electrónica
  por hacer (por usuario).
- I-012: quien tiene «Escala» en una actividad la recibe cuando el atraso
  pasa de sus días hábiles.
- I-007: semanas como «semana del dd/mm/aaaa» y documentos con su título
  limpio (sin la clave del Dropbox).
- I-021: el semáforo de Mi equipo sale de la persona, no del usuario: la
  gente de planta sin usuario ya tiene semáforo (sus actividades y acuses).
- Un solo semáforo: el estado de las actividades es el de Mi procedimiento
  (``hr.job._sgi_mp_status_map``) y el de los renglones, ``pending_state``.

56.38.1 (decisiones de Jose sobre la entrega 8a):

- Capturar medición: 5 días hábiles (antes 3); validar sigue en 3 y el
  acuse en 5. El aviso «Capturar indicador» del cron usa la misma fecha.
- El aprobador solo ve lo que ya le toca (aprobación nativa, solicitud o
  firma, que nacen cuando el ejecutor actuó); el atraso del ejecutor va al
  rol «Escala».
"""
from datetime import timedelta

from dateutil.relativedelta import relativedelta

from odoo import api, fields, models
from odoo.exceptions import AccessError, UserError

# sgi_calendar no define modelos: importarlo aquí no cambia el registro.
from .sgi_calendar import sgi_add_business_days, sgi_nth_business_day

PENDING_KINDS = [
    ('accion', "Acción"),
    ('nc', "No conformidad"),
    ('medicion', "Capturar medición"),
    ('validacion', "Validar medición"),
    ('legal', "Requisito legal"),
    ('documento', "Revisión de documento"),
    ('aprobacion', "Aprobación"),
    ('solicitud', "Solicitud por aprobar"),
    ('actividad', "Actividad atrasada"),
    ('acuse', "Acuse de lectura"),
    ('firma', "Firma"),
]
# Los tipos que salen de la persona (hr.employee) y no del usuario: la gente
# de planta sin usuario también los tiene.
EMPLOYEE_KINDS = ('actividad', 'acuse')
PENDING_STATES = [
    ('atrasada', "Atrasada"),
    ('por_vencer', "Por vencer"),
    ('al_dia', "Al día"),
]
STATE_RANK = {'atrasada': 0, 'por_vencer': 1, 'al_dia': 2}
SOON_DAYS = 7
HORIZON_DAYS = 60
# Plazos en días hábiles (parámetros del sistema; default entre paréntesis).
CAPTURE_DAYS_PARAM = 'quimibond_sgi.measure_capture_business_days'   # (5)
VALIDATE_DAYS_PARAM = 'quimibond_sgi.measure_validate_business_days'  # (3)
ACK_DAYS_PARAM = 'quimibond_sgi.ack_business_days'                    # (5)
# Indicadores automáticos cuya medición sí tiene que capturar alguien.
_CALC_NEEDS_PERSON = ('error', 'sin_formula', 'manual')


def pending_state(due, today):
    if not due:
        return 'al_dia'
    if due < today:
        return 'atrasada'
    if due <= today + timedelta(days=SOON_DAYS):
        return 'por_vencer'
    return 'al_dia'


def _int_param(env, key, default):
    raw = env['ir.config_parameter'].sudo().get_param(key)
    try:
        return int(raw) if raw not in (None, False, '') else default
    except (TypeError, ValueError):
        return default


class SgiIndicatorMeasureDue(models.Model):
    """G-001 / I-006: una sola regla de vencimiento de la medición."""
    _inherit = 'sgi.indicator.measure'

    captured_date = fields.Date(
        string="Capturada el", readonly=True, copy=False,
        help="Día en que la medición pasó a «Capturado» (a mano o por el cálculo "
             "automático). El dueño del indicador tiene 3 días hábiles desde aquí "
             "para validarla.")

    @api.model_create_multi
    def create(self, vals_list):
        today = fields.Date.context_today(self)
        for vals in vals_list:
            if vals.get('state') == 'capturado' and not vals.get('captured_date'):
                vals['captured_date'] = today
        return super().create(vals_list)

    def write(self, vals):
        newly = self.browse()
        if vals.get('state') == 'capturado' and 'captured_date' not in vals:
            # Recalcular una ya capturada no mueve su plazo.
            newly = self.filtered(lambda m: m.state != 'capturado')
        res = super().write(vals)
        if newly:
            newly.write({'captured_date': fields.Date.context_today(self)})
        return res

    def _sgi_run_day(self):
        """Día hábil en que se mide el periodo: el N-ésimo del mes siguiente
        (``quimibond_sgi.monthly_measure_business_day``, 3) o el primero
        después del domingo de la semana medida."""
        self.ensure_one()
        period = self.period_date
        if self.indicator_id.frequency == 'weekly':
            sunday = period - timedelta(days=period.weekday()) + timedelta(days=6)
            return sgi_add_business_days(self.env, sunday, 1)
        nth = _int_param(self.env, 'quimibond_sgi.monthly_measure_business_day', 3) or 3
        following = period.replace(day=1) + relativedelta(months=1)
        return sgi_nth_business_day(self.env, following.year, following.month, nth)

    def _sgi_capture_due(self):
        """«Capturar»: el día en que se mide más el plazo de captura (5 días
        hábiles desde 56.38.1, decisión de Jose). Es la única regla: la usan
        Mis pendientes y el aviso «Capturar indicador» del cron."""
        self.ensure_one()
        return sgi_add_business_days(self.env, self._sgi_run_day(),
                                     _int_param(self.env, CAPTURE_DAYS_PARAM, 5))

    def _sgi_validate_due(self):
        """«Validar»: 3 días hábiles desde la captura (decisión 3, tanda 2).
        Sin fecha de captura (mediciones anteriores a 56.36.0), desde que se
        creó: el cálculo automático la crea ya capturada."""
        self.ensure_one()
        start = self.captured_date or (self.create_date and self.create_date.date()) \
            or fields.Date.context_today(self)
        return sgi_add_business_days(self.env, start, _int_param(self.env, VALIDATE_DAYS_PARAM, 3))

    def _sgi_period_label(self):
        """«semana del 14/09/2026» o «09/2026» (I-007)."""
        self.ensure_one()
        if not self.period_date:
            return ''
        if self.indicator_id.frequency == 'weekly':
            monday = self.period_date - timedelta(days=self.period_date.weekday())
            return "semana del %s" % monday.strftime('%d/%m/%Y')
        return self.period_date.strftime('%m/%Y')

    def _sgi_needs_capture(self):
        """Una medición pendiente es de alguien solo si el indicador se captura
        a mano o si su cálculo automático no pudo dar el dato."""
        self.ensure_one()
        indicator = self.indicator_id
        if indicator.calc_mode == 'manual':
            return True
        return bool('calc_status' in indicator._fields and indicator.calc_status in _CALC_NEEDS_PERSON)


class SgiMyPending(models.TransientModel):
    """Mis pendientes: bandeja de cada persona con actividades atrasadas, firmas, acuses, capturas,
    validaciones y aprobaciones. Se recalcula al abrirla; no guarda historia."""
    _name = 'sgi.my.pending'
    _description = "Mis pendientes (SGI)"
    _order = 'state_rank, date_due, id'
    _SGI_REQUEST_DAYS = 3  # días para contestar una solicitud de Aprobaciones

    user_id = fields.Many2one('res.users', string="Usuario", readonly=True)
    employee_id = fields.Many2one('hr.employee.public', string="Persona", readonly=True)
    kind = fields.Selection(PENDING_KINDS, string="Tipo", required=True, readonly=True)
    name = fields.Char(string="Qué", readonly=True)
    process_id = fields.Many2one('sgi.process', string="Proceso", readonly=True, ondelete='restrict')
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
        que tenía cada botón de la pantalla. Los tipos de la persona
        (EMPLOYEE_KINDS) salen de ``_sgi_employee_rows``."""
        env = self.env
        today = fields.Date.context_today(self)
        soon = today + timedelta(days=HORIZON_DAYS)
        ids = users.ids
        Alert = env['quality.alert'].sudo()
        measures = env['sgi.indicator.measure'].sudo().search(
            [('indicator_id.responsible_id', 'in', ids), ('indicator_id.active', '=', True),
             ('state', 'in', ('pendiente', 'capturado')), ('period_date', '<=', today)],
            order='period_date')
        records = {
            'accion': env['sgi.action.line'].sudo().search(
                [('responsible_id', 'in', ids), ('state', 'in', ('abierta', 'vencida'))],
                order='date_commit, id'),
            'nc': Alert.search(
                [('sgi_responsible_ids', 'in', ids), ('sgi_stage_is_closing', '=', False),
                 ('sgi_stage_is_cancel', '=', False)], order='create_date')
            if 'sgi_responsible_ids' in Alert._fields else Alert,
            # G-001: «Capturar» solo lo que alguien tiene que capturar.
            'medicion': measures.filtered(lambda m: m.state == 'pendiente' and m._sgi_needs_capture()),
            # I-006: lo ya calculado lo valida el dueño.
            'validacion': measures.filtered(lambda m: m.state == 'capturado'),
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
        # 56.5.0: aprobaciones nativas por dar (reglas de aprobación de Odoo).
        if 'studio.approval.request' in env:
            records['aprobacion'] = env['studio.approval.request'].sudo().search(
                [('mail_activity_id.user_id', 'in', ids)], order='create_date')
        # 56.6.0: solicitudes de Aprobaciones esperando a la persona.
        records['solicitud'] = env['approval.approver'].sudo().search(
            [('user_id', 'in', ids), ('status', '=', 'pending'),
             ('request_id.request_status', '=', 'pending')], order='create_date')
        # 56.36.0 (D-04): firmas de Firma electrónica por hacer.
        if 'sign.request.item' in env:
            records['firma'] = env['sign.request.item'].sudo().search(
                [('partner_id', 'in', users.partner_id.ids), ('state', '=', 'sent'),
                 ('sign_request_id.state', '=', 'sent')], order='create_date')
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
        if kind in ('medicion', 'validacion'):
            indicator = rec.indicator_id
            verb = "Capturar" if kind == 'medicion' else "Validar"
            return {'name': "%s %s %s (%s)" % (verb, indicator.code or '', indicator.name or '',
                                               rec._sgi_period_label()),
                    'date_due': rec._sgi_capture_due() if kind == 'medicion' else rec._sgi_validate_due(),
                    'process_id': indicator.process_id.id}
        if kind == 'legal':
            dates = [d for d in (rec.next_eval_date, rec.expiry_date) if d]
            return {'name': "Evaluar %s" % (rec.display_name or rec.name),
                    'date_due': min(dates) if dates else False,
                    'process_id': rec.process_ids[:1].id}
        if kind == 'solicitud':
            req = rec.request_id
            category = req.category_id
            return {'name': "Aprobar %s" % (req.name or category.name or ''),
                    # Vence 3 días después de enviada: antes vencía el mismo
                    # día del envío y todo salía «Atrasada».
                    'date_due': req.date_confirmed and fields.Date.add(
                        fields.Date.to_date(req.date_confirmed), days=self._SGI_REQUEST_DAYS),
                    'process_id': (category.sgi_role_id.activity_id.process_id.id
                                   if 'sgi_role_id' in category._fields else False)
                    or req.sgi_affected_process_ids[:1].id}
        if kind == 'aprobacion':
            rule = rec.rule_id
            record = self.env[rule.model_name].sudo().browse(rec.res_id).exists() if rule.model_name else None
            return {'name': "Aprobar %s%s" % (rule.message or rule.name or '',
                                                (" — %s" % record.display_name) if record else ''),
                    'date_due': rec.mail_activity_id.date_deadline,
                    'process_id': rule.sgi_role_id.activity_id.process_id.id
                    if 'sgi_role_id' in rule._fields else False}
        if kind == 'firma':
            request = rec.sign_request_id
            due = request.validity if 'validity' in request._fields else False
            if not due and request.create_date:
                due = sgi_add_business_days(self.env, request.create_date, self._SGI_REQUEST_DAYS)
            return {'name': "Firmar %s" % (request.reference or request.display_name or ''),
                    'date_due': due, 'process_id': False}
        # I-007: el documento se nombra por su título limpio, sin la clave vieja.
        title = rec.sgi_title if 'sgi_title' in rec._fields else False
        return {'name': "Revisar %s" % (title or rec.name or ''),
                'date_due': rec.sgi_next_review_date, 'process_id': rec.sgi_process_id.id}

    @api.model
    def _sgi_row_users(self, kind, rec):
        if kind == 'accion':
            return rec.responsible_id
        if kind == 'nc':
            return rec.sgi_responsible_ids
        if kind in ('medicion', 'validacion'):
            return rec.indicator_id.responsible_id
        if kind == 'legal':
            return rec.responsible_id
        if kind == 'aprobacion':
            return rec.mail_activity_id.user_id
        if kind == 'solicitud':
            return rec.user_id
        if kind == 'firma':
            return rec.partner_id.user_ids
        return rec.sgi_owner_id

    # ------------------------------------------------------------------
    # Tipos de la persona: actividades atrasadas, escalamientos y acuses.
    # ------------------------------------------------------------------
    @api.model
    def _sgi_activity_late_due(self, activity, today):
        """Fecha en que la actividad quedó atrasada, o False si no se sabe.
        Con vencimiento periódico, el vencimiento que se pasó (el del periodo
        en curso si ya pasó; si no, el del periodo anterior); sin él, la
        última ejecución más la ventana de su cadencia."""
        due = activity._sgi_periodic_due(today)
        if due is not None:
            if due < today:
                return due
            start = activity._sgi_period_start(today)
            previous = activity._sgi_periodic_due(start - timedelta(days=1)) if start else None
            return previous or False
        days = activity._SGI_CADENCE_DAYS.get(activity.measure_cadence)
        last = activity.measure_last_date
        if days and last:
            return fields.Datetime.to_datetime(last).date() + timedelta(days=days)
        return False

    @api.model
    def _sgi_employee_rows(self, employees):
        """{empleado.id: [renglones]} de los tipos de la persona (I-001,
        I-012): actividades que ejecuta y van atrasadas en Mi procedimiento,
        escalamientos cuyo atraso ya pasó sus días hábiles y acuses de
        «leído y entendido» pendientes. Solo empleados de la empresa del SGI
        (D-03).

        56.38.1 (decisión de Jose): el rol «Aprueba» ya no recibe renglón
        por el atraso del ejecutor. La actividad del procedimiento no sabe
        si el ejecutor ya cumplió y falta la aprobación: su semáforo sale de
        la evidencia (``measure_state`` y entradas con plazo vencido), que no
        distingue «no la hizo» de «la hizo y falta aprobar». Lo que sí le
        toca al aprobador llega por Odoo cuando el ejecutor ya actuó: la
        regla de aprobación nativa del botón (tipo ``aprobacion``), la
        solicitud de Aprobaciones (``solicitud``) o la firma (``firma``). El
        atraso del ejecutor le llega a quien tiene «Escala», pasados sus días
        hábiles."""
        employees = employees.sudo()
        result = {emp.id: [] for emp in employees}
        if not employees:
            return result
        env = self.env
        today = fields.Date.context_today(self)
        company = env['sgi.config']._sgi_company()
        employees = employees.filtered(lambda e: not e.company_id or e.company_id == company)
        roles_by_emp = {}
        # 57.13.0: los escalamientos a un rol relativo («Dueño del proceso»)
        # no son de un puesto: le llegan al dueño del proceso de la actividad,
        # o a su jefe si el dueño también la ejecuta (sgi_relative_roles).
        relative = env['sgi.activity.role'].sudo()._sgi_relative_escalations(employees)
        for emp in employees:
            detail = emp.sgi_mp_role_ids.filtered(lambda r: r.role == 'ejecuta')
            received = emp.sgi_mp_received_role_ids | relative.get(emp.id, env['sgi.activity.role'])
            roles_by_emp[emp.id] = (detail, received)
        all_roles = env['sgi.activity.role'].sudo()
        for detail, received in roles_by_emp.values():
            all_roles |= detail | received
        all_roles = all_roles.filtered(
            lambda r: r.activity_id.active and r.activity_id.process_id.active
            and (not r.company_id or r.company_id == company))
        activities = all_roles.activity_id
        status = env['hr.job']._sgi_mp_status_map(activities) if activities else {}
        late = activities.filtered(lambda a: status.get(a, ('',))[0] == 'atrasada')
        late_due = {act.id: self._sgi_activity_late_due(act, today) for act in late}

        def label(activity):
            number = activity.number or activity.legacy_number or ''
            return ("%s %s" % (number, activity.name or '')).strip()

        escalation_dates = {}
        for emp in employees:
            detail, received = roles_by_emp[emp.id]
            rows = result[emp.id]
            seen = set()
            for role in detail & all_roles:
                activity = role.activity_id
                if activity not in late or activity.id in seen:
                    continue
                seen.add(activity.id)
                detail_txt = status[activity][2]
                rows.append({
                    'kind': 'actividad', 'name': "Hacer %s%s" % (
                        label(activity), (" — %s" % detail_txt) if detail_txt else ''),
                    'date_due': late_due[activity.id], 'process_id': activity.process_id.id,
                    'res_model': 'sgi.process.activity', 'res_id': activity.id,
                    # Van atrasadas por definición, aunque no se sepa desde cuándo.
                    'state': 'atrasada'})
            for role in received & all_roles:
                activity = role.activity_id
                due = late_due.get(activity.id) if activity in late else False
                if not due or activity.id in seen:
                    continue
                key = (due, role.after_days or 0)
                if key not in escalation_dates:  # una consulta al calendario por fecha y plazo
                    escalation_dates[key] = sgi_add_business_days(env, due, key[1])
                escalated = escalation_dates[key]
                if escalated > today:
                    continue
                seen.add(activity.id)
                rows.append({
                    'kind': 'actividad', 'name': "Escalamiento: %s (atrasada desde el %s)" % (
                        label(activity), due.strftime('%d/%m/%Y')),
                    'date_due': escalated, 'process_id': activity.process_id.id,
                    'res_model': 'sgi.process.activity', 'res_id': activity.id,
                    'state': 'atrasada'})
        ack_days = _int_param(env, ACK_DAYS_PARAM, 5)
        acks = env['sgi.document.ack'].sudo().search(
            [('employee_id', 'in', employees.ids), ('state', '=', 'pendiente'),
             ('document_id.active', '=', True)], order='create_date')
        ack_dues = {}
        for ack in acks:
            doc = ack.document_id
            title = doc.sgi_title if 'sgi_title' in doc._fields else False
            revision = doc.sgi_revision_label if 'sgi_revision_label' in doc._fields else False
            start = (ack.create_date or fields.Datetime.now()).date()
            if start not in ack_dues:
                ack_dues[start] = sgi_add_business_days(env, start, ack_days)
            due = ack_dues[start]
            result[ack.employee_id.id].append({
                'kind': 'acuse', 'name': "Leer y firmar %s%s" % (
                    title or doc.name or '', (" (rev. %s)" % revision) if revision else ''),
                'date_due': due,
                'process_id': doc.sgi_process_id.id if 'sgi_process_id' in doc._fields else False,
                'res_model': 'sgi.document.ack', 'res_id': ack.id})
        return result

    @api.model
    def _sgi_finish(self, row, today):
        if not row.get('state'):
            row['state'] = pending_state(row['date_due'], today)
        row['state_rank'] = STATE_RANK[row['state']]
        return row

    @staticmethod
    def _sgi_sort(rows):
        rows.sort(key=lambda r: (r['state_rank'], r['date_due'] or fields.Date.to_date('9999-12-31')))

    @api.model
    def _sgi_user_employees(self, users):
        """{usuario.id: empleado} de la empresa del SGI."""
        company = self.env['sgi.config']._sgi_company()
        found = {}
        for emp in self.env['hr.employee'].sudo().search(
                [('user_id', 'in', users.ids), ('company_id', 'in', (company.id, False))]):
            found.setdefault(emp.user_id.id, emp)
        return found

    @api.model
    def _sgi_pending_values(self, users):
        """{usuario.id: [valores de renglón]} ya con estado y orden."""
        today = fields.Date.context_today(self)
        result = {uid: [] for uid in users.ids}
        for kind, records in self._sgi_pending_records(users).items():
            for rec in records:
                row = dict(self._sgi_row(kind, rec), kind=kind,
                           res_model=rec._name, res_id=rec.id)
                if kind == 'aprobacion':
                    # «Abrir» lleva al documento que espera la aprobación.
                    row.update(res_model=rec.rule_id.model_name, res_id=rec.res_id)
                elif kind == 'solicitud':
                    row.update(res_model='approval.request', res_id=rec.request_id.id)
                self._sgi_finish(row, today)
                for user in self._sgi_row_users(kind, rec):
                    if user.id in result:
                        result[user.id].append(dict(row, user_id=user.id))
        by_user = self._sgi_user_employees(users)
        if by_user:
            emp_rows = self._sgi_employee_rows(self.env['hr.employee'].sudo().browse(
                [emp.id for emp in by_user.values()]))
            for uid, emp in by_user.items():
                for row in emp_rows.get(emp.id, []):
                    result[uid].append(dict(self._sgi_finish(row, today), user_id=uid))
        for rows in result.values():
            self._sgi_sort(rows)
        return result

    @api.model
    def _sgi_pending_values_employees(self, employees):
        """{empleado.id: [renglones]}: con usuario, lo de su usuario (que ya
        trae lo de la persona); sin usuario, lo de la persona (I-021)."""
        employees = employees.sudo()
        today = fields.Date.context_today(self)
        with_user = employees.filtered('user_id')
        by_user = self._sgi_pending_values(with_user.user_id) if with_user else {}
        without = employees - with_user
        emp_rows = self._sgi_employee_rows(without) if without else {}
        result = {}
        for emp in employees:
            if emp.user_id:
                result[emp.id] = [dict(row) for row in by_user.get(emp.user_id.id, [])]
            else:
                rows = [self._sgi_finish(dict(row), today) for row in emp_rows.get(emp.id, [])]
                self._sgi_sort(rows)
                result[emp.id] = rows
        return result

    @staticmethod
    def _sgi_count(rows):
        late = sum(1 for r in rows if r['state'] == 'atrasada')
        worst = rows[0]['state'] if rows else False
        return (len(rows), late, worst)

    @api.model
    def _sgi_summary(self, users):
        """{usuario.id: (total, atrasadas, peor estado)} para botones y Mi equipo."""
        return {uid: self._sgi_count(rows) for uid, rows in self._sgi_pending_values(users).items()}

    @api.model
    def _sgi_summary_employees(self, employees):
        """{empleado.id: (total, atrasadas, peor estado)}, con o sin usuario."""
        return {eid: self._sgi_count(rows)
                for eid, rows in self._sgi_pending_values_employees(employees).items()}

    @api.model
    def _sgi_build(self, employees):
        """Renglones de los empleados dados (hr.employee, sudo) para abrirlos
        en la lista; los anteriores del mismo usuario y personas se reemplazan.
        Desde 56.36.0 también los de quien no tiene usuario (I-021)."""
        employees = employees.sudo()
        self.search([('create_uid', '=', self.env.uid),
                     ('employee_id', 'in', employees.ids)]).unlink()
        values = self._sgi_pending_values_employees(employees)
        vals_list = []
        for emp in employees:
            for row in values.get(emp.id, []):
                vals_list.append(dict(row, employee_id=emp.id, user_id=emp.user_id.id or False))
        return self.create(vals_list)

    @api.model
    def _sgi_action(self, rows, name, group_by_person=False):
        context = {'create': False}
        if group_by_person:
            context['search_default_group_employee'] = 1
        else:
            context['search_default_group_state'] = 1
        return {
            'type': 'ir.actions.act_window', 'name': name, 'res_model': self._name,
            'view_mode': 'list', 'domain': [('id', 'in', rows.ids)],
            'views': [(self.env.ref('quimibond_sgi.sgi_my_pending_view_list').id, 'list')],
            'search_view_id': [self.env.ref('quimibond_sgi.sgi_my_pending_view_search').id, 'search'],
            'context': context,
            # I-001: sin pendientes de verdad; las actividades sin medición
            # automática no pueden salir aquí, y se dice.
            'help': "<p class='o_view_nocontent_smiling_face'>Sin pendientes</p>"
                    "<p>No tienes nada atrasado ni por vencer. Las actividades sin medición "
                    "automática no salen aquí: revísalas en Mi procedimiento.</p>",
        }

    @api.model
    def action_open_mine(self):
        """Inicio → Mis pendientes: los del usuario actual. Sin empleado
        ligado, los renglones van solo con el usuario."""
        employee = self._sgi_user_employees(self.env.user).get(self.env.uid) \
            or self.env.user.employee_id.sudo()
        if employee:
            rows = self._sgi_build(employee)
        else:
            user = self.env.user
            self.search([('create_uid', '=', self.env.uid), ('employee_id', '=', False),
                         ('user_id', '=', user.id)]).unlink()
            rows = self.create([dict(row, user_id=user.id)
                                for row in self._sgi_pending_values(user).get(user.id, [])])
        return self._sgi_action(rows, "Mis pendientes")

    def action_open(self):
        """Abre el registro de origen (la acción, la NC, la medición…)."""
        self.ensure_one()
        if self.kind == 'firma' and self.res_model == 'sign.request.item':
            item = self.env['sign.request.item'].sudo().browse(self.res_id).exists()
            # La liga con token firma A NOMBRE del firmante: solo se le da a
            # él. Un jefe que abre el renglón desde Mi equipo ve la solicitud
            # con sus propios permisos (abajo), nunca el token ajeno.
            if item and item.partner_id == self.env.user.partner_id \
                    and 'access_token' in item._fields and item.access_token:
                # La firma se hace en la página de Firma electrónica.
                return {'type': 'ir.actions.act_url', 'target': 'self',
                        'url': '/sign/document/%d/%s' % (item.sign_request_id.id, item.access_token)}
        return {
            'type': 'ir.actions.act_window', 'res_model': self.res_model, 'res_id': self.res_id,
            'view_mode': 'form', 'target': 'current',
        }

    def action_validate_measure(self):
        """«Validar» desde el renglón (I-006). Valida quien abre la lista, con
        sus permisos: solo el dueño del indicador o el Jefe MAST pueden
        (``_sgi_check_validate_access``)."""
        self.ensure_one()
        if self.kind != 'validacion' or self.res_model != 'sgi.indicator.measure':
            raise UserError("Este renglón no es una medición por validar.")
        measure = self.env['sgi.indicator.measure'].browse(self.res_id).exists()
        if not measure:
            raise UserError("La medición ya no existe.")
        measure.action_validate()
        self.unlink()
        return {'type': 'ir.actions.client', 'tag': 'soft_reload'}


class SgiMyProcedurePending(models.TransientModel):
    _inherit = 'sgi.my.procedure'

    pending_total = fields.Integer(string="Mis pendientes", compute='_compute_pending_summary')
    pending_late = fields.Integer(string="Pendientes atrasados", compute='_compute_pending_summary')

    @api.depends('employee_id')
    @api.depends_context('uid')
    def _compute_pending_summary(self):
        Pending = self.env['sgi.my.pending']
        for wiz in self:
            # 56.36.0 (I-001, I-021): por persona, con o sin usuario.
            emp = wiz._sgi_mp_employee()
            total, late, _worst = Pending._sgi_summary_employees(emp).get(emp.id, (0, 0, False)) \
                if emp else (0, 0, False)
            wiz.pending_total = total
            wiz.pending_late = late

    @api.model
    def _sgi_check_in_scope(self, employees):
        """F-007 (auditoría 2026-09): los pendientes y el procedimiento de otra
        persona solo se ven si está en el alcance de Mi equipo (su gente, sus
        departamentos y los puestos de sus procesos; MAST, administrador y
        Dirección ven a todos). Antes se armaban con sudo() para cualquier id
        mandado por RPC."""
        if self.env.su or self._sgi_mp_is_admin():
            return True
        me = self._sgi_mp_my_employee()
        allowed = (me | me._sgi_mp_team_employees()) if me else me
        if set(employees.ids) - set(allowed.ids):
            raise AccessError("Esa persona no está en tu equipo; solo ves a tu gente, tus "
                              "departamentos y los puestos de tus procesos.")
        return True

    def action_show_pending(self):
        self.ensure_one()
        self._sgi_check_in_scope(self._sgi_mp_employee())
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
        """56.36.0 (I-021): el semáforo sale de la persona; sin usuario, de
        sus actividades atrasadas y acuses pendientes."""
        summary = self.env['sgi.my.pending']._sgi_summary_employees(employees.sudo())
        return {emp.id: summary.get(emp.id, (0, 0, False)) for emp in employees}

    def _compute_sgi_mp_pending(self):
        values = self._sgi_pending_by_employee(self.env['hr.employee'].sudo().browse(self.ids))
        for rec in self:
            total, late, worst = values.get(rec.id, (0, 0, False))
            rec.sgi_mp_pending_total = total
            rec.sgi_mp_pending_late = late
            rec.sgi_mp_pending_state = worst or ('al_dia' if total else False)

    def _sgi_pending_ids_where(self, predicate):
        company = self.env['sgi.config']._sgi_company()
        employees = self.env['hr.employee'].sudo().search(
            [('company_id', 'in', (company.id, False)),
             '|', ('user_id', '!=', False), ('job_id', '!=', False)])
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
        self.env['sgi.my.procedure']._sgi_check_in_scope(employee)
        rows = self.env['sgi.my.pending']._sgi_build(employee)
        return self.env['sgi.my.pending']._sgi_action(rows, "Pendientes — %s" % self.name)

    def action_sgi_team_pending(self):
        """«Pendientes del equipo»: una lista, agrupada por persona."""
        team = self.env['hr.employee'].sudo().browse(self.ids) if self else self._sgi_team()
        self.env['sgi.my.procedure']._sgi_check_in_scope(team)
        rows = self.env['sgi.my.pending']._sgi_build(team)
        return self.env['sgi.my.pending']._sgi_action(rows, "Pendientes del equipo", group_by_person=True)

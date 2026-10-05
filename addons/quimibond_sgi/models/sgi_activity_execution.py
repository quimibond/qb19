# -*- coding: utf-8 -*-
"""57.103.0: registro de cumplimiento por actividad, responsable y periodo.

Un renglón de ``sgi.activity.execution`` por actividad, persona que la
ejecuta y periodo de su cadencia (semana, mes, trimestre…), con el
vencimiento de la actividad. Lo crea el respaldo nocturno (``sgi.cron``,
``cron_nightly_backup``), el mismo que guarda el resumen de Mis pendientes,
para el periodo en curso.

- Estados: Pendiente → En proceso → Hecha, o «No aplica este periodo» con
  motivo. «En proceso» lleva una nota de avance y, opcional, la fecha
  estimada; no quita el atraso: el renglón sigue «Atrasada» en Mis
  pendientes si pasó su vencimiento.
- Registro manual (y correo o muestreo, que tampoco tienen fuente en Odoo):
  «Hecha» pide evidencia, una nota o un archivo. Las que se miden solas
  (Registro en Odoo, Por su entregable, Por consecuencia) se marcan hechas
  solas cuando aparece el registro de evidencia dentro del periodo; si el
  periodo terminó sin registro, quien cierra a mano también deja evidencia.
- La medición de las de Registro manual sale de aquí: verde si los renglones
  de su periodo están hechos (o no aplican); se juzga el periodo anterior
  mientras el actual no vence, como la medición de las que se miden solas.
- Las actividades «Por evento» no tienen periodo: no llevan renglones.
"""
import calendar
from datetime import date, timedelta

from odoo import api, fields, models
from odoo.exceptions import AccessError, UserError

from .sgi_activity_spec import SGI_CADENCE_MONTHS
from .sgi_calendar import (
    sgi_is_business_day, sgi_local_date, sgi_local_datetime_utc, sgi_previous_business_day,
    sgi_today)

EXEC_STATES = [
    ('pendiente', "Pendiente"),
    ('en_proceso', "En proceso"),
    ('hecha', "Hecha"),
    ('no_aplica', "No aplica este periodo"),
]
EXEC_OPEN = ('pendiente', 'en_proceso')
EXEC_CLOSED = ('hecha', 'no_aplica')
# Métodos sin fuente en Odoo: la persona registra que la hizo.
EXEC_MANUAL_METHODS = ('manual', 'correo', 'muestreo')


class SgiActivityExecution(models.Model):
    """Cumplimiento de una actividad en un periodo, por persona."""
    _name = 'sgi.activity.execution'
    _description = "Registro de cumplimiento de actividad"
    _inherit = ['mail.thread']
    _order = 'date_due desc, activity_id, employee_id, id'

    activity_id = fields.Many2one(
        'sgi.process.activity', string="Actividad", required=True, readonly=True,
        ondelete='cascade', index=True)
    process_id = fields.Many2one(
        related='activity_id.process_id', string="Proceso", store=True, index=True)
    company_id = fields.Many2one(
        related='activity_id.company_id', string="Empresa", store=True, index=True)
    employee_id = fields.Many2one(
        'hr.employee', string="Responsable", required=True, readonly=True,
        ondelete='cascade', index=True,
        help="Persona que ejecuta la actividad (su puesto tiene el rol «Ejecuta»).")
    user_id = fields.Many2one(
        related='employee_id.user_id', string="Usuario", store=True, index=True)
    period_start = fields.Date(string="Inicio del periodo", required=True, readonly=True,
                               index=True)
    period_end = fields.Date(string="Fin del periodo", required=True, readonly=True)
    date_due = fields.Date(string="Vence", required=True, readonly=True, index=True,
                           help="Vencimiento de la actividad en este periodo.")
    period_label = fields.Char(string="Periodo", compute='_compute_period_label', store=True)
    name = fields.Char(string="Qué", compute='_compute_name', store=True)
    state = fields.Selection(
        EXEC_STATES, string="Estado", default='pendiente', required=True, readonly=True,
        index=True, tracking=True,
        help="Pendiente, en proceso (con nota de avance), hecha (con evidencia si es de registro "
             "manual) o no aplica este periodo (con motivo).")
    progress_note = fields.Text(
        string="Nota de avance", readonly=True,
        help="Qué lleva y qué le falta. «En proceso» no quita el atraso.")
    date_estimated = fields.Date(
        string="Fecha estimada", readonly=True,
        help="Cuándo espera terminarla (opcional).")
    date_done = fields.Datetime(string="Hecha el", readonly=True)
    done_by_id = fields.Many2one('res.users', string="Marcada por", readonly=True)
    done_auto = fields.Boolean(
        string="Por el registro de Odoo", readonly=True,
        help="La marcó hecha el sistema al encontrar el registro de evidencia en el periodo.")
    on_time = fields.Boolean(
        string="A tiempo", compute='_compute_on_time', store=True,
        help="Hecha a más tardar en su vencimiento (hora de México).")
    evidence_note = fields.Text(
        string="Evidencia", readonly=True,
        help="Qué demuestra que se hizo: folio, documento, registro, foto…")
    evidence_attachment_ids = fields.Many2many(
        'ir.attachment', 'sgi_activity_execution_attachment_rel', 'execution_id',
        'attachment_id', string="Archivos de evidencia", readonly=True)
    na_reason = fields.Text(string="Por qué no aplica", readonly=True)
    # De la actividad, para hacerla sin salir de aquí.
    measure_method = fields.Selection(related='activity_id.measure_method')
    done_criteria = fields.Text(related='activity_id.done_criteria', string="Criterio de terminado")
    how_steps = fields.Text(related='activity_id.how_steps', string="Cómo (pasos)")
    instruction_id = fields.Many2one(related='activity_id.instruction_id', string="Instructivo")
    on_fail = fields.Text(related='activity_id.on_fail', string="Si no se puede cumplir")
    needs_evidence = fields.Boolean(
        string="Con evidencia obligatoria", compute='_compute_needs_evidence',
        help="Registro manual: «Hecha» pide una nota o un archivo.")
    has_related_screen = fields.Boolean(
        string="Tiene pantalla relacionada", compute='_compute_needs_evidence')
    can_mark = fields.Boolean(
        string="Puede marcarla", compute='_compute_can_mark',
        help="La persona, su jefe o el Jefe MAST.")

    _activity_employee_period_uniq = models.Constraint(
        'unique(activity_id, employee_id, period_start)',
        "Un renglón por actividad, responsable y periodo.")

    @api.depends('period_start', 'period_end', 'activity_id.measure_cadence')
    def _compute_period_label(self):
        for rec in self:
            rec.period_label = rec._sgi_period_text()

    @api.depends('activity_id.number', 'activity_id.legacy_number', 'activity_id.name',
                 'period_label')
    def _compute_name(self):
        for rec in self:
            activity = rec.activity_id
            number = activity.number or activity.legacy_number or ''
            label = ("%s %s" % (number, activity.name or '')).strip()
            rec.name = "%s (%s)" % (label, rec.period_label) if rec.period_label else label

    @api.depends('date_done', 'date_due', 'state')
    def _compute_on_time(self):
        for rec in self:
            rec.on_time = bool(rec.state == 'hecha' and rec.date_done and rec.date_due
                               and sgi_local_date(rec.env, rec.date_done) <= rec.date_due)

    @api.depends('activity_id.measure_method', 'activity_id.odoo_menu_id',
                 'activity_id.odoo_action_id')
    def _compute_needs_evidence(self):
        for rec in self:
            activity = rec.activity_id.sudo()
            rec.needs_evidence = activity._sgi_exec_is_manual()
            rec.has_related_screen = bool(activity.odoo_menu_id or activity.odoo_action_id)

    @api.depends_context('uid')
    def _compute_can_mark(self):
        for rec in self:
            rec.can_mark = rec._sgi_can_mark()

    def _sgi_period_text(self):
        """«semana del 05/10/2026», «10/2026», «2.º trimestre 2026», «2026»…"""
        self.ensure_one()
        start, end = self.period_start, self.period_end
        if not start:
            return ''
        cadence = self.activity_id.measure_cadence
        if cadence == 'diaria' or start == end:
            return start.strftime('%d/%m/%Y')
        if cadence == 'semanal':
            return "semana del %s" % start.strftime('%d/%m/%Y')
        if cadence == 'quincenal':
            return "%s al %s" % (start.strftime('%d/%m'), end.strftime('%d/%m/%Y'))
        if cadence == 'mensual':
            return start.strftime('%m/%Y')
        step = SGI_CADENCE_MONTHS.get(cadence)
        if step == 12:
            return str(start.year)
        if step:
            number = (start.month - 1) // step + 1
            kind = "trimestre" if step == 3 else "semestre"
            return "%d.º %s %d" % (number, kind, start.year)
        return "%s al %s" % (start.strftime('%d/%m/%Y'), end.strftime('%d/%m/%Y'))

    # ------------------------------------------------------------------
    # Quién marca
    # ------------------------------------------------------------------
    def _sgi_can_mark(self):
        """La persona del renglón, cualquiera de sus jefes (cadena de «Jefe»
        en Empleados) o el Jefe MAST."""
        self.ensure_one()
        env = self.env
        if env.su or env.user.has_group('quimibond_sgi.group_sgi_manager'):
            return True
        emp = self.sudo().employee_id
        if emp.user_id and emp.user_id == env.user:
            return True
        boss, seen = emp.parent_id, set()
        while boss and boss.id not in seen:
            if boss.user_id == env.user:
                return True
            seen.add(boss.id)
            boss = boss.parent_id
        return False

    def _sgi_check_can_mark(self):
        for rec in self:
            if not rec._sgi_can_mark():
                raise AccessError("Solo la persona responsable, su jefe o el Jefe MAST pueden "
                                  "marcar el avance de «%s»." % rec.sudo().name)

    # ------------------------------------------------------------------
    # Cambios de estado (siempre por aquí: los campos son de solo lectura)
    # ------------------------------------------------------------------
    def _sgi_after_change(self):
        """La medición de la actividad cambia en el momento, no hasta la noche."""
        activities = self.sudo().activity_id
        manual = activities.filtered(lambda a: a.measure_method in EXEC_MANUAL_METHODS)
        if manual:
            manual._sgi_measure()

    def _sgi_mark_progress(self, note, date_estimated=False):
        self._sgi_check_can_mark()
        if not (note or '').strip():
            raise UserError("Escriba la nota de avance: qué lleva y qué le falta.")
        for rec in self.sudo():
            if rec.state in EXEC_CLOSED:
                raise UserError("«%s» ya está cerrada (%s)." % (
                    rec.name, dict(EXEC_STATES)[rec.state].lower()))
            rec.write({'state': 'en_proceso', 'progress_note': note.strip(),
                       'date_estimated': date_estimated or False})
            rec.message_post(body="En proceso: %s%s" % (
                note.strip(), (" (estimada: %s)" % date_estimated.strftime('%d/%m/%Y'))
                if date_estimated else ''))
        self._sgi_after_change()
        return True

    def _sgi_mark_done(self, note=False, attachments=None):
        """Hecha a mano. Pide evidencia (nota o archivo) en las de registro
        manual y en las que se miden solas cuando el sistema no vio el
        registro (se cierran a mano)."""
        self._sgi_check_can_mark()
        attachments = attachments or self.env['ir.attachment']
        note = (note or '').strip()
        for rec in self.sudo():
            if rec.state in EXEC_CLOSED:
                raise UserError("«%s» ya está cerrada (%s)." % (
                    rec.name, dict(EXEC_STATES)[rec.state].lower()))
            if not note and not attachments:
                raise UserError(
                    "Para marcar hecha «%s» deje evidencia: una nota (folio, documento, "
                    "registro) o un archivo.%s" % (
                        rec.name, "" if rec.activity_id._sgi_exec_is_manual() else
                        " Esta actividad se marca sola cuando aparece su registro en Odoo; si "
                        "se hizo por otro lado, diga dónde."))
            if attachments:
                # El archivo se sube en el asistente: pasa a ser del renglón.
                attachments.sudo().write({'res_model': rec._name, 'res_id': rec.id})
            rec.write({'state': 'hecha', 'date_done': fields.Datetime.now(),
                       'done_by_id': self.env.uid, 'done_auto': False,
                       'evidence_note': note or False,
                       'evidence_attachment_ids': [(4, a.id) for a in attachments]})
            rec.message_post(body="Hecha. %s" % (note or "Evidencia adjunta."),
                             attachment_ids=attachments.ids)
        self._sgi_after_change()
        return True

    def _sgi_mark_not_applicable(self, reason):
        self._sgi_check_can_mark()
        if not (reason or '').strip():
            raise UserError("Diga por qué no aplica en este periodo.")
        for rec in self.sudo():
            if rec.state in EXEC_CLOSED:
                raise UserError("«%s» ya está cerrada (%s)." % (
                    rec.name, dict(EXEC_STATES)[rec.state].lower()))
            rec.write({'state': 'no_aplica', 'na_reason': reason.strip(),
                       'done_by_id': self.env.uid, 'date_done': fields.Datetime.now()})
            rec.message_post(body="No aplica este periodo: %s" % reason.strip())
        self._sgi_after_change()
        return True

    def _sgi_reopen(self):
        """Solo el Jefe MAST reabre un renglón cerrado por error."""
        if not (self.env.su or self.env.user.has_group('quimibond_sgi.group_sgi_manager')):
            raise AccessError("Solo el Jefe MAST reabre un renglón cerrado.")
        for rec in self.sudo():
            rec.write({'state': 'pendiente', 'date_done': False, 'done_by_id': False,
                       'done_auto': False})
            rec.message_post(body="Reabierta.")
        self._sgi_after_change()
        return True

    # ------------------------------------------------------------------
    # Botones de la ficha
    # ------------------------------------------------------------------
    def _sgi_mark_wizard(self, mode):
        self.ensure_one()
        self._sgi_check_can_mark()
        titles = {'progress': "En proceso", 'done': "Hecha", 'na': "No aplica este periodo"}
        return {
            'type': 'ir.actions.act_window', 'name': titles[mode],
            'res_model': 'sgi.activity.execution.mark', 'view_mode': 'form',
            'views': [(self.env.ref('quimibond_sgi.sgi_activity_execution_mark_view_form').id,
                       'form')],
            'target': 'new',
            'context': {'default_execution_id': self.id, 'default_mode': mode},
        }

    def action_mark_progress(self):
        return self._sgi_mark_wizard('progress')

    def action_mark_done(self):
        return self._sgi_mark_wizard('done')

    def action_mark_not_applicable(self):
        return self._sgi_mark_wizard('na')

    def action_reopen(self):
        self._sgi_reopen()
        return True

    def action_open_related_screen(self):
        """«Abrir pantalla relacionada»: el menú o la acción de Odoo de la
        actividad, solo si está capturado."""
        self.ensure_one()
        activity = self.sudo().activity_id
        if not (activity.odoo_menu_id or activity.odoo_action_id):
            raise UserError("La actividad no tiene pantalla de Odoo capturada.")
        return activity.action_open_odoo()

    def action_view_instruction(self):
        self.ensure_one()
        doc = self.sudo().activity_id.instruction_id
        if not doc:
            raise UserError("La actividad no tiene instructivo.")
        return self.env['documents.document'].browse(doc.id).action_sgi_view_file()

    # ------------------------------------------------------------------
    # Cron: renglones del periodo en curso y cierre por evidencia
    # ------------------------------------------------------------------
    @api.model
    def _sgi_generate(self, today=None):
        """Crea el renglón del periodo en curso de cada actividad periódica y
        cada persona que la ejecuta (las mismas que «Mis actividades» de Mi
        procedimiento), y quita los «Pendiente» sin tocar de quien ya no la
        ejecuta. Devuelve (creados, quitados)."""
        env = self.env
        today = today or sgi_today(env)
        company = env['sgi.config']._sgi_company()
        Employee = env['hr.employee'].sudo()
        employees = Employee.search(Employee._sgi_pending_scope_domain())
        periods = {}
        wanted = {}
        for emp in employees:
            for role in emp.sgi_mp_role_ids:
                activity = role.activity_id
                if role.role != 'ejecuta' or (role.company_id and role.company_id != company):
                    continue
                if activity.id not in periods:
                    periods[activity.id] = activity._sgi_exec_period(today) \
                        if activity._sgi_exec_applies() else None
                period = periods[activity.id]
                if period:
                    wanted[(activity.id, emp.id)] = period
        existing = self.sudo().search([('period_end', '>=', today)])
        have = {(r.activity_id.id, r.employee_id.id, r.period_start): r for r in existing}
        vals_list = []
        for (activity_id, emp_id), (start, end, due) in wanted.items():
            if (activity_id, emp_id, start) not in have:
                vals_list.append({'activity_id': activity_id, 'employee_id': emp_id,
                                  'period_start': start, 'period_end': end, 'date_due': due})
        created = self.sudo().with_context(mail_create_nolog=True, tracking_disable=True).create(
            vals_list) if vals_list else self.sudo()
        current = {(a, e, p[0]) for (a, e), p in wanted.items()}
        stale = existing.filtered(
            lambda r: r.state == 'pendiente' and r.period_start <= today
            and (r.activity_id.id, r.employee_id.id, r.period_start) not in current
            and not r.progress_note)
        removed = len(stale)
        stale.unlink()
        return created, removed

    @api.model
    def _sgi_auto_close(self, today=None):
        """Marca hechos los renglones abiertos de las actividades que se miden
        solas cuando su registro de evidencia cae dentro del periodo. Una
        consulta por actividad y periodo. Devuelve los renglones cerrados."""
        env = self.env
        today = today or sgi_today(env)
        rows = self.sudo().search([('state', 'in', EXEC_OPEN), ('period_start', '<=', today)])
        closed = self.sudo()
        groups = {}
        for row in rows:
            groups.setdefault((row.activity_id, row.period_start, row.period_end), self.sudo())
            groups[(row.activity_id, row.period_start, row.period_end)] |= row
        for (activity, start, end), recs in groups.items():
            source = activity._sgi_evidence_source()
            if not source:
                continue
            Model, domain, date_field = source
            is_date = Model._fields[date_field].type == 'date'
            hi_day = min(end, today) + timedelta(days=1)
            if is_date:
                lo, hi = start, hi_day
            else:
                lo = sgi_local_datetime_utc(env, start, 0)
                hi = sgi_local_datetime_utc(env, hi_day, 0)
            first = Model.search(domain + [(date_field, '>=', lo), (date_field, '<', hi)],
                                 order='%s asc, id asc' % date_field, limit=1)
            if not first:
                continue
            when = first[date_field]
            if is_date:
                # Mediodía de México: la fecha no se recorre al pasarla a UTC.
                when = sgi_local_datetime_utc(env, when, 12)
            recs.write({'state': 'hecha', 'date_done': when, 'done_auto': True,
                        'done_by_id': False,
                        'evidence_note': "Registro de Odoo: %s" % first.display_name})
            closed |= recs
        return closed


class SgiActivityExecutionMark(models.TransientModel):
    """Asistente de «En proceso», «Hecha» y «No aplica este periodo»."""
    _name = 'sgi.activity.execution.mark'
    _description = "Marcar avance de una actividad"

    execution_id = fields.Many2one('sgi.activity.execution', string="Actividad del periodo",
                                   required=True, ondelete='cascade')
    mode = fields.Selection([
        ('progress', "En proceso"),
        ('done', "Hecha"),
        ('na', "No aplica este periodo"),
    ], string="Marcar como", required=True, default='progress')
    name = fields.Char(related='execution_id.name', string="Qué")
    date_due = fields.Date(related='execution_id.date_due', string="Vence")
    done_criteria = fields.Text(related='execution_id.done_criteria')
    needs_evidence = fields.Boolean(related='execution_id.needs_evidence')
    progress_note = fields.Text(string="Nota de avance", help="Qué lleva y qué le falta.")
    evidence_note = fields.Text(string="Evidencia",
                                help="Folio, documento o registro que demuestra que se hizo.")
    na_reason = fields.Text(string="Por qué no aplica")
    date_estimated = fields.Date(string="Fecha estimada",
                                 help="Cuándo espera terminarla (opcional).")
    attachment_ids = fields.Many2many(
        'ir.attachment', 'sgi_activity_execution_mark_attachment_rel', 'wizard_id',
        'attachment_id', string="Archivos de evidencia")

    def action_confirm(self):
        self.ensure_one()
        execution = self.execution_id
        if self.mode == 'progress':
            execution._sgi_mark_progress(self.progress_note, self.date_estimated)
        elif self.mode == 'done':
            execution._sgi_mark_done(self.evidence_note, self.attachment_ids)
        else:
            execution._sgi_mark_not_applicable(self.na_reason)
        # Desde Mis pendientes: el renglón (transitorio) se actualiza o se va,
        # como «Leído y entendido» y «Validar».
        row_id = self.env.context.get('sgi_pending_row_id')
        row = self.env['sgi.my.pending'].sudo().browse(row_id).exists() if row_id else False
        if row:
            if self.mode == 'progress':
                row.write(row._sgi_execution_row(execution.sudo()))
            else:
                row.unlink()
        return {'type': 'ir.actions.client', 'tag': 'soft_reload'}


class SgiActivityExecutionSpec(models.Model):
    """La actividad: si lleva registro por periodo, su periodo, su fuente de
    evidencia y la medición de las de registro manual."""
    _inherit = 'sgi.process.activity'

    execution_ids = fields.One2many('sgi.activity.execution', 'activity_id',
                                    string="Registro de cumplimiento")

    def _sgi_exec_applies(self):
        """Lleva renglones por periodo: activa, de proceso activo, con cadencia
        periódica y un método que se mide (por registro o por evidencia)."""
        self.ensure_one()
        if not (self.active and self.process_id.active):
            return False
        if not self.measure_cadence or self.measure_cadence == 'evento':
            return False
        method = self.measure_method
        if method in EXEC_MANUAL_METHODS:
            return True
        return method in ('odoo', 'entregable', 'consecuencia') or bool(
            not method and self.measure_model_id)

    def _sgi_exec_is_manual(self):
        """Se registra a mano: registro manual (correo, muestreo) o una que se
        mide sola pero no tiene de dónde leer la evidencia."""
        self.ensure_one()
        if self.measure_method in EXEC_MANUAL_METHODS:
            return True
        return not self._sgi_evidence_source()

    def _sgi_evidence_source(self):
        """(Modelo con sudo, dominio, campo de fecha) de la evidencia, el mismo
        que cuenta la medición; None si no se mide sola o el filtro es malo."""
        self.ensure_one()
        activity = self
        if activity.measure_method == 'consecuencia':
            activity = activity._sgi_proxy_root()
        if activity.measure_method not in ('odoo', 'entregable') and not (
                not activity.measure_method and activity.measure_model_id):
            return None
        model_name = activity.measure_model_id.model
        Model = self.env.get(model_name) if model_name else None
        if Model is None or Model._transient or Model._abstract:
            return None
        Model = Model.sudo()
        date_field = activity.measure_date_field or 'create_date'
        if date_field not in Model._fields:
            date_field = 'create_date'
        try:
            domain = activity._sgi_measure_domain_strict()
            Model.search_count(domain, limit=1)
        except Exception:  # noqa: BLE001 - filtro capturado por una persona
            return None
        return Model, domain + self._sgi_measure_company_domain(Model), date_field

    def _sgi_exec_period(self, day):
        """(inicio, fin, vence) del periodo que contiene ``day``, o None.

        Con vencimiento periódico capturado, sus periodos (los de
        ``_sgi_periodic_due``). Sin él, el periodo de calendario de la
        cadencia y vence su último día hábil. La diaria solo tiene periodo
        en día hábil."""
        self.ensure_one()
        cadence = self.measure_cadence
        if not cadence or cadence == 'evento':
            return None
        company = self.company_id
        if cadence == 'diaria':
            if not sgi_is_business_day(self.env, day, company):
                return None
            return day, day, day
        start = self._sgi_period_start(day)
        due = self._sgi_periodic_due(day) if start is not None else None
        if cadence == 'semanal':
            start = start or day - timedelta(days=day.weekday())
            end = start + timedelta(days=6)
        elif cadence == 'quincenal':
            last = calendar.monthrange(day.year, day.month)[1]
            start, end = (day.replace(day=1), day.replace(day=15)) if day.day <= 15 \
                else (day.replace(day=16), day.replace(day=last))
        elif cadence == 'mensual':
            start = start or day.replace(day=1)
            end = start.replace(day=calendar.monthrange(start.year, start.month)[1])
        elif cadence in SGI_CADENCE_MONTHS:
            step = SGI_CADENCE_MONTHS[cadence]
            start = start or date(day.year, ((day.month - 1) // step) * step + 1, 1)
            last_month = start.month + step - 1
            end = date(start.year, last_month, calendar.monthrange(start.year, last_month)[1])
        else:
            return None
        if due is None:
            due = sgi_previous_business_day(self.env, end, floor=start, company=company)
        return start, end, due

    def _sgi_measure(self):
        """Las de registro manual con periodo se miden con su registro de
        cumplimiento; las demás, como siempre."""
        from_exec = self.filtered(
            lambda a: a.measure_method in EXEC_MANUAL_METHODS and a._sgi_exec_applies())
        res = super(SgiActivityExecutionSpec, self - from_exec)._sgi_measure()
        if from_exec:
            from_exec._sgi_measure_from_executions()
        return res

    def _sgi_measure_from_executions(self, today=None):
        """Verde si los renglones del periodo en curso están hechos o no
        aplican; vencido el periodo sin eso, rojo; antes de vencer, manda el
        periodo anterior. Sin renglones (nadie la ejecuta o aún no corre el
        cron) queda «pendiente», como antes."""
        today = today or sgi_today(self.env)
        Exec = self.env['sgi.activity.execution'].sudo()
        now = fields.Datetime.now()
        start_window = self._sgi_exec_window_start()
        for activity in self:
            activity._sgi_replace_exec_stats(start_window, [])
            rows = Exec.search([('activity_id', '=', activity.id)])
            period = activity._sgi_exec_period(today)
            if period is None:
                # Diaria en día inhábil: el último periodo que ya empezó.
                past = rows.filtered(lambda r: r.period_start <= today)
                period = (max(past.mapped('period_start')), None, None) if past else None
            current = rows.filtered(lambda r: period and r.period_start == period[0])
            previous_rows = rows.filtered(lambda r: period and r.period_end < period[0])
            previous = previous_rows.filtered(
                lambda r: r.period_start == max(previous_rows.mapped('period_start'))) \
                if previous_rows else previous_rows

            def closed(recs):
                return bool(recs) and all(r.state in EXEC_CLOSED for r in recs)

            if closed(current):
                state = 'verde'
            elif not current and not previous:
                state = 'pendiente'
            elif current and period[2] and today > period[2]:
                state = 'rojo'
            elif not previous:
                state = 'pendiente'
            else:
                state = 'verde' if closed(previous) else 'rojo'
            done = rows.filtered(lambda r: r.state == 'hecha' and r.date_done)
            last = max(done.mapped('date_done')) if done else False
            vals = dict(self._SGI_EXECUTOR_RESET, measure_state=state,
                        measure_last_date=last,
                        measure_count_30d=len(done.filtered(
                            lambda r: r.date_done >= now - timedelta(days=30))))
            activity._sgi_write_if_changed(vals)
        return True

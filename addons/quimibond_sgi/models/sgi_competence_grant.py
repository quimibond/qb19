# -*- coding: utf-8 -*-
"""57.100.0 (N-13, ISO 9001 7.2 y 45001 7.2): competencia por examen o curso.

Lo nativo escribe en el currículum del empleado la certificación aprobada
(``hr_skills_survey``: ``hr.resume.line.survey_id`` con la vigencia de la
certificación) o el curso terminado (``hr_skills_slides``:
``hr.resume.line.channel_id``). El SGI convierte esa línea en la competencia
(``hr.employee.skill``) que pide el puesto, con vigencia: la de la
certificación o la del curso («Vigencia (meses)»).

Un solo camino para otorgar: ``hr.employee._sgi_grant_skill`` (crea, sube de
nivel o renueva; nunca baja de nivel ni acorta la vigencia). Lo usan el gancho
de la línea de currículum, el cron de cursos (respaldo para quien terminó sin
línea) y el examen aprobado de quien no tiene usuario (I-2). Solo empleados
de la empresa del SGI (I-6).

Cada competencia nueva o subida de nivel abre una evaluación de eficacia
(``sgi.training.effectiveness``, ISO 9001 7.2 c): a los 90 días su jefe
inmediato dice «Eficaz» o «No eficaz»; «No eficaz» avisa a RH para
reprogramar la capacitación (la competencia no se quita, P7). Son datos de
RH: los ve quien evalúa, RH y el Jefe MAST (P8).
"""
import logging
from datetime import timedelta

from dateutil.relativedelta import relativedelta

from odoo import api, fields, models
from odoo.exceptions import UserError

from .sgi_calendar import sgi_add_business_days, sgi_today

_logger = logging.getLogger(__name__)

EFFECTIVENESS_DAYS_PARAM = 'quimibond_sgi.training_effectiveness_days'
EFFECTIVENESS_SURVEY_PARAM = 'quimibond_sgi.training_effectiveness_survey_id'

# Campos del SGI en la encuesta: los liga el Jefe MAST desde «Exámenes y
# competencias» aunque no tenga permisos de la app Encuestas (I-7).
SURVEY_SKILL_FIELDS = frozenset({'sgi_skill_id', 'sgi_skill_level_id'})


class SurveySurveySkill(models.Model):
    _inherit = 'survey.survey'

    sgi_skill_id = fields.Many2one(
        'hr.skill', string="Competencia SGI que otorga",
        help="Quien aprueba esta certificación recibe la competencia, con la vigencia "
             "de la certificación.")
    sgi_skill_type_id = fields.Many2one(
        related='sgi_skill_id.skill_type_id', string="Tipo de competencia",
        help="Tipo de la competencia que otorga el examen.")
    sgi_skill_level_id = fields.Many2one(
        'hr.skill.level', string="Nivel que otorga",
        domain="[('skill_type_id', '=', sgi_skill_type_id)]",
        help="Nivel de competencia que obtiene quien aprueba.")

    def write(self, vals):
        """I-7: ligar un examen con una competencia es del Jefe MAST, por el
        servidor y con sudo (no necesita permisos de la app Encuestas)."""
        if self.env.su:
            return super().write(vals)
        touched = set(vals) & SURVEY_SKILL_FIELDS
        if touched and not self.env.user.has_group('quimibond_sgi.group_sgi_manager'):
            raise UserError("Solo el Jefe MAST liga exámenes con competencias del SGI.")
        # El Jefe MAST tiene escritura sobre las encuestas solo para la liga
        # (ACL access_survey_survey_sgi_manager): sin permisos de la app
        # Encuestas no cambia nada más de la encuesta.
        other = {k for k in vals if k not in SURVEY_SKILL_FIELDS
                 and not k.startswith(('message_', 'activity_'))}
        if other and not self.env.user.has_group('survey.group_survey_user'):
            raise UserError("Desde «Exámenes y competencias» usted solo liga la competencia y su "
                            "nivel; el examen se edita en la app Encuestas.")
        if touched and not other:
            return super(SurveySurveySkill, self.sudo()).write(vals)
        return super().write(vals)


class SlideChannelValidity(models.Model):
    _inherit = 'slide.channel'

    sgi_skill_validity_months = fields.Integer(
        string="Vigencia (meses)", default=0,
        help="Meses que dura la competencia que otorga el curso. 0: no vence.")

    def _sgi_skill_date_to(self, date_from):
        self.ensure_one()
        months = self.sgi_skill_validity_months
        return date_from + relativedelta(months=months) if months and date_from else False


class HrEmployeeGrant(models.Model):
    _inherit = 'hr.employee'

    def _sgi_grant_skill(self, skill, level, date_from, date_to, origin,
                         channel=None, survey=None, renew=True):
        """Crea, sube de nivel o renueva la competencia ``skill`` del empleado.

        Nunca baja de nivel ni acorta la vigencia. Devuelve ``'nueva'``,
        ``'subida'``, ``'renovada'`` o ``False`` (nada cambió: idempotente).
        Con ``renew=False`` (cron de cursos) no renueva: si el empleado ya
        tuvo la competencia a ese nivel o más, no hace nada. Con ``'nueva'`` o
        ``'subida'`` abre la evaluación de eficacia (P6; no al renovar)."""
        self.ensure_one()
        if not (skill and level) or level.skill_type_id != skill.skill_type_id:
            return False
        # I-6: el SGI es de una sola empresa (D-03).
        if self.company_id and self.company_id != self.env['sgi.config']._sgi_company():
            return False
        date_from = date_from or sgi_today(self.env)
        if date_to and date_to < date_from:
            return False
        Skill = self.env['hr.employee.skill'].sudo()
        rows = Skill.search([('employee_id', '=', self.id), ('skill_id', '=', skill.id)])
        progress = level.level_progress
        best_before = max(rows.mapped('skill_level_id.level_progress') or [-1])
        if not renew and best_before >= progress:
            return False
        is_cert = skill.skill_type_id.is_certification
        current = rows.filtered(lambda r: not r.valid_to or r.valid_to >= date_from).sorted(
            lambda r: (r.skill_level_id.level_progress, r.valid_to or date_from.max))[-1:]
        new_vals = {'employee_id': self.id, 'skill_id': skill.id,
                    'skill_type_id': skill.skill_type_id.id, 'skill_level_id': level.id,
                    'valid_from': date_from, 'valid_to': date_to or False}
        if not current:
            Skill.create(new_vals)
            if not rows:
                result = 'nueva'
            else:
                result = 'subida' if progress > best_before else 'renovada'
        else:
            current_progress = current.skill_level_id.level_progress
            if progress < current_progress:
                return False
            higher = progress > current_progress
            longer = bool(current.valid_to) and (not date_to or date_to > current.valid_to)
            if not (higher or longer):
                return False
            if higher:
                # Nunca acorta: si la vigente dura más, la nueva hereda su fecha.
                if date_to and current.valid_to and current.valid_to > date_to:
                    new_vals['valid_to'] = current.valid_to
                if current.valid_from and current.valid_from < date_from:
                    # I-4: el renglón viejo se cierra el día anterior (historia).
                    current.write({'valid_to': date_from - timedelta(days=1)})
                    Skill.create(new_vals)
                else:
                    current.write({'skill_level_id': level.id, 'valid_to': new_vals['valid_to']})
                result = 'subida'
            elif is_cert:
                # Historia nativa de las certificaciones: renglón nuevo por periodo.
                Skill.create(new_vals)
                result = 'renovada'
            else:
                current.write({'valid_to': date_to or False})
                result = 'renovada'
        if result in ('nueva', 'subida'):
            self._sgi_after_grant(skill, level, date_from, origin, channel=channel, survey=survey)
        return result

    def _sgi_after_grant(self, skill, level, granted, origin, channel=None, survey=None):
        """Competencia nueva o subida de nivel: evaluación de eficacia (7.2 c)."""
        return self.env['sgi.training.effectiveness'].sudo()._sgi_open(
            self, skill, level, granted, origin, channel=channel, survey=survey)


class HrResumeLineSgi(models.Model):
    _inherit = 'hr.resume.line'

    _SGI_GRANT_FIELDS = ('survey_id', 'channel_id', 'date_start', 'date_end', 'employee_id')

    @api.model_create_multi
    def create(self, vals_list):
        lines = super().create(vals_list)
        lines._sgi_grant_from_resume()
        return lines

    def write(self, vals):
        """La recertificación nativa reescribe la línea existente (fechas
        nuevas) en lugar de crear otra: también otorga."""
        res = super().write(vals)
        if set(vals) & set(self._SGI_GRANT_FIELDS):
            self._sgi_grant_from_resume()
        return res

    def _sgi_grant_from_resume(self):
        """Certificación aprobada o curso terminado → competencia. Cada línea en
        su savepoint: si algo falla, la línea nativa se queda y se avisa en el
        log (nunca tumba la entrega del examen ni el fin del curso)."""
        for line in self.sudo():
            survey, channel = line.survey_id, line.channel_id
            if survey.sgi_skill_id:
                source, origin = survey, 'examen'
            elif channel.sgi_skill_id:
                source, origin = channel, 'curso'
            else:
                continue
            if not line.employee_id:
                continue
            date_from = line.date_start or sgi_today(self.env)
            if origin == 'examen':
                date_to = line.date_end or False
            else:
                date_to = channel._sgi_skill_date_to(date_from)
            try:
                with self.env.cr.savepoint():
                    line.employee_id._sgi_grant_skill(
                        source.sgi_skill_id, source.sgi_skill_level_id, date_from, date_to,
                        origin, channel=channel or None, survey=survey or None)
            except Exception as exc:  # noqa: BLE001 — la línea nativa no se pierde
                _logger.warning("SGI: no se otorgó la competencia de la línea de currículum %s "
                                "(%s); el cron de cursos la reintenta si es de un curso.",
                                line.id, type(exc).__name__)


class SurveyUserInputSgi(models.Model):
    _inherit = 'survey.user_input'

    def _mark_done(self):
        """I-2: lo nativo (hr_skills_survey) solo encuentra al empleado por su
        usuario. Quien aprueba un examen que otorga competencia sin tener
        usuario se encuentra por su contacto de trabajo, y se le escribe la
        misma línea de currículum (que otorga la competencia)."""
        res = super()._mark_done()
        try:
            with self.env.cr.savepoint():
                self._sgi_resume_without_user()
        except Exception as exc:  # noqa: BLE001 — nunca tumba la entrega del examen
            _logger.warning("SGI: no se registró el examen aprobado sin usuario (%s).",
                            type(exc).__name__)
        return res

    def _sgi_resume_without_user(self):
        inputs = self.sudo().filtered(
            lambda i: i.state == 'done' and not i.test_entry and i.scoring_success and i.partner_id
            and i.survey_id.certification and i.survey_id.sgi_skill_id)
        if not inputs:
            return
        Employee = self.env['hr.employee'].sudo()
        Line = self.env['hr.resume.line'].sudo()
        line_type = self.env.ref('hr_skills_survey.resume_type_certification',
                                 raise_if_not_found=False)
        today = fields.Date.today()
        for answer in inputs:
            if Employee.search_count([('user_id.partner_id', '=', answer.partner_id.id)]):
                continue  # lo nativo ya escribió la línea
            employee = Employee.search([('work_contact_id', '=', answer.partner_id.id)], limit=1)
            if not employee:
                continue
            survey = answer.survey_id
            months = survey.certification_validity_months
            vals = {
                'employee_id': employee.id,
                'name': survey.title,
                'date_start': today,
                'date_end': today + relativedelta(months=months) if months else False,
                'line_type_id': line_type.id if line_type else False,
                'survey_id': survey.id,
            }
            existing = Line.search([('employee_id', '=', employee.id),
                                    ('survey_id', '=', survey.id)], limit=1)
            if existing:
                existing.write(vals)
            else:
                Line.create(vals)


class SgiTrainingEffectiveness(models.Model):
    """Eficacia de la capacitación (ISO 9001 7.2 c): a los 90 días de otorgar
    una competencia por examen o curso, el jefe inmediato dice si fue eficaz."""
    _name = 'sgi.training.effectiveness'
    _description = "Eficacia de la capacitación"
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _order = 'due_date desc, id desc'
    _rec_name = 'skill_id'

    employee_id = fields.Many2one(
        'hr.employee', string="Empleado", required=True, ondelete='restrict', index=True,
        readonly=True, help="Persona que recibió la competencia.")
    # 1.8-4: quien evalúa sin permisos de RH no lee la ficha del empleado;
    # el nombre le llega por este campo (relacionado, se lee como sistema).
    employee_name = fields.Char(related='employee_id.name', string="Persona", compute_sudo=True,
                                help="Nombre de la persona que recibió la competencia.")
    company_id = fields.Many2one(related='employee_id.company_id', store=True, index=True,
                                 string="Empresa")
    skill_id = fields.Many2one('hr.skill', string="Competencia", required=True,
                               ondelete='restrict', readonly=True,
                               help="Competencia que se otorgó.")
    skill_level_id = fields.Many2one('hr.skill.level', string="Nivel", readonly=True,
                                     help="Nivel otorgado.")
    origin = fields.Selection([
        ('curso', "Curso de eLearning"),
        ('examen', "Examen de certificación"),
    ], string="Origen", required=True, readonly=True,
        help="Cómo se otorgó la competencia: curso terminado o examen aprobado.")
    channel_id = fields.Many2one('slide.channel', string="Curso", readonly=True)
    survey_id = fields.Many2one('survey.survey', string="Examen", readonly=True)
    granted_date = fields.Date(string="Otorgada el", required=True, readonly=True,
                               help="Día en que la persona recibió la competencia.")
    due_date = fields.Date(
        string="Evaluar a más tardar", required=True, index=True, readonly=True,
        help="Día en que vence la evaluación de eficacia (90 días después de otorgarla; "
             "parámetro quimibond_sgi.training_effectiveness_days).")
    evaluator_id = fields.Many2one(
        'res.users', string="Evalúa", required=True, index=True, readonly=True,
        help="Jefe inmediato; si no tiene usuario, el responsable del departamento, RH o "
             "el Jefe MAST.")
    state = fields.Selection([
        ('pendiente', "Pendiente"),
        ('eficaz', "Eficaz"),
        ('no_eficaz', "No eficaz"),
    ], string="Resultado", default='pendiente', required=True, tracking=True, index=True,
        help="Pendiente hasta que quien evalúa dice si la capacitación fue eficaz.")
    result_note = fields.Text(
        string="Comentario", tracking=True,
        help="Qué se observó en el trabajo de la persona. Obligatorio si no fue eficaz.")
    evaluated_date = fields.Date(string="Evaluada el", readonly=True)
    evaluated_by = fields.Many2one('res.users', string="Evaluada por", readonly=True)
    survey_input_id = fields.Many2one(
        'survey.user_input', string="Encuesta al jefe", readonly=True,
        help="Invitación a la encuesta de eficacia (opcional; parámetro "
             "quimibond_sgi.training_effectiveness_survey_id).")

    _employee_skill_date_uniq = models.Constraint(
        'unique(employee_id, skill_id, granted_date)',
        "Ya hay una evaluación de eficacia para esa competencia y fecha.")

    # ---- abrir --------------------------------------------------------------
    @api.model
    def _sgi_evaluator(self, employee):
        """Jefe inmediato → responsable del departamento → RH → Jefe MAST (P6)."""
        employee = employee.sudo()
        for user in (employee.parent_id.user_id, employee.department_id.manager_id.user_id):
            if user and user.active and not user.share:
                return user
        cron = self.env['sgi.cron']
        return self.env['res.users'].browse(cron._sgi_rh_user_id() or cron._sgi_manager_user_id())

    @api.model
    def _sgi_open(self, employee, skill, level, granted, origin, channel=None, survey=None):
        """Abre la evaluación (idempotente) y su aviso al evaluador."""
        existing = self.search([('employee_id', '=', employee.id), ('skill_id', '=', skill.id),
                                ('granted_date', '=', granted)], limit=1)
        if existing:
            return existing
        evaluator = self._sgi_evaluator(employee)
        if not evaluator:
            _logger.warning("SGI: sin evaluador para la eficacia de la competencia %s del "
                            "empleado %s (ni jefe, ni RH, ni Jefe MAST).", skill.id, employee.id)
            return self.browse()
        raw = self.env['ir.config_parameter'].sudo().get_param(EFFECTIVENESS_DAYS_PARAM, '90')
        days = int(raw) if str(raw).strip().isdigit() else 90
        rec = self.create({
            'employee_id': employee.id, 'skill_id': skill.id, 'skill_level_id': level.id,
            'origin': origin, 'channel_id': channel.id if channel else False,
            'survey_id': survey.id if survey else False, 'granted_date': granted,
            'due_date': granted + timedelta(days=days), 'evaluator_id': evaluator.id})
        rec._sgi_invite_survey()
        self.env['sgi.cron']._sgi_schedule(
            rec, "Evaluar la eficacia de la capacitación: %s — %s" % (
                skill.name, employee.sudo().name),
            rec._sgi_activity_note(), evaluator.id, date_deadline=rec.due_date,
            key='eficacia_capacitacion:%d' % rec.id)
        return rec

    def _sgi_invite_survey(self):
        """Invitación opcional a una encuesta de eficacia para el evaluador."""
        raw = self.env['ir.config_parameter'].sudo().get_param(EFFECTIVENESS_SURVEY_PARAM, '0')
        survey_id = int(raw) if str(raw).strip().isdigit() else 0
        survey = self.env['survey.survey'].sudo().browse(survey_id).exists() if survey_id else None
        if not survey or not survey.active:
            return False
        for rec in self.sudo():
            try:
                with self.env.cr.savepoint():
                    answer = survey._create_answer(
                        user=rec.evaluator_id, partner=rec.evaluator_id.partner_id,
                        check_attempts=False)
                    rec.survey_input_id = answer[:1]
            except UserError as exc:
                _logger.warning("SGI: no se creó la encuesta de eficacia de %s (%s).",
                                rec.id, type(exc).__name__)
        return True

    def _sgi_activity_note(self):
        self.ensure_one()
        note = ("Diga si la capacitación cambió el trabajo de la persona: abra la evaluación y "
                "pulse «Eficaz» o «No eficaz». Si no fue eficaz, escriba qué observó; RH "
                "reprograma la capacitación.")
        answer = self.sudo().survey_input_id
        if answer:
            note += " Puede contestar también la encuesta: %s" % answer.get_start_url()
        return note

    # ---- evaluar ------------------------------------------------------------
    def _sgi_is_admin(self):
        user = self.env.user
        return self.env.su or user.has_group('hr.group_hr_user') \
            or user.has_group('quimibond_sgi.group_sgi_manager')

    def _sgi_check_can_evaluate(self):
        if self._sgi_is_admin():
            return
        for rec in self:
            if rec.evaluator_id != self.env.user:
                raise UserError("Solo quien evalúa (%s), RH o el Jefe MAST dicen si la "
                                "capacitación fue eficaz." % rec.sudo().evaluator_id.name)

    def _sgi_mark(self, state):
        self._sgi_check_can_evaluate()
        pending = self.filtered(lambda r: r.state == 'pendiente')
        if pending != self:
            raise UserError("Esta evaluación ya tiene resultado. Solo el Jefe MAST lo cambia.")
        # sudo: quién puede evaluar ya se revisó arriba; el resultado, la fecha
        # y quién evaluó solo se escriben por aquí (el contexto no basta).
        self.sudo().write({
            'state': state, 'evaluated_date': sgi_today(self.env),
            'evaluated_by': self.env.user.id})
        Cron = self.env['sgi.cron']
        for rec in self:
            acts = self.env['mail.activity'].sudo().search([
                ('res_model', '=', self._name), ('res_id', '=', rec.id),
                ('sgi_cron_key', '=', 'eficacia_capacitacion:%d' % rec.id)])
            if acts:
                Cron._sgi_close_activities(acts, "evaluación registrada")
        return True

    def action_mark_effective(self):
        return self._sgi_mark('eficaz')

    def action_mark_ineffective(self):
        missing = self.filtered(lambda r: not (r.result_note or '').strip())
        if missing:
            raise UserError("Escriba en «Comentario» qué observó en el trabajo de la persona: "
                            "RH lo usa para reprogramar la capacitación.")
        self._sgi_mark('no_eficaz')
        Cron = self.env['sgi.cron'].sudo()
        rh_id = Cron._sgi_rh_user_id()
        deadline = sgi_add_business_days(self.env, sgi_today(self.env), 5)
        for rec in self.sudo():
            Cron._sgi_schedule(
                rec, "Reprogramar capacitación: %s — %s" % (rec.skill_id.name, rec.employee_id.name),
                "La capacitación no fue eficaz según %s: %s. Reprograme la capacitación o "
                "acuerde con el jefe cómo reforzarla." % (
                    rec.evaluated_by.name or rec.evaluator_id.name, rec.result_note.strip()),
                rh_id, date_deadline=deadline, key='reprogramar_capacitacion:%d' % rec.id)
        return True

    # ---- candados -----------------------------------------------------------
    _SGI_FREE_PREFIXES = ('message_', 'activity_')

    def write(self, vals):
        if not self.env.su:
            admin = self._sgi_is_admin()
            mast = self.env.user.has_group('quimibond_sgi.group_sgi_manager')
            keys = {k for k in vals if not k.startswith(self._SGI_FREE_PREFIXES)}
            # I-11: quien evalúa sin ser RH ni Jefe MAST solo escribe su
            # comentario; el resultado, la fecha y quién evaluó, con los
            # botones (que escriben con sudo). Ningún contexto lo salta.
            if not admin and keys - {'result_note'}:
                raise UserError("En la evaluación de eficacia usted solo escribe el comentario; "
                                "el resultado se registra con «Eficaz» o «No eficaz».")
            if keys & {'state', 'result_note'}:
                if 'state' in vals:
                    self._sgi_check_can_evaluate()
                done = self.filtered(lambda r: r.state != 'pendiente')
                if done and not mast:
                    raise UserError("La evaluación de eficacia ya tiene resultado: solo el Jefe "
                                    "MAST lo cambia.")
        return super().write(vals)

    def unlink(self):
        if not self.env.su:
            raise UserError("La evaluación de eficacia es evidencia de la competencia (7.2) y no "
                            "se borra.")
        return super().unlink()

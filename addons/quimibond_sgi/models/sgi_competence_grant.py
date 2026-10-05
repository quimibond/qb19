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
"""
import logging
from datetime import timedelta

from dateutil.relativedelta import relativedelta

from odoo import api, fields, models
from odoo.exceptions import UserError

from .sgi_calendar import sgi_today

_logger = logging.getLogger(__name__)

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
        touched = set(vals) & SURVEY_SKILL_FIELDS
        if touched and not self.env.su:
            if not self.env.user.has_group('quimibond_sgi.group_sgi_manager'):
                raise UserError("Solo el Jefe MAST liga exámenes con competencias del SGI.")
            if set(vals) <= SURVEY_SKILL_FIELDS:
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
                if current.valid_from < date_from:
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
        """Lo que sigue a una competencia nueva o subida (la eficacia, Task 10.5)."""
        return False


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
            lambda i: i.state == 'done' and i.scoring_success and i.partner_id
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

# -*- coding: utf-8 -*-
"""57.135.0 (C1, Jose 5.5): escalamiento por tiempo de los pasos del desarrollo (brief §6.13).

El proyecto de desarrollo siempre tiene **un paso pendiente** (``_sgi_dev_pending_step``): sale de
su etapa de avance y de lo que ya está capturado (sin resultado del análisis → C1.02; con
resultado y sin revisión de Ventas → C1.03; …; en «Muestra» sin solicitud aprobada → C1.07, sin
artículo → C1.04, sin orden → C1.09, orden sin terminar → C1.10, corrida sin validar → C1.11,
sin envío → C1.12; «Respuesta del cliente» → C1.12; «Pilotaje» → C1.14). Cada paso sabe **desde
cuándo** está pendiente (la fecha que dejó el paso anterior o el inicio de la etapa).

Tres avisos, cada uno con su parámetro **vacío** (el brief propone 2 h, 4 h y «si se venció el
día» como valores no confirmados; vacío = ese nivel no avisa):

- ``quimibond_sgi.dev_notice_executor_hours``: al **responsable del paso** (quien «ejecuta» la
  actividad de C1 en el SGI) a las N horas de tener el paso pendiente.
- ``quimibond_sgi.dev_notice_owner_hours``: al **dueño del proceso** C1 (primer nivel) a las N
  horas.
- ``quimibond_sgi.dev_escalation_director_days`` (ya existía): a **Dirección de Operaciones**
  (segundo nivel; puesto por nombre, en producción el 230) a los N días hábiles. Dirección no
  recibe nada antes.

Las horas se cuentan **sin el tiempo con materia prima pendiente** (regla del brief: los 15 días
no cuentan la espera de materia prima): se restan las esperas de ``sgi.dev.mp.wait`` que caen
dentro del paso. Un cron cada hora revisa los desarrollos abiertos: por paso y nivel manda **un
solo aviso** por persona (fila en ``sgi.dev.escalation`` + actividad «Por hacer» sobre el
proyecto, deduplicada con la clave del SGI); cuando el paso avanza, los avisos abiertos se
cierran solos con nota. Nunca cambia etapa ni datos del proyecto.
"""
import logging
from datetime import timedelta

from odoo import api, fields, models

from .sgi_calendar import sgi_add_business_days
from .sgi_dev_process import C1_PROCESS_CODE, DIRECTOR_JOB_NAME, PARAM_ESCALATION_DAYS

_logger = logging.getLogger(__name__)

PARAM_EXECUTOR_HOURS = 'quimibond_sgi.dev_notice_executor_hours'
PARAM_OWNER_HOURS = 'quimibond_sgi.dev_notice_owner_hours'
OPEN_STAGE_KEYS = ('solicitud', 'analisis', 'cotizacion', 'aprobacion_cliente', 'muestra',
                   'respuesta_cliente', 'pilotaje')
LEVELS = [
    ('ejecutor', "Responsable del paso"),
    ('dueno', "Dueño del proceso (primer nivel)"),
    ('direccion', "Dirección de Operaciones (segundo nivel)"),
]
# Nombre de respaldo del paso cuando la actividad de C1 no está en el SGI (copia sin mapa).
STEP_NAMES = {
    'C1.02': "Analizar la muestra o la especificación",
    'C1.03': "Checklist de factibilidad y revisión de Ventas",
    'C1.04': "Dar de alta el artículo de desarrollo",
    'C1.05': "Costear y cotizar",
    'C1.06': "Seguimiento de la cotización y aprobación del cliente",
    'C1.07': "Aprobar la solicitud de desarrollo",
    'C1.08': "Materia prima para la muestra",
    'C1.09': "Pedir la corrida de muestra",
    'C1.10': "Producir la muestra",
    'C1.11': "Medir y validar la corrida",
    'C1.12': "Enviar la muestra y registrar la respuesta del cliente",
    'C1.14': "Pilotaje (primeros lotes)",
}


def _hours(delta):
    return max(delta.total_seconds(), 0.0) / 3600.0


class SgiDevEscalation(models.Model):
    _name = 'sgi.dev.escalation'
    _description = "Aviso por tiempo de un desarrollo"
    _order = 'sent_at desc, id desc'

    project_id = fields.Many2one('project.project', string="Desarrollo", required=True, ondelete='cascade', index=True)
    step_code = fields.Char(string="Paso", required=True, index=True)
    step_name = fields.Char(string="Qué estaba pendiente")
    since = fields.Datetime(string="Pendiente desde", required=True)
    level = fields.Selection(LEVELS, string="Nivel", required=True, index=True)
    user_id = fields.Many2one('res.users', string="Avisado a", required=True, index=True)
    job_id = fields.Many2one('hr.job', string="Puesto")
    hours = fields.Float(string="Horas de desarrollo al avisar", digits=(16, 1))
    sent_at = fields.Datetime(string="Avisado el", required=True, default=fields.Datetime.now)
    mail_activity_id = fields.Many2one('mail.activity', string="Actividad", ondelete='set null')
    state = fields.Selection([('abierto', "Abierto"), ('cerrado', "Cerrado")], default='abierto', required=True, index=True)
    closed_at = fields.Datetime(string="Cerrado el")
    close_reason = fields.Char(string="Por qué se cerró")
    company_id = fields.Many2one('res.company', related='project_id.company_id', store=True)

    # ------------------------------------------------------------------
    # Parámetros y destinatarios
    # ------------------------------------------------------------------
    @api.model
    def _sgi_dev_param_int(self, key):
        raw = (self.env['ir.config_parameter'].sudo().get_param(key, '') or '').strip()
        try:
            return max(int(float(raw)), 0)
        except ValueError:
            return 0

    @api.model
    def _sgi_dev_thresholds(self):
        """{nivel: umbral} solo con los niveles configurados (horas para ejecutor y dueño, días
        hábiles para Dirección)."""
        out = {}
        for level, key in (('ejecutor', PARAM_EXECUTOR_HOURS), ('dueno', PARAM_OWNER_HOURS),
                           ('direccion', PARAM_ESCALATION_DAYS)):
            value = self._sgi_dev_param_int(key)
            if value > 0:
                out[level] = value
        return out

    @api.model
    def _sgi_dev_users_of_jobs(self, jobs):
        if not jobs:
            return self.env['res.users']
        company = self.env['sgi.config']._sgi_company()
        employees = self.env['hr.employee'].sudo().search(
            [('job_id', 'in', jobs.ids), ('user_id', '!=', False),
             '|', ('company_id', '=', False), ('company_id', '=', company.id)])
        return employees.user_id.filtered(lambda u: u.active and not u.share)

    @api.model
    def _sgi_dev_c1_process(self):
        return self.env['sgi.process'].sudo().search([('code', '=', C1_PROCESS_CODE)], limit=1)

    @api.model
    def _sgi_dev_targets(self, step_code):
        """{nivel: (usuarios, puesto)} del paso: quien ejecuta la actividad de C1, el dueño del
        proceso y Dirección de Operaciones. Un nivel sin persona no avisa."""
        Users = self.env['res.users']
        activity = self.env['sgi.process.activity']._sgi_dev_c1_activity(step_code)
        exec_users, exec_job = Users, self.env['hr.job']
        if activity:
            roles = activity.role_ids.filtered(lambda r: r.role == 'ejecuta')
            jobs = roles._sgi_jobs() if roles else None
            if jobs:
                exec_job = jobs[:1]
                exec_users = self._sgi_dev_users_of_jobs(jobs)
        process = activity.process_id if activity else self._sgi_dev_c1_process()
        owner = process.owner_id if process else self.env['hr.employee']
        owner_users = owner.user_id.filtered(lambda u: u.active and not u.share) if owner else Users
        director_job = self.env['sgi.process.activity']._sgi_dev_jobs_named([DIRECTOR_JOB_NAME])[:1]
        director_users = self._sgi_dev_users_of_jobs(director_job)
        return {
            'ejecutor': (exec_users, exec_job),
            'dueno': (owner_users, owner.job_id if owner else self.env['hr.job']),
            'direccion': (director_users, director_job),
        }

    # ------------------------------------------------------------------
    # Cron
    # ------------------------------------------------------------------
    @api.model
    def cron_sgi_dev_escalation(self, now=None):
        """Cada hora: por desarrollo abierto, el paso pendiente, sus horas de desarrollo y los
        avisos que ya tocan. Devuelve los avisos creados."""
        now = now or fields.Datetime.now()
        thresholds = self._sgi_dev_thresholds()
        created = self.browse()
        projects = self.env['project.project'].sudo().search(
            [('sgi_is_ft', '=', True), ('is_template', '=', False), ('active', '=', True)])
        targets_cache = {}
        for project in projects:
            step = project._sgi_dev_pending_step()
            if not step:
                self._sgi_dev_close_open(project, None, "el desarrollo ya no tiene paso pendiente")
                continue
            self._sgi_dev_close_open(project, step, "el paso avanzó")
            if not thresholds:
                continue
            hours = project._sgi_dev_hours_since(step['since'], now)
            if step['code'] not in targets_cache:
                targets_cache[step['code']] = self._sgi_dev_targets(step['code'])
            targets = targets_cache[step['code']]
            for level, threshold in thresholds.items():
                if not self._sgi_dev_level_due(project, step, level, threshold, hours, now):
                    continue
                users, job = targets.get(level, (self.env['res.users'], self.env['hr.job']))
                for user in users:
                    created |= self._sgi_dev_notify(project, step, level, user, job, hours, now)
        return created

    @api.model
    def _sgi_dev_level_due(self, project, step, level, threshold, hours, now):
        if level in ('ejecutor', 'dueno'):
            return hours >= threshold
        # Dirección: días hábiles desde que quedó pendiente, corridos por la espera de materia prima.
        shifted = step['since'] + timedelta(hours=project._sgi_dev_mp_hours_between(step['since'], now))
        due_day = sgi_add_business_days(self.env, shifted.date(), threshold)
        return now.date() >= due_day

    @api.model
    def _sgi_dev_close_open(self, project, step, reason):
        domain = [('project_id', '=', project.id), ('state', '=', 'abierto')]
        if step:
            domain += ['|', ('step_code', '!=', step['code']), ('since', '!=', step['since'])]
        rows = self.sudo().search(domain)
        if not rows:
            return rows
        activities = rows.mail_activity_id.exists().filtered('active')
        if activities:
            self.env['sgi.cron']._sgi_close_activities(activities, reason)
        rows.write({'state': 'cerrado', 'closed_at': fields.Datetime.now(), 'close_reason': reason})
        return rows

    @api.model
    def _sgi_dev_notify(self, project, step, level, user, job, hours, now):
        existing = self.sudo().search([
            ('project_id', '=', project.id), ('step_code', '=', step['code']), ('since', '=', step['since']),
            ('level', '=', level), ('user_id', '=', user.id)], limit=1)
        if existing:
            return self.browse()
        label = dict(LEVELS)[level]
        folio = project.sgi_ft_folio or project.name
        if level == 'ejecutor':
            summary = "Desarrollo %s: %s %s lleva %.0f h pendiente" % (folio, step['code'], step['name'], hours)
            note = ("El paso <b>%s %s</b> del desarrollo <b>%s</b> está pendiente desde el %s "
                    "(%.1f horas de desarrollo, sin contar materia prima pendiente)."
                    % (step['code'], step['name'], folio, fields.Datetime.to_string(step['since']), hours))
        else:
            summary = "Escalamiento (%s): %s %s en %s, %.0f h sin avanzar" % (label, step['code'], step['name'], folio, hours)
            note = ("<b>%s</b>: el paso <b>%s %s</b> del desarrollo <b>%s</b> lleva %.1f horas de desarrollo "
                    "pendiente desde el %s y no ha avanzado." % (label, step['code'], step['name'], folio, hours,
                                                               fields.Datetime.to_string(step['since'])))
        key = "dev_esc:%s:%s:%s:%d" % (step['code'], fields.Datetime.to_string(step['since']), level, user.id)
        activity = False
        try:
            activity = self.env['sgi.cron']._sgi_schedule(project, summary, note, user.id,
                                                          date_deadline=now.date(), key=key)
        except Exception:  # noqa: BLE001 — el aviso no debe tumbar el cron
            _logger.exception("SGI desarrollo: no se pudo agendar el aviso %s", key)
        row = self.sudo().create({
            'project_id': project.id, 'step_code': step['code'], 'step_name': step['name'],
            'since': step['since'], 'level': level, 'user_id': user.id, 'job_id': job.id if job else False,
            'hours': hours, 'sent_at': now, 'mail_activity_id': activity.id if activity else False,
        })
        if level != 'ejecutor':
            project.message_post(body=note, partner_ids=user.partner_id.ids, message_type='comment',
                                 subtype_xmlid='mail.mt_note')
        return row


class ProjectProjectDevEscalation(models.Model):
    _inherit = 'project.project'

    sgi_dev_escalation_ids = fields.One2many('sgi.dev.escalation', 'project_id', string="Avisos por tiempo")
    sgi_dev_escalation_count = fields.Integer(compute='_compute_sgi_dev_escalation_count')
    sgi_dev_pending_step = fields.Char(string="Paso pendiente", compute='_compute_sgi_dev_pending_step')
    sgi_dev_pending_hours = fields.Float(string="Horas de desarrollo en el paso", compute='_compute_sgi_dev_pending_step',
                                         digits=(16, 1))

    @api.depends('sgi_dev_escalation_ids')
    def _compute_sgi_dev_escalation_count(self):
        for project in self:
            project.sgi_dev_escalation_count = len(project.sgi_dev_escalation_ids)

    def _compute_sgi_dev_pending_step(self):
        now = fields.Datetime.now()
        for project in self:
            step = project._sgi_dev_pending_step() if project.sgi_is_ft else None
            project.sgi_dev_pending_step = ("%s %s" % (step['code'], step['name'])) if step else False
            project.sgi_dev_pending_hours = project._sgi_dev_hours_since(step['since'], now) if step else 0.0

    # ------------------------------------------------------------------
    # Paso pendiente y reloj del paso
    # ------------------------------------------------------------------
    def _sgi_dev_stage_start(self):
        self.ensure_one()
        logs = self.sgi_dev_stage_log_ids.filtered(lambda l: not l.date_end).sorted('date_start')
        if logs:
            return logs[-1].date_start
        logs = self.sgi_dev_stage_log_ids.sorted('date_start')
        return logs[-1].date_start if logs else self.create_date

    def _sgi_dev_step(self, code, since):
        activity = self.env['sgi.process.activity']._sgi_dev_c1_activity(code)
        name = (activity.name or '').strip() if activity else ''
        return {'code': code, 'name': name or STEP_NAMES.get(code, code),
                'since': fields.Datetime.to_datetime(since or self.create_date or fields.Datetime.now())}

    def _sgi_dev_pending_step(self):
        """El paso de C1 que está pendiente y desde cuándo; None si el desarrollo está cerrado o
        liberado. Solo lee: nunca cambia nada."""
        self.ensure_one()
        key = self.sgi_dev_stage_key
        if not self.sgi_is_ft or self.is_template or key not in OPEN_STAGE_KEYS:
            return None
        stage_start = self._sgi_dev_stage_start()
        if key in ('solicitud', 'analisis'):
            if not self.sgi_dev_analysis_result:
                return self._sgi_dev_step('C1.02', stage_start)
            if self.sgi_dev_review_state != 'aprobado':
                return self._sgi_dev_step('C1.03', self.sgi_dev_analysis_date or stage_start)
            if self.sgi_dev_analysis_result in ('linea', 'no_factible'):
                return self._sgi_dev_step('C1.03', self.sgi_dev_review_date or stage_start)
            return self._sgi_dev_step('C1.05', self.sgi_dev_review_date or stage_start)
        if key == 'cotizacion':
            return self._sgi_dev_step('C1.05', stage_start)
        if key == 'aprobacion_cliente':
            return self._sgi_dev_step('C1.06', stage_start)
        if key == 'muestra':
            if not self.sgi_dev_approved_by_id:
                return self._sgi_dev_step('C1.07', self.sgi_dev_customer_approved_at or stage_start)
            after_approval = self.sgi_dev_approved_date or stage_start
            if not self.sgi_dev_product_id:
                return self._sgi_dev_step('C1.04', after_approval)
            if self.sgi_dev_mp_pending or (self.sgi_dev_mp_missing_count and not self.sgi_dev_mo_ids):
                waits = self.sgi_dev_mp_wait_ids.filtered(lambda w: not w.date_end).sorted('date_start')
                return self._sgi_dev_step('C1.08', waits[:1].date_start if waits else after_approval)
            mos = self.sgi_dev_mo_ids.filtered(lambda m: m.state != 'cancel').sorted('id')
            if not mos:
                return self._sgi_dev_step('C1.09', self.sgi_dev_product_id.create_date or after_approval)
            if any(mo.state != 'done' for mo in mos):
                return self._sgi_dev_step('C1.10', mos[:1].create_date)
            if not self.sgi_dev_run_validated_by_id:
                finished = [mo.date_finished for mo in mos if mo.date_finished]
                return self._sgi_dev_step('C1.11', max(finished) if finished else mos[-1].create_date)
            shipped = self.sgi_dev_shipment_ids.filtered('shipped_at').sorted('shipped_at')
            if not shipped:
                return self._sgi_dev_step('C1.12', self.sgi_dev_run_validated_date or stage_start)
            return self._sgi_dev_step('C1.12', shipped[-1].shipped_at)
        if key == 'respuesta_cliente':
            shipped = self.sgi_dev_shipment_ids.filtered('shipped_at').sorted('shipped_at')
            return self._sgi_dev_step('C1.12', shipped[-1].shipped_at if shipped else stage_start)
        if key == 'pilotaje':
            return self._sgi_dev_step('C1.14', stage_start)
        return None

    def _sgi_dev_mp_hours_between(self, start, end):
        """Horas con materia prima pendiente dentro de [start, end]."""
        self.ensure_one()
        total = 0.0
        for wait in self.sgi_dev_mp_wait_ids:
            w_start = fields.Datetime.to_datetime(wait.date_start)
            w_end = fields.Datetime.to_datetime(wait.date_end) if wait.date_end else end
            lo, hi = max(w_start, start), min(w_end, end)
            if hi > lo:
                total += _hours(hi - lo)
        return total

    def _sgi_dev_hours_since(self, since, now=None):
        """Horas calendario desde ``since`` sin las esperas de materia prima."""
        self.ensure_one()
        now = now or fields.Datetime.now()
        since = fields.Datetime.to_datetime(since)
        if not since or since >= now:
            return 0.0
        return max(_hours(now - since) - self._sgi_dev_mp_hours_between(since, now), 0.0)

    def action_sgi_dev_escalations(self):
        self.ensure_one()
        action = self.env['ir.actions.act_window']._for_xml_id('quimibond_sgi.sgi_dev_escalation_action')
        action['domain'] = [('project_id', '=', self.id)]
        action['context'] = {'search_default_project_id': self.id}
        return action


class ResConfigSettingsDevEscalation(models.TransientModel):
    _inherit = 'res.config.settings'

    sgi_dev_notice_executor_hours = fields.Integer(
        string="Avisar al responsable del paso a las (horas)", config_parameter=PARAM_EXECUTOR_HOURS,
        help="Horas de desarrollo (sin materia prima pendiente) que un paso de C1 puede llevar pendiente antes de "
             "avisar a quien lo ejecuta. El brief propone 2; vacío o 0: no se avisa.")
    sgi_dev_notice_owner_hours = fields.Integer(
        string="Escalar al dueño del proceso a las (horas)", config_parameter=PARAM_OWNER_HOURS,
        help="Primer nivel de escalamiento: horas de desarrollo tras las que el dueño del proceso C1 recibe el "
             "aviso. El brief propone 4; vacío o 0: no se escala.")

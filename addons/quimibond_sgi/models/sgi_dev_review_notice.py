# -*- coding: utf-8 -*-
"""57.182.0 (C1, Administración de Ventas): avisos entre Diseño de Producto,
Diseño de Procesos y Ventas en la revisión del análisis.

Antes «Aprobar análisis y factibilidad» y «Regresar a Diseño» solo dejaban
una nota interna (no notifica) y nadie se enteraba de que el análisis estaba
listo ni de que ya se había corregido. Ahora cada paso deja una **actividad
«Por hacer»** (llega a la campana y por correo) al **puesto** que sigue, leído
de los roles de la actividad de C1 en el SGI (no por nombre de persona), con
fecha límite = el plazo de esa actividad (días hábiles de su «recibe»), y
cierra como hecha la del paso anterior:

1. Resultado del análisis «nuevo» → «Contestar checklist de factibilidad» a
   quien ejecuta C1.03 (Diseño de Procesos). «Línea» o «no factible» →
   «Revisar análisis y factibilidad» a quien aprueba C1.02/C1.03 (Ventas).
2. Checklist sin renglones pendientes → «Revisar análisis y factibilidad» a
   Ventas.
3. «Regresar a Diseño» → «Corregir lo que pidió Ventas» a quien ejecuta C1.02
   y C1.03 (Diseño de Producto y de Procesos), con el motivo en la nota, más
   un mensaje con correo a esas personas.
4. «Ya corregí» (Diseño de Producto o de Procesos, solo con la revisión
   regresada) → la revisión vuelve a «Pendiente», se marca hecha la
   corrección y «Revisar corrección» le llega a quien regresó, con correo.
5. Aprobar → se marcan hechas las actividades abiertas del ciclo.

Las actividades del ciclo llevan la clave ``dev_c1_revision:<paso>`` del SGI
(``mail.activity.sgi_cron_key``); ningún barrido de cron usa esa clase. No
toca la ficha de C1 en el SGI (roles, plazos ni aprobaciones).
"""
from markupsafe import escape

from odoo import api, fields, models
from odoo.exceptions import UserError

from .sgi_calendar import sgi_add_business_days, sgi_today

NOTICE_KIND = 'dev_c1_revision'
NOTICE_SUMMARIES = {
    'checklist': "Contestar checklist de factibilidad",
    'revisar': "Revisar análisis y factibilidad",
    'corregir': "Corregir lo que pidió Ventas",
    'correccion': "Revisar corrección",
}
REVIEW_STAGE_KEYS = ('solicitud', 'analisis', 'cotizacion')
# Respaldo por nombre si la actividad de C1 no está en el SGI (copia sin mapa).
JOB_FALLBACK = {
    'producto': 'diseño y desarrollo de producto',
    'procesos': 'diseño y desarrollo de procesos',
    'ventas': 'administrador de ventas',
}


class SgiDevFeasibilityNotice(models.Model):
    _inherit = 'sgi.dev.feasibility'

    @api.model_create_multi
    def create(self, vals_list):
        lines = super().create(vals_list)
        lines.project_id._sgi_dev_notice_checklist_done()
        return lines

    def write(self, vals):
        res = super().write(vals)
        if 'answer' in vals:
            self.project_id._sgi_dev_notice_checklist_done()
        return res


class ProjectProjectDevReviewNotice(models.Model):
    _inherit = 'project.project'

    sgi_dev_fix_note = fields.Char(string="Qué se corrigió", copy=False,
                                   help="Comentario corto (opcional) para Ventas al usar «Ya corregí».")
    sgi_dev_can_fix = fields.Boolean(compute='_compute_sgi_dev_can_fix',
                                     help="El usuario es de Diseño de Producto o de Procesos (o su jefe directo).")

    def _compute_sgi_dev_can_fix(self):
        users = self._sgi_dev_fixer_users() if self else self.env['res.users']
        can = self.env.user in users or self.env.user.has_group('quimibond_sgi.group_sgi_manager')
        for project in self:
            project.sgi_dev_can_fix = can

    # ------------------------------------------------------------------
    # Puestos y plazos (de la ficha de C1 en el SGI)
    # ------------------------------------------------------------------
    @api.model
    def _sgi_dev_c1_role_jobs(self, numbers, role, fallback):
        """Puestos con ``role`` en las actividades de C1 ``numbers``; por nombre si no hay."""
        Activity = self.env['sgi.process.activity']
        jobs = self.env['hr.job']
        for number in numbers:
            activity = Activity._sgi_dev_c1_activity(number)
            roles = activity.role_ids.filtered(lambda r: r.role == role) if activity else None
            found = roles._sgi_jobs() if roles else None
            if found:
                jobs |= found
        return jobs or Activity._sgi_dev_jobs_named([JOB_FALLBACK[fallback]])[:1]

    @api.model
    def _sgi_dev_job_users(self, jobs):
        return self.env['sgi.dev.escalation']._sgi_dev_users_of_jobs(jobs)

    @api.model
    def _sgi_dev_fixer_users(self):
        """Diseño de Producto y de Procesos (quien ejecuta C1.02 y C1.03) y sus jefes directos."""
        jobs = self._sgi_dev_c1_role_jobs(['C1.02'], 'ejecuta', 'producto') \
            | self._sgi_dev_c1_role_jobs(['C1.03'], 'ejecuta', 'procesos')
        return jobs._sgi_users(with_bosses=True)

    def _sgi_dev_c1_deadline(self, number):
        """Hoy más los días hábiles del «recibe» de la actividad de C1; hoy si no tiene plazo."""
        self.ensure_one()
        activity = self.env['sgi.process.activity']._sgi_dev_c1_activity(number)
        days = max(activity.input_ids.mapped('max_days') or [0]) if activity else 0
        company = self.company_id or self.env.company
        today = sgi_today(self.env, company)
        return sgi_add_business_days(self.env, today, days, company) if days else today

    # ------------------------------------------------------------------
    # Actividades del ciclo
    # ------------------------------------------------------------------
    def _sgi_dev_notice_open(self, steps=None):
        self.ensure_one()
        keys = ['%s:%s' % (NOTICE_KIND, step) for step in (steps or NOTICE_SUMMARIES)]
        return self.env['mail.activity'].sudo().search([
            ('res_model', '=', self._name), ('res_id', '=', self.id), ('sgi_cron_key', 'in', keys)])

    def _sgi_dev_notice_close(self, steps, reason):
        self.ensure_one()
        activities = self._sgi_dev_notice_open(steps)
        if activities:
            self.env['sgi.cron']._sgi_close_activities(activities, reason)
        return activities

    def _sgi_dev_notice(self, step, users, number, note):
        """Una actividad por persona del puesto (sin repetir una abierta del mismo paso)."""
        self.ensure_one()
        key = '%s:%s' % (NOTICE_KIND, step)
        summary = NOTICE_SUMMARIES[step]
        folio = self.sgi_ft_folio or self.name
        if not users:
            self.message_post(body="Aviso «%s» sin destinatario: nadie ocupa el puesto en el SGI (%s)."
                                   % (summary, number), message_type='comment', subtype_xmlid='mail.mt_note')
            return self.env['mail.activity']
        deadline = self._sgi_dev_c1_deadline(number)
        open_users = self._sgi_dev_notice_open([step]).user_id
        created = self.env['mail.activity']
        for user in users - open_users:
            activity = self.activity_schedule(
                'mail.mail_activity_data_todo', date_deadline=deadline, user_id=user.id,
                summary="%s · %s" % (summary, folio), note=note)
            activity.sudo().write({'sgi_cron_key': key})
            created |= activity
        return created

    def _sgi_dev_notice_review(self):
        """Paso 2 (y 1 para línea / no factible): Ventas revisa el análisis."""
        for project in self:
            project._sgi_dev_notice_close(['checklist'], "el análisis quedó listo para revisión de Ventas")
            jobs = project._sgi_dev_c1_role_jobs(['C1.02', 'C1.03'], 'aprueba', 'ventas')
            result = dict(project._fields['sgi_dev_analysis_result']._description_selection(project.env)).get(
                project.sgi_dev_analysis_result, '')
            project._sgi_dev_notice('revisar', project._sgi_dev_job_users(jobs), 'C1.03',
                                    "Diseño terminó el análisis (%s) del desarrollo <b>%s</b>. Revise el análisis "
                                    "y la factibilidad: apruebe o regrese a Diseño con el motivo."
                                    % (result, escape(project.sgi_ft_folio or project.name)))

    def _sgi_dev_notice_applies(self):
        self.ensure_one()
        return (self.sgi_is_ft and not self.is_template and self.sgi_dev_review_state == 'pendiente'
                and self.sgi_dev_stage_key in REVIEW_STAGE_KEYS)

    def _sgi_dev_notice_result(self):
        """Paso 1: Diseño de Producto registró el resultado del análisis."""
        for project in self.filtered(lambda p: p._sgi_dev_notice_applies() and p.sgi_dev_analysis_result):
            if project.sgi_dev_analysis_result != 'nuevo':
                project._sgi_dev_notice_review()
                continue
            project._sgi_dev_notice_close(['revisar'], "el resultado cambió a producto nuevo")
            if project.sgi_dev_feasibility_ids and not project.sgi_dev_feasibility_pending:
                project._sgi_dev_notice_review()
                continue
            jobs = project._sgi_dev_c1_role_jobs(['C1.03'], 'ejecuta', 'procesos')
            project._sgi_dev_notice('checklist', project._sgi_dev_job_users(jobs), 'C1.03',
                                    "Diseño de Producto terminó el análisis del desarrollo <b>%s</b>: producto "
                                    "nuevo. Cargue y conteste el checklist de factibilidad en la pestaña «Análisis "
                                    "de proyecto»." % escape(project.sgi_ft_folio or project.name))

    def _sgi_dev_notice_checklist_done(self):
        """Paso 2: el checklist quedó sin renglones pendientes."""
        for project in self.filtered(lambda p: p._sgi_dev_notice_applies()
                                     and p.sgi_dev_analysis_result == 'nuevo' and p.sgi_dev_feasibility_ids
                                     and not p.sgi_dev_feasibility_pending):
            if not project._sgi_dev_notice_open(['revisar']):
                project._sgi_dev_notice_review()

    def write(self, vals):
        res = super().write(vals)
        if self.env.context.get('sgi_dev_migration'):
            return res
        if vals.get('sgi_dev_analysis_result'):
            self._sgi_dev_notice_result()
        if 'stage_id' in vals:
            for project in self.filtered(lambda p: p.sgi_is_ft and p.sgi_dev_stage_key not in REVIEW_STAGE_KEYS
                                         and p.sgi_dev_review_state != 'regresado'):
                project._sgi_dev_notice_close(None, "el desarrollo salió de la revisión del análisis")
        return res

    # ------------------------------------------------------------------
    # Pasos 3 a 5 (botones)
    # ------------------------------------------------------------------
    def action_sgi_dev_review_approve(self):
        res = super().action_sgi_dev_review_approve()
        for project in self.filtered(lambda p: p.sgi_is_ft and p.sgi_dev_review_state == 'aprobado'):
            project._sgi_dev_notice_close(None, "Ventas aprobó el análisis y la factibilidad")
        return res

    def action_sgi_dev_review_return(self):
        res = super().action_sgi_dev_review_return()
        for project in self.filtered(lambda p: p.sgi_is_ft and p.sgi_dev_review_state == 'regresado'):
            project._sgi_dev_notice_close(['revisar', 'correccion'], "Ventas regresó el análisis a Diseño")
            reason = project.sgi_dev_review_note or ''
            folio = project.sgi_ft_folio or project.name
            note = ("Ventas regresó el análisis del desarrollo <b>%s</b>. Motivo: %s<br/>Al terminar use «Ya "
                    "corregí» en la pestaña «Análisis de proyecto»." % (escape(folio), escape(reason)))
            users = self.env['res.users']
            for number, fallback in (('C1.02', 'producto'), ('C1.03', 'procesos')):
                job_users = project._sgi_dev_job_users(project._sgi_dev_c1_role_jobs([number], 'ejecuta', fallback))
                project._sgi_dev_notice('corregir', job_users - users, number, note)
                users |= job_users
            if users:
                project.message_post(body="Ventas regresó el análisis a Diseño: %s" % reason,
                                     partner_ids=users.partner_id.ids, message_type='comment',
                                     subtype_xmlid='mail.mt_comment')
        return res

    def action_sgi_dev_review_fixed(self):
        """Paso 4: Diseño de Producto o de Procesos corrigió lo que pidió Ventas."""
        for project in self.filtered('sgi_is_ft'):
            if project.sgi_dev_review_state != 'regresado':
                raise UserError("La revisión de Ventas no está regresada.")
            if not (self.env.user in project._sgi_dev_fixer_users()
                    or self.env.user.has_group('quimibond_sgi.group_sgi_manager')):
                raise UserError("Solo Diseño de Producto o Diseño de Procesos (o su jefe directo) marca la "
                                "corrección.")
            comment = (project.sgi_dev_fix_note or '').strip()
            project.write({'sgi_dev_review_state': 'pendiente', 'sgi_dev_fix_note': False})
            mine = project._sgi_dev_notice_open(['corregir']).filtered(lambda a: a.user_id == self.env.user)
            if mine:
                mine.action_feedback(feedback=comment or "Corregido.")
            project._sgi_dev_notice_close(['corregir'], "%s ya corrigió" % self.env.user.name)
            folio = project.sgi_ft_folio or project.name
            text = "%s corrigió lo que pidió Ventas en el desarrollo %s%s" % (
                self.env.user.name, folio, (": %s" % comment) if comment else ".")
            reviewer = project.sgi_dev_reviewed_by_id
            if reviewer:
                project._sgi_dev_notice('correccion', reviewer, 'C1.03',
                                        "%s<br/>Motivo del regreso: %s<br/>Revise y apruebe o vuelva a regresar."
                                        % (escape(text), escape(project.sgi_dev_review_note or '')))
            project.message_post(body=text, partner_ids=reviewer.partner_id.ids, message_type='comment',
                                 subtype_xmlid='mail.mt_comment')
        return True

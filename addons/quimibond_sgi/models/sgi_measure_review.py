# -*- coding: utf-8 -*-
"""57.107.0: revisión mensual de la medición por el dueño del proceso.

Cada mes, por cada actividad que se mide sola (Registro en Odoo o Por su
entregable) y tiene evidencia reciente, el dueño del proceso recibe en Mis
pendientes tres registros al azar de esa evidencia y contesta:

- «Sí, esto es lo que hago»: la medición cuenta lo que debe.
- «No corresponde», con nota: la evidencia no es la actividad (filtro,
  modelo, fecha o usuario mal). La actividad gana el faltante «Revisión del
  dueño: no corresponde» en Diagnóstico → Faltantes de especificación (filtro
  «Medición por revisar») hasta que una revisión posterior la confirme.

La crea el cron de medición (diario, idempotente: una por actividad y mes).
Al crear las del mes, las del mes anterior que nadie contestó quedan «Sin
respuesta». Sin dueño con usuario activo, la recibe el Jefe MAST.
"""
import random
from datetime import timedelta

from odoo import api, fields, models
from odoo.exceptions import AccessError, UserError

from .sgi_calendar import sgi_add_business_days, sgi_local_date, sgi_today

REVIEW_STATES = [
    ('pendiente', "Pendiente"),
    ('confirmada', "Sí, esto es lo que hago"),
    ('no_corresponde', "No corresponde"),
    ('sin_respuesta', "Sin respuesta"),
]
REVIEW_SAMPLE = 3
# Ventana de la que salen las muestras (días hacia atrás desde hoy).
REVIEW_WINDOW_DAYS = 60
REVIEW_DAYS_PARAM = 'quimibond_sgi.measure_review_business_days'   # (5)


class SgiMeasureReview(models.Model):
    """Revisión mensual de la evidencia de una actividad por el dueño del proceso."""
    _name = 'sgi.measure.review'
    _description = "Revisión mensual de medición"
    _inherit = ['mail.thread']
    _order = 'month desc, process_id, activity_id, id'

    activity_id = fields.Many2one(
        'sgi.process.activity', string="Actividad", required=True, readonly=True,
        ondelete='cascade', index=True)
    process_id = fields.Many2one(
        related='activity_id.process_id', string="Proceso", store=True, index=True)
    company_id = fields.Many2one(
        related='activity_id.company_id', string="Empresa", store=True, index=True)
    month = fields.Date(string="Mes", required=True, readonly=True, index=True,
                        help="Primer día del mes revisado.")
    user_id = fields.Many2one(
        'res.users', string="Revisa", required=True, readonly=True, index=True,
        help="Usuario del dueño del proceso (sin él, el Jefe MAST).")
    date_due = fields.Date(string="Vence", readonly=True,
                           help="Días hábiles desde que se creó (parámetro "
                                "quimibond_sgi.measure_review_business_days, 5).")
    name = fields.Char(string="Qué", compute='_compute_name', store=True)
    measure_model = fields.Char(string="Modelo de evidencia", readonly=True)
    measure_domain = fields.Char(string="Filtro de evidencia", readonly=True,
                                 help="El filtro con el que se tomaron las muestras.")
    measure_user_field = fields.Char(string="Campo de usuario", readonly=True)
    line_ids = fields.One2many('sgi.measure.review.line', 'review_id', string="Muestras",
                               readonly=True)
    state = fields.Selection(REVIEW_STATES, string="Estado", default='pendiente',
                             required=True, readonly=True, index=True, tracking=True)
    note = fields.Text(string="Qué no corresponde", readonly=True,
                       help="Obligatoria con «No corresponde»: qué está mal (el filtro, el modelo, "
                            "la fecha o quién aparece).")
    reviewed_by_id = fields.Many2one('res.users', string="Contestó", readonly=True)
    reviewed_on = fields.Datetime(string="Contestada el", readonly=True)
    done_criteria = fields.Text(related='activity_id.done_criteria', string="Criterio de terminado")
    can_answer = fields.Boolean(string="Puede contestar", compute='_compute_can_answer')

    _activity_month_uniq = models.Constraint(
        'unique(activity_id, month)', "Una revisión por actividad y mes.")

    @api.depends('activity_id.number', 'activity_id.legacy_number', 'activity_id.name', 'month')
    def _compute_name(self):
        for rec in self:
            activity = rec.activity_id
            number = activity.number or activity.legacy_number or ''
            label = ("%s %s" % (number, activity.name or '')).strip()
            rec.name = "Revisar medición de %s (%s)" % (
                label, rec.month.strftime('%m/%Y') if rec.month else '')

    @api.depends_context('uid')
    def _compute_can_answer(self):
        for rec in self:
            rec.can_answer = rec.state == 'pendiente' and rec._sgi_can_answer()

    def _sgi_can_answer(self):
        self.ensure_one()
        env = self.env
        return bool(env.su or env.user.has_group('quimibond_sgi.group_sgi_manager')
                    or self.sudo().user_id == env.user)

    def _sgi_answer(self, state, note=False):
        note = (note or '').strip()
        for rec in self:
            if not rec._sgi_can_answer():
                raise AccessError("Solo quien revisa (el dueño del proceso) o el Jefe MAST "
                                  "contestan «%s»." % rec.sudo().name)
            if rec.state != 'pendiente':
                raise UserError("«%s» ya se contestó." % rec.sudo().name)
            if state == 'no_corresponde' and not note:
                raise UserError("Diga qué no corresponde: el filtro, el modelo, la fecha o quién "
                                "aparece.")
            rec.sudo().write({'state': state, 'note': note or False,
                              'reviewed_by_id': self.env.uid,
                              'reviewed_on': fields.Datetime.now()})
            rec.sudo().message_post(body="%s%s" % (
                dict(REVIEW_STATES)[state], (": %s" % note) if note else ''))
        # El faltante «Revisión del dueño: no corresponde» cambia en el momento.
        self.sudo().activity_id._sgi_refresh_spec_gaps()
        return True

    def action_confirm(self):
        """«Sí, esto es lo que hago»."""
        self._sgi_answer('confirmada')
        return {'type': 'ir.actions.client', 'tag': 'soft_reload'}

    def action_not_matching(self):
        """«No corresponde»: pide la nota en un asistente."""
        self.ensure_one()
        if not self._sgi_can_answer():
            raise AccessError("Solo quien revisa (el dueño del proceso) o el Jefe MAST contestan "
                              "esta revisión.")
        return {
            'type': 'ir.actions.act_window', 'name': "No corresponde",
            'res_model': 'sgi.measure.review.reject', 'view_mode': 'form',
            'views': [(self.env.ref('quimibond_sgi.sgi_measure_review_reject_view_form').id, 'form')],
            'target': 'new', 'context': {'default_review_id': self.id},
        }

    def action_open_activity(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'res_model': 'sgi.process.activity',
                'res_id': self.activity_id.id, 'view_mode': 'form', 'target': 'current'}

    # ------------------------------------------------------------------
    # Cron
    # ------------------------------------------------------------------
    @api.model
    def _sgi_review_user(self, activity):
        owner = activity.process_id.owner_id.user_id
        if owner and owner.active:
            return owner
        manager_id = self.env['sgi.cron']._sgi_manager_user_id()
        return self.env['res.users'].browse(manager_id) if manager_id else owner.browse()

    @api.model
    def _sgi_generate_month(self, today=None):
        """Crea las revisiones del mes que falten (una por actividad que se
        mide sola y tiene evidencia en los últimos 60 días) y deja «Sin
        respuesta» las pendientes de meses anteriores. Devuelve las creadas."""
        env = self.env
        today = today or sgi_today(env)
        month = today.replace(day=1)
        Review = self.sudo()
        stale = Review.search([('state', '=', 'pendiente'), ('month', '<', month)])
        if stale:
            stale.write({'state': 'sin_respuesta'})
        done = set(Review.search([('month', '=', month)]).mapped('activity_id').ids)
        activities = env['sgi.process.activity'].sudo().search([
            ('measure_model_id', '!=', False), ('measure_method', 'in', (False, 'odoo', 'entregable')),
            ('process_id.active', '=', True)])
        days = int(env['ir.config_parameter'].sudo().get_param(REVIEW_DAYS_PARAM, 5) or 5)
        due = sgi_add_business_days(env, today, days)
        created = Review.browse()
        for activity in activities:
            if activity.id in done:
                continue
            user = self._sgi_review_user(activity)
            if not user:
                continue
            source = activity._sgi_evidence_source()
            if not source:
                continue
            Model, domain, date_field = source
            since = fields.Datetime.now() - timedelta(days=REVIEW_WINDOW_DAYS)
            if Model._fields[date_field].type == 'date':
                since = since.date()
            ids = Model.search(domain + [(date_field, '>=', since)],
                               order='%s desc, id desc' % date_field, limit=200).ids
            if not ids:
                continue
            # Al azar, pero en el orden de la evidencia (la más reciente arriba).
            chosen = sorted(random.sample(range(len(ids)), min(REVIEW_SAMPLE, len(ids))))
            sample = Model.browse([ids[i] for i in chosen])
            user_field = (activity.measure_user_field or '').strip()
            if user_field not in Model._fields or Model._fields[user_field].type != 'many2one' \
                    or Model._fields[user_field].comodel_name != 'res.users':
                user_field = False
            lines = []
            for rec in sample:
                value = rec[date_field]
                lines.append((0, 0, {
                    'res_model': Model._name, 'res_id': rec.id,
                    'name': rec.display_name or "%s,%d" % (Model._name, rec.id),
                    'record_date': sgi_local_date(env, value) if value else False,
                    'record_user_id': rec[user_field].id if user_field else False,
                }))
            created |= Review.with_context(mail_create_nolog=True).create({
                'activity_id': activity.id, 'month': month, 'user_id': user.id,
                'date_due': due, 'measure_model': Model._name,
                'measure_domain': activity.measure_domain or '[]',
                'measure_user_field': user_field or False, 'line_ids': lines,
            })
        return created


class SgiMeasureReviewLine(models.Model):
    """Un registro de la evidencia tomado al azar para la revisión."""
    _name = 'sgi.measure.review.line'
    _description = "Muestra de la revisión de medición"
    _order = 'review_id, record_date desc, id'

    review_id = fields.Many2one('sgi.measure.review', string="Revisión", required=True,
                                ondelete='cascade', index=True)
    res_model = fields.Char(string="Modelo", required=True)
    res_id = fields.Integer(string="Registro", required=True)
    name = fields.Char(string="Registro de evidencia")
    record_date = fields.Date(string="Fecha que cuenta la medición")
    record_user_id = fields.Many2one('res.users', string="A quién se le atribuye")

    def action_open_record(self):
        """Abre el registro con los permisos de quien revisa."""
        self.ensure_one()
        if self.res_model not in self.env:
            raise UserError("El modelo %s ya no existe." % self.res_model)
        record = self.env[self.res_model].browse(self.res_id)
        if not record.sudo().exists():
            raise UserError("El registro ya no existe.")
        return {'type': 'ir.actions.act_window', 'res_model': self.res_model,
                'res_id': self.res_id, 'view_mode': 'form', 'target': 'new'}


class SgiMeasureReviewReject(models.TransientModel):
    """Asistente de «No corresponde» (la nota es obligatoria)."""
    _name = 'sgi.measure.review.reject'
    _description = "Revisión de medición: no corresponde"

    review_id = fields.Many2one('sgi.measure.review', required=True, ondelete='cascade')
    note = fields.Text(string="Qué no corresponde", required=True,
                       help="El filtro, el modelo, la fecha o quién aparece.")

    def action_confirm(self):
        self.ensure_one()
        self.review_id._sgi_answer('no_corresponde', self.note)
        return {'type': 'ir.actions.client', 'tag': 'soft_reload'}


class SgiActivityMeasureReview(models.Model):
    """El faltante «Revisión del dueño: no corresponde» y el paso del cron."""
    _inherit = 'sgi.process.activity'

    def _sgi_review_rejected(self):
        """Mensaje si la última revisión contestada dice «No corresponde»."""
        self.ensure_one()
        if not self.id:
            return None
        last = self.env['sgi.measure.review'].sudo().search(
            [('activity_id', '=', self.id), ('state', 'in', ('confirmada', 'no_corresponde'))],
            order='month desc, reviewed_on desc, id desc', limit=1)
        if not last or last.state != 'no_corresponde':
            return None
        return ("El dueño del proceso revisó la evidencia de %s y dijo que no corresponde: %s" % (
            last.month.strftime('%m/%Y'), (last.note or '').strip()))

    @api.model
    def cron_measure_activities(self):
        res = super().cron_measure_activities()
        self.env['sgi.cron']._sgi_step(
            "revisión mensual de medición",
            lambda: self.env['sgi.measure.review']._sgi_generate_month())
        return res

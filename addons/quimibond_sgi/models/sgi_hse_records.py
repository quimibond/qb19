# -*- coding: utf-8 -*-
"""Seguridad e higiene fuera del papel (56.21.0).

- **Estudios de higiene y exámenes médicos** por trabajador (E2.31, E2.32):
  qué estudio o examen, cuándo se hizo, resultado y cuándo vence. El cron
  diario de competencias avisa a RH 30 días antes y cuando ya venció.
  Son datos de salud: solo el grupo «Salud ocupacional (SGI)» y el Jefe MAST
  los ven (F-004, auditoría 2026-09; antes también «Empleados / Encargado»).
- **Recorrido de la Comisión de Seguridad e Higiene** (E2.29): el acta del
  recorrido con quién participó y sus hallazgos; cada hallazgo se corrige en
  el momento, se queda sin acción (con motivo) o genera su no conformidad
  con origen «Recorrido de la Comisión de Seguridad e Higiene».
"""
from dateutil.relativedelta import relativedelta

from odoo import api, fields, models
from odoo.exceptions import UserError

from .sgi_guard import sgi_require_system

_HEALTH_KINDS = [
    ('examen_medico', "Examen médico"),
    ('estudio_higiene', "Estudio de higiene"),
]
_HEALTH_RESULTS = [
    ('apto', "Apto / conforme"),
    ('restricciones', "Apto con restricciones / con observaciones"),
    ('no_apto', "No apto / no conforme"),
]


class SgiHealthRecord(models.Model):
    _name = 'sgi.health.record'
    _description = "Estudio de higiene o examen médico por trabajador"
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _order = 'next_date, employee_id'

    employee_id = fields.Many2one('hr.employee', string="Trabajador", required=True, index=True, tracking=True)
    department_id = fields.Many2one(related='employee_id.department_id', string="Departamento", store=True)
    job_id = fields.Many2one(related='employee_id.job_id', string="Puesto", store=True)
    kind = fields.Selection(_HEALTH_KINDS, string="Tipo", required=True, default='examen_medico', tracking=True)
    name = fields.Char(string="Estudio o examen", required=True, tracking=True,
                       help="Audiometría, espirometría, examen de ingreso, ruido (NOM-011), iluminación (NOM-025)…")
    date = fields.Date(string="Fecha", required=True, default=fields.Date.context_today, tracking=True)
    validity_months = fields.Integer(string="Vigencia (meses)", default=12)
    next_date = fields.Date(string="Vence", compute='_compute_next_date', store=True, readonly=False,
                            tracking=True, help="Fecha + vigencia; se puede corregir a mano.")
    result = fields.Selection(_HEALTH_RESULTS, string="Resultado", tracking=True)
    provider_id = fields.Many2one('res.partner', string="Laboratorio / médico")
    notes = fields.Text(string="Observaciones y restricciones")
    attachment_ids = fields.Many2many('ir.attachment', string="Resultados (PDF)")
    state = fields.Selection([
        ('vigente', "Vigente"),
        ('por_vencer', "Por vencer (30 días)"),
        ('vencido', "Vencido"),
    ], string="Vigencia", compute='_compute_state', store=True)
    company_id = fields.Many2one('res.company', default=lambda self: self.env.company, required=True)

    @api.depends('date', 'validity_months')
    def _compute_next_date(self):
        for rec in self:
            rec.next_date = rec.date + relativedelta(months=rec.validity_months) \
                if rec.date and rec.validity_months else False

    @api.depends('next_date')
    def _compute_state(self):
        today = fields.Date.context_today(self)
        for rec in self:
            if not rec.next_date or rec.next_date > today + relativedelta(days=30):
                rec.state = 'vigente'
            elif rec.next_date < today:
                rec.state = 'vencido'
            else:
                rec.state = 'por_vencer'

    @api.model
    def _sgi_expiry_notices(self):
        """Cron diario: RH recibe una actividad por estudio o examen por
        vencer (30 días) y otra cuando ya venció."""
        today = fields.Date.context_today(self)
        records = self.sudo().search([('next_date', '!=', False),
                                      ('next_date', '<=', today + relativedelta(days=30))])
        records._compute_state()
        Cron = self.env['sgi.cron']
        rh_id = Cron._sgi_rh_user_id()
        latest = {}
        for rec in self.sudo().search([], order='date desc, id desc'):
            latest.setdefault((rec.employee_id.id, rec.name.strip().lower()), rec)
        for rec in records:
            if latest.get((rec.employee_id.id, rec.name.strip().lower())) != rec:
                continue  # ya hay uno más reciente del mismo estudio
            late = rec.next_date < today
            Cron._sgi_schedule(
                rec, "%s %s de %s" % ("Vencido:" if late else "Por vencer:", rec.name, rec.employee_id.name),
                "Vence el %s. Programa el %s." % (rec.next_date, dict(_HEALTH_KINDS)[rec.kind].lower()),
                rh_id)
        return len(records)


class HrEmployeeHealth(models.Model):
    _inherit = 'hr.employee'

    sgi_health_record_ids = fields.One2many(
        'sgi.health.record', 'employee_id', string="Estudios y exámenes",
        groups='quimibond_sgi.group_sgi_health,quimibond_sgi.group_sgi_manager')


class SgiCronHealth(models.AbstractModel):
    _inherit = 'sgi.cron'

    @api.model
    def cron_competences(self):
        sgi_require_system(self.env)  # F-008
        res = super().cron_competences()
        self._sgi_step("estudios de higiene y exámenes médicos por vencer",
                       lambda: self.env['sgi.health.record']._sgi_expiry_notices())
        return res


class SgiCshInspection(models.Model):
    _name = 'sgi.csh.inspection'
    _description = "Recorrido de la Comisión de Seguridad e Higiene"
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _order = 'date desc, id desc'

    name = fields.Char(string="Folio", readonly=True, copy=False, default="Nuevo")
    date = fields.Date(string="Fecha del recorrido", required=True, default=fields.Date.context_today,
                       tracking=True)
    area = fields.Char(string="Áreas recorridas")
    department_ids = fields.Many2many('hr.department', string="Departamentos")
    participant_ids = fields.Many2many('hr.employee', string="Integrantes de la Comisión")
    notes = fields.Text(string="Acta / observaciones generales")
    attachment_ids = fields.Many2many('ir.attachment', string="Acta firmada (PDF) y fotos")
    finding_ids = fields.One2many('sgi.csh.finding', 'inspection_id', string="Hallazgos")
    finding_count = fields.Integer(compute='_compute_counts', string="Número de hallazgos")
    nc_count = fields.Integer(compute='_compute_counts', string="NC")
    state = fields.Selection([
        ('borrador', "En captura"),
        ('cerrado', "Cerrado"),
    ], string="Estado", default='borrador', tracking=True)
    company_id = fields.Many2one('res.company', default=lambda self: self.env.company, required=True)

    @api.depends('finding_ids.alert_id')
    def _compute_counts(self):
        for rec in self:
            rec.finding_count = len(rec.finding_ids)
            rec.nc_count = len(rec.finding_ids.alert_id)

    @api.model_create_multi
    def create(self, vals_list):
        for vals in vals_list:
            if vals.get('name', "Nuevo") == "Nuevo":
                vals['name'] = "CSH-%s" % fields.Date.to_string(
                    fields.Date.to_date(vals.get('date')) or fields.Date.context_today(self))
        return super().create(vals_list)

    def action_close(self):
        for rec in self:
            pending = rec.finding_ids.filtered(lambda f: not f.disposition or (
                f.disposition == 'nc' and not f.alert_id) or (
                f.disposition == 'sin_accion' and not (f.justification or '').strip()))
            if pending:
                raise UserError(
                    "No se puede cerrar el recorrido %s: cada hallazgo necesita qué se hizo "
                    "(corregido en el momento, NC generada o sin acción con motivo). Faltan %d." % (
                        rec.name, len(pending)))
            rec.state = 'cerrado'

    def action_reopen(self):
        self.write({'state': 'borrador'})

    def action_view_ncs(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'name': "NC del recorrido %s" % self.name,
                'res_model': 'quality.alert', 'view_mode': 'list,form',
                'domain': [('id', 'in', self.finding_ids.alert_id.ids)]}


class SgiCshFinding(models.Model):
    _name = 'sgi.csh.finding'
    _description = "Hallazgo del recorrido de la Comisión de Seguridad e Higiene"
    _order = 'inspection_id, sequence, id'

    inspection_id = fields.Many2one('sgi.csh.inspection', required=True, ondelete='cascade', index=True)
    sequence = fields.Integer(default=10)
    description = fields.Text(string="Condición o acto inseguro", required=True)
    location = fields.Char(string="Lugar")
    severity = fields.Selection([
        ('alta', "Alta"), ('media', "Media"), ('baja', "Baja"),
    ], string="Riesgo", default='media')
    responsible_id = fields.Many2one('res.users', string="Responsable")
    disposition = fields.Selection([
        ('corregido', "Corregido en el momento"),
        ('nc', "Genera no conformidad"),
        ('sin_accion', "Sin acción"),
    ], string="Qué se hizo")
    justification = fields.Char(string="Motivo (sin acción)")
    alert_id = fields.Many2one('quality.alert', string="No conformidad", readonly=True, copy=False)

    def action_generate_nc(self):
        """El hallazgo nace como NC con origen «Recorrido de la Comisión de
        Seguridad e Higiene»."""
        team = self.env.ref('quimibond_sgi.sgi_quality_team_internal', raise_if_not_found=False)
        for finding in self:
            if finding.alert_id:
                raise UserError("Este hallazgo ya tiene su no conformidad.")
            inspection = finding.inspection_id
            vals = {
                'title': "Recorrido CSH %s: %s" % (inspection.name, (finding.description or '')[:60]),
                'sgi_origin_type': 'recorrido_csh',
                'sgi_classification': 'mayor' if finding.severity == 'alta' else 'menor',
                'sgi_deviation': "%s%s" % (finding.description or '',
                                           " (lugar: %s)" % finding.location if finding.location else ''),
            }
            if finding.responsible_id:
                vals['user_id'] = finding.responsible_id.id
            if team:
                vals['team_id'] = team.id
            # sudo: quien captura el recorrido no siempre crea NC en Calidad.
            alert = self.env['quality.alert'].sudo().create(vals)
            finding.write({'disposition': 'nc', 'alert_id': alert.id})
            alert.message_post(body="Hallazgo del recorrido de la Comisión de Seguridad e Higiene %s (%s)." % (
                inspection.name, inspection.date))
        return True

# -*- coding: utf-8 -*-
from odoo import models, fields, api
from odoo.exceptions import UserError, ValidationError

MONTH_SELECTION = [
    ('1', "Enero"), ('2', "Febrero"), ('3', "Marzo"), ('4', "Abril"),
    ('5', "Mayo"), ('6', "Junio"), ('7', "Julio"), ('8', "Agosto"),
    ('9', "Septiembre"), ('10', "Octubre"), ('11', "Noviembre"), ('12', "Diciembre"),
]

FINDING_TO_CLASS = {
    'nc_mayor': 'mayor',
    'nc_menor': 'menor',
    'observacion': 'observacion',
}


class SgiAuditProgram(models.Model):
    _name = 'sgi.audit.program'
    _description = "Programa anual de auditorías (P-G03)"
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _order = 'year desc'

    name = fields.Char(string="Nombre", compute='_compute_name', store=True)
    year = fields.Integer(string="Año", required=True,
                          default=lambda self: fields.Date.context_today(self).year)
    state = fields.Selection([
        ('borrador', "Borrador"),
        ('aprobado', "Aprobado"),
        ('cerrado', "Cerrado"),
    ], string="Estado", default='borrador', required=True, tracking=True)
    line_ids = fields.One2many('sgi.audit.program.line', 'program_id', string="Líneas")

    _year_uniq = models.Constraint(
        'unique(year)',
        "Ya existe un programa de auditorías para ese año.",
    )

    @api.depends('year')
    def _compute_name(self):
        for program in self:
            program.name = "Programa de auditorías %s" % (program.year or '')

    def action_approve(self):
        for program in self:
            program.state = 'aprobado'
            manager_id = self.env['sgi.cron']._sgi_manager_user_id()
            if manager_id:
                program.activity_schedule(
                    'mail.mail_activity_data_todo',
                    summary="Programa de auditorías %s aprobado" % program.year,
                    note="Coordine la ejecución de las auditorías planificadas.",
                    user_id=manager_id)
        return True

    def action_close(self):
        self.write({'state': 'cerrado'})

    def action_draft(self):
        self.write({'state': 'borrador'})


class SgiAuditProgramLine(models.Model):
    _name = 'sgi.audit.program.line'
    _description = "Línea del programa de auditorías"
    _order = 'planned_month, id'

    program_id = fields.Many2one('sgi.audit.program', string="Programa",
                                 required=True, ondelete='cascade')
    process_id = fields.Many2one('sgi.process', string="Proceso")
    planned_month = fields.Selection(MONTH_SELECTION, string="Mes planificado", required=True)
    audit_type = fields.Selection([
        ('interna', "Interna"),
        ('externa', "Externa"),
    ], string="Tipo", default='interna', required=True)
    norm_ids = fields.Many2many('sgi.norm', string="Normas")
    lead_auditor_id = fields.Many2one('res.users', string="Auditor líder")
    state = fields.Selection([
        ('pendiente', "Pendiente"),
        ('creada', "Auditoría creada"),
    ], string="Estado", default='pendiente', required=True)
    audit_id = fields.Many2one('sgi.audit', string="Auditoría", readonly=True)

    def action_create_audit(self):
        self.ensure_one()
        audit = self.env['sgi.audit'].create({
            'program_line_id': self.id,
            'audit_type': self.audit_type,
            'norm_ids': [(6, 0, self.norm_ids.ids)],
            'process_ids': [(6, 0, self.process_id.ids)],
            'lead_auditor_id': self.lead_auditor_id.id,
        })
        self.write({'state': 'creada', 'audit_id': audit.id})
        return {
            'type': 'ir.actions.act_window',
            'name': "Auditoría",
            'res_model': 'sgi.audit',
            'res_id': audit.id,
            'view_mode': 'form',
        }


class SgiAudit(models.Model):
    _name = 'sgi.audit'
    _description = "Auditoría interna (P-G03)"
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _order = 'date_planned desc, folio desc'

    folio = fields.Char(string="Folio", readonly=True, copy=False, index=True, tracking=True)
    name = fields.Char(string="Nombre", compute='_compute_name', store=True)
    program_line_id = fields.Many2one('sgi.audit.program.line', string="Línea de programa")
    audit_type = fields.Selection([
        ('interna', "Interna"),
        ('externa', "Externa"),
    ], string="Tipo", default='interna', required=True, tracking=True)
    norm_ids = fields.Many2many('sgi.norm', string="Normas")
    process_ids = fields.Many2many('sgi.process', string="Procesos auditados")
    lead_auditor_id = fields.Many2one('res.users', string="Auditor líder", tracking=True)
    auditor_ids = fields.Many2many('res.users', 'sgi_audit_auditor_rel',
                                   'audit_id', 'user_id', string="Equipo auditor")
    auditee_ids = fields.Many2many('res.users', 'sgi_audit_auditee_rel',
                                   'audit_id', 'user_id', string="Auditados")
    date_planned = fields.Date(string="Fecha planificada")
    date_start = fields.Date(string="Inicio real")
    date_end = fields.Date(string="Fin real")
    survey_id = fields.Many2one('survey.survey', string="Checklist (encuesta)")
    survey_input_ids = fields.Many2many('survey.user_input', 'sgi_audit_input_rel',
                                        'audit_id', 'input_id', string="Respuestas del checklist")
    conclusion = fields.Text(string="Conclusión")
    state = fields.Selection([
        ('borrador', "Borrador"),
        ('planificada', "Planificada"),
        ('en_ejecucion', "En ejecución"),
        ('informe', "Informe"),
        ('cerrada', "Cerrada"),
    ], string="Estado", default='borrador', required=True, tracking=True)
    finding_ids = fields.One2many('sgi.audit.finding', 'audit_id', string="Hallazgos")
    finding_count = fields.Integer(string="# Hallazgos", compute='_compute_finding_count')

    @api.depends('folio', 'audit_type')
    def _compute_name(self):
        for audit in self:
            audit.name = audit.folio or ("Auditoría %s" % dict(
                self._fields['audit_type'].selection).get(audit.audit_type, ''))

    def _compute_finding_count(self):
        for audit in self:
            audit.finding_count = len(audit.finding_ids)

    @api.model_create_multi
    def create(self, vals_list):
        seq = self.env['ir.sequence']
        for vals in vals_list:
            if not vals.get('folio'):
                vals['folio'] = seq.next_by_code('sgi.audit') or '/'
        return super().create(vals_list)

    @api.constrains('process_ids', 'lead_auditor_id', 'auditor_ids')
    def _check_auditor_independence(self):
        for audit in self:
            auditor_users = audit.lead_auditor_id | audit.auditor_ids
            if not auditor_users or not audit.process_ids:
                continue
            owner_users = audit.process_ids.mapped('owner_id.user_id')
            conflict = auditor_users & owner_users
            if conflict:
                raise ValidationError(
                    "El auditor no puede auditar su propio trabajo: %s es dueño de "
                    "un proceso auditado." % ", ".join(conflict.mapped('name')))

    def action_plan(self):
        self.write({'state': 'planificada'})

    def action_start(self):
        self.write({'state': 'en_ejecucion'})
        for audit in self:
            if not audit.date_start:
                audit.date_start = fields.Date.context_today(audit)

    def action_report(self):
        self.write({'state': 'informe'})
        for audit in self:
            if not audit.date_end:
                audit.date_end = fields.Date.context_today(audit)

    def action_close(self):
        for audit in self:
            audit._sgi_check_can_close()
            audit.state = 'cerrada'
            if audit.program_line_id:
                audit.program_line_id.state = 'creada'
        return True

    def action_draft(self):
        self.write({'state': 'borrador'})

    def _sgi_check_can_close(self):
        self.ensure_one()
        problems = []
        for finding in self.finding_ids:
            if not finding.disposition:
                problems.append("• El hallazgo '%s' no tiene disposición." % (
                    finding.description or finding.finding_type))
            elif finding.disposition == 'genera_nc' and not finding.alert_id:
                problems.append("• El hallazgo '%s' debe generar su NC." % (
                    finding.description or finding.finding_type))
            elif finding.disposition == 'sin_accion' and not finding.reason_no_action:
                problems.append("• El hallazgo '%s' requiere justificar 'sin acción'." % (
                    finding.description or finding.finding_type))
        if problems:
            raise UserError(
                "No se puede cerrar la auditoría %s:\n%s" % (
                    self.folio or self.name, "\n".join(problems)))

    def action_open_findings(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Hallazgos — %s" % (self.folio or self.name),
            'res_model': 'sgi.audit.finding',
            'view_mode': 'list,form',
            'domain': [('audit_id', '=', self.id)],
            'context': {'default_audit_id': self.id},
        }


class SgiAuditFinding(models.Model):
    _name = 'sgi.audit.finding'
    _description = "Hallazgo de auditoría"
    _order = 'audit_id, id'

    audit_id = fields.Many2one('sgi.audit', string="Auditoría",
                               required=True, ondelete='cascade')
    finding_type = fields.Selection([
        ('conformidad', "Conformidad"),
        ('observacion', "Observación"),
        ('nc_menor', "No conformidad menor"),
        ('nc_mayor', "No conformidad mayor"),
        ('oportunidad', "Oportunidad de mejora"),
    ], string="Tipo", default='observacion', required=True)
    norm_clause_id = fields.Many2one('sgi.norm.clause', string="Cláusula")
    process_id = fields.Many2one('sgi.process', string="Proceso")
    description = fields.Text(string="Descripción")
    evidence = fields.Text(string="Evidencia")
    disposition = fields.Selection([
        ('genera_nc', "Genera NC"),
        ('sin_accion', "Sin acción"),
        ('mejora', "Mejora"),
    ], string="Disposición")
    alert_id = fields.Many2one('quality.alert', string="No Conformidad", readonly=True)
    reason_no_action = fields.Text(string="Justificación sin acción")

    def action_generate_nc(self):
        self.ensure_one()
        if self.alert_id:
            raise UserError("Este hallazgo ya tiene una NC ligada.")
        origin = 'auditoria_externa' if self.audit_id.audit_type == 'externa' \
            else 'auditoria_interna'
        team = self.env.ref('quimibond_sgi.sgi_quality_team_internal', raise_if_not_found=False)
        vals = {
            'title': "Hallazgo auditoría %s" % (self.audit_id.folio or ''),
            'sgi_origin_type': origin,
            'sgi_classification': FINDING_TO_CLASS.get(self.finding_type),
            'sgi_norm_clause_id': self.norm_clause_id.id,
            'sgi_process_id': self.process_id.id,
            'sgi_lead_auditor_id': self.audit_id.lead_auditor_id.id,
            'sgi_deviation': self.description or '',
        }
        if team:
            vals['team_id'] = team.id
        alert = self.env['quality.alert'].create(vals)
        self.write({'disposition': 'genera_nc', 'alert_id': alert.id})
        return {
            'type': 'ir.actions.act_window',
            'name': "No Conformidad",
            'res_model': 'quality.alert',
            'res_id': alert.id,
            'view_mode': 'form',
        }

# -*- coding: utf-8 -*-
"""Preparación y respuesta ante emergencias (ISO 14001 8.2 / ISO 45001 8.2).

El plan define el escenario (incendio, sismo, derrame...) y su frecuencia de
simulacro; el simulacro registra la ejecución real y sus hallazgos, que se
tratan con el modelo único de acciones CAPA (sgi.action.line con drill_id).
Un cron diario vigila simulacros vencidos o por vencer.
"""
from dateutil.relativedelta import relativedelta

from odoo import models, fields, api
from odoo.exceptions import UserError


class SgiEmergencyPlan(models.Model):
    _name = 'sgi.emergency.plan'
    _description = "Plan de emergencia (ISO 14001/45001 8.2)"
    _inherit = ['sgi.base.mixin']
    _order = 'folio desc'
    _sgi_sequence_code = 'sgi.emergency.plan'

    name = fields.Char(string="Escenario de emergencia", required=True, tracking=True)
    plan_type = fields.Selection([
        ('incendio', "Incendio"),
        ('sismo', "Sismo"),
        ('derrame_quimico', "Derrame químico"),
        ('fuga_gas', "Fuga de gas"),
        ('emergencia_medica', "Emergencia médica"),
        ('inundacion', "Inundación"),
        ('otro', "Otro"),
    ], string="Tipo", default='incendio', required=True, tracking=True)
    location = fields.Char(string="Ubicación / zona")
    responsible_id = fields.Many2one('res.users', string="Responsable (brigada)",
                                     tracking=True)
    document_id = fields.Many2one('documents.document', string="Plan documentado",
                                  domain=[('sgi_is_controlled', '=', True)])
    risk_ids = fields.Many2many('sgi.risk', string="Riesgos ligados (IPER/ambiental)",
                                domain=[('instrument', 'in', ('iper', 'ambiental'))])
    drill_frequency_months = fields.Integer(string="Frecuencia de simulacro (meses)",
                                            default=12)
    state = fields.Selection([
        ('borrador', "Borrador"),
        ('vigente', "Vigente"),
        ('obsoleto', "Obsoleto"),
    ], string="Estado", default='borrador', required=True, tracking=True)
    drill_ids = fields.One2many('sgi.emergency.drill', 'plan_id', string="Simulacros")
    # drill_count vive en su PROPIO compute: compartir método con los campos
    # almacenados de fechas (store=True) mezclaba store/compute_sudo en el
    # mismo grupo y el registry lo marcaba como inconsistente en el log de
    # producción (además de recomputar de más).
    drill_count = fields.Integer(string="# Simulacros", compute='_compute_drill_count')
    last_drill_date = fields.Date(string="Último simulacro",
                                  compute='_compute_drill_dates', store=True)
    next_drill_date = fields.Date(string="Próximo simulacro",
                                  compute='_compute_drill_dates', store=True)

    _folio_uniq = models.Constraint(
        'unique(folio)', "Ya existe un plan de emergencia con ese folio.")

    @api.depends('drill_ids')
    def _compute_drill_count(self):
        for plan in self:
            plan.drill_count = len(plan.drill_ids)

    @api.depends('drill_ids.state', 'drill_ids.date_done', 'drill_frequency_months')
    def _compute_drill_dates(self):
        for plan in self:
            done = plan.drill_ids.filtered(
                lambda d: d.state == 'realizado' and d.date_done)
            plan.last_drill_date = max(done.mapped('date_done')) if done else False
            months = plan.drill_frequency_months or 12
            plan.next_drill_date = (
                plan.last_drill_date + relativedelta(months=months)
                if plan.last_drill_date else False)

    @api.depends('folio', 'name')
    def _compute_display_name(self):
        for plan in self:
            plan.display_name = ("%s - %s" % (plan.folio, plan.name)
                                 if plan.folio else plan.name)

    def action_set_vigente(self):
        for plan in self:
            if not plan.responsible_id:
                raise UserError(
                    "El plan de emergencia %s no puede pasar a Vigente sin "
                    "responsable de brigada." % (plan.folio or plan.name))
            plan.state = 'vigente'
        return True

    def action_set_borrador(self):
        self.write({'state': 'borrador'})
        return True

    def action_set_obsoleto(self):
        self.write({'state': 'obsoleto'})
        return True

    def action_view_drills(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Simulacros — %s" % (self.folio or self.name),
            'res_model': 'sgi.emergency.drill',
            'view_mode': 'list,form',
            'domain': [('plan_id', '=', self.id)],
            'context': {'default_plan_id': self.id},
        }


class SgiEmergencyDrill(models.Model):
    _name = 'sgi.emergency.drill'
    _description = "Simulacro de emergencia"
    _inherit = ['sgi.base.mixin']
    _order = 'date_planned desc, folio desc'
    _sgi_sequence_code = 'sgi.emergency.drill'
    # Un simulacro realizado es evidencia: solo el Jefe MAST lo reabre/edita.
    _sgi_locked_states = ('realizado',)

    plan_id = fields.Many2one('sgi.emergency.plan', string="Plan de emergencia",
                              required=True, ondelete='cascade', index=True)
    plan_type = fields.Selection(related='plan_id.plan_type', store=True)
    date_planned = fields.Date(string="Fecha programada", required=True,
                               default=fields.Date.context_today, tracking=True)
    date_done = fields.Date(string="Fecha realizada", tracking=True)
    participants_count = fields.Integer(string="Participantes")
    duration_minutes = fields.Integer(string="Duración (min)")
    result = fields.Selection([
        ('satisfactorio', "Satisfactorio"),
        ('con_observaciones', "Con observaciones"),
        ('no_satisfactorio', "No satisfactorio"),
    ], string="Resultado", tracking=True)
    findings = fields.Text(string="Hallazgos / observaciones")
    action_line_ids = fields.One2many('sgi.action.line', 'drill_id',
                                      string="Acciones")
    state = fields.Selection([
        ('programado', "Programado"),
        ('realizado', "Realizado"),
        ('cancelado', "Cancelado"),
    ], string="Estado", default='programado', required=True, tracking=True)

    _folio_uniq = models.Constraint(
        'unique(folio)', "Ya existe un simulacro con ese folio.")

    @api.depends('folio', 'plan_id.name')
    def _compute_display_name(self):
        for drill in self:
            drill.display_name = ("%s - %s" % (drill.folio, drill.plan_id.name)
                                  if drill.folio else (drill.plan_id.name or ''))

    def _sgi_check_can_realize(self):
        """Requisitos para marcar realizado. Viven aparte del botón porque
        write() los aplica por cualquier vía: un write directo de state dejaba
        sellar como evidencia un simulacro sin resultado ni acciones."""
        for drill in self:
            problems = []
            if not drill.result:
                problems.append("• Falta el resultado del simulacro.")
            if not drill.participants_count:
                problems.append("• Falta el número de participantes.")
            if drill.result in ('con_observaciones', 'no_satisfactorio'):
                if not drill.findings:
                    problems.append(
                        "• Un simulacro con observaciones o no satisfactorio "
                        "requiere capturar los hallazgos.")
                if not drill.action_line_ids:
                    problems.append(
                        "• Un simulacro con observaciones o no satisfactorio "
                        "requiere al menos una acción de mejora.")
            if problems:
                raise UserError("No se puede marcar realizado el simulacro %s:\n%s"
                                % (drill.folio or '', "\n".join(problems)))

    def write(self, vals):
        if vals.get('state') == 'realizado' and not self.env.su:
            # El resultado/participantes pueden venir en el mismo write del
            # botón: se valida el estado RESULTANTE, no el previo.
            checking = self.filtered(lambda d: d.state != 'realizado')
            res = super().write(vals)
            checking._sgi_check_can_realize()
            return res
        return super().write(vals)

    def action_set_realizado(self):
        for drill in self:
            vals = {'state': 'realizado'}
            if not drill.date_done:
                vals['date_done'] = fields.Date.context_today(drill)
            drill.write(vals)  # el candado vive en write()
        return True

    def action_set_cancelado(self):
        self.write({'state': 'cancelado'})
        return True

    def action_set_programado(self):
        # Reabrir un simulacro realizado pasa por el candado de evidencia.
        self.write({'state': 'programado'})
        return True

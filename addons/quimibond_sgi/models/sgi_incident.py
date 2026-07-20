# -*- coding: utf-8 -*-
from odoo import models, fields, api
from odoo.exceptions import UserError


class SgiIncident(models.Model):
    _name = 'sgi.incident'
    _description = "Incidente / Accidente SST (P-S02, SCAT)"
    _inherit = ['sgi.base.mixin']
    _order = 'folio desc'
    _sgi_sequence_code = 'sgi.incident'
    _sgi_locked_states = ('cerrado',)

    _folio_uniq = models.Constraint(
        'unique(folio)',
        "Ya existe un incidente con ese folio.",
    )

    name = fields.Char(string="Título", required=True, tracking=True)
    date = fields.Datetime(string="Fecha y hora", required=True,
                           default=fields.Datetime.now, tracking=True)
    incident_type = fields.Selection([
        ('lesion', "Lesión / accidente"),
        ('casi_accidente', "Casi accidente"),
        ('dano_propiedad', "Daño a la propiedad"),
        ('ambiental', "Incidente ambiental"),
        ('enfermedad_laboral', "Enfermedad laboral"),
    ], string="Tipo", default='casi_accidente', required=True, tracking=True)
    severity = fields.Selection([
        ('leve', "Leve"),
        ('moderado', "Moderado"),
        ('grave', "Grave"),
        ('fatal', "Fatal"),
    ], string="Severidad", default='leve', required=True, tracking=True)
    employee_ids = fields.Many2many('hr.employee', string="Personas afectadas")
    reporter_id = fields.Many2one('res.users', string="Reportado por",
                                  default=lambda self: self.env.user, tracking=True)
    location = fields.Char(string="Lugar")
    process_id = fields.Many2one('sgi.process', string="Proceso")
    sgi_area_id = fields.Many2one('sgi.area', string="Área SGI")
    description = fields.Text(string="Descripción del evento")
    days_lost = fields.Integer(string="Días perdidos")

    # --- Análisis SCAT (3 capas de causas) ---
    immediate_causes = fields.Text(string="Causas inmediatas (actos/condiciones)")
    basic_causes = fields.Text(string="Causas básicas (factores personales/de trabajo)")
    lack_of_control = fields.Text(string="Falta de control (sistema de gestión)")

    risk_id = fields.Many2one('sgi.risk', string="Riesgo / IPER relacionado",
                              domain="[('instrument', '=', 'iper')]")
    action_line_ids = fields.One2many('sgi.action.line', 'incident_id', string="Acciones")

    state = fields.Selection([
        ('reportado', "Reportado"),
        ('investigacion', "En investigación"),
        ('acciones', "Acciones"),
        ('cerrado', "Cerrado"),
    ], string="Estado", default='reportado', required=True, tracking=True)

    @api.model_create_multi
    def create(self, vals_list):
        incidents = super().create(vals_list)
        for incident in incidents:
            incident._sgi_notify_if_serious()
        return incidents

    def _sgi_notify_if_serious(self):
        """Incidentes graves/fatales: aviso inmediato a Jefe MAST y Dirección."""
        self.ensure_one()
        if self.severity not in ('grave', 'fatal'):
            return
        Cron = self.env['sgi.cron']
        manager_id = Cron._sgi_manager_user_id()
        summary = "Incidente %s (%s): %s" % (
            dict(self._fields['severity'].selection).get(self.severity),
            self.folio or '', self.name)
        note = "Se registró un incidente %s. Inicie la investigación SCAT de inmediato." % \
            dict(self._fields['severity'].selection).get(self.severity)
        recipients = set()
        if manager_id:
            recipients.add(manager_id)
        director_group = self.env.ref('quimibond_sgi.group_sgi_director',
                                      raise_if_not_found=False)
        if director_group:
            for user in director_group.all_user_ids:
                recipients.add(user.id)
        for user_id in recipients:
            Cron._sgi_schedule(self, summary, note, user_id)

    def _sgi_check_can_close(self):
        for incident in self:
            problems = []
            if not (incident.immediate_causes and incident.basic_causes
                    and incident.lack_of_control):
                problems.append(
                    "• Falta completar las 3 capas del análisis SCAT "
                    "(causas inmediatas, básicas y falta de control).")
            if not incident.action_line_ids:
                problems.append("• El incidente no tiene NINGUNA acción registrada.")
            pending = incident.action_line_ids.filtered(lambda l: not l.date_done)
            if pending:
                problems.append(
                    "• Hay %d acción(es) sin fecha de terminación." % len(pending))
            if problems:
                raise UserError(
                    "No se puede cerrar el incidente %s:\n%s" % (
                        incident.folio or incident.name, "\n".join(problems)))

    def action_set_investigacion(self):
        self.write({'state': 'investigacion'})
        return True

    def action_set_acciones(self):
        self.write({'state': 'acciones'})
        return True

    def action_set_cerrado(self):
        self._sgi_check_can_close()
        self.write({'state': 'cerrado'})
        return True

    def action_set_reportado(self):
        self.write({'state': 'reportado'})
        return True

    @api.depends('folio', 'name')
    def _compute_display_name(self):
        for incident in self:
            incident.display_name = "%s - %s" % (incident.folio, incident.name) \
                if incident.folio else incident.name

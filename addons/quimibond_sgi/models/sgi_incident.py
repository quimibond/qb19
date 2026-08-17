# -*- coding: utf-8 -*-
from odoo import models, fields, api
from odoo.exceptions import UserError


class QualityAlert(models.Model):
    _inherit = 'quality.alert'

    sgi_incident_id = fields.Many2one(
        'sgi.incident', string="Incidente SST de origen", readonly=True, copy=False)


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
    sgi_alert_id = fields.Many2one('quality.alert', string="No Conformidad generada",
                                   readonly=True, copy=False)

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
            incident._sgi_create_alert()
        return incidents

    def write(self, vals):
        """Reclasificar la severidad a grave/fatal FUERZA la NC y el aviso, igual
        que en el alta: un incidente que entró leve/moderado y la investigación
        eleva a grave/fatal no puede quedarse sin su NC. Se apoya en la
        idempotencia de ambos métodos y sólo dispara para los registros cuya
        severidad ANTES del write no era grave/fatal (sin duplicar avisos)."""
        escalating = self.browse()
        if vals.get('severity') in ('grave', 'fatal'):
            escalating = self.filtered(
                lambda i: i.severity not in ('grave', 'fatal'))
        res = super().write(vals)
        for incident in escalating:
            incident._sgi_notify_if_serious()
            incident._sgi_create_alert()
        return res

    def _sgi_create_alert(self):
        """Un incidente grave/fatal FUERZA una No Conformidad del SGI (45001 10.2):
        la investigación SCAT y las acciones correctivas viven en la NC, ligada al
        incidente en ambos sentidos. Idempotente."""
        self.ensure_one()
        if self.severity not in ('grave', 'fatal') or self.sgi_alert_id:
            return
        team = self.env.ref('quimibond_sgi.sgi_quality_team_internal',
                            raise_if_not_found=False)
        Cron = self.env['sgi.cron']
        manager_id = Cron._sgi_manager_user_id()
        label = dict(self._fields['severity'].selection).get(self.severity)
        vals = {
            'title': "Incidente %s: %s" % (label, self.name),
            'sgi_origin_type': 'proceso',
            'sgi_classification': 'mayor',
            'sgi_process_id': self.process_id.id,
            'sgi_deviation': "Incidente SST %s (%s). Realice la investigación SCAT y "
                             "las acciones correctivas." % (self.folio or self.name, label),
            'sgi_incident_id': self.id,
        }
        if team:
            vals['team_id'] = team.id
        alert = self.env['quality.alert'].create(vals)
        self.sgi_alert_id = alert.id
        if manager_id:
            Cron._sgi_schedule(
                alert,
                "Investigar incidente %s: %s" % (label, self.folio or self.name),
                "Un incidente SST grave/fatal generó esta NC. Complete la "
                "investigación SCAT y las acciones correctivas.",
                manager_id)
        return alert

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
            # Un incidente grave/fatal debe cerrar la cadena SST: el peligro que lo
            # originó tiene que estar en la matriz IPER (45001 6.1.2 / 10.2).
            if incident.severity in ('grave', 'fatal') and not incident.risk_id:
                problems.append(
                    "• Incidente grave/fatal: falta ligar el riesgo IPER del que "
                    "surge (actualice la matriz IPER y enlácelo).")
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

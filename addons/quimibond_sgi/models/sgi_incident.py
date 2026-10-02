# -*- coding: utf-8 -*-
from markupsafe import Markup

from odoo import models, fields, api
from odoo.exceptions import UserError


class QualityAlert(models.Model):
    _inherit = 'quality.alert'

    sgi_incident_id = fields.Many2one(
        'sgi.incident', string="Incidente SST de origen", readonly=True, copy=False,
        help="Incidente o accidente de seguridad del que nació esta NC.")


class SgiIncident(models.Model):
    """Incidente o accidente de SST (P-S02) con análisis SCAT (causas inmediatas, básicas y falta de
    control). Todos reportan; SST, MAST y Salud ocupacional investigan y cierran."""
    _name = 'sgi.incident'
    _description = "Incidente / Accidente SST (P-S02, SCAT)"
    # 57.67.0: ``hr.mixin`` para que quien reporta ponga a las personas
    # afectadas (Many2many a hr.employee) sin ser de RH (Odoo 19).
    # 57.94.0 (U-01): firma con PIN desde SGI en planta (tableta y hora).
    _inherit = ['sgi.base.mixin', 'hr.mixin', 'sgi.pin.signature.mixin']
    _order = 'folio desc'
    _sgi_sequence_code = 'sgi.incident'
    _sgi_locked_states = ('cerrado',)

    _folio_uniq = models.Constraint(
        'unique(folio)',
        "Ya existe un incidente con ese folio.",
    )

    name = fields.Char(string="Título", required=True, tracking=True)
    date = fields.Datetime(string="Fecha y hora", required=True,
                           default=fields.Datetime.now, tracking=True,
                           help="Fecha y hora en que ocurrió.")
    incident_type = fields.Selection([
        ('lesion', "Lesión / accidente"),
        ('casi_accidente', "Casi accidente"),
        ('dano_propiedad', "Daño a la propiedad"),
        ('ambiental', "Incidente ambiental"),
        ('enfermedad_laboral', "Enfermedad laboral"),
    ], string="Tipo", default='casi_accidente', required=True, tracking=True,
        help="Lesión, casi accidente, daño a la propiedad, incidente ambiental o enfermedad laboral.")
    severity = fields.Selection([
        ('leve', "Leve"),
        ('moderado', "Moderado"),
        ('grave', "Grave"),
        ('fatal', "Fatal"),
    ], string="Severidad", default='leve', required=True, tracking=True,
        help="Leve, moderado, grave o fatal. Los graves y fatales avisan de inmediato.")
    employee_ids = fields.Many2many('hr.employee', string="Personas afectadas", help="Personas afectadas.")
    reporter_id = fields.Many2one('res.users', string="Reportado por",
                                  default=lambda self: self.env.user, tracking=True,
                                  help="Persona que reporta. Puede consultar cómo se cerró.")
    # 57.94.0 (U-01): la persona que reporta, aunque no tenga usuario (SGI en
    # planta). En el backend cada quien solo se pone a sí mismo.
    reporter_employee_id = fields.Many2one(
        'hr.employee', string="Reportado por (empleado)", index=True,
        ondelete='restrict', default=lambda self: self.env.user.employee_id,
        help="Empleado que reporta. Desde la tableta de planta queda el de quien tecleó su PIN.")
    location = fields.Char(string="Lugar")
    process_id = fields.Many2one('sgi.process', string="Proceso", ondelete='restrict',
                                 help="Proceso donde ocurrió.")
    sgi_area_id = fields.Many2one('sgi.area', string="Área SGI", ondelete='restrict',
                                  help="Área del SGI donde ocurrió.")
    description = fields.Text(string="Descripción del evento")
    days_lost = fields.Integer(string="Días perdidos", help="Días de trabajo perdidos por el evento.")

    # --- Análisis SCAT (3 capas de causas) ---
    immediate_causes = fields.Text(string="Causas inmediatas (actos/condiciones)")
    basic_causes = fields.Text(string="Causas básicas (factores personales/de trabajo)")
    lack_of_control = fields.Text(string="Falta de control (sistema de gestión)")

    risk_id = fields.Many2one('sgi.risk', string="Riesgo / IPER relacionado",
                              domain="[('instrument', '=', 'iper')]",
                              help="Riesgo de la matriz IPER relacionado con el evento.")
    action_line_ids = fields.One2many('sgi.action.line', 'incident_id', string="Acciones")
    sgi_alert_id = fields.Many2one('quality.alert', string="No conformidad generada",
                                   readonly=True, copy=False,
                                   help="No conformidad generada desde el incidente.")
    # 57.96.0 (N-06, 45001 5.4 y 10.2): quién investigó y si funcionó.
    investigation_team_ids = fields.Many2many(
        'hr.employee', 'sgi_incident_investigation_rel', 'incident_id', 'employee_id',
        string="Equipo de investigación",
        help="Quiénes investigaron el evento. Para cerrar: al menos un trabajador sin personal "
             "a su cargo o un integrante de la Comisión de Seguridad e Higiene (ISO 45001 5.4).")
    sgi_effective = fields.Selection([
        ('eficaz', "Eficaz"),
        ('no_eficaz', "No eficaz"),
    ], string="Resultado de la eficacia", tracking=True, copy=False,
        help="¿Las acciones evitaron que se repita? El incidente solo cierra con «Eficaz». "
             "«No eficaz» lo regresa a Acciones y pide una acción nueva.")
    sgi_effectiveness_date = fields.Date(string="Fecha de la verificación", tracking=True, copy=False)
    sgi_effectiveness_note = fields.Text(string="Qué se verificó", copy=False)
    sgi_effectiveness_by = fields.Many2one('res.users', string="Verificó", readonly=True, copy=False)
    sgi_ineffective_count = fields.Integer(
        string="Verificaciones no eficaces", readonly=True, copy=False,
        help="Veces que la verificación salió «No eficaz».")
    # 57.96.0: nació de una incapacidad por riesgo de trabajo (sgi_incident_leave.py).
    sgi_from_leave = fields.Boolean(string="Desde una incapacidad", readonly=True, copy=False)
    sgi_leave_count = fields.Integer(string="Incapacidades", compute='_compute_sgi_leave',
                                     help="Incapacidades por riesgo de trabajo aprobadas ligadas.")
    sgi_leave_days = fields.Float(string="Días de incapacidad", compute='_compute_sgi_leave')

    state = fields.Selection([
        ('reportado', "Reportado"),
        ('investigacion', "En investigación"),
        ('acciones', "Acciones"),
        ('cerrado', "Cerrado"),
    ], string="Estado", default='reportado', required=True, tracking=True,
        help="Reportado, en investigación, acciones o cerrado. No se cierra sin el análisis SCAT ni con "
             "acciones abiertas.")

    def _sgi_check_reporter_employee(self, vals_list):
        """57.94.0 (U-01): nadie reporta «a nombre» de otro, salvo MAST, Salud
        ocupacional o el sistema (la tableta escribe con sudo después de
        validar el PIN). ``vals_list``: lo que se escribe; en el alta se
        revisan los registros ya creados (los ``default_*`` del contexto no
        pasan por los valores)."""
        if self.env.su or self._sgi_can_investigate():
            return
        # sudo: hr.employee solo lo lee RH en Odoo 19; sin sudo, un Usuario
        # SGI recibiría AccessError (subclase de UserError) en vez del aviso.
        mine = self.env.user.sudo().employee_ids.ids
        for vals in vals_list:
            employee_id = vals.get('reporter_employee_id')
            # Mismo candado para «Reportado por» (usuario): antes cualquiera
            # podía poner a otro usuario como reportante por RPC.
            reporter_id = vals.get('reporter_id')
            if (employee_id and employee_id not in mine) \
                    or (reporter_id and reporter_id != self.env.uid):
                raise UserError(
                    "Solo puede reportar a su nombre. Si otra persona vio el evento, que lo reporte "
                    "ella (en SGI en planta, con su PIN) o anótela en la descripción.")

    _sgi_pin_employee_field = 'reporter_employee_id'

    def _sgi_pin_employee(self):
        return self.reporter_employee_id

    sgi_user_can_investigate = fields.Boolean(
        compute='_compute_sgi_user_can_investigate',
        help="Usted es Jefe MAST o de Salud ocupacional: puede cambiar quién reportó.")

    @api.depends_context('uid')
    def _compute_sgi_user_can_investigate(self):
        can = self._sgi_can_investigate()
        for incident in self:
            incident.sgi_user_can_investigate = can

    def _compute_sgi_leave(self):
        # sudo: hr.leave solo lo lee RH; aquí solo se muestran conteos. Sin
        # la liga (sgi_incident_leave.py) o sin Ausencias, cero.
        Leave = self.env['hr.leave'].sudo() if 'hr.leave' in self.env else None
        linked = Leave is not None and 'sgi_incident_id' in Leave._fields
        for incident in self:
            leaves = Leave.search([('sgi_incident_id', '=', incident.id), ('state', '=', 'validate')]) \
                if linked and incident.id else None
            incident.sgi_leave_count = len(leaves) if leaves else 0
            incident.sgi_leave_days = sum(leaves.mapped('number_of_days')) if leaves else 0.0

    # 57.96.0 (N-06): campos que solo escribe el sistema (con sudo).
    _SGI_SYSTEM_FIELDS = ('sgi_ineffective_count', 'sgi_effectiveness_by', 'sgi_from_leave')

    @api.model_create_multi
    def create(self, vals_list):
        if not self.env.su:
            vals_list = [{k: v for k, v in vals.items() if k not in self._SGI_SYSTEM_FIELDS}
                         for vals in vals_list]
        incidents = super().create(vals_list)
        incidents._sgi_check_reporter_employee([
            {'reporter_employee_id': inc.sudo().reporter_employee_id.id,
             'reporter_id': inc.sudo().reporter_id.id} for inc in incidents])
        for incident in incidents:
            incident._sgi_notify_if_serious()
            incident._sgi_create_alert()
        return incidents

    def write(self, vals):
        """Reclasificar la severidad a grave/fatal FUERZA la NC y el aviso, igual
        que en el alta: un incidente que entró leve/moderado y la investigación
        eleva a grave/fatal no puede quedarse sin su NC. Se apoya en la
        idempotencia de ambos métodos y sólo dispara para los registros cuya
        severidad ANTES del write no era grave/fatal (sin duplicar avisos).

        57.96.0 (N-06): la eficacia solo la registran el Jefe MAST y Salud
        ocupacional; «No eficaz» regresa a Acciones; el candado de cierre se
        revisa después de escribir (cuenta lo que el formulario manda junto con
        el estado), como la NC desde 57.93.0."""
        self._sgi_check_reporter_employee([vals])
        if not self.env.su and any(f in vals for f in self._SGI_SYSTEM_FIELDS):
            vals = {k: v for k, v in vals.items() if k not in self._SGI_SYSTEM_FIELDS}
        # D-06 / D-009 (entrega 4): investigar, cerrar y reabrir es de Jefe
        # MAST y Salud ocupacional. El reportante solo edita mientras está
        # «Reportado» (regla de registro) y no cambia el estado.
        if 'state' in vals and not self.env.su and not self._sgi_can_investigate():
            if self.filtered(lambda i: i.state != vals['state']):
                raise UserError(
                    "Solo el Jefe MAST y Salud ocupacional investigan, cierran o reabren "
                    "un incidente. Usted puede reportarlo y consultar cómo se cerró.")
        if 'sgi_effective' in vals and not self.env.su and not self._sgi_can_investigate():
            raise UserError("Solo el Jefe MAST y Salud ocupacional registran la verificación de "
                            "eficacia de un incidente.")
        if vals.get('sgi_effective'):
            vals = dict(vals, sgi_effectiveness_by=self.env.uid)
        escalating = self.browse()
        if vals.get('severity') in ('grave', 'fatal'):
            escalating = self.filtered(
                lambda i: i.severity not in ('grave', 'fatal'))
        # Candado de cierre por CUALQUIER vía (botón, RPC, import, server
        # action) — mismo patrón que sgi.risk: validar solo en el botón dejaba
        # cerrar sin SCAT con un write directo de state. 57.96.0: se revisa
        # después de escribir; un UserError deshace el write completo.
        closing = self.filtered(lambda i: i.state != 'cerrado') \
            if vals.get('state') == 'cerrado' and not self.env.su else self.browse()
        newly_ineffective = self.filtered(lambda i: i.sgi_effective != 'no_eficaz') \
            if vals.get('sgi_effective') == 'no_eficaz' else self.browse()
        res = super().write(vals)
        closing._sgi_check_can_close()
        newly_ineffective._sgi_on_ineffective()
        for incident in escalating:
            incident._sgi_notify_if_serious()
            incident._sgi_create_alert()
        return res

    def _sgi_locked_records(self):
        """Un incidente cerrado lo reabre el Jefe MAST o Salud ocupacional
        (D-06). El candado del mixin solo exceptúa al Jefe MAST."""
        if self.env.user.has_group('quimibond_sgi.group_sgi_health'):
            return self.browse()
        return super()._sgi_locked_records()

    def _sgi_readonly_records(self):
        """V-A03: fuera de Jefe MAST y Salud ocupacional, el incidente solo es
        editable para quien lo reportó y mientras siga «Reportado» (regla
        rule_sgi_incident_user_edit_reported)."""
        if self.env.su or self._sgi_can_investigate():
            return self.browse()
        uid = self.env.uid
        # 57.94.0 (U-01): «lo creé yo» no cuenta para lo reportado en SGI en
        # planta (lo crea la cuenta compartida de la tableta).
        return self.filtered(
            lambda i: i.state != 'reportado'
            or (i._origin.id and uid != i._origin.reporter_id.id
                and (uid != i._origin.create_uid.id or i._origin.sgi_pin_tablet_id)))

    @api.model
    def _sgi_can_investigate(self):
        user = self.env.user
        return user.has_group('quimibond_sgi.group_sgi_manager') \
            or user.has_group('quimibond_sgi.group_sgi_health')

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
            'sgi_origin_type': 'incidente_sst',
            'sgi_classification': 'mayor',
            'sgi_process_id': self.process_id.id,
            'sgi_deviation': "Incidente SST %s (%s). Realice la investigación SCAT y "
                             "las acciones correctivas." % (self.folio or self.name, label),
            'sgi_incident_id': self.id,
        }
        if team:
            vals['team_id'] = team.id
        alert = self.env['quality.alert'].sgi_auto_create('incidente_sst_grave', vals)
        if not alert:
            return alert
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
        # Además de la actividad, correo inmediato: un incidente grave no
        # puede esperar a que Dirección abra Odoo.
        Cron._sgi_send_critical_mail(
            'quimibond_sgi.mail_template_sgi_incident_grave', self)

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
            # 57.96.0 (N-06): equipo de investigación (45001 5.4).
            problems += incident._sgi_team_problems()
            # 57.96.0 (N-06): IPER reevaluado después del incidente.
            risk = incident.sudo().risk_id
            if incident.severity in ('moderado', 'grave', 'fatal') and risk and incident.date:
                happened = fields.Date.context_today(incident, incident.date)
                if not risk.last_eval_date or risk.last_eval_date < happened:
                    problems.append(
                        "• Reevalúe el IPER %s después del incidente («Registrar evaluación» en el "
                        "riesgo; última evaluación: %s)." % (risk.folio or risk.name,
                                                             risk.last_eval_date or "ninguna"))
            # 57.96.0 (N-06): eficacia como en la NC.
            problems += incident._sgi_effectiveness_problems()
            if problems:
                raise UserError(
                    "No se puede cerrar el incidente %s:\n%s" % (
                        incident.folio or incident.name, "\n".join(problems)))

    def _sgi_team_problems(self):
        """57.96.0 (N-06): al menos un trabajador (sin personal a su cargo) o
        un integrante de la Comisión de Seguridad e Higiene. sudo: hr.employee
        solo lo lee RH."""
        self.ensure_one()
        team = self.sudo().investigation_team_ids
        if not team:
            return ["• Registre el equipo de investigación (pestaña Investigación y eficacia)."]
        csh = self.env.ref('quimibond_sgi.group_sgi_csh', raise_if_not_found=False)
        csh_users = csh.sudo().all_user_ids if csh else self.env['res.users']
        if any(not member.child_ids or (member.user_id and member.user_id in csh_users)
               for member in team):
            return []
        return ["• El equipo de investigación necesita al menos un trabajador sin personal a su "
                "cargo o un integrante de la Comisión de Seguridad e Higiene (ISO 45001 5.4)."]

    def _sgi_effectiveness_problems(self):
        """57.96.0 (N-06): verificación de eficacia como en la NC (57.93.0)."""
        self.ensure_one()
        problems = []
        if self.sgi_effective != 'eficaz':
            problems.append("• Falta verificar la eficacia de las acciones: «Eficaz», con fecha y "
                            "qué se verificó (pestaña Investigación y eficacia).")
        elif not (self.sgi_effectiveness_date and (self.sgi_effectiveness_note or '').strip()):
            problems.append("• Falta la fecha o la nota de la verificación de eficacia.")
        eff_date = self.sgi_effectiveness_date
        if eff_date:
            if eff_date > fields.Date.context_today(self):
                problems.append("• La fecha de la verificación (%s) no puede ser futura." % eff_date)
            done = [d for d in self.action_line_ids.mapped('date_done') if d]
            if done and eff_date < max(done):
                problems.append("• La eficacia (%s) se registró antes de que terminara la última "
                                "acción (%s)." % (eff_date, max(done)))
        if self.sgi_ineffective_count and not self.action_line_ids.filtered(
                lambda l: l.date_done and l.effectiveness_round >= self.sgi_ineffective_count):
            problems.append("• La verificación anterior salió «No eficaz»: registre y termine una "
                            "acción nueva.")
        return problems

    def _sgi_on_ineffective(self):
        """57.96.0 (N-06): «No eficaz» queda en el historial, suma el
        contador, limpia la verificación, regresa el incidente a Acciones y
        pide la acción nueva a quien la registró (o al Jefe MAST)."""
        Cron = self.env['sgi.cron']
        user_id = self.env.uid if self.env.user.active and not self.env.user._is_superuser() \
            else Cron._sgi_manager_user_id()
        for incident in self:
            folio = incident.folio or incident.name
            incident.message_post(body=Markup(
                "<b>Verificación de eficacia: no eficaz</b> (%s).<br/>%s") % (
                    incident.sgi_effectiveness_date or fields.Date.context_today(incident),
                    incident.sgi_effectiveness_note or ''))
            incident.sudo().write({
                'sgi_ineffective_count': incident.sgi_ineffective_count + 1,
                'sgi_effective': False, 'sgi_effectiveness_date': False,
                'sgi_effectiveness_note': False, 'state': 'acciones'})
            if user_id:
                Cron._sgi_schedule(
                    incident, "Registrar acción nueva del incidente %s (no eficaz)" % folio,
                    "La verificación de eficacia salió «No eficaz». Revise las causas y registre "
                    "una acción nueva; el incidente no cierra sin ella.", user_id)

    def action_set_investigacion(self):
        self.write({'state': 'investigacion'})
        return True

    def action_set_acciones(self):
        self.write({'state': 'acciones'})
        return True

    def action_set_cerrado(self):
        # El candado de cierre vive en write() (aplica por cualquier vía).
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

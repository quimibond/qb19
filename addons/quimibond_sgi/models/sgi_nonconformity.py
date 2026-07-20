# -*- coding: utf-8 -*-
from odoo import models, fields, api
from odoo.exceptions import UserError, ValidationError


class QualityAlertStage(models.Model):
    _inherit = 'quality.alert.stage'

    sgi_is_closing_stage = fields.Boolean(string="Etapa de cierre SGI")
    sgi_is_cancel_stage = fields.Boolean(string="Etapa de cancelación SGI")


class QualityAlertTeam(models.Model):
    _inherit = 'quality.alert.team'

    sgi_sequence_id = fields.Many2one('ir.sequence', string="Secuencia de folio SGI",
                                      help="Secuencia anual para el folio de las NC de este equipo.")


class QualityAlert(models.Model):
    _inherit = 'quality.alert'

    sgi_folio = fields.Char(string="Folio SGI", readonly=True, copy=False, index=True, tracking=True)
    sgi_stage_is_closing = fields.Boolean(related='stage_id.sgi_is_closing_stage')
    sgi_stage_is_cancel = fields.Boolean(related='stage_id.sgi_is_cancel_stage')
    sgi_origin_type = fields.Selection([
        ('proceso', "Proceso"),
        ('auditoria_interna', "Auditoría interna"),
        ('auditoria_externa', "Auditoría externa"),
        ('reclamacion', "Reclamación de cliente"),
        ('indicador', "Indicador incumplido"),
    ], string="Origen", default='proceso', tracking=True)
    sgi_classification = fields.Selection([
        ('mayor', "Mayor"),
        ('menor', "Menor"),
        ('observacion', "Observación"),
    ], string="Clasificación", tracking=True)
    sgi_norm_clause_id = fields.Many2one('sgi.norm.clause', string="Requisito (cláusula)")
    sgi_requester_id = fields.Many2one('res.users', string="Solicitante")
    sgi_requester_job = fields.Char(related='sgi_requester_id.employee_id.job_title',
                                    string="Cargo del solicitante", readonly=True)
    sgi_lead_auditor_id = fields.Many2one('res.users', string="Auditor líder")
    sgi_process_id = fields.Many2one('sgi.process', string="Proceso detectado")
    sgi_responsible_ids = fields.Many2many('res.users', 'sgi_alert_responsible_rel',
                                           'alert_id', 'user_id', string="Responsables a contestar")
    sgi_deviation = fields.Text(string="Desviación detectada")
    sgi_why_1 = fields.Char(string="¿Por qué? 1")
    sgi_why_2 = fields.Char(string="¿Por qué? 2")
    sgi_why_3 = fields.Char(string="¿Por qué? 3")
    sgi_why_4 = fields.Char(string="¿Por qué? 4")
    sgi_why_5 = fields.Char(string="¿Por qué? 5")
    sgi_root_cause = fields.Text(string="Causa raíz")
    sgi_ishikawa_notes = fields.Text(string="Notas Ishikawa (5-6M)")
    sgi_effectiveness_note = fields.Text(string="Verificación de eficacia")
    sgi_effectiveness_date = fields.Date(string="Fecha de eficacia")
    sgi_effectiveness_by = fields.Many2one('res.users', string="Eficacia verificada por")
    sgi_followup_comments = fields.Text(string="Comentarios de seguimiento")
    sgi_required_capa = fields.Boolean(string="¿Requirió acción correctiva?")
    sgi_followup_action = fields.Selection([
        ('exhorto', "Exhorto"),
        ('administrativa', "Acción administrativa"),
        ('na', "N/A"),
    ], string="Acción a seguir")
    sgi_verified_by = fields.Many2one('res.users', string="Verificó")
    sgi_verified_date = fields.Date(string="Fecha de verificación")
    sgi_approved_by = fields.Many2one('res.users', string="Aprobó")
    sgi_approved_date = fields.Date(string="Fecha de aprobación")
    sgi_complaint_ticket_id = fields.Many2one('helpdesk.ticket', string="Reclamación ligada", readonly=True)
    sgi_external_ref = fields.Char(string="N° NCR externo")

    sgi_action_line_ids = fields.One2many('sgi.action.line', 'alert_id', string="Correcciones y acciones")

    @api.model_create_multi
    def create(self, vals_list):
        alerts = super().create(vals_list)
        for alert in alerts:
            if not alert.sgi_folio and alert.team_id.sgi_sequence_id:
                alert.sgi_folio = alert.team_id.sgi_sequence_id.next_by_id()
        return alerts

    def _sgi_check_can_close(self):
        """Valida los candados de cierre de una NC."""
        for alert in self:
            problems = []
            if not alert.sgi_root_cause:
                problems.append("• Falta la causa raíz.")
            if not alert.sgi_action_line_ids:
                problems.append("• La NC no tiene NINGUNA corrección/acción registrada "
                                "(ISO 10.2: sin acción no hay tratamiento).")
            pending = alert.sgi_action_line_ids.filtered(lambda l: not l.date_done)
            if pending:
                problems.append("• Hay %d acción(es) sin fecha de terminación." % len(pending))
            if not alert.sgi_effectiveness_note or not alert.sgi_effectiveness_date:
                problems.append("• Falta la verificación de eficacia (nota y fecha).")
            if problems:
                raise UserError(
                    "No se puede cerrar la NC %s:\n%s" % (
                        alert.sgi_folio or alert.name, "\n".join(problems)))

    def write(self, vals):
        newly_closed = self.env['quality.alert']
        if 'stage_id' in vals:
            new_stage = self.env['quality.alert.stage'].browse(vals['stage_id'])
            if new_stage.sgi_is_closing_stage and not self.env.context.get('sgi_force_close'):
                for alert in self:
                    if alert.stage_id != new_stage:
                        alert._sgi_check_can_close()
            if new_stage.sgi_is_closing_stage:
                newly_closed = self.filtered(
                    lambda a: a.stage_id != new_stage and a.sgi_classification == 'mayor')
        res = super().write(vals)
        for alert in newly_closed:
            alert._sgi_notify_mayor_closed()
        return res

    def _sgi_notify_mayor_closed(self):
        """PROT-05/D7: al cerrar una NC mayor, recordar actualizar AMEF y plan de
        control (lecciones aprendidas)."""
        self.ensure_one()
        Cron = self.env['sgi.cron']
        manager_id = Cron._sgi_manager_user_id()
        if not manager_id:
            return
        Cron._sgi_schedule(
            self,
            "NC mayor cerrada: actualizar AMEF y plan de control (%s)" % (
                self.sgi_folio or self.name),
            "Se cerró una No Conformidad mayor. Revise si el AMEF y el plan de "
            "control del proceso/producto deben actualizarse con la lección aprendida.",
            manager_id)
        return True

    def action_sgi_force_close(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Cierre forzado (Jefe MAST)",
            'res_model': 'sgi.nc.force.close',
            'view_mode': 'form',
            'target': 'new',
            'context': {'default_alert_id': self.id},
        }

    def action_sgi_escalate_to_nc(self):
        """Escala una alerta operativa de piso a una No Conformidad sistémica del
        SGI: la mueve al equipo NC Internas, le asigna folio y origen 'proceso',
        conservando producto/orden/picking. Las alertas rutinarias de los equipos
        de piso siguen su flujo normal; solo lo sistémico se escala (así el
        concentrado F-P-G05-02 no se contamina)."""
        team = self.env.ref('quimibond_sgi.sgi_quality_team_internal',
                            raise_if_not_found=False)
        if not team or not team.sgi_sequence_id:
            raise UserError(
                "No está configurado el equipo de No Conformidades Internas del SGI.")
        # Etapa "Abierta" del equipo NC Internas: las etapas de quality.alert son por
        # equipo, así que al cambiar de equipo hay que moverla a una etapa propia.
        open_stage = self.env.ref('quimibond_sgi.sgi_nc_int_stage_open',
                                  raise_if_not_found=False)
        for alert in self:
            if alert.sgi_folio:
                raise UserError(
                    "La alerta «%s» ya es una NC del SGI (%s)." % (
                        alert.name or alert.title, alert.sgi_folio))
            vals = {
                'team_id': team.id,
                'sgi_origin_type': 'proceso',
                'sgi_folio': team.sgi_sequence_id.next_by_id(),
            }
            if open_stage:
                vals['stage_id'] = open_stage.id
            alert.write(vals)
            alert.message_post(
                body="Alerta escalada a No Conformidad del SGI: <b>%s</b>." % alert.sgi_folio)
        return True


class SgiActionLine(models.Model):
    _name = 'sgi.action.line'
    _description = "Acción / corrección de No Conformidad"
    _order = 'date_commit, id'

    alert_id = fields.Many2one('quality.alert', string="No Conformidad", ondelete='cascade')
    risk_id = fields.Many2one('sgi.risk', string="Riesgo / Oportunidad", ondelete='cascade')
    fmea_line_id = fields.Many2one('sgi.fmea.line', string="Modo de falla (AMEF)",
                                   ondelete='cascade')
    incident_id = fields.Many2one('sgi.incident', string="Incidente SST", ondelete='cascade')
    action_type = fields.Selection([
        ('correccion', "Corrección"),
        ('correctiva', "Acción correctiva"),
        ('preventiva', "Acción preventiva"),
    ], string="Tipo", default='correccion', required=True)
    name = fields.Char(string="Descripción", required=True)
    responsible_id = fields.Many2one('res.users', string="Responsable", required=True)
    date_commit = fields.Date(string="Compromiso", required=True)
    date_done = fields.Date(string="Terminada el")
    progress = fields.Selection([
        ('0', "0%"),
        ('50', "50%"),
        ('100', "100%"),
    ], string="Avance", default='0')
    state = fields.Selection([
        ('abierta', "Abierta"),
        ('vencida', "Vencida"),
        ('terminada', "Terminada"),
    ], string="Estado", compute='_compute_state', store=True)
    # Actividad nativa que hace accionable la acción en el registro origen.
    activity_id = fields.Many2one('mail.activity', string="Actividad",
                                  readonly=True, copy=False, index=True)

    @api.constrains('alert_id', 'risk_id', 'fmea_line_id', 'incident_id', 'name')
    def _check_parent_xor(self):
        for line in self:
            parents = [line.alert_id, line.risk_id, line.fmea_line_id, line.incident_id]
            if sum(1 for p in parents if p) != 1:
                raise ValidationError(
                    "Una acción debe pertenecer exactamente a un origen: una No "
                    "Conformidad, un Riesgo, un modo de falla de AMEF o un incidente "
                    "SST (exactamente uno, no varios ni ninguno).")

    @api.depends('date_commit', 'date_done')
    def _compute_state(self):
        today = fields.Date.context_today(self)
        for line in self:
            if line.date_done:
                line.state = 'terminada'
            elif line.date_commit and line.date_commit < today:
                line.state = 'vencida'
            else:
                line.state = 'abierta'

    # ------------------------------------------------------------------
    # Acciones como actividades nativas (corazón accionable del SGI)
    # ------------------------------------------------------------------
    def _sgi_origin(self):
        """Registro origen (con chatter) al que se cuelga la actividad."""
        self.ensure_one()
        if self.alert_id:
            return self.alert_id
        if self.risk_id:
            return self.risk_id
        if self.incident_id:
            return self.incident_id
        if self.fmea_line_id:
            return self.fmea_line_id.fmea_id
        return self.env['sgi.action.line'].browse()

    def _sgi_activity_note(self):
        self.ensure_one()
        label = dict(self._fields['action_type'].selection).get(
            self.action_type, self.action_type)
        return "%s del SGI. Responsable: %s. Compromiso: %s." % (
            label, self.responsible_id.display_name or '-',
            self.date_commit or '-')

    def _sgi_sync_activity(self):
        """Crea/actualiza la actividad ligada a la acción (idempotente)."""
        Todo = 'mail.mail_activity_data_todo'
        for line in self:
            if line.date_done or not line.responsible_id or not line.date_commit:
                continue
            origin = line._sgi_origin()
            if not origin:
                continue
            if line.activity_id:
                line.activity_id.write({
                    'user_id': line.responsible_id.id,
                    'date_deadline': line.date_commit,
                    'summary': line.name,
                })
            else:
                act = origin.activity_schedule(
                    Todo,
                    summary=line.name,
                    note=line._sgi_activity_note(),
                    user_id=line.responsible_id.id,
                    date_deadline=line.date_commit)
                line.activity_id = act.id

    def _sgi_close_activity(self):
        """Marca hecha la actividad cuando la acción se termina.

        En Odoo 19 action_feedback archiva la actividad (conserva historia en
        el chatter). Soltamos el enlace para que, si se reabre la acción, se
        genere una actividad nueva en lugar de reactivar una archivada.
        """
        for line in self:
            if line.activity_id:
                line.activity_id.action_feedback(
                    feedback="Acción terminada el %s." % (line.date_done or ''))
                line.activity_id = False

    @api.model_create_multi
    def create(self, vals_list):
        lines = super().create(vals_list)
        lines._sgi_sync_activity()
        return lines

    def write(self, vals):
        res = super().write(vals)
        if 'date_done' in vals:
            done = self.filtered('date_done')
            done._sgi_close_activity()
        if {'responsible_id', 'date_commit', 'name'} & set(vals):
            self.filtered(lambda l: not l.date_done)._sgi_sync_activity()
        return res


class SgiNcForceClose(models.TransientModel):
    _name = 'sgi.nc.force.close'
    _description = "Cierre forzado de No Conformidad"

    alert_id = fields.Many2one('quality.alert', string="No Conformidad", required=True)
    reason = fields.Text(string="Motivo del cierre forzado", required=True)

    def action_confirm(self):
        self.ensure_one()
        if not self.env.user.has_group('quimibond_sgi.group_sgi_manager'):
            raise UserError("Solo el Jefe de MAST y SGI puede realizar un cierre forzado.")
        alert = self.alert_id
        closing_stage = self.env['quality.alert.stage'].search([
            ('sgi_is_closing_stage', '=', True),
            '|', ('team_ids', '=', False), ('team_ids', 'in', alert.team_id.id),
        ], limit=1)
        if not closing_stage:
            raise UserError("No hay una etapa de cierre configurada para este equipo.")
        alert.message_post(
            body="<b>Cierre forzado</b> por %s.<br/>Motivo: %s" % (
                self.env.user.name, self.reason))
        alert.with_context(sgi_force_close=True).write({'stage_id': closing_stage.id})
        return {'type': 'ir.actions.act_window_close'}

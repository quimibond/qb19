# -*- coding: utf-8 -*-
from odoo import models, fields, api
from odoo.exceptions import UserError


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
        if 'stage_id' in vals:
            new_stage = self.env['quality.alert.stage'].browse(vals['stage_id'])
            if new_stage.sgi_is_closing_stage and not self.env.context.get('sgi_force_close'):
                for alert in self:
                    if alert.stage_id != new_stage:
                        alert._sgi_check_can_close()
        return super().write(vals)

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


class SgiActionLine(models.Model):
    _name = 'sgi.action.line'
    _description = "Acción / corrección de No Conformidad"
    _order = 'date_commit, id'

    alert_id = fields.Many2one('quality.alert', string="No Conformidad", required=True, ondelete='cascade')
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

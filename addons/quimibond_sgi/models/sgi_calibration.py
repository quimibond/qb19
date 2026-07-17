# -*- coding: utf-8 -*-
from dateutil.relativedelta import relativedelta

from odoo import models, fields, api
from odoo.exceptions import UserError


class MaintenanceEquipment(models.Model):
    _inherit = 'maintenance.equipment'

    # --- Equipo de medición (P-C03) ---
    sgi_is_measuring = fields.Boolean(string="Equipo de medición")
    sgi_magnitude = fields.Char(string="Magnitud")
    sgi_range = fields.Char(string="Rango")
    sgi_resolution = fields.Char(string="Resolución")
    sgi_calibration_interval_months = fields.Integer(string="Intervalo de calibración (meses)",
                                                     default=12)
    sgi_last_calibration_date = fields.Date(string="Última calibración")
    sgi_next_calibration_date = fields.Date(string="Próxima calibración",
                                            compute='_compute_next_calibration_date',
                                            store=True, readonly=False,
                                            help="Se calcula como última + intervalo, pero el "
                                                 "laboratorio puede fijar otra fecha (prevalece).")
    sgi_calibration_state = fields.Selection([
        ('vigente', "Vigente"),
        ('por_vencer', "Por vencer"),
        ('vencido', "Vencido"),
    ], string="Estado de calibración", compute='_compute_calibration_state', store=True)
    sgi_do_not_use = fields.Boolean(string="No usar", tracking=True,
                                    help="Equipo bloqueado (fuera de tolerancia o calibración vencida).")
    sgi_calibration_ids = fields.One2many('sgi.calibration', 'equipment_id',
                                          string="Calibraciones")
    sgi_calibration_count = fields.Integer(string="N° de calibraciones",
                                           compute='_compute_calibration_count')

    # --- EPP (P-S03) ---
    sgi_is_ppe = fields.Boolean(string="Equipo de protección personal (EPP)")
    sgi_ppe_expiry_date = fields.Date(string="Vencimiento del EPP")

    @api.depends('sgi_last_calibration_date', 'sgi_calibration_interval_months')
    def _compute_next_calibration_date(self):
        for eq in self:
            if eq.sgi_last_calibration_date and eq.sgi_calibration_interval_months:
                eq.sgi_next_calibration_date = eq.sgi_last_calibration_date + relativedelta(
                    months=eq.sgi_calibration_interval_months)
            else:
                eq.sgi_next_calibration_date = False

    @api.depends('sgi_next_calibration_date')
    def _compute_calibration_state(self):
        today = fields.Date.context_today(self)
        for eq in self:
            if not eq.sgi_next_calibration_date:
                eq.sgi_calibration_state = False
            elif eq.sgi_next_calibration_date < today:
                eq.sgi_calibration_state = 'vencido'
            elif eq.sgi_next_calibration_date <= today + relativedelta(days=30):
                eq.sgi_calibration_state = 'por_vencer'
            else:
                eq.sgi_calibration_state = 'vigente'

    @api.depends('sgi_calibration_ids')
    def _compute_calibration_count(self):
        for eq in self:
            eq.sgi_calibration_count = len(eq.sgi_calibration_ids)

    def action_view_calibrations(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Calibraciones",
            'res_model': 'sgi.calibration',
            'view_mode': 'list,form',
            'domain': [('equipment_id', '=', self.id)],
            'context': {'default_equipment_id': self.id},
        }


class SgiCalibration(models.Model):
    _name = 'sgi.calibration'
    _description = "Calibración de equipo de medición (P-C03)"
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _order = 'date desc, id desc'

    equipment_id = fields.Many2one('maintenance.equipment', string="Equipo", required=True,
                                   domain="[('sgi_is_measuring', '=', True)]", tracking=True)
    date = fields.Date(string="Fecha", required=True, default=fields.Date.context_today,
                       tracking=True)
    calibration_type = fields.Selection([
        ('interna', "Interna"),
        ('externa', "Externa"),
    ], string="Tipo", default='externa', required=True)
    provider_id = fields.Many2one('res.partner', string="Laboratorio / Proveedor")
    certificate_ref = fields.Char(string="N° de certificado")
    result = fields.Selection([
        ('conforme', "Conforme"),
        ('fuera_tolerancia', "Fuera de tolerancia"),
    ], string="Resultado", required=True, default='conforme', tracking=True)
    notes = fields.Text(string="Notas")
    next_date = fields.Date(string="Próxima calibración", compute='_compute_next_date',
                            store=True, readonly=False)
    sgi_alert_id = fields.Many2one('quality.alert', string="NC generada", readonly=True)

    @api.depends('date', 'equipment_id.sgi_calibration_interval_months')
    def _compute_next_date(self):
        for cal in self:
            months = cal.equipment_id.sgi_calibration_interval_months or 12
            cal.next_date = cal.date + relativedelta(months=months) if cal.date else False

    @api.model_create_multi
    def create(self, vals_list):
        calibrations = super().create(vals_list)
        for cal in calibrations:
            cal._sgi_apply_to_equipment()
        return calibrations

    def _sgi_apply_to_equipment(self):
        """Actualiza el equipo y dispara la NC IATF 7.1.5 si aplica."""
        self.ensure_one()
        eq = self.equipment_id
        # Se escriben ambos campos en un solo write: al fijar la fecha explícita en
        # la misma operación que su dependencia (última calibración), el valor
        # explícito prevalece sobre el recálculo (última + intervalo). Así persiste
        # la fecha que fije el laboratorio.
        vals = {'sgi_last_calibration_date': self.date}
        if self.next_date:
            vals['sgi_next_calibration_date'] = self.next_date
        eq.write(vals)
        if self.result == 'conforme':
            if eq.sgi_do_not_use:
                eq.sgi_do_not_use = False
        else:
            eq.sgi_do_not_use = True
            self._sgi_create_alert()

    def _sgi_create_alert(self):
        """Crea la NC interna de evaluación de impacto (IATF 7.1.5)."""
        self.ensure_one()
        if self.sgi_alert_id:
            return
        team = self.env.ref('quimibond_sgi.sgi_quality_team_internal',
                            raise_if_not_found=False)
        Cron = self.env['sgi.cron']
        manager_id = Cron._sgi_manager_user_id()
        vals = {
            'title': "Equipo %s fuera de tolerancia" % self.equipment_id.name,
            'sgi_origin_type': 'proceso',
            'sgi_classification': 'mayor',
            'sgi_deviation': "Equipo %s fuera de tolerancia: evaluar el producto medido "
                             "desde la última calibración (%s)." % (
                                 self.equipment_id.name, self.date),
        }
        if team:
            vals['team_id'] = team.id
        alert = self.env['quality.alert'].create(vals)
        self.sgi_alert_id = alert.id
        if manager_id:
            Cron._sgi_schedule(
                alert,
                "Evaluar impacto: equipo %s fuera de tolerancia" % self.equipment_id.name,
                "Evalúe el producto medido con el equipo desde la última calibración conforme.",
                manager_id)
        return alert

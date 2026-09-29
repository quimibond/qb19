# -*- coding: utf-8 -*-
"""Campos que faltaban para que los indicadores capturados a mano pasen a
fórmula configurable (55.0.0, 2026-09-25); guardados en la base para poder
filtrarlos desde un término (``sgi.indicator.term``). A-022 (auditoría
2026-09): antes vivían todos en ``sgi_kpi_fields.py``; ahora un archivo por
tema (``sgi_kpi_sales``, ``sgi_kpi_quality``, ``sgi_kpi_account``,
``sgi_kpi_review``, ``sgi_kpi_hr``). Mover código entre archivos no toca la
base.

Recursos humanos y usuarios:

- S4-01 ``hr.version.sgi_departure_reason_id`` / ``sgi_departure_registered_at``:
  motivo de la baja (el del empleado si la versión no lo trae) y cuándo se
  registró.
- S4-02 ``hr.employee.sgi_trial_date_end``: fin del periodo de prueba del
  contrato vigente, guardado en el empleado para filtrar evaluaciones.
- S4-03 vive en ``quimibond_nomina`` (``hr.payslip.run``), porque este módulo
  no depende de nómina.
- S4-04: el modelo ``sgi.employer.obligation`` (obligaciones patronales) se
  retiró en 56.15.0 sin haber tenido registros; las obligaciones viven en
  ``qb_obligation``.
- S6-02 ``res.users.sgi_deactivated_date``: fecha en que se desactivó el
  usuario, para compararla con la baja del empleado.
"""
from odoo import api, fields, models


# ---- S4-01 / S4-02 ----------------------------------------------------------
class HrVersionDeparture(models.Model):
    _inherit = 'hr.version'

    sgi_departure_reason_id = fields.Many2one(
        'hr.departure.reason', string="Motivo de baja (SGI)", compute='_compute_sgi_departure',
        store=True, help="El motivo de la versión o, si no lo trae, el del empleado (S4-01).")
    sgi_departure_registered_at = fields.Datetime(
        string="Baja registrada el", readonly=True, copy=False,
        help="Cuándo se capturó la fecha de baja (S4-01).")

    @api.depends('departure_reason_id', 'employee_id.departure_reason_id')
    def _compute_sgi_departure(self):
        for version in self:
            version.sgi_departure_reason_id = (version.departure_reason_id
                                               or version.employee_id.sudo().departure_reason_id)

    def write(self, vals):
        if vals.get('departure_date') and 'sgi_departure_registered_at' not in vals:
            vals = dict(vals, sgi_departure_registered_at=fields.Datetime.now())
        return super().write(vals)


class HrEmployeeTrial(models.Model):
    _inherit = 'hr.employee'

    sgi_trial_date_end = fields.Date(
        string="Fin del periodo de prueba", related='version_id.trial_date_end', store=True,
        help="Del contrato vigente; para medir las evaluaciones del periodo de prueba (S4-02).")


# ---- S6-02 ----------------------------------------------------------------
class ResUsersDeactivated(models.Model):
    _inherit = 'res.users'

    sgi_deactivated_date = fields.Date(
        string="Desactivado el", readonly=True, copy=False,
        help="Fecha en que se desactivó el usuario (S6-02, contra la baja del empleado).")

    def write(self, vals):
        if 'active' in vals and 'sgi_deactivated_date' not in vals:
            vals = dict(vals, sgi_deactivated_date=False if vals['active'] else fields.Date.context_today(self))
        return super().write(vals)

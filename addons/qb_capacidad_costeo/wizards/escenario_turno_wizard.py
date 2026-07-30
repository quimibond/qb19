# -*- coding: utf-8 -*-
"""Escenarios de turno: ¿qué pasa con la capacidad y el costo unitario si
agrego/quito turnos en un centro? (parametrizado por horas/semana)."""
from odoo import api, fields, models


class QbEscenarioTurnoWizard(models.TransientModel):
    _name = 'qb.escenario.turno.wizard'
    _description = 'Escenario de turnos por centro'

    centro_id = fields.Many2one('qb.costeo.centro', required=True)
    hours_per_week_new = fields.Float(
        string='Horas/semana nuevas', required=True,
        help='Horas de operación semanales del centro en el escenario '
             '(ej. 90 → 132 al agregar un turno).')
    resultado = fields.Text(readonly=True)

    @api.onchange('centro_id')
    def _onchange_centro(self):
        if self.centro_id:
            balance = self.env['qb.balance'].search(
                [('centro_id', '=', self.centro_id.id)], limit=1)
            if balance and balance.capacity_month_units and self.centro_id.std_output_per_hour:
                weeks = self.env['qb.costeo.factor.config'].get_param(
                    'weeks_per_month', 4.33)
                self.hours_per_week_new = (
                    balance.capacity_month_units
                    / self.centro_id.std_output_per_hour / weeks)

    def action_simular(self):
        self.ensure_one()
        centro = self.centro_id
        weeks = self.env['qb.costeo.factor.config'].get_param('weeks_per_month', 4.33)
        std = centro.std_output_per_hour or 0.0
        oci = self.env['qb.ociosidad'].search([('centro_id', '=', centro.id)], limit=1)
        balance = self.env['qb.balance'].search([('centro_id', '=', centro.id)], limit=1)

        cap_actual = balance.capacity_month_units if balance else 0.0
        prod = balance.prod_month_units if balance else 0.0
        fixed = oci.fixed_pool_month if oci else 0.0
        cap_nueva = self.hours_per_week_new * weeks * std

        unit_fixed_actual = fixed / cap_actual if cap_actual else 0.0
        unit_fixed_nuevo = fixed / cap_nueva if cap_nueva else 0.0
        idle_actual = fixed * max(1 - prod / cap_actual, 0) if cap_actual else 0.0
        idle_nuevo = fixed * max(1 - prod / cap_nueva, 0) if cap_nueva else 0.0

        self.resultado = (
            'Centro %s (throughput %.1f u/h, %s):\n\n'
            'Capacidad actual: %s u/mes → escenario: %s u/mes (Δ %+.0f%%).\n'
            'Costo fijo/mes: $%s (no cambia con el turno; la nómina extra '
            'del turno NO está incluida — agregarla aparte).\n\n'
            'Fijo por unidad a capacidad: $%.2f → $%.2f.\n'
            'Costo ocioso a producción actual: $%s → $%s.\n\n'
            'Nota: más turnos bajan el fijo unitario solo si el volumen los '
            'llena; si no, la ociosidad sube en la misma proporción.'
        ) % (
            centro.code, std, centro.driver_principal,
            f'{cap_actual:,.0f}', f'{cap_nueva:,.0f}',
            100.0 * (cap_nueva - cap_actual) / cap_actual if cap_actual else 0.0,
            f'{fixed:,.0f}',
            unit_fixed_actual, unit_fixed_nuevo,
            f'{idle_actual:,.0f}', f'{idle_nuevo:,.0f}',
        )
        return {
            'type': 'ir.actions.act_window',
            'res_model': self._name,
            'res_id': self.id,
            'view_mode': 'form',
            'target': 'new',
        }

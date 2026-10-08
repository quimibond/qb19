# -*- coding: utf-8 -*-
"""Capacidad y throughput medidos en el centro de tejido."""
from datetime import date

from dateutil.relativedelta import relativedelta

from odoo import fields, models


class QbCentro(models.Model):
    _inherit = 'qb.centro'

    capacidad_medida_h_mes = fields.Float(
        string='Capacidad medida (h/mes)', digits=(16, 2), readonly=True,
        help='Σ máquina de horas programadas del mes × % en corrida de los '
             'últimos meses (parámetro ritmo_meses_capacidad). Sale del pesaje.')
    capacidad_medida_kg_mes = fields.Float(
        string='Capacidad medida (kg/mes)', digits=(16, 0), readonly=True,
        help='Σ máquina de horas programadas × % en corrida × kg/h en corrida '
             'de lo que teje (ponderado por kilos).')
    throughput_medido = fields.Float(
        string='Throughput medido (kg/h por máquina)', digits=(16, 2), readonly=True,
        help='kg/h en corrida de todas las máquinas del centro, ponderado por kilos.')
    kg_perdidos_mes = fields.Float(
        string='Kilos perdidos por mes', digits=(16, 0), readonly=True,
        help='Horas de paro y cambio × kg/h en corrida, promedio mensual.')
    ritmo_calculado_el = fields.Datetime(readonly=True)

    def _calcular_ritmo_medido(self):
        """Promedio de los últimos N meses completos de ritmos totales por
        máquina. No sobrescribe la capacidad normal: eso lo aprueba
        Administración con el botón «Aplicar»."""
        Ritmo = self.env['qb.tejido.ritmo']
        meses = int(self.env['qb.parametro'].get_float('ritmo_meses_capacidad', 3)) or 3
        for c in self:
            if c.driver != 'workorder' or not c.workcenter_ids:
                continue
            hoy = date.today()
            desde = hoy.replace(day=1) - relativedelta(months=meses)
            filas = Ritmo.search([
                ('company_id', '=', c.company_id.id), ('tipo', '=', 'mes'),
                ('es_total', '=', True), ('workcenter_id', 'in', c.workcenter_ids.ids),
                ('periodo', '>=', desde), ('periodo', '<', hoy.replace(day=1))])
            if not filas:
                continue
            n_meses = len(set(filas.mapped('periodo'))) or 1
            kg = sum(filas.mapped('kg_corrida'))
            h_corr = sum(filas.mapped('horas_corrida'))
            cap_h = cap_kg = 0.0
            for wc in c.workcenter_ids:
                fw = filas.filtered(lambda r: r.workcenter_id == wc)
                if not fw:
                    continue
                prog = sum(fw.mapped('horas_programadas')) / n_meses
                pct = (sum(fw.mapped('horas_corrida')) / sum(fw.mapped('horas_programadas'))
                       if sum(fw.mapped('horas_programadas')) else 0.0)
                kgh = (sum(fw.mapped('kg_corrida')) / sum(fw.mapped('horas_corrida'))
                       if sum(fw.mapped('horas_corrida')) else 0.0)
                cap_h += prog * pct
                cap_kg += prog * pct * kgh
            c.write({
                'capacidad_medida_h_mes': cap_h,
                'capacidad_medida_kg_mes': cap_kg,
                'throughput_medido': kg / h_corr if h_corr else 0.0,
                'kg_perdidos_mes': sum(filas.mapped('kg_perdidos')) / n_meses,
                'ritmo_calculado_el': fields.Datetime.now(),
            })

    def action_aplicar_ritmo(self):
        """Copia la capacidad medida a la capacidad normal del centro. Lo
        aprueba Administración y Finanzas; queda el motivo."""
        for c in self:
            if not c.capacidad_medida_h_mes:
                continue
            c.write({
                'capacidad_h_mes': round(c.capacidad_medida_h_mes, 2),
                'capacidad_motivo': 'Pesaje de rollos: %s h/mes medidas al %s '
                                    '(%s kg/mes, %.2f kg/h por máquina)' % (
                                        '{:,.0f}'.format(c.capacidad_medida_h_mes),
                                        fields.Date.to_string(c.ritmo_calculado_el.date())
                                        if c.ritmo_calculado_el else '',
                                        '{:,.0f}'.format(c.capacidad_medida_kg_mes),
                                        c.throughput_medido),
            })
        return True

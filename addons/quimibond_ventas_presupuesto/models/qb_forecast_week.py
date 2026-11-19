# -*- coding: utf-8 -*-
"""Catálogo de semanas para la matriz del pronóstico (19.0.3.2.0).

En Odoo 19 la vista ``grid`` sobre un campo de fecha solo da columnas por día
(rango semana o mes) o por mes (rango año): no hay columnas por semana (3.1.1).
La matriz usa como columna este catálogo: una fila por lunes, con nombre
«2026-S48 · 23 nov». Las semanas se crean solas (al guardar un renglón del
pronóstico y al abrir la matriz) y la matriz siempre muestra las próximas
``GRID_WEEKS`` aunque estén vacías (``group_expand``).
"""
from datetime import timedelta

from odoo import api, fields, models

GRID_WEEKS = 13
# 3.18.0 (Jose, 19-nov): capacidad de venta semanal que da Producción; cambia por ausencias o fallas,
# así que cada semana la puede tener propia.
PARAM_CAPACITY = 'quimibond_ventas_presupuesto.forecast_capacity_m'
DEFAULT_CAPACITY = 250000.0
MONTHS = ('ene', 'feb', 'mar', 'abr', 'may', 'jun', 'jul', 'ago', 'sep', 'oct', 'nov', 'dic')


def week_label(monday):
    year, number = monday.isocalendar()[0], monday.isocalendar()[1]
    return "%d-S%02d · %d %s" % (year, number, monday.day, MONTHS[monday.month - 1])


class QbForecastWeek(models.Model):
    _name = 'qb.forecast.week'
    _description = "Semana del pronóstico"
    _order = 'monday'

    name = fields.Char(string="Semana", required=True)
    monday = fields.Date(string="Lunes", required=True, index=True)
    iso_year = fields.Integer(string="Año ISO", required=True)
    iso_week = fields.Integer(string="Número de semana", required=True)

    capacidad_m = fields.Float(
        string="Capacidad de venta (m)", digits=(16, 0),
        help="Metros que Producción puede entregar esa semana. Vacío = la de Ajustes (250,000 m).")
    capacidad_efectiva = fields.Float(string="Capacidad (m)", digits=(16, 0), compute='_compute_carga')
    qty_m = fields.Float(string="Pronóstico (m)", digits=(16, 0), compute='_compute_carga')
    disponible_m = fields.Float(string="Disponible (m)", digits=(16, 0), compute='_compute_carga')
    uso_pct = fields.Float(string="Uso (%)", digits=(16, 1), compute='_compute_carga')
    excedida = fields.Boolean(string="Se pasa", compute='_compute_carga')

    _monday_uniq = models.Constraint('unique (monday)', "Ya existe esa semana.")

    @api.model
    def _qb_default_capacity(self):
        value = self.env['ir.config_parameter'].sudo().get_param(PARAM_CAPACITY)
        try:
            return float(value) if value else DEFAULT_CAPACITY
        except ValueError:
            return DEFAULT_CAPACITY

    @api.depends('capacidad_m')
    @api.depends_context('allowed_company_ids')
    def _compute_carga(self):
        """Metros del pronóstico vigente de la semana (todas las compañías activas) contra la capacidad."""
        totals = {}
        if self.ids:
            groups = self.env['qb.forecast.line']._read_group(
                [('week_id', 'in', self.ids), ('company_id', 'in', self.env.companies.ids)],
                ['week_id'], ['qty_m:sum'])
            totals = {week.id: qty for week, qty in groups}
        default = self._qb_default_capacity()
        for week in self:
            capacity = week.capacidad_m or default
            qty = totals.get(week.id, 0.0)
            week.capacidad_efectiva = capacity
            week.qty_m = qty
            week.disponible_m = capacity - qty
            week.uso_pct = qty / capacity * 100.0 if capacity else 0.0
            week.excedida = qty > capacity + 1e-6

    @api.model
    def _qb_get(self, mondays):
        """{lunes: semana} para los lunes dados; crea las que falten (sudo: nadie las captura a mano)."""
        mondays = {m for m in mondays if m}
        if not mondays:
            return {}
        Week = self.sudo()
        found = {w.monday: w for w in Week.search([('monday', 'in', list(mondays))])}
        missing = sorted(mondays - set(found))
        if missing:
            created = Week.create([{'name': week_label(m), 'monday': m, 'iso_year': m.isocalendar()[0],
                                    'iso_week': m.isocalendar()[1]} for m in missing])
            found.update({w.monday: w for w in created})
        return {m: w.with_env(self.env) for m, w in found.items()}

    @api.model
    def _qb_next(self, count=GRID_WEEKS):
        """Las próximas ``count`` semanas desde la semana en curso."""
        today = fields.Date.context_today(self)
        current = today - timedelta(days=today.weekday())
        weeks = self._qb_get([current + timedelta(weeks=i) for i in range(count)])
        return self.browse([weeks[m].id for m in sorted(weeks)])

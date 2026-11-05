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

    _monday_uniq = models.Constraint('unique (monday)', "Ya existe esa semana.")

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

# -*- coding: utf-8 -*-
"""Descanso semanal de tejido, con vigencia.

El calendario de las circulares en Odoo («Jornada 24/7 3 Turnos») arranca la
semana el sábado a las 19:00, pero desde el 25 de septiembre de 2026 tejido
para de viernes 18:00 a domingo 19:00. Como el cálculo recorre meses hacia
atrás, el descanso se guarda aquí por fecha de vigencia: las semanas viejas
se siguen midiendo con su descanso de entonces.
"""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from odoo import api, fields, models

from .motor import Descansos

TZ = 'America/Mexico_City'
DIAS = [('0', 'Lunes'), ('1', 'Martes'), ('2', 'Miércoles'), ('3', 'Jueves'),
        ('4', 'Viernes'), ('5', 'Sábado'), ('6', 'Domingo')]


def a_local(dt):
    """Datetime naive UTC de Odoo → naive en hora de planta."""
    if not dt:
        return None
    return dt.replace(tzinfo=ZoneInfo('UTC')).astimezone(ZoneInfo(TZ)).replace(tzinfo=None)


def a_utc(dt):
    if not dt:
        return None
    return dt.replace(tzinfo=ZoneInfo(TZ)).astimezone(ZoneInfo('UTC')).replace(tzinfo=None)


class QbTejidoDescanso(models.Model):
    _name = 'qb.tejido.descanso'
    _description = 'Descanso semanal de tejido'
    _order = 'vigente_desde desc'

    name = fields.Char(required=True)
    company_id = fields.Many2one('res.company', required=True,
                                 default=lambda self: self.env.company)
    vigente_desde = fields.Date(help='Vacío = desde siempre.')
    vigente_hasta = fields.Date(help='Vacío = sigue vigente.')
    dia_desde = fields.Selection(DIAS, required=True, default='4')
    hora_desde = fields.Float(required=True, default=18.0,
                              help='Hora local de planta, 18.5 = 18:30.')
    dia_hasta = fields.Selection(DIAS, required=True, default='6')
    hora_hasta = fields.Float(required=True, default=19.0)
    horas_semana = fields.Float(compute='_compute_horas_semana', string='Horas de descanso')

    @api.depends('dia_desde', 'hora_desde', 'dia_hasta', 'hora_hasta')
    def _compute_horas_semana(self):
        for rec in self:
            ini = int(rec.dia_desde) * 24 + rec.hora_desde
            fin = int(rec.dia_hasta) * 24 + rec.hora_hasta
            if fin <= ini:
                fin += 168
            rec.horas_semana = fin - ini

    @api.model
    def motor(self, workcenters=None, desde=None, hasta=None):
        """Objeto `Descansos` del motor con las reglas vigentes y los
        festivos globales de los calendarios de esas máquinas."""
        reglas = [(r.vigente_desde or None, r.vigente_hasta or None, int(r.dia_desde), r.hora_desde,
                   int(r.dia_hasta), r.hora_hasta)
                  for r in self.search([('company_id', '=', self.env.company.id)],
                                       order='vigente_desde desc NULLS LAST')]
        festivos = []
        if workcenters:
            cals = workcenters.mapped('resource_calendar_id')
            dom = [('resource_id', '=', False),
                   '|', ('calendar_id', 'in', cals.ids), ('calendar_id', '=', False)]
            if desde:
                dom.append(('date_to', '>=', datetime.combine(desde, datetime.min.time())
                            - timedelta(days=1)))
            if hasta:
                dom.append(('date_from', '<=', datetime.combine(hasta, datetime.min.time())
                            + timedelta(days=1)))
            for leave in self.env['resource.calendar.leaves'].search(dom):
                festivos.append((a_local(leave.date_from), a_local(leave.date_to)))
        return Descansos(reglas, festivos)

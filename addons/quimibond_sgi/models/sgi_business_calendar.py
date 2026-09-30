# -*- coding: utf-8 -*-
"""57.15.0 (G-007, G-008, decisión 4 de la tanda 2): festivos del calendario
del SGI y horario de los crons en hora de México.

- ``sgi.config._sgi_load_holidays(years)``: carga en el calendario de días
  hábiles del SGI (parámetro ``quimibond_sgi.business_calendar_id``; en
  producción el 21, «Standard 40 hours/week», que no usa ningún empleado ni
  centro de trabajo) los descansos obligatorios de la LFT, art. 74, como
  ausencias globales. Idempotente: un día que ya tiene una ausencia global en
  ese calendario no se toca; nada se borra. Los del contrato colectivo no
  están documentados en el repositorio: se cargan a mano en el calendario
  (Ajustes → Técnico → Horas laborales → Ausencias globales) cuando RH los
  confirme; esta función no los pisa.
- ``sgi.config._sgi_move_cron_hours()``: los crons son ``noupdate`` (A-007),
  así que la hora se mueve por migración. Checklists a las 05:30 y medición
  de actividades a las 03:00 hora local; los que corrían entre las 20:00 y
  las 23:00 del día anterior en México (02:xx-04:xx UTC) pasan a las 06:xx
  local del mismo día UTC, para que su «hoy» siga siendo el mismo día.
"""
import logging
from datetime import datetime, time, timedelta

import pytz

from odoo import api, fields, models

from .sgi_calendar import (
    BUSINESS_CALENDAR_PARAM, sgi_lft_holidays, sgi_local_datetime_utc, sgi_tz)

_logger = logging.getLogger(__name__)

# xmlid del cron → hora local (hora, minuto) a la siguiente corrida.
_SGI_CRON_NEXT_AT = {
    'quimibond_sgi.sgi_cron_checklists': (5, 30),
    'quimibond_sgi.sgi_cron_measure_activities': (3, 0),
}
# Crons que corrían de noche en México (02:xx-04:xx UTC): pasan a las 06:xx
# local del mismo día UTC de su siguiente corrida (se conserva el minuto).
_SGI_CRON_MORNING = (
    'quimibond_sgi.sgi_cron_legal_requirements',
    'quimibond_sgi.sgi_cron_context_review',
    'quimibond_sgi.sgi_cron_worker_participation',
    'quimibond_sgi.sgi_cron_sign_elearning',
    'quimibond_sgi.sgi_cron_my_procedure_stale',
)
_SGI_MORNING_HOUR = 6


class SgiConfigBusinessCalendar(models.AbstractModel):
    _inherit = 'sgi.config'

    @api.model
    def _sgi_business_calendar(self):
        """El calendario del parámetro, o vacío (no se cae al de la compañía:
        ese sí lo usan los empleados)."""
        raw = self.env['ir.config_parameter'].sudo().get_param(BUSINESS_CALENDAR_PARAM)
        if raw and str(raw).strip().isdigit():
            return self.env['resource.calendar'].sudo().browse(int(raw)).exists()
        return self.env['resource.calendar']

    @api.model
    def _sgi_load_holidays(self, years, calendar=None):
        """Carga los festivos de la LFT (art. 74) de ``years`` como ausencias
        globales del calendario. Devuelve las fechas creadas."""
        calendar = calendar or self._sgi_business_calendar()
        if not calendar:
            _logger.warning("SGI festivos: sin %s; no se carga nada.", BUSINESS_CALENDAR_PARAM)
            return []
        tz = pytz.timezone(calendar.tz or 'America/Mexico_City')
        Leave = self.env['resource.calendar.leaves'].sudo()
        created = []
        for year in years:
            for day, label in sgi_lft_holidays(year):
                start = tz.localize(datetime.combine(day, time.min)).astimezone(pytz.utc)
                end = tz.localize(datetime.combine(day, time(23, 59, 59))).astimezone(pytz.utc)
                start, end = start.replace(tzinfo=None), end.replace(tzinfo=None)
                if Leave.search_count([
                        ('calendar_id', '=', calendar.id), ('resource_id', '=', False),
                        ('date_from', '<=', end), ('date_to', '>=', start)], limit=1):
                    continue
                Leave.create({
                    'name': "Festivo: %s" % label,
                    'calendar_id': calendar.id,
                    'company_id': calendar.company_id.id or False,
                    'resource_id': False,
                    'date_from': start,
                    'date_to': end,
                })
                created.append(day)
        if created:
            _logger.info("SGI festivos: %d cargados en «%s» (id %s): %s.", len(created),
                         calendar.name, calendar.id, ", ".join(str(d) for d in created))
        return created

    @api.model
    def _sgi_move_cron_hours(self, now=None):
        """Mueve la siguiente corrida de los crons del SGI a su hora local.
        Idempotente: un cron que ya está a su hora no se toca. Devuelve
        {xmlid: (antes, después)}."""
        now = now or fields.Datetime.now()
        tz = sgi_tz(self.env)
        moved = {}
        for xmlid, (hour, minute) in _SGI_CRON_NEXT_AT.items():
            cron = self.env.ref(xmlid, raise_if_not_found=False)
            if not cron:
                continue
            local_day = pytz.utc.localize(now).astimezone(tz).date()
            target = sgi_local_datetime_utc(self.env, local_day, hour, minute)
            if target <= now:
                target = sgi_local_datetime_utc(self.env, local_day + timedelta(days=1), hour, minute)
            current = cron.nextcall
            if current and pytz.utc.localize(current).astimezone(tz).time() == time(hour, minute):
                continue
            cron.sudo().write({'nextcall': target})
            moved[xmlid] = (current, target)
        for xmlid in _SGI_CRON_MORNING:
            cron = self.env.ref(xmlid, raise_if_not_found=False)
            if not cron or not cron.nextcall:
                continue
            current = cron.nextcall
            local = pytz.utc.localize(current).astimezone(tz)
            # Solo los que siguen de noche en México (18:00-23:59 local).
            if local.hour < 18:
                continue
            target = sgi_local_datetime_utc(self.env, current.date(), _SGI_MORNING_HOUR,
                                            current.minute)
            cron.sudo().write({'nextcall': target})
            moved[xmlid] = (current, target)
        for xmlid, (before, after) in moved.items():
            _logger.info("SGI crons: %s de %s a %s UTC.", xmlid, before, after)
        return moved

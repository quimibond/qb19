# -*- coding: utf-8 -*-
"""Días hábiles del SGI, con sus días festivos.

El calendario sale del parámetro ``quimibond_sgi.business_calendar_id`` (en
producción, el de lunes a viernes). Sin parámetro, el de la compañía; sin
ninguno, lunes a viernes. El parámetro existe porque el calendario de la
compañía no siempre es el de la oficina (hay turnos de lunes a jueves y sábado).

Funciones puras sobre un env: las usan el eslabón atorado, la medición
semanal (a tiempo / vencido) y el escalamiento.
"""
from datetime import datetime, time, timedelta

import pytz


BUSINESS_CALENDAR_PARAM = 'quimibond_sgi.business_calendar_id'


def _calendar(env, company=None):
    raw = env['ir.config_parameter'].sudo().get_param(BUSINESS_CALENDAR_PARAM)
    if raw and str(raw).strip().isdigit():
        calendar = env['resource.calendar'].sudo().browse(int(raw)).exists()
        if calendar:
            return calendar
    company = company or env.company
    return company.resource_calendar_id


def _working_dates(env, start_date, end_date, company=None):
    """Fechas laborables entre start_date y end_date (ambas incluidas)."""
    if end_date < start_date:
        return set()
    calendar = _calendar(env, company)
    if not calendar:
        day, out = start_date, set()
        while day <= end_date:
            if day.weekday() < 5:
                out.add(day)
            day += timedelta(days=1)
        return out
    tz = pytz.timezone(calendar.tz or 'UTC')
    start_dt = tz.localize(datetime.combine(start_date, time.min))
    end_dt = tz.localize(datetime.combine(end_date, time.max))
    intervals = calendar._work_intervals_batch(start_dt, end_dt)[False]
    return {begin.astimezone(tz).date() for begin, _end, _rec in intervals}


def sgi_business_days(env, start, end, company=None):
    """Días hábiles completos que pasaron después de ``start`` hasta ``end``
    (el día de inicio no cuenta)."""
    if not start or not end or end <= start:
        return 0
    first = start.date() + timedelta(days=1)
    return len(_working_dates(env, first, end.date(), company))


def sgi_add_business_days(env, start, days, company=None):
    """Fecha (date) en que vence algo que llegó en ``start`` con ``days``
    días hábiles de plazo: la entrada del viernes con 1 día vence el lunes."""
    day = start.date() if isinstance(start, datetime) else start
    if not days:
        return day
    horizon = day + timedelta(days=days * 3 + 15)
    dates = sorted(d for d in _working_dates(env, day + timedelta(days=1), horizon, company))
    if len(dates) >= days:
        return dates[days - 1]
    return horizon


def sgi_nth_business_day(env, year, month, nth, company=None):
    """El día hábil número ``nth`` del mes (o el último si el mes tiene menos)."""
    first = datetime(year, month, 1).date()
    last = (first.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
    dates = sorted(_working_dates(env, first, last, company))
    if not dates:
        return last
    return dates[min(nth, len(dates)) - 1]

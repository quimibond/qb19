# -*- coding: utf-8 -*-
"""Días hábiles del SGI, con sus días festivos.

El calendario sale del parámetro ``quimibond_sgi.business_calendar_id`` (en
producción, el de lunes a viernes). Sin parámetro, el de la compañía; sin
ninguno, lunes a viernes. El parámetro existe porque el calendario de la
compañía no siempre es el de la oficina (hay turnos de lunes a jueves y sábado).

Funciones puras sobre un env: las usan el eslabón atorado, la medición
semanal (a tiempo / vencido) y el escalamiento.

57.15.0 (G-007, G-008, G-009, decisión 4 de la tanda 2):

- ``sgi_today(env)``: el «hoy» del SGI es el de la zona del calendario de
  días hábiles (en producción, America/Mexico_City). Los crons corren como
  OdooBot, que no tiene zona: con ``context_today`` tomaban la fecha UTC y a
  partir de las 18:00 de México ya era «mañana». No se toca OdooBot.
- Un datetime (Odoo lo guarda en UTC) se pasa a fecha local antes de contar
  días hábiles: una entrada de las 19:00 es de ese día, no del siguiente.
- ``sgi_previous_business_day``: un vencimiento que cae en día inhábil se
  adelanta al hábil anterior.
- ``sgi_lft_holidays(year)``: los descansos obligatorios de la LFT, art. 74.
"""
from bisect import bisect_left, bisect_right
from datetime import date, datetime, time, timedelta

import pytz


BUSINESS_CALENDAR_PARAM = 'quimibond_sgi.business_calendar_id'
SGI_DEFAULT_TZ = 'America/Mexico_City'


def _calendar(env, company=None):
    raw = env['ir.config_parameter'].sudo().get_param(BUSINESS_CALENDAR_PARAM)
    if raw and str(raw).strip().isdigit():
        calendar = env['resource.calendar'].sudo().browse(int(raw)).exists()
        if calendar:
            return calendar
    company = company or env.company
    return company.resource_calendar_id


def sgi_tz(env, company=None):
    """Zona del calendario de días hábiles del SGI (o la de México)."""
    calendar = _calendar(env, company)
    name = (calendar.tz if calendar else None) or SGI_DEFAULT_TZ
    try:
        return pytz.timezone(name)
    except pytz.UnknownTimeZoneError:
        return pytz.timezone(SGI_DEFAULT_TZ)


def sgi_today(env, company=None):
    """Fecha de hoy en la zona del calendario del SGI (no la del usuario ni
    la UTC de OdooBot)."""
    return datetime.now(pytz.utc).astimezone(sgi_tz(env, company)).date()


def sgi_local_date(env, value, company=None):
    """``value`` como fecha local: un ``date`` se queda igual; un datetime sin
    zona (Odoo lo guarda en UTC) pasa a la zona del calendario."""
    if not value:
        return value
    if isinstance(value, datetime):
        aware = value if value.tzinfo else pytz.utc.localize(value)
        return aware.astimezone(sgi_tz(env, company)).date()
    return value


def sgi_local_datetime_utc(env, day, hour, minute=0, company=None):
    """El datetime UTC sin zona (como lo guarda Odoo) de ``day`` a la hora
    local ``hour:minute`` del calendario del SGI."""
    tz = sgi_tz(env, company)
    local = tz.localize(datetime.combine(day, time(hour, minute)))
    return local.astimezone(pytz.utc).replace(tzinfo=None)


def _nth_weekday(year, month, weekday, nth):
    """El ``nth`` (1 = primero) ``weekday`` (0 = lunes) del mes."""
    first = date(year, month, 1)
    offset = (weekday - first.weekday()) % 7
    return first + timedelta(days=offset + 7 * (nth - 1))


def sgi_lft_holidays(year):
    """[(fecha, motivo)] de los descansos obligatorios de la Ley Federal del
    Trabajo, art. 74, en ``year``:

    I 1 de enero; II primer lunes de febrero; III tercer lunes de marzo;
    IV 1 de mayo; V 16 de septiembre; VI tercer lunes de noviembre; VII 1 de
    octubre cada seis años, cuando toma posesión el Ejecutivo Federal (2024,
    2030…); VIII 25 de diciembre. La fracción IX (día de la jornada
    electoral) no se carga: la fija cada ley electoral y las elecciones
    federales ordinarias caen en domingo."""
    out = [
        (date(year, 1, 1), "1 de enero (LFT art. 74 I)"),
        (_nth_weekday(year, 2, 0, 1), "Día de la Constitución (LFT art. 74 II)"),
        (_nth_weekday(year, 3, 0, 3), "Natalicio de Benito Juárez (LFT art. 74 III)"),
        (date(year, 5, 1), "1 de mayo (LFT art. 74 IV)"),
        (date(year, 9, 16), "16 de septiembre (LFT art. 74 V)"),
        (_nth_weekday(year, 11, 0, 3), "Revolución Mexicana (LFT art. 74 VI)"),
        (date(year, 12, 25), "25 de diciembre (LFT art. 74 VIII)"),
    ]
    if year >= 2024 and (year - 2024) % 6 == 0:
        out.append((date(year, 10, 1), "Transmisión del Poder Ejecutivo Federal (LFT art. 74 VII)"))
    return sorted(out)


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
    (el día de inicio no cuenta). Acepta fechas o datetimes (UTC, se pasan a
    fecha local)."""
    if not start or not end:
        return 0
    first_day = sgi_local_date(env, start, company)
    last_day = sgi_local_date(env, end, company)
    if last_day <= first_day:
        return 0
    return len(_working_dates(env, first_day + timedelta(days=1), last_day, company))


def sgi_is_business_day(env, day, company=None):
    return bool(_working_dates(env, day, day, company))


def sgi_previous_business_day(env, day, floor=None, company=None):
    """Decisión 4 de la tanda 2: un vencimiento que cae en día inhábil se
    adelanta al día hábil anterior. ``floor`` es el primer día del periodo:
    si no hay día hábil entre ``floor`` y ``day`` (un lunes festivo en el
    vencimiento semanal), se recorre al siguiente hábil para no vencer en el
    periodo anterior."""
    if not day:
        return day
    lower = floor or (day - timedelta(days=31))
    dates = _working_dates(env, lower, day, company)
    if dates:
        return max(dates)
    if floor:
        ahead = _working_dates(env, day + timedelta(days=1), day + timedelta(days=31), company)
        if ahead:
            return min(ahead)
    return day


def sgi_add_business_days(env, start, days, company=None):
    """Fecha (date) en que vence algo que llegó en ``start`` con ``days``
    días hábiles de plazo: la entrada del viernes con 1 día vence el lunes.
    ``days`` negativo cuenta hacia atrás (-2 = dos días hábiles antes)."""
    day = sgi_local_date(env, start, company)
    if not days:
        return day
    if days < 0:
        back = -days
        horizon = day - timedelta(days=back * 3 + 15)
        dates = sorted((d for d in _working_dates(env, horizon, day - timedelta(days=1), company)),
                       reverse=True)
        if len(dates) >= back:
            return dates[back - 1]
        return horizon
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


class SgiWorkdays:
    """57.17.0 (G-016): los días hábiles de un rango, calculados UNA vez por
    corrida. ``add`` da lo mismo que ``sgi_add_business_days`` sin volver a
    consultar el calendario mientras la fecha caiga en el rango; fuera de él
    usa la función normal."""

    def __init__(self, env, start, end, company=None):
        self.env = env
        self.company = company
        self.start = start
        self.end = end
        self.dates = sorted(_working_dates(env, start, end, company))

    def add(self, start, days):
        day = sgi_local_date(self.env, start, self.company)
        if not days:
            return day
        if days > 0:
            if day + timedelta(days=1) >= self.start:
                index = bisect_right(self.dates, day) + days - 1
                if index < len(self.dates):
                    return self.dates[index]
        elif day - timedelta(days=1) <= self.end:
            index = bisect_left(self.dates, day) + days
            if index >= 0:
                return self.dates[index]
        return sgi_add_business_days(self.env, day, days, self.company)

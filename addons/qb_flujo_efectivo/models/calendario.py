# -*- coding: utf-8 -*-
"""Calendario del flujo a 13 semanas, sin Odoo (lo prueban los tests puros).

* Semana 1 = de la fecha del reporte al domingo siguiente; las demás, de
  lunes a domingo.
* Fechas de cada compromiso (semanal, quincenal, mensual, bimestral, única)
  dentro de un rango.
* Periodos hacia atrás para estimar un compromiso con lo que se pagó.
"""
import calendar
from datetime import date, timedelta

SEMANAS = 13


def semanas(hoy, n=SEMANAS):
    """Lista de ``(inicio, fin)`` de las ``n`` semanas a partir de ``hoy``."""
    fin1 = hoy + timedelta(days=6 - hoy.weekday())
    out = [(hoy, fin1)]
    for i in range(1, n):
        ini = fin1 + timedelta(days=1 + 7 * (i - 1))
        out.append((ini, ini + timedelta(days=6)))
    return out


def indice_semana(fecha, sems):
    """Índice de la semana de ``fecha``; lo anterior a la semana 1 cae en la 1
    (lo vencido) y lo posterior a la última devuelve ``None``."""
    if fecha <= sems[0][1]:
        return 0
    for i, (ini, fin) in enumerate(sems):
        if ini <= fecha <= fin:
            return i
    return None


def _ultimo_dia(anio, mes):
    return calendar.monthrange(anio, mes)[1]


def _dia_del_mes(anio, mes, dia):
    return date(anio, mes, min(dia, _ultimo_dia(anio, mes)))


def _meses(desde, hasta):
    anio, mes = desde.year, desde.month
    while (anio, mes) <= (hasta.year, hasta.month):
        yield anio, mes
        mes += 1
        if mes > 12:
            anio, mes = anio + 1, 1


def meses(desde, hasta):
    """``(año, mes)`` de cada mes entre dos fechas, incluidos los extremos."""
    return list(_meses(desde, hasta))


def dias_restantes(anio, mes, desde):
    """Días del mes a partir de ``desde`` (o el mes completo si empieza después)."""
    ini = max(date(anio, mes, 1), desde)
    fin = date(anio, mes, _ultimo_dia(anio, mes))
    return [ini + timedelta(days=i) for i in range((fin - ini).days + 1)]


def repartir(fecha_vence, hoy, atraso, sems, semanas_reparto):
    """Fechas e importes relativos (suman 1) en que se espera una partida.

    * Por vencer o vencida dentro del atraso normal del contacto: vencimiento
      más el atraso (nunca antes de hoy).
    * Vencida más allá de su atraso normal: en partes iguales en las primeras
      ``semanas_reparto`` semanas (1 = todo en la semana en curso).
    """
    esperado = fecha_vence + timedelta(days=atraso or 0)
    if esperado >= hoy or fecha_vence >= hoy:
        return [(max(esperado, hoy), 1.0)]
    n = max(1, min(semanas_reparto or 1, len(sems)))
    return [(sems[i][0], 1.0 / n) for i in range(n)]


def fechas(periodicidad, desde, hasta, dia_semana=4, dia_mes=17, meses_pares=False, fecha_unica=None, mes=12):
    """Fechas de pago de un compromiso entre ``desde`` y ``hasta`` (incluidas).

    * ``semanal``: cada semana en ``dia_semana`` (0 = lunes, 4 = viernes).
    * ``quincenal``: el 15 y el último día del mes.
    * ``mensual``: el ``dia_mes`` de cada mes (o el último si el mes es corto).
    * ``bimestral``: el ``dia_mes`` de los meses nones (o pares con
      ``meses_pares``).
    * ``anual``: el ``dia_mes`` del ``mes`` (aguinaldo, PTU).
    * ``unica``: ``fecha_unica``.
    """
    out = []
    if periodicidad == 'semanal':
        d = desde + timedelta(days=(dia_semana - desde.weekday()) % 7)
        while d <= hasta:
            out.append(d)
            d += timedelta(days=7)
    elif periodicidad == 'quincenal':
        for anio, mes in _meses(desde, hasta):
            out += [date(anio, mes, 15), date(anio, mes, _ultimo_dia(anio, mes))]
    elif periodicidad == 'mensual':
        out = [_dia_del_mes(a, m, dia_mes) for a, m in _meses(desde, hasta)]
    elif periodicidad == 'bimestral':
        resto = 0 if meses_pares else 1
        out = [_dia_del_mes(a, m, dia_mes) for a, m in _meses(desde, hasta) if m % 2 == resto]
    elif periodicidad == 'anual':
        out = [_dia_del_mes(a, m, dia_mes) for a, m in _meses(desde, hasta) if m == (mes or 12)]
    elif periodicidad == 'unica' and fecha_unica:
        out = [fecha_unica]
    return [d for d in out if desde <= d <= hasta]


def clave_periodo(periodicidad, fecha):
    """Periodo al que pertenece un pago, para promediar por periodo pagado."""
    if periodicidad == 'semanal':
        anio, sem, _dia = fecha.isocalendar()
        return (anio, sem)
    if periodicidad == 'quincenal':
        return (fecha.year, fecha.month, 1 if fecha.day <= 15 else 2)
    if periodicidad == 'bimestral':
        return (fecha.year, (fecha.month + 1) // 2)
    if periodicidad == 'anual':
        return (fecha.year,)
    return (fecha.year, fecha.month)


def inicio_historial(periodicidad, hoy, periodos):
    """Fecha desde la que se toman los pagos para estimar ``periodos`` periodos."""
    if periodicidad == 'semanal':
        return hoy - timedelta(days=7 * periodos)
    dias = {'quincenal': 15, 'mensual': 30, 'bimestral': 61, 'anual': 366}.get(periodicidad, 30)
    return hoy - timedelta(days=dias * periodos)


def estimar(periodicidad, pagos):
    """Promedio por periodo que tuvo pago. ``pagos`` = ``[(fecha, importe)]``.

    Se promedia entre los periodos con pago, no entre todos los del rango,
    para que un mes que aún no se paga (o que se pagó junto con otro) no baje
    el promedio a la mitad."""
    por_periodo = {}
    for fecha, importe in pagos:
        clave = clave_periodo(periodicidad, fecha)
        por_periodo[clave] = por_periodo.get(clave, 0.0) + importe
    if not por_periodo:
        return 0.0
    return sum(por_periodo.values()) / len(por_periodo)


def promedio_ponderado(pares):
    """``[(valor, peso)]`` → promedio ponderado (``None`` sin peso)."""
    peso = sum(p for _v, p in pares if p > 0)
    if not peso:
        return None
    return sum(v * p for v, p in pares if p > 0) / peso

# -*- coding: utf-8 -*-
"""Calendario de pagos de nómina: funciones puras, sin modelos.

Un solo conteo de días de pago para todo el módulo: el divisor del Infonavit
(pagos del bimestre, ``infonavit.py``), las nóminas que paga el mes para el
FONACOT y el ISR mensual (``isr_mensual._qb_nomina_periodos_del_mes``,
``retenciones._qb_fonacot_periodos``). Sin modelos a propósito: se puede
importar desde cualquier archivo de ``models/`` sin depender del orden de
``models/__init__.py``."""
import calendar
from datetime import date, timedelta

QUINCENAS_POR_BIMESTRE = 4


def _qb_pagos_en_ventana(desde, hasta, dia_semana):
    """Cuántas veces cae ``dia_semana`` (lunes = 0) entre ``desde`` y ``hasta``, inclusive."""
    if not desde or not hasta or hasta < desde:
        return 0
    adelanto = (dia_semana - desde.weekday()) % 7
    primero = desde + timedelta(days=adelanto)
    if primero > hasta:
        return 0
    return (hasta - primero).days // 7 + 1


def _qb_dias_de_pago(desde, hasta, dia_semana):
    """Las fechas en que cae ``dia_semana`` entre ``desde`` y ``hasta``, inclusive
    (las mismas que cuenta ``_qb_pagos_en_ventana``)."""
    n = _qb_pagos_en_ventana(desde, hasta, dia_semana)
    if not n:
        return []
    primero = desde + timedelta(days=(dia_semana - desde.weekday()) % 7)
    return [primero + timedelta(weeks=i) for i in range(n)]


def _qb_mes_de(dia):
    """Primer y último día del mes de ``dia``."""
    return (date(dia.year, dia.month, 1),
            date(dia.year, dia.month, calendar.monthrange(dia.year, dia.month)[1]))


def _qb_bimestre_de(dia):
    """Primer y último día del bimestre del IMSS al que pertenece ``dia``
    (enero–febrero, marzo–abril, … noviembre–diciembre)."""
    primer_mes = dia.month - ((dia.month - 1) % 2)
    segundo_mes = primer_mes + 1
    return (date(dia.year, primer_mes, 1),
            date(dia.year, segundo_mes, calendar.monthrange(dia.year, segundo_mes)[1]))

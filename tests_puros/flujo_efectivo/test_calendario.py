# -*- coding: utf-8 -*-
"""Pytest puro del calendario del flujo a 13 semanas (sin Odoo)."""
import importlib.util
import os
from datetime import date

_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'addons', 'qb_flujo_efectivo', 'models',
                     'calendario.py')
_spec = importlib.util.spec_from_file_location('qb_flujo_calendario', _PATH)
cal = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cal)


def test_semana_uno_termina_el_domingo_y_son_13():
    sems = cal.semanas(date(2026, 11, 24))  # martes
    assert len(sems) == 13
    assert sems[0] == (date(2026, 11, 24), date(2026, 11, 29))
    assert sems[1] == (date(2026, 11, 30), date(2026, 12, 6))
    assert sems[-1][1] == date(2027, 2, 21)


def test_indice_semana_vencido_cae_en_la_primera_y_lo_lejano_fuera():
    sems = cal.semanas(date(2026, 11, 24))
    assert cal.indice_semana(date(2026, 9, 1), sems) == 0
    assert cal.indice_semana(date(2026, 11, 29), sems) == 0
    assert cal.indice_semana(date(2026, 11, 30), sems) == 1
    assert cal.indice_semana(date(2027, 2, 21), sems) == 12
    assert cal.indice_semana(date(2027, 2, 22), sems) is None


def test_fechas_por_periodicidad():
    desde, hasta = date(2026, 11, 24), date(2027, 2, 21)
    viernes = cal.fechas('semanal', desde, hasta, dia_semana=4)
    assert viernes[0] == date(2026, 11, 27) and len(viernes) == 13
    quincenas = cal.fechas('quincenal', desde, hasta)
    assert quincenas[0] == date(2026, 11, 30) and date(2027, 2, 15) in quincenas
    assert date(2026, 11, 15) not in quincenas
    assert cal.fechas('mensual', desde, hasta, dia_mes=17) == [date(2026, 12, 17), date(2027, 1, 17),
                                                                 date(2027, 2, 17)]
    assert cal.fechas('mensual', date(2027, 2, 1), date(2027, 2, 28), dia_mes=31) == [date(2027, 2, 28)]
    assert cal.fechas('bimestral', desde, hasta, dia_mes=17) == [date(2027, 1, 17)]
    assert cal.fechas('bimestral', desde, hasta, dia_mes=17, meses_pares=True) == [date(2026, 12, 17),
                                                                                    date(2027, 2, 17)]
    assert cal.fechas('unica', desde, hasta, fecha_unica=date(2026, 12, 1)) == [date(2026, 12, 1)]
    assert cal.fechas('unica', desde, hasta, fecha_unica=date(2026, 11, 1)) == []


def test_estimar_promedia_entre_periodos_con_pago():
    # Septiembre se pagó junto con agosto: no baja el promedio de los demás.
    pagos = [(date(2026, 8, 20), 400.0), (date(2026, 8, 20), 100.0), (date(2026, 10, 23), 480.0),
             (date(2026, 11, 17), 520.0)]
    assert cal.estimar('mensual', pagos) == 500.0
    assert cal.estimar('mensual', []) == 0.0
    semanal = [(date(2026, 11, 20), 300.0), (date(2026, 11, 19), 50.0), (date(2026, 11, 13), 350.0)]
    assert cal.estimar('semanal', semanal) == 350.0


def test_anual_y_dias_restantes():
    assert cal.fechas('anual', date(2026, 11, 24), date(2027, 2, 21), dia_mes=18, mes=12) == [date(2026, 12, 18)]
    dias = cal.dias_restantes(2026, 11, date(2026, 11, 24))
    assert dias[0] == date(2026, 11, 24) and dias[-1] == date(2026, 11, 30) and len(dias) == 7
    assert len(cal.dias_restantes(2026, 12, date(2026, 11, 24))) == 31


def test_repartir_por_atraso_y_vencido_de_mas():
    hoy = date(2026, 11, 24)
    sems = cal.semanas(hoy)
    # por vencer, cliente que paga 10 días tarde
    assert cal.repartir(date(2026, 12, 1), hoy, 10, sems, 4) == [(date(2026, 12, 11), 1.0)]
    # vencida dentro de su atraso normal
    assert cal.repartir(date(2026, 11, 20), hoy, 10, sems, 4) == [(date(2026, 11, 30), 1.0)]
    # vencida más allá: en partes iguales en 4 semanas
    partes = cal.repartir(date(2026, 10, 1), hoy, 10, sems, 4)
    assert [f for f, _p in partes] == [hoy, date(2026, 11, 30), date(2026, 12, 7), date(2026, 12, 14)]
    assert abs(sum(p for _f, p in partes) - 1) < 1e-9
    # paga antes del vencimiento: nunca antes de hoy
    assert cal.repartir(date(2026, 11, 25), hoy, -5, sems, 4) == [(hoy, 1.0)]


def test_promedio_ponderado():
    assert cal.promedio_ponderado([(10, 1), (20, 3)]) == 17.5
    assert cal.promedio_ponderado([]) is None

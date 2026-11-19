# -*- coding: utf-8 -*-
"""Reglas después del release (Jose, 19-nov-2026): semanas congeladas, embarques cada N semanas y
redondeo a rollos. Datos inventados."""
import os
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(__file__))
from release_loader import load  # noqa: E402

reglas = load('reglas')
HOY = date(2026, 11, 16)  # lunes


def w(n):
    return HOY + timedelta(weeks=n)


def test_sin_reglas_queda_como_lo_pide_el_cliente():
    pide = {w(0): 100.0, w(1): 200.0}
    result, notes = reglas.aplicar_reglas(pide, {}, HOY)
    assert result == pide
    assert notes == {}


def test_aumento_en_congeladas_pasa_a_la_semana_4():
    pide = {w(0): 100.0, w(1): 300.0, w(2): 100.0, w(3): 50.0, w(4): 80.0}
    prog = {w(0): 100.0, w(1): 200.0, w(2): 100.0}
    result, notes = reglas.aplicar_reglas(pide, prog, HOY, congeladas=3)
    assert [result[w(i)] for i in range(5)] == [100.0, 200.0, 100.0, 150.0, 80.0]
    assert "Recibe +100" in notes[w(3)]
    assert "Congelada" in notes[w(1)]


def test_baja_en_congeladas_se_resta_de_la_4_y_la_5():
    pide = {w(0): 0.0, w(1): 0.0, w(2): 100.0, w(3): 50.0, w(4): 200.0}
    prog = {w(0): 100.0, w(1): 100.0, w(2): 100.0}
    result, _notes = reglas.aplicar_reglas(pide, prog, HOY, congeladas=3)
    assert [result[w(i)] for i in range(5)] == [100.0, 100.0, 100.0, 0.0, 50.0]


def test_baja_sin_de_donde_descontar_no_queda_negativa():
    pide = {w(3): 10.0}
    prog = {w(0): 100.0}
    result, notes = reglas.aplicar_reglas(pide, prog, HOY, congeladas=3)
    assert result[w(0)] == 100.0
    assert result[w(3)] == 0.0
    assert "Quedan 90" in notes[w(3)]


def test_congelada_sin_programa_pasa_todo_a_la_4():
    result, _notes = reglas.aplicar_reglas({w(1): 70.0}, {}, HOY, congeladas=3)
    assert result[w(1)] == 0.0
    assert result[w(3)] == 70.0


def test_lo_anterior_a_la_semana_en_curso_no_se_toca():
    result, _notes = reglas.aplicar_reglas({w(-1): 40.0}, {w(-1): 10.0}, HOY, congeladas=3,
                                           redondear=lambda q: reglas.redondear_arriba(q, 500))
    assert result[w(-1)] == 40.0


def test_embarques_cada_4_semanas_desde_una_fecha_fija():
    pide = {w(i): 10.0 for i in range(3, 12)}
    result, notes = reglas.aplicar_reglas(pide, {}, HOY, congeladas=3, agrupar=4, inicio_bloques=w(3))
    assert result[w(3)] == 40.0 and result[w(7)] == 40.0 and result[w(11)] == 10.0
    assert all(result[w(i)] == 0.0 for i in (4, 5, 6, 8, 9, 10))
    assert "Junta" in notes[w(3)]
    # La semana siguiente los bloques no se recorren: siguen anclados en la misma fecha.
    later, _ = reglas.aplicar_reglas(pide, {}, HOY + timedelta(weeks=1), agrupar=4, inicio_bloques=w(3))
    assert later[w(7)] == 40.0


def test_redondeo_hacia_arriba_a_rollos():
    up = lambda q: reglas.redondear_arriba(q, 500)  # noqa: E731
    result, notes = reglas.aplicar_reglas({w(0): 505.0, w(1): 1000.0, w(2): 0.0}, {}, HOY, redondear=up)
    assert result == {w(0): 1000.0, w(1): 1000.0, w(2): 0.0}
    assert "Redondeado de 505 a 1000" in notes[w(0)]


def test_redondear_arriba_tolera_decimales():
    assert reglas.redondear_arriba(1000.0000001, 500) == 1000
    assert reglas.redondear_arriba(1.0, 1000) == 1000
    assert reglas.redondear_arriba(250.0, 0) == 250.0


def test_las_congeladas_no_se_redondean():
    up = lambda q: reglas.redondear_arriba(q, 500)  # noqa: E731
    result, _notes = reglas.aplicar_reglas({w(0): 120.0, w(3): 600.0}, {w(0): 120.0}, HOY, congeladas=3,
                                           redondear=up)
    assert result[w(0)] == 120.0
    assert result[w(3)] == 1000.0


def test_dias_habiles_sin_fines_de_semana():
    # Lunes 23-nov menos 5 hábiles = lunes 16-nov; jueves 26 = jueves 19.
    assert reglas.restar_dias_habiles(date(2026, 11, 23), 5) == date(2026, 11, 16)
    assert reglas.restar_dias_habiles(date(2026, 11, 26), 5) == date(2026, 11, 19)
    # Domingo 22-nov: se cuenta desde el viernes 20 → viernes 13.
    assert reglas.restar_dias_habiles(date(2026, 11, 22), 5) == date(2026, 11, 13)
    # Miércoles 25 menos 2 = lunes 23; menos 3 = viernes 20 (salta el fin de semana).
    assert reglas.restar_dias_habiles(date(2026, 11, 25), 2) == date(2026, 11, 23)
    assert reglas.restar_dias_habiles(date(2026, 11, 25), 3) == date(2026, 11, 20)
    assert reglas.restar_dias_habiles(date(2026, 11, 21), 0) == date(2026, 11, 20)


def test_congelada_con_pedido_se_marca():
    """3.28.0: el programado de una congelada con pedido es lo pedido (lo pone el modelo); la nota lo dice."""
    pide = {w(0): 100.0, w(1): 250.0, w(2): 300.0, w(3): 400.0}
    prog = {w(0): 100.0, w(1): 400.0, w(2): 300.0}
    result, notes = reglas.aplicar_reglas(pide, prog, HOY, congeladas=3, con_pedido={w(1)})
    assert [result[w(i)] for i in range(4)] == [100.0, 400.0, 300.0, 250.0]
    assert notes[w(1)].startswith("Congelada con pedido")

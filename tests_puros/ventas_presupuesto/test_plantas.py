# -*- coding: utf-8 -*-
"""La planta escrita distinto en el perfil, en «Cómo pide cada cliente» y en Condiciones de entrega
(producción, 19-nov-2026)."""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from release_loader import load  # noqa: E402

plantas = load('plantas')


def test_reglas_de_produccion():
    saltillo = ['LEÓN', 'SALTILLO']
    assert plantas.elegir('SALTILLO LAMINATION LEON', saltillo, 'SALTILLO LAMINATION') == 0
    assert plantas.elegir('SALTILLO LAMINATION SALTILLO', saltillo, 'SALTILLO LAMINATION') == 1
    fxi = ['CIUDAD JUAREZ', 'SANTA TERESA']
    assert plantas.elegir('FXI CD JUAREZ', fxi, 'FXI INC') == 0
    assert plantas.elegir('SANTA TERESA', fxi, 'FXI INC') == 1
    shawmut = ['SILAO', 'TENNESSEE']
    assert plantas.elegir('Silao', shawmut, 'SHAWMUT LLC') == 0
    assert plantas.elegir('Tennessee', shawmut, 'SHAWMUT LLC') == 1


def test_condiciones_con_direccion_y_planta():
    opciones = ['Silao', 'Tennessee']
    assert plantas.elegir('Silao', opciones, 'SHAWMUT LLC') == 0


def test_sin_coincidencia_o_empate():
    assert plantas.elegir('Monterrey', ['SILAO', 'TENNESSEE'], 'SHAWMUT LLC') is None
    assert plantas.elegir('Planta Norte', ['Norte A', 'Norte B'], 'X') is None
    assert plantas.elegir('', ['SILAO'], 'X') is None
    assert plantas.elegir('SILAO', [False, ''], 'X') is None

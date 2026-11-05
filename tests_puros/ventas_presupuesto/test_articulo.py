# -*- coding: utf-8 -*-
"""Pytest puro de las reglas de código del artículo (Dirección 2026-11-04: con «I» es kilos, sin «I»
es metros; la versión en m² lleva «M2» al final)."""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(__file__))
from release_loader import load  # noqa: E402

art = load('articulo')


def test_kg_code_and_meters_code():
    assert art.es_codigo_tela('WJ045Q22JNT160')
    assert art.es_codigo_kg('IWJ045Q22JNT160')
    assert not art.es_codigo_kg('WJ045Q22JNT160')
    assert art.codigo_metros('IWJ045Q22JNT160') == 'WJ045Q22JNT160'
    assert art.codigo_metros('WJ045Q22JNT160M2') == 'WJ045Q22JNT160'
    assert art.codigo_metros('WJ045Q22JNT160') == 'WJ045Q22JNT160'
    assert art.codigo_metros('IK20BL155') == 'IK20BL155'  # no es código de tela: se queda
    assert art.codigo_metros(' wj045q22jnt160. ') == 'WJ045Q22JNT160'


def test_grammage_width_and_yield():
    assert art.gramaje_ancho('WJ038Q22JNT160') == (38.0, 160.0)
    assert art.gramaje_ancho('IWC090Q11JNT168') == (90.0, 168.0)
    assert art.gramaje_ancho('PSITPBOMBAYAZ/JI') == (None, None)
    assert art.rendimiento(38, 160) == pytest.approx(16.4474, rel=1e-4)
    assert art.rendimiento(0, 160) == 0.0


def test_convert_never_invents():
    assert art.convertir(1000, 'yd', 'm') == pytest.approx(914.4)
    assert art.convertir(100, 'kg', 'm', m_por_kg=14.1887) == pytest.approx(1418.87)
    assert art.convertir(1418.87, 'm', 'kg', m_por_kg=14.1887) == pytest.approx(100)
    assert art.convertir(100, 'kg', 'm') is None          # sin rendimiento
    assert art.convertir(160, 'm2', 'm', ancho_m=1.6) == pytest.approx(100)
    assert art.convertir(100, 'm', 'm2') is None          # sin ancho
    assert art.convertir(5, 'pza', 'm') is None

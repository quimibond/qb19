# -*- coding: utf-8 -*-
"""Pytest puro de la conversión unidad del cliente → unidad del producto."""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(__file__))
from release_loader import load  # noqa: E402

units = load('units')


def test_same_dimension():
    assert units.convert(100, 'MT', 'length') == 100
    assert units.convert(100, 'LY', 'length') == pytest.approx(91.44)
    assert units.convert(1, 'LY', 'length', product_factor=1000) == pytest.approx(0.0009144)


def test_length_to_kg_needs_yield():
    # 6.54 m/kg: 654 m son 100 kg.
    assert units.convert(654, 'MT', 'weight', yield_m_kg=6.54) == pytest.approx(100)
    assert units.convert(654, 'MT', 'weight') is None


def test_yards_to_kg():
    assert units.convert(1000, 'LY', 'weight', yield_m_kg=9.144) == pytest.approx(100)


def test_area_uses_width():
    assert units.convert(165, 'M2', 'length', width_m=1.65) == pytest.approx(100)
    assert units.convert(165, 'M2', 'length') is None
    assert units.convert(100, 'MT', 'area', width_m=1.6) == pytest.approx(160)


def test_fixed_factor_wins_and_unknown_unit_is_none():
    assert units.convert(10, 'CAJA', 'length', fixed_factor=50) == 500
    assert units.convert(10, 'CAJA', 'length') is None

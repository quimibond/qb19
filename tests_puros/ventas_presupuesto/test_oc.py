# -*- coding: utf-8 -*-
"""Pytest puro de la OC del pronóstico (19.0.3.15.0): los valores que hay hoy en producción."""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(__file__))
from release_loader import load  # noqa: E402

oc = load('oc')


@pytest.mark.parametrize('texto, esperado', [
    ('12033', ('12033', '')),
    ('6700/PV17173', ('6700', 'PV17173')),
    ('6600319/', ('6600319', '')),
    ('PO/14792', ('14792', '')),
    ('PO 70152', ('70152', '')),
    ('4500575808/PV17190', ('4500575808', 'PV17190')),
    ('SMX26-J109/PV17045', ('SMX26-J109', 'PV17045')),
    ('PO', ('', '')),
    ('No.parte cliente', ('', '')),
    ('PV17123', ('', 'PV17123')),
    ('', ('', '')),
    (None, ('', '')),
])
def test_separa_oc(texto, esperado):
    assert oc.separa_oc(texto) == esperado

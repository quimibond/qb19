# -*- coding: utf-8 -*-
"""Carga los módulos puros de ``release/`` sin importar el addon (que
necesita Odoo). Solo lo usan las pruebas de pytest puro."""
import importlib.util
import os

_RELEASE_DIR = os.path.join(
    os.path.dirname(__file__), '..', '..', 'addons', 'quimibond_ventas_presupuesto',
    'release')


def load(name):
    path = os.path.join(_RELEASE_DIR, '%s.py' % name)
    spec = importlib.util.spec_from_file_location('qb_release_%s' % name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

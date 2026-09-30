# -*- coding: utf-8 -*-
"""57.26.0 (entrega 5, `e5-herencias-propias` 5/9, A-008): el nivel del
indicador (tablero de Dirección) va en la ficha del indicador y el banner
«formato controlado» en la ficha de la revisión por la dirección, cada uno
en su vista base. Mismo resultado en pantalla.

Se borran de la base las herencias propias integradas, antes de cargar los
XML (``migrations/herencias_propias.py``). Solo metadatos de vista.

Esperado en producción (MCP, solo lectura, 2026-09-30):
sgi_indicator_view_form_level (id 15065) y sgi_format_banner_mgmt_review
(14740); ninguna otra vista las hereda.
"""
import importlib.util
import os

_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'herencias_propias.py')
_spec = importlib.util.spec_from_file_location('quimibond_sgi_herencias_57_26_0', _path)
herencias = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(herencias)

STALE_VIEWS = (
    'sgi_indicator_view_form_level',
    'sgi_format_banner_mgmt_review',
)


def migrate(cr, version):
    if not version:
        return
    herencias.borrar_herencias(cr, '57.26.0', STALE_VIEWS)

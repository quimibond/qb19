# -*- coding: utf-8 -*-
"""57.24.0 (entrega 5, `e5-herencias-propias` 3/9, A-008): la trayectoria del
indicador (pestaña «Trayectoria», meta final, rango y fecha de arranque) y
el rango de la medición van en su vista base (`sgi_indicator_views.xml`).
Sale `views/sgi_indicator_trajectory_views.xml`. Mismo resultado en
pantalla.

Se borran de la base las herencias propias integradas, antes de cargar los
XML (``migrations/herencias_propias.py``). Solo metadatos de vista.

Esperado en producción (MCP, solo lectura, 2026-09-30):
sgi_indicator_view_form_trajectory (id 15034) y
sgi_measure_view_form_trajectory (15035); ninguna otra vista las hereda.
"""
import importlib.util
import os

_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'herencias_propias.py')
_spec = importlib.util.spec_from_file_location('quimibond_sgi_herencias_57_24_0', _path)
herencias = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(herencias)

STALE_VIEWS = (
    'sgi_indicator_view_form_trajectory',
    'sgi_measure_view_form_trajectory',
)


def migrate(cr, version):
    if not version:
        return
    herencias.borrar_herencias(cr, '57.24.0', STALE_VIEWS)

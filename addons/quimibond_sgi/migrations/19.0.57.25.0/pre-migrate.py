# -*- coding: utf-8 -*-
"""57.25.0 (entrega 5, `e5-herencias-propias` 4/9, A-008): el plan de acción de
la medición roja y la ventana de la medición (ficha y lista de mediciones,
ficha del indicador) van en `sgi_indicator_views.xml`, y «Validar mediciones
del periodo» en la ficha de la revisión por la dirección. Sale
`views/sgi_indicator_plan_views.xml`. Mismo resultado en pantalla.

Se borran de la base las herencias propias integradas, antes de cargar los
XML (``migrations/herencias_propias.py``). Solo metadatos de vista.

Esperado en producción (MCP, solo lectura, 2026-09-30):
sgi_measure_view_form_plan (id 15030), sgi_measure_view_list_plan (15031),
sgi_indicator_view_form_window (15032) y
sgi_management_review_view_form_validate (15033); ninguna otra vista las
hereda.
"""
import importlib.util
import os

_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'herencias_propias.py')
_spec = importlib.util.spec_from_file_location('quimibond_sgi_herencias_57_25_0', _path)
herencias = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(herencias)

STALE_VIEWS = (
    'sgi_measure_view_form_plan',
    'sgi_measure_view_list_plan',
    'sgi_indicator_view_form_window',
    'sgi_management_review_view_form_validate',
)


def migrate(cr, version):
    if not version:
        return
    herencias.borrar_herencias(cr, '57.25.0', STALE_VIEWS)

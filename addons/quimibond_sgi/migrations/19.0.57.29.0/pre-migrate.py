# -*- coding: utf-8 -*-
"""57.29.0 (entrega 5, `e5-herencias-propias` 8/9, A-008): la solicitud de
mantenimiento de origen en la NC, los acuses y la orden de producción en el
albarán y el plan de control en el producto van en su vista base.
`sgi_links_views.xml` ya no hereda vistas propias. Mismo resultado en
pantalla.

Se borran de la base las herencias propias integradas, antes de cargar los
XML (``migrations/herencias_propias.py``). Solo metadatos de vista.

Esperado en producción (MCP, solo lectura, 2026-09-30):
sgi_quality_alert_view_form_links (id 15099),
sgi_stock_picking_view_form_links (15102) y
sgi_product_template_view_form_links (15097); ninguna otra vista las hereda.
"""
import importlib.util
import os

_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'herencias_propias.py')
_spec = importlib.util.spec_from_file_location('quimibond_sgi_herencias_57_29_0', _path)
herencias = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(herencias)

STALE_VIEWS = (
    'sgi_quality_alert_view_form_links',
    'sgi_stock_picking_view_form_links',
    'sgi_product_template_view_form_links',
)


def migrate(cr, version):
    if not version:
        return
    herencias.borrar_herencias(cr, '57.29.0', STALE_VIEWS)

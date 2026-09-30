# -*- coding: utf-8 -*-
"""57.27.0 (entrega 5, `e5-herencias-propias` 6/9, A-008): la tarea del
desarrollo en AMEF, plan de control y PPAP, «Proveedor crítico» en la
pestaña SGI del proveedor y «Categorías de proveedores críticos» en Ajustes
van en su vista base. `sgi_links_views.xml` queda con sus herencias sobre
vistas de OTROS módulos (y las tres sobre NC, albarán y producto que salen
en 8/9). Mismo resultado en pantalla.

Se borran de la base las herencias propias integradas, antes de cargar los
XML (``migrations/herencias_propias.py``). Solo metadatos de vista.

Esperado en producción (MCP, solo lectura, 2026-09-30):
sgi_fmea_view_form_links (id 15092), sgi_control_plan_view_form_links
(15093), sgi_ppap_view_form_links (15094), sgi_res_partner_view_form_links
(15104) y sgi_settings_view_form_links (15105); ninguna otra vista las
hereda.
"""
import importlib.util
import os

_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'herencias_propias.py')
_spec = importlib.util.spec_from_file_location('quimibond_sgi_herencias_57_27_0', _path)
herencias = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(herencias)

STALE_VIEWS = (
    'sgi_fmea_view_form_links',
    'sgi_control_plan_view_form_links',
    'sgi_ppap_view_form_links',
    'sgi_res_partner_view_form_links',
    'sgi_settings_view_form_links',
)


def migrate(cr, version):
    if not version:
        return
    herencias.borrar_herencias(cr, '57.27.0', STALE_VIEWS)

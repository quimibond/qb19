# -*- coding: utf-8 -*-
"""57.28.0 (entrega 5, `e5-herencias-propias` 7/9, A-008): la NC a proveedor
(botón «Enviar al proveedor» y pestaña «Proveedor») va en la ficha de NC;
cliente o proveedor auditado en la auditoría; «Programa sugerido» y la
columna de proveedor en el programa de auditorías; los botones de firma en
albarán y producto; «Exige firmado» y encuesta en el entregable. Todo en su
vista base; `sgi_supplier_audit_sign_views.xml` queda con el asistente de
firma y las herencias sobre vistas de otros módulos. Mismo resultado en
pantalla.

Se borran de la base las herencias propias integradas, antes de cargar los
XML (``migrations/herencias_propias.py``). Solo metadatos de vista.

Esperado en producción (MCP, solo lectura, 2026-09-30):
sgi_quality_alert_view_form_supplier (id 15070), sgi_audit_view_form_pr6
(15071), sgi_audit_program_view_form_pr6 (15072),
sgi_stock_picking_view_form_sign (15077),
sgi_product_template_view_form_sign (15079) y sgi_deliverable_view_form_pr6
(15080); ninguna otra vista las hereda.
"""
import importlib.util
import os

_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'herencias_propias.py')
_spec = importlib.util.spec_from_file_location('quimibond_sgi_herencias_57_28_0', _path)
herencias = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(herencias)

STALE_VIEWS = (
    'sgi_quality_alert_view_form_supplier',
    'sgi_audit_view_form_pr6',
    'sgi_audit_program_view_form_pr6',
    'sgi_stock_picking_view_form_sign',
    'sgi_product_template_view_form_sign',
    'sgi_deliverable_view_form_pr6',
)


def migrate(cr, version):
    if not version:
        return
    herencias.borrar_herencias(cr, '57.28.0', STALE_VIEWS)

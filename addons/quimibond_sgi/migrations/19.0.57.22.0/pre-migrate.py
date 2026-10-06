# -*- coding: utf-8 -*-
"""57.22.0 (entrega 5, `e5-herencias-propias`, A-008): el pie «formato
controlado» de los reportes de NC, CoA y acta de revisión va dentro de cada
plantilla. Se borran las 3 herencias QWeb propias que lo agregaban
(``migrations/herencias_propias.py``). Solo metadatos de vista.

Esperado en producción (MCP, solo lectura, 2026-09-30): report_nc_document_sgi
(id 14745), report_coa_document_sgi (14746) y report_mgmt_review_document_sgi
(14747); ninguna otra vista las hereda.
"""
import importlib.util
import os

_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'herencias_propias.py')
_spec = importlib.util.spec_from_file_location('quimibond_sgi_herencias_57_22', _path)
herencias = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(herencias)

STALE_VIEWS = (
    'report_nc_document_sgi',
    'report_coa_document_sgi',
    'report_mgmt_review_document_sgi',
)


def migrate(cr, version):
    if not version:
        return
    herencias.borrar_herencias(cr, '57.22.0', STALE_VIEWS)

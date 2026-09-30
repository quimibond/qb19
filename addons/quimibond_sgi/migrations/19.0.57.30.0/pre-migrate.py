# -*- coding: utf-8 -*-
"""57.30.0 (entrega 5, `e5-herencias-propias` 9/9, A-008): la pestaña «SGI» del
puesto (roles, familia, vacante y los botones de «Mi procedimiento») y el
botón «Actividades SGI» van en la herencia del puesto
(`sgi_integration_views.xml`); el botón «Ver su procedimiento» en la del
empleado; el filtro «Mis actividades» con todos los roles del puesto y su
familia en la búsqueda de actividades; «Ir a hacerlo», «Ver registros
recientes», «Ver instructivo» y «Registrar hallazgo» en el encabezado de la
ficha de actividad. Sale `views/sgi_structure_views.xml`. **Con esto el SGI
ya no hereda ninguna vista propia** (de 32 herencias propias a 0; sobre
vistas del SGI quedan solo las de los satélites). Mismo resultado en
pantalla.

Se borran de la base las herencias propias integradas, antes de cargar los
XML (``migrations/herencias_propias.py``). Solo metadatos de vista.

Esperado en producción (MCP, solo lectura, 2026-09-30):
sgi_hr_job_view_form_roles (id 15007) y su hija
sgi_hr_job_view_form_my_procedure (15039),
sgi_hr_employee_view_form_my_procedure (15040),
sgi_process_activity_view_search_my_procedure (15041) y
sgi_process_activity_view_form_structure (15044). La vista de
quimibond_sgi_knowledge (15073) hereda la ficha de actividad (el padre,
14759), no la herencia que se borra: no se toca.
"""
import importlib.util
import os

_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'herencias_propias.py')
_spec = importlib.util.spec_from_file_location('quimibond_sgi_herencias_57_30_0', _path)
herencias = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(herencias)

STALE_VIEWS = (
    'sgi_hr_job_view_form_roles',
    'sgi_hr_job_view_form_my_procedure',
    'sgi_hr_employee_view_form_my_procedure',
    'sgi_process_activity_view_search_my_procedure',
    'sgi_process_activity_view_form_structure',
)


def migrate(cr, version):
    if not version:
        return
    herencias.borrar_herencias(cr, '57.30.0', STALE_VIEWS)

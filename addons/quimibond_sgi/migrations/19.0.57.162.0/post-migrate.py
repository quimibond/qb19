# -*- coding: utf-8 -*-
"""19.0.57.162.0 — MIID y P-G01 con menos texto.

Jose 2026-10-20: «sigo viendo mucho texto». El texto de cada sección queda en
una o dos frases y los renglones largos se parten en uno por idea (las
responsabilidades del P-G01, una por actividad de E2; las reglas del sistema,
15 en vez de 12).

En una base existente este paso, solo en las secciones que nadie editó a mano:

1. Pone el texto nuevo de las secciones.
2. Pone los renglones de la siembra: actualiza los sembrados y quita los que
   ya no vienen (los nuevos los crea la actualización).
3. Pone el formato (títulos de columna) y corre la comparación del MIID y
   del P-G01.

Idempotente; cada paso deja su línea en el log."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

TAG = "57.162.0"
REASON = "el texto corto de la pantalla por capítulos"
BODY_XMLIDS = (
    'sgi_miid_section_01_identificacion',
    'sgi_miid_section_04_objeto',
    'sgi_miid_section_05_alcance',
    'sgi_miid_section_06_no_aplica',
    'sgi_miid_section_07_control_manual',
    'sgi_miid_section_08_referencias',
    'sgi_miid_section_09_terminos',
    'sgi_miid_section_14_procesos',
    'sgi_miid_section_17_politica',
    'sgi_miid_section_21_objetivos',
    'sgi_miid_section_18_roles',
    'sgi_miid_section_42_correspondencia',
    'sgi_miid_section_52_reglas',
    'sgi_miid_section_39_nc',
    'sgi_miid_section_31_control_ambiental_sst',
    'sgi_miid_section_53_documentada_cap',
    'sgi_miid_section_28_informacion',
    'sgi_miid_section_59_transicion_cap',
    'sgi_miid_section_pg01_01_identificacion',
    'sgi_miid_section_pg01_02_proposito',
    'sgi_miid_section_pg01_03_alcance',
    'sgi_miid_section_pg01_04_responsabilidades',
    'sgi_miid_section_pg01_06_tipos',
    'sgi_miid_section_pg01_07_elaboracion',
    'sgi_miid_section_pg01_08_aprobacion',
    'sgi_miid_section_pg01_09_plazos',
    'sgi_miid_section_pg01_10_difusion',
    'sgi_miid_section_pg01_11_resguardo',
    'sgi_miid_section_pg01_12_rutinas',
    'sgi_miid_section_pg01_13_normativo',
    'sgi_miid_section_pg01_14_historial',
)
SECTION_XMLIDS = (
    'sgi_miid_section_04_objeto',
    'sgi_miid_section_05_alcance',
    'sgi_miid_section_06_no_aplica',
    'sgi_miid_section_07_control_manual',
    'sgi_miid_section_08_referencias',
    'sgi_miid_section_09_terminos',
    'sgi_miid_section_14_procesos',
    'sgi_miid_section_17_politica',
    'sgi_miid_section_21_objetivos',
    'sgi_miid_section_18_roles',
    'sgi_miid_section_42_correspondencia',
    'sgi_miid_section_52_reglas',
    'sgi_miid_section_39_nc',
    'sgi_miid_section_31_control_ambiental_sst',
    'sgi_miid_section_53_documentada_cap',
    'sgi_miid_section_28_informacion',
    'sgi_miid_section_59_transicion_cap',
    'sgi_miid_section_43_anteriores',
    'sgi_miid_section_45_anexos',
    'sgi_miid_section_46_historial',
    'sgi_miid_section_pg01_03_alcance',
    'sgi_miid_section_pg01_04_responsabilidades',
    'sgi_miid_section_pg01_06_tipos',
    'sgi_miid_section_pg01_07_elaboracion',
    'sgi_miid_section_pg01_08_aprobacion',
    'sgi_miid_section_pg01_09_plazos',
    'sgi_miid_section_pg01_10_difusion',
    'sgi_miid_section_pg01_11_resguardo',
    'sgi_miid_section_pg01_12_rutinas',
    'sgi_miid_section_pg01_14_historial',
)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    Section = env['sgi.miid.section']
    bodies = Section._sgi_seed_update(BODY_XMLIDS, {}, TAG, reason=REASON)
    _logger.info("SGI %s: texto de las secciones: %s", TAG, bodies)
    items = Section._sgi_seed_items_update(SECTION_XMLIDS, TAG)
    _logger.info("SGI %s: renglones de las secciones: %s", TAG, items)
    formats = Section._sgi_seed_format_update(SECTION_XMLIDS, TAG)
    _logger.info("SGI %s: formato de las secciones: %s", TAG, formats)
    for kind in ('miid', 'pg01'):
        miid = env['sgi.miid']._sgi_get(kind=kind)
        if miid:
            miid._sgi_check()
            _logger.info("SGI %s: %s de %s: %s.", TAG, kind, miid.company_id.name, miid.state)

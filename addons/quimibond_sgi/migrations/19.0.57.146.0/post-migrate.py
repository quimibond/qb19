# -*- coding: utf-8 -*-
"""19.0.57.146.0 — El MIID deja de ser un manual del Dropbox.

Dirección General 2026-10-13: «el MIID está como en el Dropbox». El manual
generado queda con 29 secciones (texto fijo mínimo y 14 bloques vivos) en
vez de las 46 que describían en prosa lo que el sistema ya muestra, y el
P-G01 (control de información documentada) vive en su capítulo 6.

La siembra es ``noupdate``: en una base existente el módulo crea las 10
secciones nuevas, pero no reacomoda ni archiva las que ya están. Eso lo
hace este paso, siempre respetando lo editado a mano:

1. Archiva las 27 secciones narrativas (su texto queda en la sección
   archivada; lo que decían vive en las actividades, en la correspondencia
   por cláusula y en los manuales de Conocimiento).
2. Reacomoda las 19 que siguen: orden, numeral, título, nivel y bloque de
   la siembra (``_sgi_seed_layout_update``; cambiar la estructura no es
   editar el texto).
3. Pone el texto nuevo en las 14 cuyo texto cambió, solo si nadie las
   editó (``_sgi_seed_update``); las notas de «Por confirmar» no cambian.

Cada paso es idempotente y deja su línea en el log."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

TAG = "57.146.0"
REASON = "el manual generado (texto fijo mínimo; lo vivo lo ponen los bloques)"
ARCHIVE_REASON = ("el manual generado ya no describe en prosa lo que el sistema muestra; "
                  "lo que decía esta sección vive en las actividades, en la correspondencia "
                  "por cláusula (5.1) y en los manuales de Conocimiento")

ARCHIVED_XMLIDS = (
    'sgi_miid_section_02_que_cambia',
    'sgi_miid_section_10_contexto_cap',
    'sgi_miid_section_11_contexto',
    'sgi_miid_section_12_partes',
    'sgi_miid_section_13_alcance_43',
    'sgi_miid_section_15_liderazgo_cap',
    'sgi_miid_section_16_liderazgo',
    'sgi_miid_section_19_planificacion_cap',
    'sgi_miid_section_20_riesgos',
    'sgi_miid_section_22_cambios',
    'sgi_miid_section_23_apoyo_cap',
    'sgi_miid_section_24_recursos',
    'sgi_miid_section_25_competencia',
    'sgi_miid_section_26_conciencia',
    'sgi_miid_section_27_comunicacion',
    'sgi_miid_section_29_operacion_cap',
    'sgi_miid_section_30_control_operacional',
    'sgi_miid_section_32_emergencias',
    'sgi_miid_section_33_evaluacion_cap',
    'sgi_miid_section_34_seguimiento',
    'sgi_miid_section_35_auditoria',
    'sgi_miid_section_36_revision',
    'sgi_miid_section_37_mejora_cap',
    'sgi_miid_section_38_generalidades',
    'sgi_miid_section_40_mejora_continua',
    'sgi_miid_section_41_correspondencia_cap',
    'sgi_miid_section_44_anexos_cap',
)

# Las 19 que siguen en el manual (en el orden nuevo).
KEPT_XMLIDS = (
    'sgi_miid_section_01_identificacion',
    'sgi_miid_section_03_objeto_cap',
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
    'sgi_miid_section_39_nc',
    'sgi_miid_section_31_control_ambiental_sst',
    'sgi_miid_section_28_informacion',
    'sgi_miid_section_43_anteriores',
    'sgi_miid_section_45_anexos',
    'sgi_miid_section_46_historial',
)

# Las que cambian de texto (05, 06, 45 y 46 siguen igual; 03 solo de título).
BODY_XMLIDS = (
    'sgi_miid_section_01_identificacion',
    'sgi_miid_section_04_objeto',
    'sgi_miid_section_07_control_manual',
    'sgi_miid_section_08_referencias',
    'sgi_miid_section_09_terminos',
    'sgi_miid_section_14_procesos',
    'sgi_miid_section_17_politica',
    'sgi_miid_section_21_objetivos',
    'sgi_miid_section_18_roles',
    'sgi_miid_section_42_correspondencia',
    'sgi_miid_section_39_nc',
    'sgi_miid_section_31_control_ambiental_sst',
    'sgi_miid_section_28_informacion',
    'sgi_miid_section_43_anteriores',
)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    Section = env['sgi.miid.section']
    archived = Section._sgi_seed_archive(ARCHIVED_XMLIDS, TAG, ARCHIVE_REASON)
    _logger.info("SGI %s: secciones del MIID archivadas: %d.", TAG, len(archived))
    layout = Section._sgi_seed_layout_update(KEPT_XMLIDS, TAG)
    _logger.info("SGI %s: secciones del MIID reacomodadas: %s", TAG, layout)
    bodies = Section._sgi_seed_update(BODY_XMLIDS, {}, TAG, reason=REASON)
    _logger.info("SGI %s: secciones del MIID con texto nuevo: %s", TAG, bodies)

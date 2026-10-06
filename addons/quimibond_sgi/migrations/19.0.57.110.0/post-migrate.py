# -*- coding: utf-8 -*-
"""19.0.57.110.0 — MIID completado con lo que traía la revisión 02.

La siembra de las secciones es noupdate. Este paso pone el texto nuevo del
archivo de datos solo en las secciones que nadie ha editado (sin el mensaje
«Texto de la sección editado.») y corrige las notas de «Por confirmar» de
1.2, 1.3 y 12.1 solo si siguen idénticas a las sembradas en 57.105.0. Lo que
el Jefe MAST ya corrigió no se toca: queda un aviso en el chatter de la
sección. No quita ningún «Por confirmar» (eso lo decide Dirección) ni crea
revisión del MIID."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

BODY_XMLIDS = (
    'sgi_miid_section_11_contexto',
    'sgi_miid_section_12_partes',
    'sgi_miid_section_20_riesgos',
    'sgi_miid_section_22_cambios',
    'sgi_miid_section_24_recursos',
    'sgi_miid_section_26_conciencia',
    'sgi_miid_section_27_comunicacion',
    'sgi_miid_section_30_control_operacional',
    'sgi_miid_section_32_emergencias',
    'sgi_miid_section_34_seguimiento',
    'sgi_miid_section_39_nc',
)

# Notas sembradas en 57.105.0: solo se reemplazan si siguen así.
OLD_NOTES = {
    'sgi_miid_section_05_alcance': "Alcance de ISO 45001: confirmar la redacción contra el certificado o el "
                                   "informe de la auditoría de septiembre de 2026.",
    'sgi_miid_section_06_no_aplica': "Justificación de 8.4.1 b) y 8.5.1 f): es redacción propuesta; la "
                                     "revisión 02 solo declara las exclusiones.",
    'sgi_miid_section_45_anexos': "Situación de cada anexo: el Jefe MAST y SGI y la Dirección deben confirmar "
                                  "qué anexos se sustituyen y cuáles se conservan.",
}


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    result = env['sgi.miid.section']._sgi_seed_update(BODY_XMLIDS, OLD_NOTES, "57.110.0")
    _logger.info("SGI 57.110.0: secciones del MIID: %s", result)

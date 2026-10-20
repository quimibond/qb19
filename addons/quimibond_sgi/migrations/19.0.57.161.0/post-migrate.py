# -*- coding: utf-8 -*-
"""19.0.57.161.0 — MIID y P-G01 por capítulos, con renglones y menús.

Jose 2026-10-20: «no se entiende ni se puede leer bien y no usa las mejores
prácticas de Odoo; no quiero texto plano». La pantalla del MIID y del P-G01
es una pestaña por capítulo con listas nativas; las viñetas y tablas que se
escribían en el texto son renglones (``sgi.miid.item``) y las rutas escritas
son menús que se abren.

Los renglones nuevos los crea la actualización (registros nuevos de la
siembra ``noupdate``). En una base existente este paso:

1. Pone el texto nuevo (sin las viñetas ni las tablas que pasaron a
   renglones, ni las rutas que pasaron a menús) solo en las secciones que
   nadie editó a mano; en las editadas deja un aviso en su historial.
2. Pone el formato de la siembra (menú, tabla o lista y títulos de columna).
3. Corre la comparación del MIID y del P-G01: la huella cambia (los
   renglones entran a ella), así que una revisión con línea base saldrá
   «desactualizada» hasta la próxima solicitud de cambio.

Idempotente; cada paso deja su línea en el log."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

TAG = "57.161.0"
REASON = "los renglones y los menús de la pantalla por capítulos"
BODY_XMLIDS = (
    'sgi_miid_section_05_alcance',
    'sgi_miid_section_06_no_aplica',
    'sgi_miid_section_07_control_manual',
    'sgi_miid_section_08_referencias',
    'sgi_miid_section_09_terminos',
    'sgi_miid_section_14_procesos',
    'sgi_miid_section_17_politica',
    'sgi_miid_section_21_objetivos',
    'sgi_miid_section_18_roles',
    'sgi_miid_section_52_reglas',
    'sgi_miid_section_39_nc',
    'sgi_miid_section_31_control_ambiental_sst',
    'sgi_miid_section_53_documentada_cap',
    'sgi_miid_section_28_informacion',
    'sgi_miid_section_59_transicion_cap',
    'sgi_miid_section_pg01_03_alcance',
    'sgi_miid_section_pg01_04_responsabilidades',
    'sgi_miid_section_pg01_06_tipos',
    'sgi_miid_section_pg01_07_elaboracion',
    'sgi_miid_section_pg01_08_aprobacion',
    'sgi_miid_section_pg01_10_difusion',
    'sgi_miid_section_pg01_11_resguardo',
    'sgi_miid_section_pg01_12_rutinas',
)
FORMAT_XMLIDS = (
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
    for xmlid, result in bodies.items():
        if 'editada a mano' in result:
            section = env.ref('quimibond_sgi.%s' % xmlid)
            section.message_post(body=(
                "%s: esta sección recibió renglones nuevos, pero su texto se editó a mano y se quedó como "
                "estaba. Si el texto repite lo de los renglones, quite lo repetido." % TAG))
    formats = Section._sgi_seed_format_update(FORMAT_XMLIDS, TAG)
    _logger.info("SGI %s: formato de las secciones: %s", TAG, formats)
    for kind in ('miid', 'pg01'):
        miid = env['sgi.miid']._sgi_get(kind=kind)
        if miid:
            miid._sgi_check()
            _logger.info("SGI %s: %s de %s: %s.", TAG, kind, miid.company_id.name, miid.state)

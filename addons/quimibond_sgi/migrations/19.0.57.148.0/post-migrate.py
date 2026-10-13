# -*- coding: utf-8 -*-
"""19.0.57.148.0 — El P-G01 es un documento generado propio.

Dirección General 2026-10-13: «prefiero otra cosa» al capítulo 6 del MIID
(57.146.0). El P-G01 «Elaboración y control de información documentada» se
genera con el mismo motor que el Manual (``sgi.miid`` con ``kind = pg01``);
sus 14 secciones las crea la siembra ``noupdate`` al actualizar.

En una base existente este paso:

1. Archiva las cinco secciones del MIID que detallaban el P-G01 (6.2 a 6.6
   de 57.146.0): su texto vive ahora en las secciones del P-G01.
2. Pone el texto nuevo de «6 Información documentada» (apunta al P-G01) y
   de «1.4 Revisión y control de este Manual» solo si nadie los editó.
3. Crea el registro del P-G01 de la empresa del SGI (lo haría la primera
   apertura) y corre su primera comparación.

Idempotente; cada paso deja su línea en el log."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

TAG = "57.148.0"
ARCHIVED_XMLIDS = (
    'sgi_miid_section_54_doc_elaboracion',
    'sgi_miid_section_55_doc_aprobacion',
    'sgi_miid_section_56_doc_plazos',
    'sgi_miid_section_57_doc_difusion',
    'sgi_miid_section_58_doc_resguardo',
)
BODY_XMLIDS = ('sgi_miid_section_53_documentada_cap', 'sgi_miid_section_07_control_manual')
ARCHIVE_REASON = ("el P-G01 es un documento generado propio (SGI → Sistema → Manual del SGI → Control de "
                  "información documentada (P-G01)); lo que decía esta sección vive ahí")
REASON = "el P-G01 como documento propio (el capítulo 6 del Manual solo lo cita)"


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    Section = env['sgi.miid.section']
    archived = Section._sgi_seed_archive(ARCHIVED_XMLIDS, TAG, ARCHIVE_REASON)
    _logger.info("SGI %s: secciones del MIID archivadas (viven en el P-G01): %d.", TAG, len(archived))
    bodies = Section._sgi_seed_update(BODY_XMLIDS, {}, TAG, reason=REASON)
    _logger.info("SGI %s: secciones del MIID con texto nuevo: %s", TAG, bodies)
    pg01 = env['sgi.miid']._sgi_get(kind='pg01')
    if pg01:
        pg01._sgi_check()
        _logger.info("SGI %s: P-G01 generado de %s: %s.", TAG, pg01.company_id.name, pg01.state)

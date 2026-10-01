# -*- coding: utf-8 -*-
"""57.82.0 (bloque 3 de formularios 1/3, propuesta de formatos aprobada por
Jose el 2026-10-01, §3 y §1): duplicados y datos malos, en el orden de la
propuesta (``documents.document._sgi_formatos_bloque3``, tablas en
``models/sgi_formatos_bloque3.py``).

Cada documento se localiza por id y se verifica su clave (o su clave
anterior) y su empresa; si no coincide, se salta con aviso en el log.
Idempotente. Nada se borra ni se archiva (archivar en Documentos manda a la
papelera, que se vacía sola a los 30 días): las bajas quedan obsoletas y
activas, con su motivo.

Esperado en producción (MCP, solo lectura, 2026-10-01, compañía 1):

1. Ligas que se mueven antes de dar de baja: C1.11 (574) 4023 → 3920
   (conserva 3926); C5.16 (562) pierde 4026 (ya tenía 3902); C4.14 (526)
   pierde 3989 (ya tenía 3999). Ligas nuevas: C2.43 (785) vacío → 3875
   F-P-A28-04; C6.12 (470) 3700 → 3700 + 3698 F-IT-P-A05-01-01; E2.37 (735)
   vacío → 4063 F-IT-P-G03-01-01 (evaluación de auditores, propuesta §1 #8).
2. Bajas (obsoleto + «Baja tramitada», activos): 4026, 3752, 4023, 3725,
   3943, 3989 y 4001. Ninguno está en ``sgi.format.map``.
3. 5152 F-P-V01-04 (reporte de visita lleno): deja de ser controlado. 3359
   sigue archivado como estaba.
4. 4060 «Evaluación luminaria»: F-P-E01-01 → F-P-S01-02 (clave y clave
   anterior) y se liga a E2.31 (729, estudios de higiene y evaluaciones NOM,
   vacío). F-P-E01-01 queda libre; el mapeo 42 (matriz de aspectos
   ambientales) imprime «F-P-E01-01» sin revisión hasta que MAST dé de
   alta la matriz en blanco.
5. Altas como Formulario de Odoo, sin archivo (propuesta §1 #1, #5 y #6):
   F-P-A28-13 «Pronóstico de ventas» (C2, menú Pronósticos), ligado a C2.40
   (751, vacío) y como documento alternativo del mapeo 9
   (``sgi.sales.budget``, que ya imprimía la clave); F-P-A28-11 «Encuesta de
   satisfacción del cliente» (E2, menú SGI → Dirección → Satisfacción del
   cliente), ligado a E2.12 (703, vacío). Ninguna de las dos claves existe
   hoy (ni como clave anterior).

Marca «cambió» el procedimiento de C1, C2, C4, C5, C6 y E2 (cambio de
formatos en actividades: revisión documental normal).
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    result = env['documents.document']._sgi_formatos_bloque3()
    _logger.info("SGI 57.82.0 (bloque 3): bajas %s; ligas %s; ya no controlados %s; claves "
                 "corregidas %s; altas %s.", sorted(result['merges']), result['links'],
                 sorted(result['uncontrol']), result['recode'], result['forms'])

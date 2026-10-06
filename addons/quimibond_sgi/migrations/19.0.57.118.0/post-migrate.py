# -*- coding: utf-8 -*-
"""19.0.57.118.0 — Proyecto único de desarrollo (C1, bloque 2; decisión 3 de
Jose, 2026-10-06). La bandera «desarrollo de producto» ya no sale del nombre:

- marca ``sgi_is_ft`` en los proyectos cuyo nombre empieza con FT- (77 en
  producción el 2026-10-06), en las plantillas «PLANTILLA - Diseño y
  Desarrollo …» (480 y 481) y en el proyecto de análisis «ANALISIS DE
  PROYECTO …» (490);
- separa del nombre viejo el folio (unificado a guiones, FT-043-2024), el
  producto pedido y la revisión («REV3»), y vuelve a armar el nombre;
- cuando el proyecto no tenía cliente, lo toma de la etapa cuyo nombre era
  el cliente (SHAWMUT, BOWEN…);
- reescribe los filtros de medición de entregables y actividades que decían
  ``name =like 'FT-%'`` para que usen la bandera.

Idempotente: solo escribe lo que falta. No borra las etapas por cliente (eso
es dato de producción, sección 7 del brief)."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    Project = env['project.project']
    touched = Project._sgi_dev_migrate_legacy()
    domains = Project._sgi_dev_migrate_measure_domains()
    _logger.info("SGI 57.118.0: %d proyecto(s) marcados como desarrollo de producto; %d filtro(s) de medición "
                 "reescritos.", len(touched), domains)

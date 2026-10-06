# -*- coding: utf-8 -*-
"""19.0.57.120.2 — Vuelve a correr la migración de datos de C1 que 57.118.0 no
dejó en la base de producción (2026-10-06 23:22 UTC): los 77 proyectos FT-
quedaron sin bandera, folio, cliente ni origen, y las plantillas 480 y 481 y
el proyecto 490 quedaron renombrados «Análisis» solo en en_US.

Qué hace, en orden, y deja en el log con el prefijo «SGI 57.120.2»:

1. Tabla etapa → cliente y etapa de avance destino.
2. Marca los FT-, plantillas y análisis; separa folio, producto y revisión;
   cliente desde la etapa; QUIMIBOND = origen interno; nombres en todos los
   idiomas (restaura los de plantillas y análisis).
3. Filtros de medición: bandera y sin plantillas.
4. Etapas: Cancelada → Cerrado sin producto, Hecha → Liberado, ANALISIS DE
   PROYECTOS → Análisis, lo demás → Muestra; archiva las etapas viejas que
   quedan sin proyectos.
5. Puesto autorizador de laboratorio (hr.job «Coordinador de Laboratorio»).
6. Modelos nuevos expuestos al MCP.

Idempotente: se puede volver a correr. Hace flush al final de cada paso para
que un error detenga el update y se vea, en lugar de perderse."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    Project = env['project.project']
    for row in Project._sgi_dev_migration_preview():
        _logger.info("SGI 57.120.2: etapa «%s» → cliente %s [%s] → etapa «%s»; proyectos sin cliente: %s",
                     row['stage'].name, row['partner'].display_name or '—', row['rule'], row['target'],
                     ", ".join(row['projects'].mapped('name')) or '—')
    touched = Project._sgi_dev_migrate_legacy()
    marked = touched.filtered('sgi_is_ft')
    _logger.info("SGI 57.120.2: %d proyecto(s) marcados como desarrollo de producto (%d con folio FT, "
                 "%d con cliente, %d de origen interno, %d plantillas).",
                 len(marked), len(marked.filtered('sgi_ft_folio')), len(marked.filtered('partner_id')),
                 len(marked.filtered(lambda p: p.sgi_dev_origin == 'interno')),
                 len(marked.filtered('is_template')))
    domains = Project._sgi_dev_migrate_measure_domains()
    _logger.info("SGI 57.120.2: %d filtro(s) de medición reescritos.", domains)
    moved, archived = Project._sgi_dev_migrate_stages()
    _logger.info("SGI 57.120.2: %d proyecto(s) pasaron a etapas de avance; %d etapa(s) viejas archivadas: %s.",
                 len(moved), len(archived), ", ".join(archived.mapped('name')) or '—')
    job = env['sgi.dev.lab.request']._sgi_dev_set_lab_job_default()
    _logger.info("SGI 57.120.2: puesto autorizador de pruebas de laboratorio: %s.",
                 job.display_name if job else "ya configurado o sin puesto «Coordinador de Laboratorio»")
    models = Project._sgi_dev_enable_mcp_models()
    _logger.info("SGI 57.120.2: %d modelo(s) expuestos al MCP: %s.", len(models), ", ".join(models.mapped('model')) or '—')

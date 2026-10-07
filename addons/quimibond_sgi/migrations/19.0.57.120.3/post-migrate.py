# -*- coding: utf-8 -*-
"""19.0.57.120.3 — Tercera corrida de la migración de datos de C1, ahora en el
idioma de la empresa y buscando los nombres en todos los idiomas instalados.

Qué pasó en 57.118.0 y 57.120.2 (producción, 2026-10-06 23:22 y 2026-10-07
00:08 UTC): la migración corría sin idioma en el contexto, así que
``search([('name', '=ilike', 'FT-%')])`` miraba la columna en_US. Los 75
proyectos se crearon como «Análisis de proyecto …» y se renombraron «FT-…» en
es_MX, el idioma de los usuarios; en en_US conservan el nombre original. Lo
mismo con los nombres de las etapas viejas («Cancelada», «ANALISIS DE
PROYECTOS») y con el puesto «Coordinador de Laboratorio y MP». Por eso 57.120.2
movió a Muestra los 3 proyectos que ya tenían bandera y no tocó los demás.

Qué hace, en orden, y deja en el log con el prefijo «SGI 57.120.3»:

1. Tabla etapa → cliente y etapa de avance destino.
2. Marca los FT-, plantillas y análisis; separa folio, producto y revisión;
   cliente desde la etapa; QUIMIBOND = origen interno; un solo nombre en todos
   los idiomas.
3. Filtros de medición: bandera y sin plantillas.
4. Etapas: Cancelada → Cerrado sin producto, Hecha → Liberado, ANALISIS DE
   PROYECTOS → Análisis, lo demás → Muestra; lo que quedó en Muestra sin folio
   ni nombre FT- regresa a Análisis; archiva las etapas viejas vacías.
5. Puesto autorizador de laboratorio (hr.job «Coordinador de Laboratorio»).
6. Modelos nuevos expuestos al MCP.

Idempotente: se puede volver a correr."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    lang = env.company.partner_id.lang or 'en_US'
    env = api.Environment(cr, SUPERUSER_ID, {'lang': lang})
    Project = env['project.project']
    _logger.info("SGI 57.120.3: idioma de la migración %s; idiomas instalados %s; proyectos FT- encontrados: %d.",
                 lang, ", ".join(Project._sgi_dev_langs()), len(Project._sgi_dev_legacy_ft_projects()))
    for row in Project._sgi_dev_migration_preview():
        _logger.info("SGI 57.120.3: etapa «%s» → cliente %s [%s] → etapa «%s»; proyectos sin cliente: %s",
                     row['stage'].name, row['partner'].display_name or '—', row['rule'], row['target'],
                     ", ".join(row['projects'].mapped('name')) or '—')
    touched = Project._sgi_dev_migrate_legacy()
    marked = touched.filtered('sgi_is_ft')
    _logger.info("SGI 57.120.3: %d proyecto(s) marcados como desarrollo de producto (%d con folio FT, "
                 "%d con cliente, %d de origen interno, %d plantillas).",
                 len(marked), len(marked.filtered('sgi_ft_folio')), len(marked.filtered('partner_id')),
                 len(marked.filtered(lambda p: p.sgi_dev_origin == 'interno')),
                 len(marked.filtered('is_template')))
    domains = Project._sgi_dev_migrate_measure_domains()
    _logger.info("SGI 57.120.3: %d filtro(s) de medición reescritos.", domains)
    moved, archived = Project._sgi_dev_migrate_stages()
    _logger.info("SGI 57.120.3: %d proyecto(s) pasaron a etapas de avance; %d etapa(s) viejas archivadas: %s.",
                 len(moved), len(archived), ", ".join(archived.mapped('name')) or '—')
    job = env['sgi.dev.lab.request']._sgi_dev_set_lab_job_default()
    _logger.info("SGI 57.120.3: puesto autorizador de pruebas de laboratorio: %s.",
                 job.display_name if job else "ya configurado o sin puesto «Coordinador de Laboratorio»")
    models = Project._sgi_dev_enable_mcp_models()
    _logger.info("SGI 57.120.3: %d modelo(s) expuestos al MCP: %s.", len(models), ", ".join(models.mapped('model')) or '—')

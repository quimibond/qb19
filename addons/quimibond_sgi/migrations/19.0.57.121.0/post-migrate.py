# -*- coding: utf-8 -*-
"""19.0.57.121.0 — Las fichas de C1 se miden con el proyecto de desarrollo.

1. Fechas de análisis y de aprobación de los proyectos que ya las tenían,
   tomadas del seguimiento del chatter (lo que no tiene rastro queda vacío).
2. Entregables de C1 re-apuntados por código (C1-ANALISIS, C1-AMEF, C1-PLAN,
   C1-PRUEBAS, C1-RESPUESTA, C1-APROBADO) y C1-MP-ESPERA creado y ligado a
   C1.08. Las actividades que se miden con ellos vuelven a copiar modelo,
   filtro, fecha y usuario.

No toca nombre, pasos, criterios, menú, formatos ni roles de las fichas: eso
es lo que Jose capturó en la base. Idempotente. Prefijo en el log: «SGI 57.121.0»."""
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
    filled = Project._sgi_dev_backfill_measure_dates()
    _logger.info("SGI 57.121.0: %d proyecto(s) con fecha de análisis o de aprobación tomada del chatter: %s.",
                 len(filled), ", ".join(filled.mapped('name')) or '—')
    report = Project._sgi_dev_apply_c1_measures()
    _logger.info("SGI 57.121.0: entregables de C1 re-apuntados: %s; creados: %s; sin actividad o modelo: %s.",
                 ", ".join(report['updated']) or '—', ", ".join(report['created']) or '—',
                 ", ".join(report['missing']) or '—')
    for code in ('C1-ANALISIS', 'C1-AMEF', 'C1-PLAN', 'C1-MP-ESPERA', 'C1-PRUEBAS', 'C1-RESPUESTA', 'C1-APROBADO'):
        deliverable = env['sgi.deliverable'].sudo().with_context(active_test=False).search([('code', '=', code)], limit=1)
        if deliverable:
            _logger.info("SGI 57.121.0: %s → %s %s; miden con él: %s", code, deliverable.odoo_model_name,
                         deliverable.measure_domain, ", ".join(deliverable.measured_activity_ids.mapped('number')) or '—')

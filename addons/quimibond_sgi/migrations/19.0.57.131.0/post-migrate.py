# -*- coding: utf-8 -*-
"""19.0.57.131.0 — Correcciones de Jose (2026-10-08, puntos 3 y 4).

- C1.11 se mide con quien dio el dictamen (`verdict_by_id`, `date_verdict`).
- Las entradas de C1.05 que son listas de materiales (C1-BOM, C1-RUTA) se
  ligan con la salida (la cotización) por el artículo del proyecto:
  `project_id.sgi_dev_product_tmpl_ids.bom_ids`.
- Las aprobaciones por botón NO se sincronizan aquí: el satélite
  quimibond_sgi_studio todavía no está en el registro cuando corre esta
  migración (por eso 57.127.0 las dejó «por sincronizar»); lo hace la
  migración 1.0.4 del satélite, que corre después.
No toca datos de origen. Idempotente. Prefijo en el log: «SGI 57.131.0»."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

MATCH_PATH = 'project_id.sgi_dev_product_tmpl_ids.bom_ids'
INPUT_CODES = ('C1-BOM', 'C1-RUTA')


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    lang = env.company.partner_id.lang or 'en_US'
    env = api.Environment(cr, SUPERUSER_ID, {'lang': lang})
    report = env['project.project']._sgi_dev_apply_c1_measures()
    Input = env['sgi.activity.input'].sudo()
    linked = []
    for line in Input.search([('deliverable_id.code', 'in', INPUT_CODES), ('match_path', '=', False)]):
        output = line.activity_id._sgi_output_deliverable()
        if output and output.odoo_model_id.model == 'qb.cotizador.cotizacion':
            line.write({'match_path': MATCH_PATH})
            linked.append('%s/%s' % (line.activity_id.number, line.deliverable_id.code))
    _logger.info("SGI 57.131.0: mediciones %s; entradas ligadas por el artículo del proyecto: %s.",
                 report, linked or 'ninguna')

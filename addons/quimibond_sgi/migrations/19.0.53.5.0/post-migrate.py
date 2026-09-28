# -*- coding: utf-8 -*-
"""53.5.0 (ligas entrada ↔ salida y limpieza de datos, 2026-09-25).
- Los documentos de Odoo que no son del SGI quedan sin «Estado SGI» (había
  2,524 marcados como borrador solo por el default del campo).
- Se borran los faltantes de especificación de procesos archivados o
  actividades inactivas (1,079 colgados de los procesos viejos).
- Las entradas de las actividades que ya tienen campo de liga reciben su
  `match_path` si estaba vacío (C1.04, C1.09, C1.15, C1.16, C1.17, S5.06,
  S1.09, S2.08, C2.34, C4.19).
- Los adjuntos ACUSE-… de las entregas reciben `sgi_picking_id`.
Idempotente; grep "SGI 53.5" en el log."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

# (numeral, modelo de la entrada) → campo de la salida que apunta a la entrada.
MATCH_PATHS = {
    ('C1.04', 'sgi.fmea'): 'sgi_dyd_task_id.sgi_fmea_ids',
    ('C1.09', 'project.task'): 'sgi_dyd_task_id',
    ('C1.15', 'project.task'): 'sgi_dyd_task_id',
    ('C1.16', 'project.task'): 'sgi_dyd_task_id',
    ('C1.17', 'sgi.control.plan'): 'sgi_control_plan_id',
    ('C2.34', 'stock.picking'): 'sgi_picking_id',
    ('C4.19', 'stock.picking'): 'sgi_release_picking_ids',
    ('S1.09', 'approval.request'): 'sgi_approval_request_id',
    ('S2.08', 'stock.picking'): 'sgi_picking_ids',
    ('S5.06', 'maintenance.request'): 'sgi_maintenance_request_id',
}


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})

    cr.execute("""
        UPDATE documents_document SET sgi_state = NULL
         WHERE sgi_is_controlled IS NOT TRUE AND sgi_state IS NOT NULL
    """)
    _logger.info("SGI 53.5: %d documentos fuera del SGI sin estado", cr.rowcount)

    Gap = env['sgi.activity.spec.gap'].sudo()
    gaps = Gap.search(['|', ('activity_id.active', '=', False), ('process_id.active', '=', False)])
    _logger.info("SGI 53.5: %d faltantes de procesos archivados borrados", len(gaps))
    gaps.unlink()

    Input = env['sgi.activity.input'].sudo()
    updated = 0
    for line in Input.search([('match_path', '=', False), ('activity_id.number', 'in',
                                                            [k[0] for k in MATCH_PATHS])]):
        model = line.deliverable_id.odoo_model_id.model
        path = MATCH_PATHS.get((line.activity_id.number, model))
        if path:
            line.match_path = path
            updated += 1
    _logger.info("SGI 53.5: %d entradas con campo de liga", updated)

    cr.execute("""
        UPDATE ir_attachment SET sgi_picking_id = res_id
         WHERE res_model = 'stock.picking' AND res_id IS NOT NULL
           AND sgi_picking_id IS NULL AND upper(name) LIKE 'ACUSE%%'
    """)
    _logger.info("SGI 53.5: %d acuses ligados a su entrega", cr.rowcount)

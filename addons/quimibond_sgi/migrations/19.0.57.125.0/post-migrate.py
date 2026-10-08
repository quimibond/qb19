# -*- coding: utf-8 -*-
"""19.0.57.125.0 — Orden de muestra (C1, bloque F; brief §6.10).

Siembra, solo si están vacíos, los parámetros de la orden de muestra: tipo de
operación «Tejido Desarrollo», ubicación «31 DESARROLLOS», puesto Planeador
de Producción (por nombre, en todos los idiomas) y los 50 m PQ. Los que el
brief no define (rendimiento esperado, mínimo de baño) se quedan vacíos.

Idempotente. Prefijo en el log: «SGI 57.125.0»."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    from odoo.addons.quimibond_sgi.models.sgi_dev_sample import (
        DEFAULT_PQ_M, LOCATION_NAME, PARAM_LOCATION, PARAM_PICKING_TYPE, PARAM_PLANNING_JOB, PARAM_PQ_M,
        PICKING_TYPE_NAME, PLANNING_JOB_NAME)
    Param = env['ir.config_parameter'].sudo()
    Project = env['project.project']
    company = env['res.company'].browse(1).exists() or env.company
    found = {}

    def seed(key, rec):
        if (Param.get_param(key, '') or '').strip():
            found[key] = 'ya tenía valor'
            return
        if rec:
            Param.set_param(key, str(rec.id))
            found[key] = rec.display_name
        else:
            found[key] = 'no encontrado (vacío)'

    picking = Project._sgi_dev_search_langs('stock.picking.type', [
        ('name', '=ilike', PICKING_TYPE_NAME), ('code', '=', 'mrp_operation'), ('company_id', '=', company.id)])[:1]
    seed(PARAM_PICKING_TYPE, picking)
    location = env['stock.location'].sudo().search([('complete_name', 'ilike', LOCATION_NAME),
                                                     ('usage', '=', 'internal'),
                                                     ('company_id', '=', company.id)], limit=1)
    seed(PARAM_LOCATION, location)
    job = Project._sgi_dev_search_langs('hr.job', [('name', '=ilike', PLANNING_JOB_NAME)])[:1]
    seed(PARAM_PLANNING_JOB, job)
    if not (Param.get_param(PARAM_PQ_M, '') or '').strip():
        Param.set_param(PARAM_PQ_M, str(DEFAULT_PQ_M))
    _logger.info("SGI 57.125.0: orden de muestra: %s", "; ".join("%s = %s" % kv for kv in found.items()))

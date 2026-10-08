# -*- coding: utf-8 -*-
"""19.0.57.126.0 — Ruta y fichas de proceso (C1, bloque G; brief §6.7).

Siembra, solo si están vacíos, los puestos que validan cada ficha (Jefe de
Manufactura, Supervisor Tintorería, Supervisor TAC) y Diseño de Producto para
el aviso del tercer pase, por nombre y en todos los idiomas; expone los
modelos nuevos al MCP. El catálogo «Motivo de ajuste de parámetro» nace sin
renglones (el brief no los define: los captura Diseño de Procesos).

Idempotente. Prefijo en el log: «SGI 57.126.0»."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    from odoo.addons.quimibond_sgi.models.sgi_dev_process_sheet import (
        PARAM_PRODUCT_DESIGN_JOB, PARAM_VALIDATOR_JOB, PRODUCT_DESIGN_JOB_NAME, VALIDATOR_JOB_NAMES)
    Param = env['ir.config_parameter'].sudo()
    Project = env['project.project']
    found = []
    pairs = [(PARAM_VALIDATOR_JOB[a], VALIDATOR_JOB_NAMES[a]) for a in ('tejido', 'tintoreria', 'acabado')]
    pairs.append((PARAM_PRODUCT_DESIGN_JOB, PRODUCT_DESIGN_JOB_NAME))
    for key, name in pairs:
        if (Param.get_param(key, '') or '').strip():
            found.append('%s: ya tenía valor' % name)
            continue
        job = Project._sgi_dev_search_langs('hr.job', [('name', '=ilike', name)])[:1]
        if job:
            Param.set_param(key, str(job.id))
        found.append('%s: %s' % (name, job.display_name if job else 'no encontrado (vacío)'))
    Project._sgi_dev_enable_mcp_models()
    _logger.info("SGI 57.126.0: puestos de las fichas de proceso: %s.", "; ".join(found))

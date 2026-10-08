# -*- coding: utf-8 -*-
"""19.0.57.126.0 — Ruta y ficha de proceso de tejido (C1, bloque G; brief §6.7).

Siembra, solo si está vacío, el puesto que valida la ficha de tejido (Jefe de
Manufactura) por nombre y en todos los idiomas, y expone la ficha de tejido al
MCP. El catálogo «Motivo de ajuste de parámetro» nace sin renglones (el brief
no los define: los captura Diseño de Procesos).

Idempotente. Prefijo en el log: «SGI 57.126.0»."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    from odoo.addons.quimibond_sgi.models.sgi_dev_process_sheet import (
        PARAM_VALIDATOR_JOB_TEJIDO, VALIDATOR_JOB_NAME_TEJIDO)
    Param = env['ir.config_parameter'].sudo()
    Project = env['project.project']
    if (Param.get_param(PARAM_VALIDATOR_JOB_TEJIDO, '') or '').strip():
        found = 'ya tenía valor'
    else:
        job = Project._sgi_dev_search_langs('hr.job', [('name', '=ilike', VALIDATOR_JOB_NAME_TEJIDO)])[:1]
        if job:
            Param.set_param(PARAM_VALIDATOR_JOB_TEJIDO, str(job.id))
        found = job.display_name if job else 'no encontrado (vacío)'
    Project._sgi_dev_enable_mcp_models()
    _logger.info("SGI 57.126.0: valida la ficha de tejido: %s.", found)

# -*- coding: utf-8 -*-
"""19.0.57.124.0 — Arranque del desarrollo (C1, bloque E; brief §6.9).

Siembra, solo si está vacío, el parámetro con los puestos que reciben la
Solicitud de desarrollo aprobada (sección 4 del brief), buscándolos por
nombre en todos los idiomas instalados. Inspección (José Luis Almazán) no
tiene puesto: queda para el parámetro de personas, que decide Jose.

Idempotente. Prefijo en el log: «SGI 57.124.0»."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    from odoo.addons.quimibond_sgi.models.sgi_dev_start import NOTIFY_JOB_NAMES, PARAM_NOTIFY_JOBS
    Param = env['ir.config_parameter'].sudo()
    if (Param.get_param(PARAM_NOTIFY_JOBS, '') or '').strip():
        _logger.info("SGI 57.124.0: el parámetro de partes interesadas ya tiene valor; no se toca.")
        return
    Project = env['project.project']
    jobs = env['hr.job']
    missing = []
    for name in NOTIFY_JOB_NAMES:
        job = Project._sgi_dev_search_langs('hr.job', [('name', '=ilike', name)])[:1]
        if job:
            jobs |= job
        else:
            missing.append(name)
    if jobs:
        Param.set_param(PARAM_NOTIFY_JOBS, ",".join(str(i) for i in jobs.ids))
    _logger.info("SGI 57.124.0: partes interesadas de la solicitud: %s; sin puesto en RH: %s.",
                 ", ".join(jobs.mapped('name')) or '—', ", ".join(missing) or '—')

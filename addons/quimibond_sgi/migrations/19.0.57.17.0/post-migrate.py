# -*- coding: utf-8 -*-
"""57.17.0 (entrega 8, rendimiento: G-015).

Llena por primera vez la huella y las cifras guardadas de «Mi procedimiento»
en los puestos con roles y personas (``hr.job._sgi_mp_refresh_all``). Sin
esto se calcularían al vuelo hasta la corrida de las 03:00 del cron de
medición. Solo escribe campos nuevos de ``hr.job``; nada se borra.

Esperado en producción (2026-09-29, 57.13.0): 129 puestos de la empresa 1;
se llenan los que tienen roles y personas.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    jobs = env['hr.job']._sgi_mp_refresh_all()
    _logger.info("SGI 57.17.0: cifras de Mi procedimiento guardadas en %d puesto(s).", len(jobs))

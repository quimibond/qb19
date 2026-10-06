# -*- coding: utf-8 -*-
"""19.0.57.120.0 — Pruebas de laboratorio de desarrollos: el puesto que las
autoriza es el Coordinador de Laboratorio y MP (hr.job 188 en producción,
decisión de Jose 2026-10-06). Si el parámetro
``quimibond_sgi.dev_lab_authorizer_job_id`` está vacío, lo llena con el puesto
cuyo nombre contiene «Coordinador de Laboratorio». Idempotente; no pisa un
valor capturado. La fecha de bloqueo del artículo genérico se queda vacía."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    job = env['sgi.dev.lab.request']._sgi_dev_set_lab_job_default()
    _logger.info("SGI 57.120.0: puesto autorizador de pruebas de laboratorio: %s.",
                 job.display_name if job else "ya configurado o sin puesto «Coordinador de Laboratorio»")

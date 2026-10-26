# -*- coding: utf-8 -*-
"""19.0.57.191.0 — Aviso «Analizar la solicitud del cliente» a Diseño de Producto y de Procesos para los
desarrollos que ya están en «Análisis» sin resultado (entraron antes de que existiera el aviso). No cambia etapa
ni datos del proyecto; solo agenda la actividad, sin repetir una abierta."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    stage = env['project.project']._sgi_dev_stage('analisis')
    if not stage:
        return
    projects = env['project.project'].search([
        ('sgi_is_ft', '=', True), ('is_template', '=', False), ('stage_id', '=', stage.id),
        ('sgi_dev_analysis_result', '=', False)])
    projects._sgi_dev_notice_analysis_start()
    _logger.info("SGI 57.191.0: aviso de entrada a Análisis para %d desarrollo(s): %s", len(projects),
                 ', '.join(projects.mapped('name')))

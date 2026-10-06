# -*- coding: utf-8 -*-
"""57.103.0: registro de cumplimiento por actividad, responsable y periodo.

1. Las actividades que se miden solas con modelo y pantalla recalculan sus
   faltantes (nuevo ``menu_model_mismatch``, «Pantalla que no va con su
   medición»).
2. Se crea el renglón del periodo en curso de cada actividad periódica y
   cada persona que la ejecuta (lo mismo que hará el respaldo nocturno), y
   se cierran los que ya tienen su registro en Odoo.
3. Las de registro manual se miden con su registro: sin periodos anteriores,
   quedan «pendiente» hasta que su periodo en curso se haga o venza.

Esperado en producción (MCP, solo lectura, 2026-10-05): 80 actividades de
registro manual con cadencia periódica (63 «por evento» no llevan
renglones) y 40 que se miden solas con cadencia periódica.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    Activity = env['sgi.process.activity']
    with_screen = Activity.search([('odoo_action_id', '!=', False),
                                   ('measure_method', 'in', (False, 'odoo', 'entregable'))])
    with_screen._sgi_refresh_spec_gaps()
    mismatched = env['sgi.activity.spec.gap'].search_count([('code', '=', 'menu_model_mismatch')])
    _logger.info("SGI 57.103.0: %d actividades con pantalla y modelo revisadas; %d con pantalla que "
                 "no va con su medición.", len(with_screen), mismatched)
    Execution = env['sgi.activity.execution']
    created, removed = Execution._sgi_generate()
    closed = Execution._sgi_auto_close()
    _logger.info("SGI 57.103.0: registro de cumplimiento: %d renglones creados, %d quitados, %d "
                 "hechos por su registro de Odoo.", len(created), removed, len(closed))
    manual = Activity.search([('measure_method', 'in', ('manual', 'correo', 'muestreo'))])
    manual._sgi_measure()
    _logger.info("SGI 57.103.0: %d actividades de registro manual medidas con su registro.",
                 len(manual))

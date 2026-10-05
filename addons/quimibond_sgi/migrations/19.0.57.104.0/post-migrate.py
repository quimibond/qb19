# -*- coding: utf-8 -*-
"""57.104.0: «Medición por revisar».

Recalcula los faltantes de las actividades con pantalla o con modelo de
medición: «Pantalla que no va con su medición» (ahora también con los menús
de acción de servidor), «Evidencia que no aparece» y «Atribución débil». El
cron de medición los mantiene después.

Esperado en producción (MCP, solo lectura, 2026-10-05): unas 39 actividades
que se miden solas sin un solo registro (todas «por evento»), y la mayoría de
las que se atribuyen con «write_uid».
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    activities = env['sgi.process.activity'].search(
        ['|', ('odoo_menu_id', '!=', False), ('measure_model_id', '!=', False)])
    activities._sgi_refresh_spec_gaps()
    Gap = env['sgi.activity.spec.gap']
    counts = {code: Gap.search_count([('code', '=', code)])
              for code in ('menu_model_mismatch', 'measure_never', 'weak_attribution')}
    _logger.info("SGI 57.104.0: %d actividades revisadas; pantalla que no va con su medición %d, "
                 "evidencia que no aparece %d, atribución débil %d.", len(activities),
                 counts['menu_model_mismatch'], counts['measure_never'], counts['weak_attribution'])

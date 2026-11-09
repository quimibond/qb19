# -*- coding: utf-8 -*-
"""3.10.0: la Demanda por cliente es una tabla que se reconstruye; se reconstruye ya para que
traiga el estado de desarrollo del artículo sin esperar al cron diario (el pronóstico vigente lo
calcula solo al crear la columna)."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    try:
        with cr.savepoint():
            env['qb.demand.line'].action_qb_rebuild()
    except Exception as exc:  # noqa: BLE001 - el cron diario la reconstruye; no detiene la actualización
        _logger.warning("3.10.0: la Demanda no se reconstruyó (la rehace el cron): %s", exc)
    marked = env['qb.forecast.line'].search_count([('dev_state', 'in', ('desarrollo', 'pilotaje', 'archivado'))])
    _logger.info("3.10.0: %d semana(s) del pronóstico con artículo en desarrollo, pilotaje o archivado.", marked)

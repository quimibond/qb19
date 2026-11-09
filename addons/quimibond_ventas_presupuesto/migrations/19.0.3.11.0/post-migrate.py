# -*- coding: utf-8 -*-
"""3.11.0: primera carga de la venta real (año en curso y anterior) y de los faltantes, para que
los reportes tengan cifras sin esperar al cron nocturno."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    total = env['qb.sales.actual'].action_qb_rebuild()
    missing = env['qb.sales.missing'].action_qb_rebuild()
    _logger.info("3.11.0: %d línea(s) de venta real y %d faltante(s).", total, missing)

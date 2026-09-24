# -*- coding: utf-8 -*-
"""I-6: los crons de medición pasan a diarios; el método decide si hoy toca
medir (tercer día hábil del mes / lunes)."""
import logging
from datetime import datetime, timedelta

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

_CRONS = {
    'quimibond_sgi.sgi_cron_indicators': 'model.cron_indicators(scheduled=True)',
    'quimibond_sgi.sgi_cron_indicators_weekly': 'model.cron_indicators_weekly(scheduled=True)',
}


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    nextcall = (datetime.utcnow() + timedelta(days=1)).replace(hour=12, minute=0, second=0, microsecond=0)
    for xmlid, code in _CRONS.items():
        cron = env.ref(xmlid, raise_if_not_found=False)
        if not cron:
            continue
        cron.write({'interval_number': 1, 'interval_type': 'days', 'code': code,
                    'nextcall': nextcall, 'active': True})
        _logger.info("SGI I-6: %s ahora corre a diario (%s).", xmlid, code)

# -*- coding: utf-8 -*-
"""3.2.0: llena la semana del catálogo (week_id) de los renglones del pronóstico que ya existían."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    cr.execute("SELECT DISTINCT week FROM qb_forecast_line WHERE week IS NOT NULL AND week_id IS NULL")
    mondays = [row[0] for row in cr.fetchall()]
    weeks = env['qb.forecast.week']._qb_get(mondays)
    for monday, week in weeks.items():
        cr.execute("UPDATE qb_forecast_line SET week_id = %s WHERE week = %s AND week_id IS NULL", (week.id, monday))
    _logger.info("3.2.0: %d semana(s) del catálogo para el pronóstico existente.", len(weeks))

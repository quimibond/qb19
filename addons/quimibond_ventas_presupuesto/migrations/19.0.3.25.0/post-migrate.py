# -*- coding: utf-8 -*-
"""3.25.0: «Día de recolección» (uno) pasa a «Días de recolección» (varios, Lear martes y viernes).
El día que ya tenía cada ficha se copia; la columna vieja se queda sin uso."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    cr.execute("SELECT 1 FROM information_schema.columns "
               "WHERE table_name = 'qb_delivery_terms' AND column_name = 'pickup_weekday'")
    if not cr.fetchone():
        return
    env = api.Environment(cr, SUPERUSER_ID, {'active_test': False})
    days = {d.code: d for d in env['qb.weekday'].search([])}
    cr.execute("SELECT id, pickup_weekday FROM qb_delivery_terms WHERE pickup_weekday IS NOT NULL")
    for terms_id, weekday in cr.fetchall():
        day = days.get(int(weekday))
        terms = env['qb.delivery.terms'].browse(terms_id)
        if day and not terms.pickup_day_ids:
            terms.pickup_day_ids = day
            _logger.info("3.25.0: %s recoge el %s", terms.display_name, day.name)

# -*- coding: utf-8 -*-
"""19.0.56.12.0 (Bloque 6 · 6.6): horarios escalonados de los crons del SGI.

Antes, 10 crons diarios arrancaban a las 20:44 UTC y «Mediciones semanales»
a la misma hora que «Mediciones de indicadores». Ahora corren de madrugada
en México (UTC-6), cinco minutos uno tras otro. Solo cambia la hora de la
próxima ejecución (los crons son noupdate): la frecuencia no se toca. Los
mensuales y trimestrales conservan su fecha y solo cambian de hora.
«Resumen semanal por correo» sigue apagado (se archiva, no se borra)."""
import logging
from datetime import datetime, time, timedelta

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

# xmlid → hora UTC (México = UTC-6: 12:05 UTC son las 06:05)
SCHEDULE = [
    ('sgi_cron_nonconformities', time(12, 5)),
    ('sgi_cron_overdue_actions', time(12, 10)),
    ('sgi_cron_documents', time(12, 15)),
    ('sgi_cron_audit_program', time(12, 20)),
    ('sgi_cron_risk_review', time(12, 25)),
    ('sgi_cron_calibrations', time(12, 30)),
    ('sgi_cron_competences', time(12, 35)),
    ('sgi_cron_emergency_drills', time(12, 40)),
    ('sgi_cron_operational_signals', time(12, 45)),
    ('sgi_cron_indicators', time(13, 0)),
    ('sgi_cron_indicators_weekly', time(13, 20)),
    ('sgi_cron_news', time(14, 0)),
    ('sgi_cron_supplier_eval', time(14, 10)),
]


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    now = datetime.utcnow()
    for xmlid, at in SCHEDULE:
        cron = env.ref('quimibond_sgi.%s' % xmlid, raise_if_not_found=False)
        if not cron:
            continue
        if cron.interval_type in ('days', 'hours', 'minutes') or (
                cron.interval_type == 'weeks' and not cron.nextcall):
            day = now.date()
        else:
            day = (cron.nextcall or now).date()
        nextcall = datetime.combine(day, at)
        if nextcall <= now:
            nextcall += timedelta(days=1)
        cron.nextcall = nextcall
        _logger.info("quimibond_sgi 56.12.0: %s → %s UTC", xmlid, nextcall)

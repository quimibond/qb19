# -*- coding: utf-8 -*-
"""Días hábiles del SGI (19.0.30.1.0): el calendario de la compañía en
producción (id 9) es de lunes a jueves y sábado. El SGI cuenta de lunes a
viernes con el «Standard 40 hours/week» (id 21). Solo se fija si el parámetro
no existe y el calendario 21 sigue siendo ese; si no, se deja al de la compañía
y se avisa en el log.

Además: las NC del SGI que nacieron sin etapa (NCI-2026-0148, creada por la
fuente «Devolución de cliente validada») no salían en el kanban ni en el
escalamiento. Se les pone la primera etapa de su equipo."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)
PARAM = 'quimibond_sgi.business_calendar_id'


def migrate(cr, version):
    if not version:
        return
    _business_calendar(cr)
    _nc_without_stage(cr)


def _nc_without_stage(cr):
    env = api.Environment(cr, SUPERUSER_ID, {})
    alerts = env['quality.alert'].search([
        ('stage_id', '=', False), ('sgi_folio', '!=', False)])
    Stage = env['quality.alert.stage']
    for alert in alerts:
        first = Stage.search([('team_ids', 'in', alert.team_id.ids)],
                             order='sequence, id', limit=1)
        if first:
            alert.with_context(tracking_disable=True).stage_id = first
            _logger.info("SGI 30.1: %s sin etapa -> %s.", alert.sgi_folio, first.name)


def _business_calendar(cr):
    cr.execute("SELECT 1 FROM ir_config_parameter WHERE key = %s", (PARAM,))
    if cr.fetchone():
        return
    cr.execute("SELECT name::text FROM resource_calendar WHERE id = 21 AND active")
    row = cr.fetchone()
    if not row or '40 hours' not in (row[0] or ''):
        _logger.warning("SGI 30.1: no se fijó %s (el calendario 21 no es el de 40 h); "
                        "se usa el de la compañía.", PARAM)
        return
    cr.execute("""
        INSERT INTO ir_config_parameter (key, value, create_uid, write_uid, create_date, write_date)
        VALUES (%s, '21', 1, 1, now() AT TIME ZONE 'UTC', now() AT TIME ZONE 'UTC')
    """, (PARAM,))
    _logger.info("SGI 30.1: días hábiles con el calendario 21 (lunes a viernes).")

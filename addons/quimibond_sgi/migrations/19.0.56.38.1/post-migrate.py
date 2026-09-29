# -*- coding: utf-8 -*-
"""56.38.1 (decisión de Jose sobre la entrega 8a): se liberan TODOS los
equipos de medición que están en «No usar».

1. Respaldo en la tabla ``sgi_equipment_do_not_use_bak_563801`` (id, name,
   sgi_do_not_use, sgi_calibration_state, sgi_next_calibration_date y la
   hora del respaldo) de cada equipo que se va a tocar.
2. ``sgi.cron._sgi_migrate_release_do_not_use``: ``sgi_do_not_use = False``
   por ORM en los equipos de medición de la empresa del SGI (o sin empresa),
   para que el cambio quede en el chatter (el campo tiene ``tracking=True``;
   nota de OdooBot, sin correo ni actividades). Nada se borra.

La inspección de calidad sigue rechazando un equipo con la calibración
vencida (``quality.check._sgi_check_equipment_calibrated`` la evalúa en
línea contra la fecha de hoy; no se toca) y el bloqueo automático del cron
sigue apagado (``quimibond_sgi.calibration_block_expired``, sin valor en
producción).

Verificado en producción por MCP (solo lectura, 2026-09-29, empresa 1):
- ``aggregate_records maintenance.equipment groupby [company_id,
  sgi_calibration_state, sgi_is_measuring] domain [sgi_do_not_use = True]``:
  144, todos de la empresa 1 y de medición: 143 «vencido» (los bloqueó el
  cron el 25-sep) y 1 sin fecha de calibración (743, IMB 02, lo bloqueó Jose
  al crearlo).
- ``aggregate_records maintenance.equipment groupby [company_id] domain
  [sgi_do_not_use = True, active = False]``: 0 archivados.
Esperado la primera vez: 144 liberados; la segunda, 0.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

BACKUP = 'sgi_equipment_do_not_use_bak_563801'


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    released = env['sgi.cron']._sgi_migrate_release_do_not_use(BACKUP)
    _logger.info("SGI 56.38.1: %d equipo(s) de medición liberados de «No usar» (esperado en "
                 "producción la primera vez: 144). Respaldo en %s. Equipos: %s", len(released), BACKUP,
                 ", ".join("%s %s" % (eq.id, eq.name) for eq in released) or "ninguno")

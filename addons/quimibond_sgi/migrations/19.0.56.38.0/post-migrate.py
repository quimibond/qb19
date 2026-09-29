# -*- coding: utf-8 -*-
"""56.38.0 (auditoría 2026-09, entrega 8a, línea «avisos»; G-006 con la
decisión 2 de la tanda 2): la calibración solo avisa, no bloquea, y los
avisos van al Coordinador de Laboratorio y al Jefe de Calidad en un resumen
diario.

1. Respaldo en la tabla ``sgi_mail_activity_bak_563800`` de cada aviso
   «Calibración VENCIDA: <equipo>».
2. ``sgi.cron._sgi_migrate_close_calibration_notices``: los cierra con
   ``action_feedback`` y nota (se archivan; nada se borra). Desde esta versión
   los reemplaza el resumen diario del cron.
3. **No toca el «No usar» de ningún equipo.** Pregunta abierta para Jose:
   ¿se liberan los equipos que el cron dejó bloqueados? Aquí solo se listan
   en el log.

Verificado en producción por MCP (solo lectura, 2026-09-29):
- ``aggregate_records mail.activity groupby [res_model, date_deadline:day]
  domain [summary =like 'Calibración VENCIDA: %']``: 143, todas sobre
  maintenance.equipment, del usuario 7, con fecha límite 2026-09-25 (una por
  equipo, creadas por OdooBot el 25-sep a las 20:46). No hay «Calibración por
  vencer» abiertas.
- ``aggregate_records maintenance.equipment groupby [sgi_calibration_state,
  sgi_do_not_use, company_id] domain [sgi_is_measuring = True]``: empresa 1,
  143 «vencido» con «No usar», 4 sin fecha y libres, 1 sin fecha y con «No
  usar» (743, IMB 02, creado así a mano el 25-sep).
- ``aggregate_records mail.tracking.value groupby [new_value_integer,
  create_uid] domain [field_id.name = sgi_do_not_use, field_id.model =
  maintenance.equipment]``: 143 cambios a «No usar», todos de OdooBot (el
  cron, 25-sep-2026 20:46).
Esperado la primera vez: 143 cerradas; la segunda, 0.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

BACKUP = 'sgi_mail_activity_bak_563800'


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    closed = env['sgi.cron']._sgi_migrate_close_calibration_notices(BACKUP)
    _logger.info("SGI 56.38.0 (G-006): %d aviso(s) «Calibración VENCIDA» cerrados (esperado en "
                 "producción: 143). Respaldo en %s.", len(closed), BACKUP)
    company = env['sgi.config']._sgi_company()
    blocked = env['maintenance.equipment'].sudo().search([
        ('sgi_is_measuring', '=', True), ('sgi_do_not_use', '=', True),
        ('company_id', 'in', (company.id, False))], order='id')
    _logger.warning("SGI 56.38.0: %d equipo(s) de medición siguen en «No usar» (no se tocan; "
                    "decisión pendiente de Jose): %s", len(blocked),
                    ", ".join("%s %s" % (eq.id, eq.name) for eq in blocked))

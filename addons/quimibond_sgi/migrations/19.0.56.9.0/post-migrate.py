# -*- coding: utf-8 -*-
"""19.0.56.9.0 (Bloque 3 · 3.6): una acción terminada tiene 100 % de avance y
fecha de término de hoy o antes.

Datos existentes (no se borra nada):
- «Terminada» con fecha FUTURA (en producción, la acción 63 «Abrir mercado
  mexicano», 0 % y fecha 2026-11-30): se reabre (sin fecha de término,
  conserva su avance) y queda en el historial del origen.
- «Terminada» con fecha pasada y avance < 100 %: el avance pasa a 100 %."""
import logging

from odoo import SUPERUSER_ID, api, fields

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    Line = env['sgi.action.line'].with_context(active_test=False)
    today = fields.Date.context_today(Line)
    future = Line.search([('date_done', '>', today)])
    for line in future:
        wrong = line.date_done
        line.write({'date_done': False, 'progress': line.progress or '0'})
        origin = line._sgi_origin()
        if origin and hasattr(origin, 'message_post'):
            origin.message_post(body="La acción «%s» estaba terminada con fecha futura (%s); se "
                                     "reabrió (19.0.56.9.0)." % (line.name, wrong))
    partial = Line.search([('date_done', '!=', False), ('date_done', '<=', today), ('progress', '!=', '100')])
    if partial:
        partial.write({'progress': '100'})
    _logger.info("quimibond_sgi 56.9.0: %d acciones reabiertas (fecha futura), %d pasan a 100%%.",
                 len(future), len(partial))

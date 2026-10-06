# -*- coding: utf-8 -*-
"""57.114.0: campos de liga entrada ↔ salida que faltaban (tanda 5).

Cada actividad que se mide por su entrada y su salida necesita un campo real
en la salida que apunte a la entrada (``sgi.activity.input.match_path``).
Esta versión agrega los que faltaban y escribe la liga en las 7 entradas que
la esperaban. Solo se escribe donde está vacía; los avisos «Entrada que no se
liga con la salida» de esas actividades se recalculan.

Esperado en producción (MCP, 2026-10-06): 9 avisos «Entrada que no se liga
con la salida» antes de esta versión; quedan 2 después (C6.27 y E2.12, que
esperan decisión).
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

# número de actividad → (modelo de la entrada, campo de la salida que la liga)
MATCHES = {
    'C1.08': ('project.task', 'sgi_dyd_task_id'),
    'C1.12': ('project.task', 'sgi_dyd_task_id'),
    'C1.13': ('stock.picking', 'sgi_dyd_picking_ids'),
    'C5.14': ('sgi.action.line', 'sgi_action_line_id'),
    'C5.22': ('quality.alert', 'sgi_alert_id'),
    'S5.03': ('maintenance.request', 'sgi_maintenance_request_id'),
    'S5.04': ('maintenance.request', 'sgi_maintenance_request_id'),
}


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    Activity = env['sgi.process.activity'].with_context(active_test=False)
    done = []
    for number, (in_model, path) in MATCHES.items():
        for activity in Activity.search([('number', '=', number)]):
            lines = activity.input_ids.filtered(
                lambda l: l.deliverable_id.odoo_model_id.model == in_model and not l.match_path)
            if lines:
                lines.write({'match_path': path})
                activity._sgi_refresh_spec_gaps()
                done.append(number)
    left = env['sgi.activity.spec.gap'].search_count([('code', '=', 'no_match')])
    _logger.info("SGI 57.114.0: liga entrada ↔ salida escrita en %s; quedan %d avisos "
                 "«Entrada que no se liga con la salida».", ", ".join(done) or "ninguna", left)

# -*- coding: utf-8 -*-
"""57.111.0: quién hizo la actividad según el historial de su estado.

Los entregables y las actividades medidas con «write_uid» cuyo modelo guarda
historial de su estado (state, stage_id, request_status…) pasan a «Quién lo
hizo: quien lo pasó a su estado (historial)». «write_uid» se queda como campo
de respaldo para los registros sin ese cambio en el historial. Los que no
guardan historial (reglas de inventario, listas de precios, contactos…) se
quedan igual y siguen con su aviso.

Esperado en producción (MCP, solo lectura, 2026-10-06): 63 actividades con
«write_uid»; en las transferencias, OdooBot es el último que edita 598 de 874
y el historial de «Hecho» nunca es de OdooBot.
"""
import logging

from odoo import SUPERUSER_ID, api

from odoo.addons.quimibond_sgi.models.sgi_measure_history import sgi_history_field

_logger = logging.getLogger(__name__)


def _with_history(env, records, model_of):
    out = records.browse()
    for rec in records:
        model = model_of(rec)
        if model and model in env and sgi_history_field(env[model]):
            out |= rec
    return out


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    deliverables = _with_history(
        env, env['sgi.deliverable'].with_context(active_test=False).search(
            [('measure_user_field', '=', 'write_uid'), ('odoo_model_id', '!=', False)]),
        lambda d: d.odoo_model_id.model)
    # El entregable copia la casilla a las actividades que se miden con él.
    deliverables.write({'measure_user_history': True})
    activities = _with_history(
        env, env['sgi.process.activity'].search(
            [('measure_user_field', '=', 'write_uid'), ('measure_method', '!=', 'entregable'),
             ('measure_model_id', '!=', False)]),
        lambda a: a.measure_model_id.model)
    activities.write({'measure_user_history': True})
    env['sgi.process.activity'].search(
        [('measure_user_history', '=', True)])._sgi_refresh_spec_gaps()
    left = env['sgi.activity.spec.gap'].search_count(
        [('code', '=', 'weak_attribution'), ('message', 'ilike', 'write_uid')])
    _logger.info("SGI 57.111.0: historial de estado en %d entregables y %d actividades; "
                 "quedan %d avisos de «write_uid».", len(deliverables), len(activities), left)

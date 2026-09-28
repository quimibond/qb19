# -*- coding: utf-8 -*-
"""49.0.0 (PR 2: NC-1 a NC-5).

- NC-5: las NC del SGI (con folio) que estén en una etapa ajena a los equipos
  del SGI (Nuevo / Confirmado / Acción propuesta / Resuelto, las nativas de
  Calidad) pasan a su equivalente (Abierta / Abierta / Seguimiento / Cerrada)
  y se quita a los equipos del SGI de esas etapas. Las etapas nativas NO se
  borran ni archivan (quality.alert.stage no tiene `active` y las usan los
  equipos de calidad de piso); simplemente dejan de aparecer para las NC del
  SGI. En producción (2026-09-25) ninguna NC con folio estaba en ellas.
- NC-1: las NC abiertas del SGI reciben sus plazos por etapa (días hábiles
  desde su fecha de creación) para que el cron empiece a vigilarlas.
Todo idempotente; grep "SGI 49" en el log.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

_STAGE_MAP = {
    'nuevo': 'quimibond_sgi.sgi_nc_int_stage_open',
    'confirmado': 'quimibond_sgi.sgi_nc_int_stage_open',
    'acción propuesta': 'quimibond_sgi.sgi_nc_int_stage_followup',
    'accion propuesta': 'quimibond_sgi.sgi_nc_int_stage_followup',
    'resuelto': 'quimibond_sgi.sgi_nc_int_stage_closed',
}


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    Alert = env['quality.alert'].sudo()
    Stage = env['quality.alert.stage'].sudo()
    teams = env['quality.alert.team'].sudo().search([('sgi_sequence_id', '!=', False)])
    official = Stage.browse()
    for xmlid in ('sgi_nc_int_stage_open', 'sgi_nc_int_stage_followup',
                  'sgi_nc_int_stage_closed', 'sgi_nc_int_stage_cancel'):
        official |= env.ref('quimibond_sgi.%s' % xmlid, raise_if_not_found=False) or Stage
    if not teams or not official:
        _logger.warning("SGI 49: sin equipos o etapas del SGI; no se migra nada.")
        return
    # NC-5: NC con folio fuera del flujo del SGI.
    moved = 0
    for alert in Alert.search([('sgi_folio', '!=', False), ('stage_id', 'not in', official.ids)]):
        target_xmlid = _STAGE_MAP.get((alert.stage_id.name or '').strip().lower())
        target = env.ref(target_xmlid, raise_if_not_found=False) if target_xmlid else False
        if not target:
            target = env.ref('quimibond_sgi.sgi_nc_int_stage_open', raise_if_not_found=False)
        old_name = alert.stage_id.name or '(sin etapa)'
        alert.with_context(sgi_force_close=True).write({'stage_id': target.id})
        alert.message_post(body="Migración 49.0.0: la NC pasó de la etapa «%s» a «%s» "
                                "(solo etapas del SGI)." % (old_name, target.name))
        moved += 1
    _logger.info("SGI 49: %d NC con folio movidas a etapas del SGI.", moved)
    # Los equipos del SGI ya no ven las etapas nativas de Calidad.
    for stage in Stage.search([('team_ids', 'in', teams.ids), ('id', 'not in', official.ids)]):
        stage.write({'team_ids': [(3, t.id) for t in teams]})
        _logger.info("SGI 49: la etapa «%s» deja de estar ligada a los equipos del SGI.", stage.name)
    # NC-1: plazos para las NC abiertas.
    open_alerts = Alert.search([
        ('sgi_folio', '!=', False), ('sgi_due_plan', '=', False),
        ('stage_id.sgi_is_closing_stage', '=', False),
        ('stage_id.sgi_is_cancel_stage', '=', False)])
    open_alerts._sgi_set_deadlines()
    _logger.info("SGI 49: plazos por etapa fijados en %d NC abiertas.", len(open_alerts))

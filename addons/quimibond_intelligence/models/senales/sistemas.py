# -*- coding: utf-8 -*-
"""Señales de sistemas (spec §4): tickets. job_caido la manda el watchdog (Edge Function health), no Odoo."""
from .base import senal, hace, dias, umbral, tiene_modelo, doc, fila


@senal('ticket_abierto')
def ticket_abierto(env, cfg):
    if not tiene_modelo(env, 'helpdesk.ticket'):
        return None
    n = umbral(cfg, 'dias', 7)
    ts = env['helpdesk.ticket'].sudo().search([('stage_id.fold', '=', False), ('create_date', '<', hace(n))], order='create_date')
    return [fila(f'ticket_abierto:helpdesk.ticket:{t.id}', [doc(t, t.display_name, etapa=t.stage_id.name, equipo=t.team_id.name)], valor=dias(t.create_date),
                 valor_texto=f'{t.display_name}: {dias(t.create_date)} días en {t.stage_id.name}', partner=t.partner_id, user=t.user_id,
                 payload={'fecha_base': str(t.create_date.date())}) for t in ts]

# -*- coding: utf-8 -*-
"""3.5.1: la migración de 3.5.0 no encontró el equipo de Industrial por nombre (sin idioma se
llama distinto), así que los renglones de Industrial quedaron sin equipo y con el real en 0.
Si falta el parámetro, se busca el equipo por su nombre en cualquier idioma; después cada
renglón toma el equipo de su mercado y se recalcula su real y su pedido."""
import logging
import unicodedata

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

NAMES = {'industrial': 'industrial', 'confeccion': 'confeccion', 'especiales': 'especiales'}


def _norm(text):
    text = (text or '').strip().lower()
    return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    Param = env['ir.config_parameter']
    langs = [code for code, _name in env['res.lang'].get_installed()]
    for team in env['crm.team'].with_context(active_test=False).search([], order='id'):
        names = {_norm(team.with_context(lang=lang).name) for lang in langs} | {_norm(team.name)}
        base = next((NAMES[n] for n in names if n in NAMES), None)
        key = 'quimibond_ventas_presupuesto.team_%s' % base if base else None
        if key and not Param.get_param(key):
            Param.set_param(key, team.id)
    changed = env['sgi.sales.budget.line']._qb_refresh_market_teams()
    _logger.info("3.5.1: %d renglón(es) del presupuesto tomaron el equipo de su mercado.", changed)

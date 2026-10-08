# -*- coding: utf-8 -*-
"""19.0.1.1.0 — Jose 2026-10-08, punto 2: los días de seguimiento (5) y de
archivo de borradores (30) los puso el instalador sin que nadie los definiera.
Se vacían solo si siguen con ese valor; `validez_dias` = 15 se queda (es la
práctica actual). Sin valor no hay seguimiento automático ni archivo.
Idempotente. Prefijo en el log: «qb_cotizador 1.1.0»."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

DEFAULTS = {'qb_cotizador.seguimiento_dias_habiles': '5', 'qb_cotizador.borrador_archivar_dias': '30'}


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    Param = env['ir.config_parameter'].sudo()
    cleared = []
    for key, default in DEFAULTS.items():
        if (Param.get_param(key, '') or '').strip() == default:
            Param.set_param(key, False)
            cleared.append(key)
    _logger.info("qb_cotizador 1.1.0: parámetros vaciados %s.", cleared or 'ninguno (ya tenían otro valor)')

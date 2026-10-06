# -*- coding: utf-8 -*-
"""57.1.0: TR-01 «Cierre de No Conformidades» pasa del modo de código
``cierre_nc`` (retirado) a fórmula configurable.

Llama ``sgi.indicator._sgi_cierre_nc_formula()``: solo los indicadores que
siguen en ``cierre_nc`` (activos o archivados). A cada uno le pone los dos
términos (NC del SGI cerradas en el periodo ÷ NC del SGI levantadas en el
periodo, sin las canceladas) si no tiene términos propios, cambia el modo a
``configurable`` y deja el modo anterior en el chatter. Un indicador al que
MAST ya le cambió el modo no se toca. Idempotente: la segunda corrida no
encuentra ninguno. Las mediciones existentes no se recalculan.

Esperado en producción (MCP, solo lectura, 2026-09-29): 1 indicador (id 1,
TR-01), en prueba, sin términos.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    done = env['sgi.indicator']._sgi_cierre_nc_formula()
    _logger.info("SGI 57.1.0: %d indicador(es) de «cierre_nc» a fórmula configurable: %s "
                 "(esperado en producción: 1, el id 1 TR-01).", len(done), done)

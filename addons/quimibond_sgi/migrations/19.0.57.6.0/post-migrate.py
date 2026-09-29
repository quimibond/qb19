# -*- coding: utf-8 -*-
"""57.6.0 (D-13): E1-02 «Acuerdos de dirección cumplidos en fecha» deja de
ser manual y se mide con ``acuerdos_rxd``.

Llama ``sgi.indicator._sgi_activate_acuerdos_rxd()``: solo el indicador
activo con clave E1-02 que siga en «manual» y sin términos de fórmula. Si
MAST ya le puso otro modo o una fórmula, no se toca. El modo anterior queda en
el chatter. Idempotente.

Esperado en producción (MCP, solo lectura, 2026-09-29): 1 indicador (id 171,
E1-02), en prueba, manual, sin términos (el campo de texto «Fórmula» solo
describe el cálculo), 0 mediciones. Hoy hay 0 acuerdos de la RxD, así que
sus mediciones saldrán «sin dato» hasta la primera revisión realizada.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    done = env['sgi.indicator']._sgi_activate_acuerdos_rxd()
    _logger.info("SGI 57.6.0: %d indicador(es) E1-02 pasan a «acuerdos_rxd»: %s "
                 "(esperado en producción: 1, el id 171).", len(done), done)

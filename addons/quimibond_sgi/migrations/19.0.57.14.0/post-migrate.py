# -*- coding: utf-8 -*-
"""57.14.0 (indicadores 2): S2-01, S1-05, C1-04, RH-01, S4-01 y S6-02 dejan
de ser manuales y se miden con sus modos nuevos.

Llama ``sgi.indicator._sgi_activate_ind2()``: solo el indicador activo con
esa clave que siga en «manual» y sin términos de fórmula (mismo criterio que
E1-02 en 57.6.0). Si MAST ya le puso otro modo o una fórmula, no se toca.
S6-02 no tiene historia (el plan de salida nace en esta versión): si no tiene
«Medir desde», se mide desde el mes siguiente. El modo anterior queda en el
chatter. Idempotente.

C4-01 NO se activa: en producción la fecha de fin de las operaciones la pone
el cierre de la orden, así que no hay término real propio (ver CHANGELOG).

Esperado en producción (MCP, solo lectura, 2026-09-29): los seis en prueba,
manuales, sin términos y sin «Medir desde»: S2-01 (id 121), S1-05 (136),
C1-04 (151), RH-01 (23), S4-01 (162) y S6-02 (167).
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    done = env['sgi.indicator']._sgi_activate_ind2()
    _logger.info("SGI 57.14.0: indicadores 2 activados: %s (esperado en producción: "
                 "S2-01 121, S1-05 136, C1-04 151, RH-01 23, S4-01 162, S6-02 167).", done)

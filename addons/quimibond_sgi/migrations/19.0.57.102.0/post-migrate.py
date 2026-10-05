# -*- coding: utf-8 -*-
"""19.0.57.102.0 — Indicadores: sin dato y cálculos.

Pasos aprobados por Jose el 2026-10-05 (plan 57.102.0, Q4, Q9 y Q10). Cada
uno es idempotente, deja su conteo en el log y su antes/después en el chatter
del indicador; ninguno toca una medición validada ni borra nada. El recálculo
de mediciones NO corre aquí: lo hace el cron diario desde el día siguiente
(Q6).

P-a (Q4) Indicadores con «Medir desde»: las mediciones no validadas
     anteriores pasan a «Sin dato» con el valor anterior en la nota
     (esperado en producción: 9, todas de S6-02).
P-b (Q9) Términos de TR-01 y C5-02, solo si siguen exactamente como el
     2026-10-05 (esperado: los dos «corregido»), con su fórmula y fuente en
     palabras.
P-c (Q10) Entregable que mide C2-06 («Salida validada»): sello de embarque en
     vez del campo de Studio «Tipo de transporte» (esperado: «corregido»)."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    Indicator = env['sgi.indicator'].with_context(active_test=False)
    marked = Indicator.search([('measure_from', '!=', False)])._sgi_mark_before_measure_from()
    _logger.info("SGI 57.102.0 P-a: %d medición(es) antes de «Medir desde» a sin dato (ids %s).",
                 len(marked), marked.ids)
    _logger.info("SGI 57.102.0 P-b: fórmulas TR-01/C5-02 por indicador: %s",
                 Indicator._sgi_formula_fixes_57102())
    _logger.info("SGI 57.102.0 P-c: entregable de C2-06: %s", Indicator._sgi_deliverable_fix_57102())

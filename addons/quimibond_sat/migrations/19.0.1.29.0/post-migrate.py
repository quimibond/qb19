# -*- coding: utf-8 -*-
"""CFDI de menos de $1 sin factura (1.29.0): el hallazgo guardado pasa de
'Solo en el SAT' a 'Importe en cero' (Edenred, Uline y AT&T en $0, notas de
centavos de Sonigas)."""
import logging

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    cr.execute("""
        UPDATE sat_cfdi SET issue = 'importe_cero'
         WHERE issue = 'solo_sat' AND move_id IS NULL AND tipo IN ('I', 'E') AND abs(coalesce(total, 0)) < 1
    """)
    _logger.info('sat.cfdi: %s CFDI de menos de $1 pasan a "Importe en cero"', cr.rowcount)

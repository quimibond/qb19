# -*- coding: utf-8 -*-
"""I-3: MA-05, EX-01 y EX-02 pasan a las fórmulas aprobadas, solo si siguen
en el modo viejo (una decisión de MAST posterior se respeta)."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

_SWITCH = {
    'MA-05': ('desperdicio', 'desperdicio_kg'),
    'EX-01': ('margen_ventas', 'margen_ebitda'),
    'EX-02': ('compras_vs_ventas', 'compras_mp_vs_ventas'),
}


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    for code, (old, new) in _SWITCH.items():
        indicator = env['sgi.indicator'].search([('code', '=', code)], limit=1)
        if indicator and indicator.calc_mode == old:
            indicator.write({'calc_mode': new})
            indicator.message_post(body="I-3: modo de cálculo «%s» → «%s»." % (old, new))
            _logger.info("SGI I-3: %s pasa de %s a %s.", code, old, new)

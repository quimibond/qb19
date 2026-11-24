# -*- coding: utf-8 -*-
"""1.30.0: cada renglón de complemento de pago se liga a su CFDI de pago (los
campos calculados nuevos ya se llenaron al actualizar), a su pago de Odoo y se
marcan las copias. Solo escribe en sat_cfdi_pago.

Ojo: aquí l10n_mx_edi puede no estar cargado todavía (no es dependencia), y
sin él no hay folio fiscal en los pagos; 1.31.1 lo repite en un end-migrate."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    n = env['sat.cfdi.pago']._cron_link_payments()
    _logger.info('sat.cfdi.pago: %s renglones ligados a su REP y su pago de Odoo', n)

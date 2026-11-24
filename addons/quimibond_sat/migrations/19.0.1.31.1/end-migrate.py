# -*- coding: utf-8 -*-
"""1.31.1: vuelve a ligar los complementos de pago con su pago de Odoo al final
de la actualización.

En 1.30.0 el post-migrate corrió antes de que cargara l10n_mx_edi (no es
dependencia del módulo), así que account.payment todavía no tenía el folio
fiscal y ningún REP emitido se ligó por UUID: los tres REP de Proyecciones de
la Moda cuyo pago está cancelado no salían. Un end-migrate corre con todos los
módulos cargados."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    n = env['sat.cfdi.pago']._cron_link_payments()
    _logger.info('sat.cfdi.pago: %s renglones ligados otra vez a su pago de Odoo', n)

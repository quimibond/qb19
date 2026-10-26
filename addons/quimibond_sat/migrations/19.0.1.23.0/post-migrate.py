# -*- coding: utf-8 -*-
"""Facturas con varios CFDI y CFDI en varias facturas (1.23.0): recalcula el
hallazgo de los CFDI cuya factura tiene otros CFDI ligados y de los que
comparten UUID con más de una factura publicada (antes salían "monto
distinto" contra una sola de las dos)."""
import logging

from odoo import SUPERUSER_ID, api
from odoo.tools.sql import column_exists

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    cr.execute("""
        SELECT id FROM sat_cfdi
         WHERE move_id IN (SELECT move_id FROM sat_cfdi WHERE move_id IS NOT NULL
                            GROUP BY move_id HAVING count(*) > 1)
    """)
    ids = {row[0] for row in cr.fetchall()}
    if column_exists(cr, 'account_move', 'l10n_mx_edi_cfdi_uuid'):
        cr.execute("""
            SELECT c.id FROM sat_cfdi c
             WHERE c.move_id IS NOT NULL AND lower(c.uuid) IN (
                   SELECT lower(l10n_mx_edi_cfdi_uuid) FROM account_move
                    WHERE state = 'posted' AND l10n_mx_edi_cfdi_uuid IS NOT NULL
                    GROUP BY lower(l10n_mx_edi_cfdi_uuid) HAVING count(*) > 1)
        """)
        ids |= {row[0] for row in cr.fetchall()}
    Cfdi = env['sat.cfdi']
    cfdis = Cfdi.browse(sorted(ids))
    env.add_to_compute(Cfdi._fields['issue'], cfdis)
    env.flush_all()
    _logger.info('quimibond_sat: hallazgo recalculado en %s CFDI de facturas compartidas', len(cfdis))

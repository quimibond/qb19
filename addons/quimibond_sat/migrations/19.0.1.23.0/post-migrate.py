# -*- coding: utf-8 -*-
"""Facturas con varios CFDI y CFDI en varias facturas (1.23.0): recalcula el
hallazgo de los CFDI cuya factura tiene otros CFDI ligados y de los que
comparten UUID con más de una factura publicada (antes salían "monto
distinto" contra una sola de las dos).

CFDI relacionados: llena tipo de relación y UUID relacionados desde el
payload de Syntage y aplica sustituciones (04) y compensados (01/03)."""
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
    cr.execute("SELECT id FROM sat_cfdi WHERE issue = 'moneda'")
    ids |= {row[0] for row in cr.fetchall()}
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

    cr.execute("""
        WITH rel AS (
            SELECT c.id,
                   string_agg(DISTINCT lpad(r->>'type', 2, '0'), ',') AS tipos,
                   string_agg(lower(r->>'relatedInvoiceUuid'), ',') AS uuids
              FROM sat_cfdi c,
                   jsonb_array_elements(CASE WHEN jsonb_typeof(c.raw_json->'relations') = 'array'
                                             THEN c.raw_json->'relations' ELSE '[]'::jsonb END) r
             GROUP BY c.id
        )
        UPDATE sat_cfdi c SET relacion_tipo = rel.tipos, relacionados = rel.uuids
          FROM rel WHERE rel.id = c.id
    """)
    _logger.info('quimibond_sat: relaciones llenadas en %s CFDI', cr.rowcount)
    env.invalidate_all()
    pending = Cfdi.search([('match_status', '=', 'solo_sat'), ('relacionados', '!=', False)])
    pending._match_move()
    _logger.info('quimibond_sat: %s CFDI relacionados revisados (sustituciones y compensados)', len(pending))

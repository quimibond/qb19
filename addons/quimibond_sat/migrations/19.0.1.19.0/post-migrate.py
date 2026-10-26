# -*- coding: utf-8 -*-
"""Conciliación sin ruido (1.19.0):

* "Datos del SAT desde" de cada compañía sincronizada: el primer mes con al
  menos 50 CFDI (I/E). En PNTQ es enero 2025; antes solo hay sueltos que no
  hacen un mes completo, y las ~19,600 facturas de Odoo de 2011-2024 salían
  como "solo Odoo" / "sin UUID".
* Recalcula el hallazgo de los CFDI ligados y de los cancelados sin factura
  (nuevos: borrador, redondeo, terceros, cancelado sin factura).
* Vuelve a cruzar los CFDI "solo en el SAT" de SAT, IMSS e Infonavit para que
  queden ignorados por póliza aunque no exista su contacto.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

MIN_CFDI_PER_MONTH = 50


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    Company = env['res.company']
    for company in Company.search([('sat_sync_enabled', '=', True), ('sat_data_since', '=', False)]):
        cr.execute("""
            SELECT date_trunc('month', fecha_emision)::date
              FROM sat_cfdi
             WHERE company_id = %s AND tipo IN ('I', 'E') AND fecha_emision IS NOT NULL
             GROUP BY 1
            HAVING count(*) >= %s
             ORDER BY 1
             LIMIT 1
        """, (company.id, MIN_CFDI_PER_MONTH))
        row = cr.fetchone()
        if row:
            company.sat_data_since = row[0]
            _logger.info('quimibond_sat: %s, datos del SAT desde %s', company.name, row[0])

    Cfdi = env['sat.cfdi']
    cfdis = Cfdi.search(['|', ('move_id', '!=', False), '&', ('match_status', '=', 'solo_sat'),
                         ('estado_sat', '=', 'cancelado')])
    env.add_to_compute(Cfdi._fields['issue'], cfdis)
    env.flush_all()
    _logger.info('quimibond_sat: hallazgo recalculado en %s CFDI', len(cfdis))

    poliza = Cfdi.search([('match_status', '=', 'solo_sat'),
                          ('counterparty_rfc', 'in', list(Cfdi._rfc_poliza()))])
    poliza._match_move()
    _logger.info('quimibond_sat: %s CFDI de SAT/IMSS/Infonavit revisados por póliza', len(poliza))

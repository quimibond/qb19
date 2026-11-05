# -*- coding: utf-8 -*-
"""3.0.0 (limpieza, decisión de Jose 2026-11-05).

1. Los pronósticos por cliente y año (sgi.sales.budget kind='pronostico') pasan a
   «Obsoleto» con una nota: el pronóstico vive en qb.forecast.line. Quedan de consulta
   en Ventas → Presupuesto y pronóstico → Histórico.
2. El pronóstico ya no escribe en el Programa Maestro: se borran las celdas
   (mrp.product.forecast) que el envío viejo dejó el 2026-11-02 entre 17:04 y 17:06
   UTC en los programas creados en esa ventana (624 en producción). Una celda que
   Planeación tocó a mano (replenish_qty_updated) se deja. Los programas quedan.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

MPS_FROM = '2026-11-02 17:04:00'
MPS_TO = '2026-11-02 17:07:00'
NOTE = ("Archivado en la versión 3.0.0: el pronóstico ya no es un documento por cliente y año. "
        "El vigente está en Ventas → Presupuesto y pronóstico → Pronóstico vigente.")


def _table_exists(cr, table):
    cr.execute("SELECT 1 FROM information_schema.tables WHERE table_name = %s", (table,))
    return bool(cr.fetchone())


def _column_exists(cr, table, column):
    cr.execute("SELECT 1 FROM information_schema.columns WHERE table_name = %s AND column_name = %s",
               (table, column))
    return bool(cr.fetchone())


def _archive_old_forecasts(env):
    old = env['sgi.sales.budget'].search([('kind', '=', 'pronostico'), ('state', '!=', 'obsoleto')])
    for budget in old:
        budget.with_context(tracking_disable=True).write({'state': 'obsoleto'})
        budget.message_post(body=NOTE)
    _logger.info("3.0.0: %d pronóstico(s) por documento archivado(s).", len(old))


def _delete_mps_cells(cr):
    if not (_table_exists(cr, 'mrp_product_forecast') and _table_exists(cr, 'mrp_production_schedule')):
        _logger.info("3.0.0: sin MPS instalado; no hay celdas que borrar.")
        return
    manual = ("AND COALESCE(f.replenish_qty_updated, false) = false"
              if _column_exists(cr, 'mrp_product_forecast', 'replenish_qty_updated') else "")
    cr.execute("""
        DELETE FROM mrp_product_forecast f
         USING mrp_production_schedule s
         WHERE f.production_schedule_id = s.id
           AND s.create_date >= %s AND s.create_date < %s
           {manual}
    """.format(manual=manual), (MPS_FROM, MPS_TO))
    _logger.info("3.0.0: %d celda(s) del MPS borrada(s) (envío del 2026-11-02).", cr.rowcount)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    _archive_old_forecasts(env)
    _delete_mps_cells(cr)

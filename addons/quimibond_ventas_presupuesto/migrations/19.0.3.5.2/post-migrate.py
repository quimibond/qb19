# -*- coding: utf-8 -*-
"""3.5.2:

- El real de los renglones contaba también las dos líneas de costo (cogs) de cada línea de
  factura: la cantidad facturada salía al triple y el precio real a un tercio (desviación de
  precio «grave» de −70 %). El importe no cambiaba (las líneas de costo se cancelan). Se
  recalcula el real, el pedido y la desviación de todos los renglones de presupuesto.
- La columna «Fecha de la revisión» nació en 3.5.0 con la fecha del update en los documentos
  que ya existían: se pone la de creación del documento.
- «Venta del año anterior» quedó en 0 en los documentos que ya existían: se calcula con la
  misma regla del VE-02 (es referencia; no entra al importe).
"""
import logging
from datetime import date

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    cr.execute("""UPDATE sgi_sales_budget SET revision_date = create_date::date
                   WHERE kind = 'presupuesto' AND create_date::date < '2026-11-05'
                     AND revision_date >= '2026-11-05'""")
    dated = cr.rowcount
    budgets = env['sgi.sales.budget'].search([('kind', '=', 'presupuesto'), ('base_anio_anterior', '=', 0)])
    for budget in budgets:
        base = budget._qb_company_net_invoiced(date(budget.year - 1, 1, 1), date(budget.year - 1, 12, 31))
        cr.execute("UPDATE sgi_sales_budget SET base_anio_anterior = %s WHERE id = %s", (base, budget.id))
    env.invalidate_all()
    lines = env['sgi.sales.budget.line'].search([('kind', '=', 'presupuesto')])
    lines._compute_real()
    _logger.info("3.5.2: %d fecha(s) de revisión, %d base(s) del año anterior, real recalculado en "
                 "%d renglón(es).", dated, len(budgets), len(lines))

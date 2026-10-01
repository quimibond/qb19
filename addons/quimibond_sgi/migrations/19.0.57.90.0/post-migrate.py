# -*- coding: utf-8 -*-
"""57.90.0: indicadores de ventas sin venta de activo fijo, e indicadores de
foto que no se recalculan hacia atrás.

1. Indicadores de foto. Los modos de código (cartera vencida, cartera a más
   de 60 días, diferencia de inventario, capacitación) quedan marcados solos
   por el cálculo de ``snapshot``. Aquí se marcan las fórmulas configurables
   que miden el estado de hoy (lista de Dirección, 2026-10-01): S2-03, S3-03,
   S6-03, E2-01 y E2-03.
2. Las mediciones de periodos anteriores al último cerrado de esos
   indicadores que se recalcularon desde el 2026-10-01 (recálculo por MCP de
   enero a septiembre) pasan a «sin dato»; el valor que traían queda en la
   nota. Las validadas no se tocan.
3. Las mediciones 2026 no validadas de los modos de ventas se recalculan con
   el filtro nuevo (líneas en cuentas 401/402): sin la venta de la rama
   ICOMATEX a Leasing Lepezo ($11.3 M en mar-2026, cuenta 704.23.0003) ni la
   de junio ($2.0 M). También CO-01 (``otd_compras``), que ahora deja fuera
   las recepciones de OC sin fecha prometida. Cada medición va en su
   savepoint.

Registra antes → después en el log. Idempotente.
"""
import logging
from datetime import date

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

SNAPSHOT_CODES = ('S2-03', 'S3-03', 'S6-03', 'E2-01', 'E2-03')
SNAPSHOT_SINCE = '2026-10-01 00:00:00'
RECALC_MODES = (
    'crecimiento_ventas', 'clientes_nuevos', 'concentracion_top3',
    'facturacion_usd', 'ventas_fuera_top10', 'notas_credito',
    'clientes_reactivados', 'retencion_clientes', 'concentracion_productos',
    'presupuesto_ventas', 'dso_cartera', 'otd_compras')


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    Indicator = env['sgi.indicator'].with_context(active_test=False)

    configurables = Indicator.search([('code', 'in', SNAPSHOT_CODES),
                                      ('calc_mode', '=', 'configurable')])
    configurables.write({'snapshot': True})
    _logger.info("SGI 57.90.0: indicadores de foto configurables: %s",
                 ', '.join(configurables.mapped('code')))

    cleared = Indicator.search([('snapshot', '=', True)])._sgi_snapshot_clear_history(SNAPSHOT_SINCE)
    for measure in cleared:
        _logger.info("SGI 57.90.0: %s %s -> sin dato (foto)",
                     measure.indicator_id.code, measure.period_date)
    _logger.info("SGI 57.90.0: %s mediciones de foto pasaron a sin dato", len(cleared))

    Measure = env['sgi.indicator.measure']
    recalculated = 0
    for indicator in Indicator.search([('calc_mode', 'in', RECALC_MODES)]):
        measures = Measure.search([
            ('indicator_id', '=', indicator.id),
            ('period_date', '>=', date(2026, 1, 1)),
            ('state', '!=', 'validado'),
        ])
        for measure in measures:
            try:
                with cr.savepoint():
                    before = measure.value
                    date_from, date_to = indicator._sgi_period_bounds(measure.period_date)
                    measure.write(indicator._sgi_measure_vals(date_from, date_to))
                    recalculated += 1
                    if round(before, 2) != round(measure.value, 2):
                        _logger.info("SGI 57.90.0: %s %s: %s -> %s", indicator.code,
                                     measure.period_date, before, measure.value)
            except Exception as error:  # noqa: BLE001 - una medición no detiene a las demás
                _logger.warning("SGI 57.90.0: %s %s no se recalculó: %s",
                                indicator.code, measure.period_date, error)
    _logger.info("SGI 57.90.0: %s mediciones de ventas y CO-01 recalculadas", recalculated)

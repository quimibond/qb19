# -*- coding: utf-8 -*-
"""Resincronizar `es_renta`: un patrón amplio no es renta por una cuenta.

`marcar_cuentas_de_renta` marcaba una clasificación entera si ALGUNA de sus
cuentas era renta de inmueble. Para una clase de patrón eso es catastrófico:
`504.01%` abarca a `504.01.0008 RENTA DEL LOCAL` junto a cuarenta gastos de
overhead, y `701.11%` a `701.11.0001 ARRENDAMIENTO FINANCIERO`.

Resultado en producción: 38 cuentas de buckets fabriles marcadas como renta,
de las cuales UNA sola lo era (`603.45.0001 RENTA DEL LOCAL (PLANTA)`, que
tiene clasificación propia). El motor las sacaba del pool fabril y en su
lugar metía la renta contractual de los centros, que no repone nada de eso:

  · overhead de fábrica bajo `504.01%`   $666,419/mes
    (mantenimientos, herramientas, uniformes, ISO, residuos, vigilancia…)
  · arrendamiento de maquinaria `701.11%` $867,721/mes
    — que es el costo de las máquinas con las que se produce

$1,534,140/mes, $12,273,123 entre enero y agosto de 2026, que ningún producto
cargaba. El pool fabril pasa de ~$4.16M/mes a ~$5.69M/mes.

La regla nueva marca una clase solo si TODA ella es renta de inmueble, y
nunca si su bucket es `arrend_maquinaria`. Cuando un patrón amplio contiene
una cuenta de renta, la salida correcta es darle a esa cuenta su propia
clasificación específica —gana por más específica— y el panel ahora lo pide
así en vez de pedir que se marque el patrón.

Se resincroniza la bandera y se recalculan los períodos abiertos. Los
cerrados se respetan.

En la misma versión: el conjunto de productos que recibe el recargo de
importación ya no incluye activo fijo ni servicios. Se quedan en la BASE del
factor —su pedimento existe y lo diluye correctamente— pero su aduana se
queda en resultados. La ventana sep-2025/ago-2026 traía una ROPE OPENER AND
SLITTING LINE de €95,000 y una decena de seguros, fletes y licencias dentro
del conjunto que lo recibía.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    Clase = env['qb.costeo.cuenta.class']
    antes = Clase.with_context(active_test=False).search_count(
        [('es_renta', '=', True)])
    Clase.marcar_cuentas_de_renta()
    despues = Clase.with_context(active_test=False).search_count(
        [('es_renta', '=', True)])
    _logger.info('qb_capacidad_costeo: clasificaciones con es_renta: %s → %s',
                 antes, despues)

    mezcladas = Clase.clases_con_renta_mezclada()
    if mezcladas:
        _logger.warning(
            'qb_capacidad_costeo: %s clasificaciones fabriles abarcan una '
            'cuenta de renta de inmueble sin separarla; la renta del GL se '
            'cuela al pool y se cuenta dos veces con la contractual. Dale a '
            'esa cuenta su propia clasificación: %s',
            len(mezcladas), ', '.join(mezcladas.mapped('name')))

    periodos = env['qb.costo.factores'].search([]).mapped('period')
    for period in sorted(set(periodos)):
        env['qb.costo.producto'].action_recompute_period(period)

    ultimo = env['qb.costo.factores'].search([], order='period DESC', limit=1)
    if ultimo:
        _logger.info(
            'qb_capacidad_costeo: %s períodos recalculados; pool fabril '
            '%.2f/mes, renta del GL sustituida %.2f/mes.',
            len(set(periodos)), ultimo.fab_pool_month,
            ultimo.renta_gl_sustituida)

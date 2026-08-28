# -*- coding: utf-8 -*-
"""La baja de un activo vendido sale del costeo.

En dic-2025 se cargaron $5,827,157 a `504.08.0001 DEPRECIACIÓN MAQUINARIA`
por dos máquinas —FONGS JET y CIRCULAR INTERLOCK— que salieron del activo.
Como esa cuenta está en el bucket `depreciacion`, entraba al pool fabril y el
suavizado a 12 meses la volvía costo recurrente: $485,596/mes, el 7.8% del
pool. Tres razones para que no sea costo del período, y apuntan al mismo lado:

  1. Ya está compensado. `704.23.0003 UTILIDAD EN VENTA DE ACTIVO FIJO` trae
     $5,896,997 el mismo mes —diferencia de $69,840, la utilidad real de la
     operación—, en una cuenta `income_other` que el costeo no mira ni debe
     mirar. El módulo veía media operación.
  2. Es un evento único: el saldo pendiente de depreciar reconocido de golpe
     al vender, no un gasto que se repita cada mes.
  3. Es doble conteo. Fue una venta con arrendamiento en reversa: esas
     máquinas hoy se pagan como renta y la renta YA está en el pool. El
     arrendamiento (`701.11%`) saltó de 10 a 16 contratos justo en dic-2025 y
     de $600,440 a $1,028,398 al mes. Repartir además su depreciación de
     cuando eran propias le cobra a cada producto la misma máquina dos veces.

Es la misma regla que ya se aplicó al régimen híbrido de TEJIDO: cuando un
costo cambia de vehículo, el vehículo viejo tiene que salir.

La exclusión por referencia de asiento —que ya existía para la póliza de
cierre anual— se generaliza a una lista configurable,
`refs_fuera_de_costeo`, con default `CIERRE ANUAL,ENAJENACI`. Si siguen
migrando maquinaria a arrendamiento habrá más bajas, y ahí se agregan sin
tocar código.

Se recalculan los períodos abiertos. Los cerrados se respetan.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    periodos = env['qb.costo.factores'].search([]).mapped('period')
    for period in sorted(set(periodos)):
        env['qb.costo.producto'].action_recompute_period(period)

    ultimo = env['qb.costo.factores'].search([], order='period DESC', limit=1)
    if ultimo:
        _logger.info(
            'qb_capacidad_costeo: %s períodos recalculados sin las bajas de '
            'activo ni la póliza de cierre. Pool fabril %.2f/mes, ajuste de '
            'MP %.4f.', len(set(periodos)), ultimo.fab_pool_month,
            ultimo.mp_ajuste)

    marcados = env['qb.costo.factores'].search(
        [('confiabilidad', '!=', 'ok')], order='period')
    _logger.info(
        'qb_capacidad_costeo: %s períodos marcados como no comparables: %s',
        len(marcados),
        ', '.join('%s (%s, %.1f%%)' % (m.period, m.confiabilidad,
                                       m.utilizacion_pond_pct)
                  for m in marcados) or 'ninguno')

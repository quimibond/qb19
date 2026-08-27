# -*- coding: utf-8 -*-
"""Solo la merma es costo; los ajustes de cantidad no.

`501.01.02 COSTO POR AJUSTES A CANTIDAD` mezcla naturalezas distintas:

  SP/10758              scrap de Odoo  -> merma real, SÍ es costo
  TL/EMB/04840          embarque       -> ajuste, NO
  TL/ENC//00103         encogimiento   -> ajuste, NO
  TVAR/ENT-REF/00471    refacciones    -> ajuste, NO

Entraba entera al bucket `mp`, que alimenta el ajuste de MP. Un asiento de
regularización de diciembre de 2025 —$5,822,686, «Merma no contabilizada
(1,136 scraps sin asiento)»— subió `mp_ajuste` de 0.781 a 0.903: +15.6% de
costo de materia prima en TODOS los productos, con los ajustes adentro.

`filtro_etiqueta` en la clasificación deja pasar solo las líneas cuyo
concepto contenga el texto. Es un filtro de LÍNEA: la clasificación sigue
siendo por cuenta y esto acota qué parte de ella entra. Vacío = toda la
cuenta, que es lo normal y el default.

La clasificación específica de `501.01.02%` con `SP/` viene en los seeds y
gana por patrón más largo sobre `501.01%`.

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
            'qb_capacidad_costeo: %s períodos recalculados sin los ajustes de '
            'cantidad en el costo. Ajuste de MP %.4f (MP del mayor %.2f/mes '
            'contra %.2f modelada).', len(set(periodos)), ultimo.mp_ajuste,
            ultimo.mp_gl_month, ultimo.mp_modelada_month)

    # El marcado de períodos no comparables (19.0.1.24.0) se puebla en este
    # recálculo, así que se reporta aquí y no allá.
    marcados = env['qb.costo.factores'].search(
        [('confiabilidad', '!=', 'ok')], order='period')
    _logger.info(
        'qb_capacidad_costeo: %s períodos marcados como no comparables: %s',
        len(marcados),
        ', '.join('%s (%s, %.1f%%)' % (m.period, m.confiabilidad,
                                       m.utilizacion_pond_pct)
                  for m in marcados) or 'ninguno')

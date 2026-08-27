# -*- coding: utf-8 -*-
"""Cargar los gastos e impuestos de importación al valor importado.

19.0.1.11.0 agrega el bucket `importacion`: los impuestos de aduana (IGI,
DTA, PRV) y los gastos de importación (agente aduanal, flete) se reparten
sobre el VALOR DE COMPRA de lo importado, que es lo que los causa — el IGI
se calcula sobre el valor en aduana. Antes caían en `no_costeo` y ningún
producto los pagaba: el importado se veía más barato de lo que es.

Esta migración mueve al bucket nuevo únicamente las cuentas de importación
que hoy están en `no_costeo`. Es seguro por construcción: esas cuentas
aportan cero a cualquier pool, así que moverlas no puede provocar doble
conteo. Las que estén clasificadas en otro bucket se dejan quietas y el
panel las reporta — moverlas cambiaría un reparto existente y esa decisión
es del usuario.

Los factores ya calculados quedan obsoletos (el costo de los importados
cambia), así que se recalculan los períodos que existan.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    Clase = env['qb.costeo.cuenta.class']
    movidas = Clase.reclasificar_cuentas_de_importacion()
    _logger.info('qb_capacidad_costeo: %s cuentas movidas al bucket de '
                 'importación', len(movidas))

    mal_ubicadas = Clase.cuentas_de_importacion_mal_ubicadas()
    if mal_ubicadas:
        _logger.warning(
            'qb_capacidad_costeo: estas cuentas de aduana siguen en un bucket '
            'que las reparte por el driver equivocado (revísalas en '
            'Configuración → Clasificación de cuentas): %s',
            ', '.join(mal_ubicadas.mapped('name')))

    periodos = env['qb.costo.factores'].search([]).mapped('period')
    for period in sorted(set(periodos)):
        env['qb.costo.producto'].action_recompute_period(period)
    _logger.info('qb_capacidad_costeo: %s períodos recalculados con la aduana '
                 'dentro del costo del importado', len(set(periodos)))

# -*- coding: utf-8 -*-
"""Sacar el precio del costo: la operación se reparte sobre producción.

19.0.1.14.0 cambia el driver del costo REPORTADO de operación. Antes era
`op = op_pct × precio`: el costo dependía del precio, así que el mismo
producto vendido a la mitad «costaba» la mitad de operación y su margen se
veía sano. Y un producto sin ventas en el mes tenía precio 0 y por lo tanto
operación 0 — justo los que hay que evaluar para decidir si vale la pena
empujarlos.

Ahora la operación se reparte sobre el costo de producción, que no se mueve
con el descuento del vendedor. El parámetro `op_driver` lo revierte a
"ventas" si se prefiere el reparto anterior.

El piso de precio del cotizador NO cambia: ahí `op_pct` sobre la venta sigue
siendo lo correcto, porque el piso a planta llena resuelve qué precio deja
cubierta una operación que es porcentaje de la venta. La circularidad ahí es
la fórmula, no un error.

Se recalculan los períodos existentes.
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
            'qb_capacidad_costeo: %s períodos recalculados. Operación: '
            '%.2f%% sobre ventas (cotizador) / x%.4f sobre costo de '
            'producción (reporte).', len(set(periodos)),
            (ultimo.op_pct or 0.0) * 100.0, ultimo.op_rate or 0.0)

# -*- coding: utf-8 -*-
"""Recalcular tras dos correcciones de precisión.

19.0.1.15.0 arregla dos cosas que cambian números ya calculados:

1. El dedup de cantidades colapsaba CUALQUIER repetición de (factura,
   producto, cantidad). Dos rollos iguales en una misma factura se contaban
   como uno: la cantidad se partía a la mitad y el precio promedio salía al
   doble. Ahora colapsa solo grupos de tres o más líneas, que es la firma del
   triplete de facturación (lista / descuento / neta).

   Medido sobre ene–ago 2026 el dedup viejo no llegó a descartar nada —la
   diferencia entre la cantidad del mayor y la del modelo era exactamente la
   de las notas de crédito, mes por mes— así que este arreglo quita un riesgo
   latente sin mover el histórico reciente.

2. `_explode_bom` recorría todas las líneas de la receta sin filtrar por
   variante. En una receta con atributos, el producto cargaba componentes que
   no consume: su MP salía inflada por todo lo de sus variantes hermanas.

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
    _logger.info('qb_capacidad_costeo: %s períodos recalculados con el dedup '
                 'por tamaño de grupo y el filtro de variante en la receta',
                 len(set(periodos)))

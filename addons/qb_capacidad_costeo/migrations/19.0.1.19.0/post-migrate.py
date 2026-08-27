# -*- coding: utf-8 -*-
"""Homologar las ventanas de los promedios.

Dos correcciones a cómo se promedian los pools.

**1. El denominador son los meses de la VENTANA, no los meses con factura.**
La renta y la energía se registran al pagarse, no al devengarse: la renta
oscila entre $506k y $1,490k al mes contra un contrato de ~$1,065k, y la
energía entre $53k y $173k según cuándo llegó el recibo. Dividir entre los
meses en que la cuenta tuvo movimiento daba el cargo por recibo, no el costo
mensual. Los meses negativos —el reverso del cierre anual, que en diciembre
2025 metió +$163M de débito a cuentas de ingreso— salen ahora de los DOS
lados de la división: dejarlos solo en el denominador subvaluaba el promedio
tanto como dejarlos en el numerador lo hundía.

**2. La ventana del pool fabril arranca en el corte de absorción.** Con un
centro migrando a absorción por workcenter, promediar doce meses mezcla
regímenes: los meses anteriores al corte llevan el gasto del centro completo y
los posteriores no. El factor de septiembre tiene que describir a septiembre.
Sale ruidoso el primer mes —el panel lo avisa— y se estabiliza solo.

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
            'qb_capacidad_costeo: %s períodos recalculados. Ventana fabril '
            'desde %s (%s meses); pool fabril %.2f/mes, absorbido por Odoo '
            '%.2f/mes.', len(set(periodos)), ultimo.fab_ventana_desde,
            ultimo.fab_ventana_meses, ultimo.fab_pool_month,
            ultimo.absorcion_pool_month)

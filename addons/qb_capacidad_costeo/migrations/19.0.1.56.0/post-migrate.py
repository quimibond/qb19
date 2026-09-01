# -*- coding: utf-8 -*-
"""Marca qué etapa produce cada centro y corrige el crudo huérfano.

Sin `etapa`, `_check_capacity` valida los tres centros contra el producto
terminado — y el terminado sale de ACABADO, así que a la tejedora se le
preguntaba por un código que esa máquina nunca hizo. Ese era el motivo de
que el catálogo de familias de máquinas (19 crudos) no cruzara con NINGUNO
de los 143 artículos vendidos en 2026: no faltaban datos, se cruzaba mal.

El renombre es el único renglón del catálogo que no resolvía a un producto:
`WJ030Q22HNN200` no existe en Odoo y el BOM del `WJ032Q22JNT160` consume
`WJ030Q22HNT200`. Una letra. Se corrige solo si el destino existe de verdad
y el original sigue huérfano — si alguien ya lo arregló, no se toca.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

ETAPA_POR_CENTRO = {
    'TEJIDO': 'crudo',
    'TINTORERIA': 'tenido',
    'ACABADO': 'terminado',
}

TYPO_CATALOGO = [('WJ030Q22HNN200', 'WJ030Q22HNT200')]


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})

    puestos = []
    for code, etapa in ETAPA_POR_CENTRO.items():
        centros = env['qb.costeo.centro'].with_context(
            active_test=False).search([('code', '=', code), ('etapa', '=', False)])
        if centros:
            centros.etapa = etapa
            puestos.append('%s=%s' % (code, etapa))
    _logger.info('qb_capacidad_costeo: etapa de centros — %s',
                 ', '.join(puestos) or 'ninguno (ya estaban puestos)')

    Prod = env['product.product']
    for viejo, nuevo in TYPO_CATALOGO:
        filas = env['qb.familia.producto'].search([('product_code', '=', viejo)])
        if not filas:
            continue
        if Prod.search_count([('default_code', '=', viejo)]):
            _logger.info('qb_capacidad_costeo: %s sí existe como producto; '
                         'no se renombra.', viejo)
            continue
        if not Prod.search_count([('default_code', '=', nuevo)]):
            _logger.warning('qb_capacidad_costeo: ni %s ni %s existen como '
                            'producto; el renglón queda huérfano.', viejo, nuevo)
            continue
        for fila in filas:
            ya = env['qb.familia.producto'].search(
                [('familia_id', '=', fila.familia_id.id),
                 ('product_code', '=', nuevo)], limit=1)
            if ya:
                fila.unlink()  # la restricción de unicidad ya la cubre
            else:
                fila.product_code = nuevo
        _logger.info('qb_capacidad_costeo: catálogo de familias %s → %s',
                     viejo, nuevo)

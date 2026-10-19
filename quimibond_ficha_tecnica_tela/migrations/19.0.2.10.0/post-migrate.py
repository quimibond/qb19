# -*- coding: utf-8 -*-
"""2.10.0: liga a su unidad del catálogo los textos de unidad que ya existen (tolerancias de las fichas de tejido y
acabado, parámetros y características). Solo escribe la columna nueva: el texto de origen no se toca. Los textos
que no corresponden a ninguna unidad se reportan en el log y la ficha los avisa."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

MODELOS = ('ficha.tecnica.tejido', 'ficha.tecnica.acabado', 'ficha.tecnica.maquina.parametro',
           'ficha.tecnica.caracteristica')


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    for model in MODELOS:
        sin_ligar = env[model]._ft_ligar_unidades()
        if sin_ligar:
            _logger.info('2.10.0 %s: unidades en texto sin unidad del catálogo (se conservan): %s', model,
                            ', '.join('«%s» ×%d' % (t, n) for t, n in sorted(sin_ligar.items())))
        else:
            _logger.info('2.10.0 %s: todas las unidades en texto quedaron ligadas al catálogo.', model)

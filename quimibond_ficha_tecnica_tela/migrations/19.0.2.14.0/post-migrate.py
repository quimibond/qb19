# -*- coding: utf-8 -*-
"""2.14.0: recalcula el rendimiento de las fichas de tejido con la marca nueva de tela tubular (el ancho de las
circulares que entregan el tubo aplanado se duplica). Odoo calcula `tela_tubular` al crear la columna, pero no
vuelve a calcular el rendimiento que depende de ella. Los rendimientos forzados a mano no se tocan; el peso y el
ancho capturados tampoco. Deja en el log cada ficha que cambia."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    fichas = env['ficha.tecnica.tejido'].with_context(active_test=False).search([('rendimiento_manual', '=', False)])
    antes = {ficha.id: ficha.rendimiento_tela_tejida for ficha in fichas}
    # Por la cola de recálculo y no asignando: asignar el rendimiento pasaría por write() y lo marcaría forzado.
    Ficha = env['ficha.tecnica.tejido']
    for fname in ('tela_tubular', 'rendimiento_tela_tejida'):
        env.add_to_compute(Ficha._fields[fname], fichas)
        fichas.flush_recordset([fname])
    for ficha in fichas:
        if abs((antes[ficha.id] or 0.0) - (ficha.rendimiento_tela_tejida or 0.0)) > 0.0001:
            _logger.info('2.14.0 ficha de tejido %s (%s): rendimiento %.4f → %.4f m/kg (tela tubular: %s)', ficha.id,
                         ficha.articulo, antes[ficha.id], ficha.rendimiento_tela_tejida, ficha.tela_tubular)
    _logger.info('2.14.0: %d fichas tubulares de %d sin rendimiento forzado.',
                 len(fichas.filtered('tela_tubular')), len(fichas))

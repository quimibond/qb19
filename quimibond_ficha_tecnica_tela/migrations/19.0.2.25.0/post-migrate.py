# -*- coding: utf-8 -*-
"""2.25.0: pone el subproducto «Desperdicio Tejido» (cantidad 0, operación Rama) en las listas en kg del flujo que ya
están generadas y siguen archivadas. Las activas no se tocan: las cambia Ingeniería."""
import logging

from odoo import SUPERUSER_ID, api

from odoo.addons.quimibond_ficha_tecnica_tela.models.ruta_familia import CODIGO_KG

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    boms = env['mrp.bom'].with_context(active_test=False).search([('code', '=', CODIGO_KG), ('active', '=', False)])
    avisos = boms._ft_poner_desperdicio()
    _logger.info('2.25.0: desperdicio de Rama en %d listas en kg archivadas del flujo.', len(boms))
    for aviso in avisos:
        _logger.warning('2.25.0: %s', aviso)

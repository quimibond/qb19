# -*- coding: utf-8 -*-
"""2.25.0: en las listas del flujo que ya están generadas y siguen archivadas (las activas no se tocan: las cambia
Ingeniería):

- el subproducto «Desperdicio Tejido» (cantidad 0, operación Rama) en las listas en kg;
- el tipo de operación (Fabricación Tela en kg / en m) donde falta («Generar listas» no lo ponía)."""
import logging

from odoo import SUPERUSER_ID, api

from odoo.addons.quimibond_ficha_tecnica_tela.models.ruta_familia import CODIGO_KG, CODIGO_M

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    cr.execute("DELETE FROM ficha_tecnica_ruta_paso WHERE paso = 'clasificacion'")
    _logger.info('2.25.0: %s pasos «Clasificación final» quitados de las rutas.', cr.rowcount)
    env = api.Environment(cr, SUPERUSER_ID, {})
    boms = env['mrp.bom'].with_context(active_test=False).search([('code', '=', CODIGO_KG), ('active', '=', False)])
    avisos = boms._ft_poner_desperdicio()
    _logger.info('2.25.0: desperdicio de Rama en %d listas en kg archivadas del flujo.', len(boms))
    Bom = env['mrp.bom'].with_context(active_test=False)
    for codigo, cual in ((CODIGO_KG, 'kg'), (CODIGO_M, 'm')):
        sin_tipo = Bom.search([('code', '=', codigo), ('active', '=', False), ('picking_type_id', '=', False)])
        avisos += sin_tipo._ft_poner_tipo_operacion(cual)
        _logger.info('2.25.0: tipo de operación en %d listas en %s archivadas del flujo.', len(sin_tipo), cual)
    for aviso in avisos:
        _logger.warning('2.25.0: %s', aviso)

# -*- coding: utf-8 -*-
"""2.25.0: en las listas del flujo que ya están generadas y siguen archivadas (las activas no se tocan: las cambia
Ingeniería):

- el subproducto «Desperdicio Tejido» (cantidad 0, operación Rama) en las listas en kg;
- el tipo de operación (Fabricación Tela en kg / en m) donde falta («Generar listas» no lo ponía);
- consumo «flexible»;
- la lista en metros sin operación (Inspección como estación única, diseño 2026-11-03): se quitan de las rutas por
  familia los pasos «Clasificación final» (Empaque) y las operaciones de las listas en metros archivadas del flujo, y
  se les ponen los saldos de segundas y desperdicio en kg.

Al final rehace la revisión de listas (con el rendimiento de lo fabricado), para que nadie vea la foto anterior al
despliegue: en el build de main la del 3-nov daba por bueno el tiempo de Rama viejo (piloto, 2026-11-04)."""
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
    metros = Bom.search([('code', '=', CODIGO_M), ('active', '=', False)])
    ops = metros.with_context(active_test=False).operation_ids
    _logger.info('2.25.0: %d operaciones quitadas de %d listas en metros archivadas.', len(ops), len(metros))
    ops.unlink()
    for bom in metros:
        avisos += env['ficha.tecnica.acabado']._ft_poner_saldos(bom)
    flujo = Bom.search([('code', 'in', [CODIGO_KG, CODIGO_M]), ('active', '=', False), ('consumption', '!=', 'flexible')])
    flujo._ft_poner_consumo_flexible()
    _logger.info('2.25.0: consumo flexible en %d listas archivadas del flujo.', len(flujo))
    for aviso in avisos:
        _logger.warning('2.25.0: %s', aviso)
    env['ficha.tecnica.revision']._cron_ft_actualizar()
    _logger.info('2.25.0: revisión de listas rehecha.')

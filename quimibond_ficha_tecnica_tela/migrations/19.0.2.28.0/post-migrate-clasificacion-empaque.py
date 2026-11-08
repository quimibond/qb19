# -*- coding: utf-8 -*-
"""2.28.0: la Clasificación final vuelve a la lista en metros, en el centro EMPAQUE (Dirección General, 2026-11-07).

- Las rutas de familia que no tienen el paso «Clasificación final» lo reciben (EMPAQUE, 60 min por ciclo).
- Las listas en metros **archivadas** del flujo reciben la operación CLASIFICACION. Las activas no se tocan: las
  cambia Ingeniería (al activar un borrador se completa sola; en una activa, con el botón «Clasificación en Empaque»).
- Cada tela en metros con ficha queda con su capacidad en EMPAQUE (400 m por ciclo, provisional).

Al final rehace la revisión de listas."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    resumen = env['ficha.tecnica.ruta']._ft_poner_clasificacion_rutas()
    for aviso in resumen['avisos']:
        _logger.warning('2.28.0: %s', aviso)
    _logger.info('2.28.0: paso «Clasificación final» en %d rutas; operación en %d listas en metros archivadas; '
                 'capacidad en EMPAQUE de %d telas.', resumen['rutas'], resumen['listas'], resumen['capacidades'])
    env['ficha.tecnica.revision']._cron_ft_actualizar()
    _logger.info('2.28.0: revisión de listas rehecha.')

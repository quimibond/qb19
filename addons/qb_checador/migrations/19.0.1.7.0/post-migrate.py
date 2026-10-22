# -*- coding: utf-8 -*-
"""1.7.0 arma una asistencia por turno en vez de alternar entrada/salida. Las asistencias que armó 1.6.x
quedaron corridas un lugar donde faltó o sobró una checada (el 20-oct-2026 el reloj empezó a contar a media
tarde y la salida de las 17:35 se emparejó con la entrada de las 07:03 del día siguiente). Se rehacen desde
las checadas; las capturadas a mano no se tocan."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    n = env['qb.checada']._reconstruir_asistencias()
    _logger.info("qb_checador 1.7.0: %s asistencias del checador rehechas por turno", n)

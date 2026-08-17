# -*- coding: utf-8 -*-
import logging

from . import models
from . import wizards

_logger = logging.getLogger(__name__)


def post_init_hook(env):
    """Al instalar: (1) resolver el matching cuenta↔clase de los seeds contra
    el plan contable real, para que las vistas SQL tengan datos desde el día 1;
    (2) cargar el maestro de pesos medidos/ingeniería nativo (sin Supabase).
    Ambos idempotentes.

    Cada paso va en su propio try/except: un hipo de datos (una cuenta rara,
    el CSV de pesos, un producto faltante) NUNCA debe tumbar la instalación
    del módulo — se loggea y se puede correr a mano después (botones en el
    Panel / Configuración)."""
    try:
        env['qb.costeo.cuenta.class'].cron_refresh_account_matching()
    except Exception:
        _logger.exception(
            'post_init: falló el matching de cuentas; córrelo con el cron o '
            'el botón "Refrescar matching" en Configuración.')
    try:
        env['qb.producto.peso'].load_weight_master()
    except Exception:
        _logger.exception(
            'post_init: falló la carga del maestro de pesos; córrela con el '
            'botón "Cargar maestro de pesos" en el Panel.')

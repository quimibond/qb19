# -*- coding: utf-8 -*-
from . import models
from . import wizards


def post_init_hook(env):
    """Al instalar: (1) resolver el matching cuenta↔clase de los seeds contra
    el plan contable real, para que las vistas SQL tengan datos desde el día 1;
    (2) cargar el maestro de pesos medidos/ingeniería nativo (sin Supabase).
    Ambos idempotentes."""
    env['qb.costeo.cuenta.class'].cron_refresh_account_matching()
    env['qb.producto.peso'].load_weight_master()

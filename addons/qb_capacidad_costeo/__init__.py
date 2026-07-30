# -*- coding: utf-8 -*-
from . import models
from . import wizards


def post_init_hook(env):
    """Al instalar: resolver el matching cuenta↔clase de los seeds contra el
    plan contable real, para que las vistas SQL tengan datos desde el día 1
    (idempotente — es lo mismo que corre el cron nocturno)."""
    env['qb.costeo.cuenta.class'].cron_refresh_account_matching()

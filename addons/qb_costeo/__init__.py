# -*- coding: utf-8 -*-
from . import models


def post_init_hook(env):
    """Si la base trae la clasificación de cuentas del módulo anterior
    (`qb_capacidad_costeo`), la importa para no clasificar 400 cuentas a mano
    otra vez. Sin ese módulo no hace nada."""
    env['qb.cuenta.clase'].importar_clasificacion_legada()
